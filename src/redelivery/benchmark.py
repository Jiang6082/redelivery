"""Local synthetic benchmark; includes fsync, connection and process-start overhead."""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import platform
import sqlite3
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from .store import Inbox


def consume(path: str, owner: str) -> list[float]:
    inbox = Inbox(path)
    latency: list[float] = []
    while True:
        started = time.perf_counter()
        lease = inbox.claim(owner)
        if lease is None:
            return latency
        inbox.complete(lease, {"n": lease.payload["n"]})
        latency.append((time.perf_counter() - started) * 1000)


def percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def benchmark(events: int, workers: int) -> dict[str, object]:
    if not 1 <= events <= 100_000 or not 1 <= workers <= 16:
        raise ValueError("events must be 1..100000 and workers 1..16")
    with TemporaryDirectory(prefix="redelivery-bench-") as directory:
        path = str(Path(directory) / "benchmark.db")
        inbox = Inbox(path)
        enqueue_latency = []
        start = time.perf_counter()
        for n in range(events):
            before = time.perf_counter()
            inbox.enqueue("synthetic", str(n), {"n": n})
            enqueue_latency.append((time.perf_counter() - before) * 1000)
        duplicate_deliveries = (events + 4) // 5
        for n in range(0, events, 5):
            assert inbox.enqueue("synthetic", str(n), {"n": n}).duplicate
        ingestion_seconds = time.perf_counter() - start
        started = time.perf_counter()
        with ProcessPoolExecutor(max_workers=workers, mp_context=mp.get_context("spawn")) as pool:
            futures = [pool.submit(consume, path, f"worker-{n}") for n in range(workers)]
            per_worker = [future.result(timeout=300) for future in futures]
        elapsed = time.perf_counter() - started
        latencies = [latency for batch in per_worker for latency in batch]
        counts = inbox.stats()
        if len(latencies) != events or counts["results"] != events or counts["succeeded"] != events:
            raise AssertionError(f"Incomplete workload: {counts}")
        # sqlite3.Connection's own context manager commits/rolls back but does NOT close.
        with closing(sqlite3.connect(path)) as db:
            integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
            journal_mode = db.execute("PRAGMA journal_mode").fetchone()[0]
        return {
            "workload": "preloaded synthetic integer payloads; one local result per unique input",
            "python": sys.version.split()[0],
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "journal_mode": journal_mode,
            "synchronous": "FULL",
            "busy_timeout_seconds": 5,
            "unique_inputs": events,
            "duplicate_deliveries": duplicate_deliveries,
            "total_deliveries": events + duplicate_deliveries,
            "workers": workers,
            "per_worker_completed": [len(batch) for batch in per_worker],
            "ingestion_seconds_including_duplicates": round(ingestion_seconds, 6),
            "unique_enqueue_p95_ms": round(percentile(enqueue_latency, 0.95), 3),
            "drain_seconds_including_process_start": round(elapsed, 6),
            "drain_results_per_second": round(events / elapsed, 2),
            "claim_complete_p50_ms": round(statistics.median(latencies), 3),
            "claim_complete_p95_ms": round(percentile(latencies, 0.95), 3),
            "claim_complete_p99_ms": round(percentile(latencies, 0.99), 3),
            "counts": counts,
            "integrity_check": integrity,
            "database_bytes": Path(path).stat().st_size,
            "limitations": (
                "Local run; warm OS cache; no network or forced crashes; "
                "latency excludes queue wait; no speedup claim."
            ),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    print(json.dumps(benchmark(args.events, args.workers), indent=2))


if __name__ == "__main__":
    main()
