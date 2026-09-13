"""One disposable public-board pilot. Never connects to an existing production inbox."""

import argparse
import hashlib
import json
import platform
import sqlite3
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from redelivery.greenhouse import fetch_board, ingest_board
from redelivery.store import Inbox
from redelivery.trace import export_trace
from redelivery.worker import run_one


def pilot(board: str, trace: Path) -> dict[str, object]:
    started = datetime.now(UTC).isoformat()
    data = fetch_board(board)
    with tempfile.TemporaryDirectory() as directory:
        inbox = Inbox(Path(directory) / "pilot.db")
        begin = time.perf_counter()
        first = ingest_board(inbox, board, data)
        again = ingest_board(inbox, board, data)
        ingestion = time.perf_counter() - begin
        begin = time.perf_counter()
        completed = 0
        while (outcome := run_one(inbox, "pilot-worker")) != "idle":
            if outcome != "succeeded":
                raise RuntimeError("Pilot handler did not succeed")
            completed += 1
        processing = time.perf_counter() - begin
        stats = inbox.stats()
        assert first["accepted"] == again["duplicates"] == completed == stats["results"]
        exported = export_trace(inbox, trace)
    return {
        "observed_at": started,
        "source": f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs",
        "feed_bytes": len(data),
        "feed_sha256": hashlib.sha256(data).hexdigest(),
        "first_delivery": first,
        "identical_redelivery": again,
        "stats": stats,
        "trace": exported,
        "ingestion_seconds": round(ingestion, 4),
        "processing_seconds": round(processing, 4),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "scope": "One HTTP response delivered twice; single local worker; no injected crash. "
        "This is a disposable integration pilot, not sustained usage or a load benchmark.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("board")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--trace", type=Path, required=True)
    args = parser.parse_args()
    report = pilot(args.board, args.trace)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
