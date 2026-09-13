"""JSON output on stdout; payload-free structured operational logs on stderr."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import Counter
from pathlib import Path

from .store import MAX_BYTES, STATES, Inbox, Payload, log, parse_object
from .worker import run_one


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, object] = {
            "at": record.created,
            "level": record.levelname,
            "event": record.getMessage(),
        }
        for key in ("job_id", "generation", "code", "line", "error_type"):
            if hasattr(record, key):
                data[key] = getattr(record, key)
        return json.dumps(data, separators=(",", ":"))


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    log.handlers[:] = [handler]
    log.setLevel(logging.INFO)
    log.propagate = False


def emit(data: object) -> None:
    print(json.dumps(data, indent=2, sort_keys=True, allow_nan=False))


def ingest(inbox: Inbox, path: Path) -> dict[str, int]:
    counts = {"accepted": 0, "duplicates": 0}
    with path.open(encoding="utf-8") as stream:
        line_number = 0
        while line := stream.readline(MAX_BYTES + 1):
            line_number += 1
            if not line.strip():
                continue
            try:
                item = parse_object(line)
                if set(item) - {"source", "key", "payload", "max_attempts"}:
                    raise ValueError("Unknown envelope field")
                source, key, payload = item.get("source"), item.get("key"), item.get("payload")
                attempts = item.get("max_attempts", 3)
                if not isinstance(source, str) or not isinstance(key, str):
                    raise ValueError("Envelope requires source and key strings")
                if not isinstance(payload, dict) or type(attempts) is not int:
                    raise ValueError("Envelope requires a payload object and integer max_attempts")
                receipt = inbox.enqueue(source, key, payload, max_attempts=attempts)
            except ValueError:
                log.error("invalid_input", extra={"line": line_number})
                raise
            counts["duplicates" if receipt.duplicate else "accepted"] += 1
    return counts


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Durable event inbox with fenced worker leases")
    root.add_argument("--db", type=Path, default=Path("runtime/inbox.db"))
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Initialize the inbox")
    load = commands.add_parser("ingest", help="Commit JSONL inputs, one transaction per line")
    load.add_argument("file", type=Path)
    worker = commands.add_parser("work", help="Drain currently eligible synthetic postings")
    worker.add_argument("--worker", default="worker-1")
    worker.add_argument("--lease-seconds", type=float, default=30)
    worker.add_argument("--max-jobs", type=int, default=1000)
    commands.add_parser("stats")
    show = commands.add_parser(
        "show", help="Inspect one job, its result, and up to 100 transitions"
    )
    show.add_argument("job_id", type=int)
    listing = commands.add_parser("list", help="List bounded job metadata")
    listing.add_argument("--status", choices=STATES)
    listing.add_argument("--after", type=int, default=0)
    listing.add_argument("--limit", type=int, default=50)
    history = commands.add_parser("history", help="Page through durable transitions")
    history.add_argument("job_id", type=int)
    history.add_argument("--after", type=int, default=0)
    history.add_argument("--limit", type=int, default=100)
    replay = commands.add_parser("replay", help="Explicitly reset a dead letter's attempt budget")
    replay.add_argument("job_id", type=int)
    commands.add_parser("demo", help="Run an isolated deterministic recovery timeline")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    configure_logging()
    try:
        if args.command == "demo":
            from .demo import run_demo

            emit(run_demo())
            return 0
        inbox = Inbox(args.db)
        if args.command == "init":
            emit({"initialized": True})
        elif args.command == "ingest":
            emit(ingest(inbox, args.file))
        elif args.command == "work":
            if not 1 <= args.max_jobs <= 1_000_000:
                raise ValueError("max-jobs must be 1 to 1000000")
            outcomes: Counter[str] = Counter()
            for _ in range(args.max_jobs):
                outcome = run_one(inbox, args.worker, lease_seconds=args.lease_seconds)
                if outcome == "idle":
                    break
                outcomes[outcome] += 1
            emit({"outcomes": dict(outcomes), "stats": inbox.stats()})
            return 1 if any(outcomes[k] for k in ("pending", "dead", "lost")) else 0
        elif args.command == "stats":
            emit(inbox.stats())
        elif args.command == "show":
            job: Payload | None = inbox.get(args.job_id)
            if job is None:
                raise ValueError("Unknown job")
            emit({"job": job, "history": inbox.history(args.job_id), "history_limit": 100})
        elif args.command == "list":
            emit(inbox.list_jobs(status=args.status, after=args.after, limit=args.limit))
        elif args.command == "history":
            emit(inbox.history(args.job_id, after=args.after, limit=args.limit))
        elif args.command == "replay":
            inbox.replay(args.job_id)
            emit({"replayed": args.job_id})
    except (ValueError, OSError, sqlite3.Error) as exc:
        log.error("operation_failed", extra={"error_type": type(exc).__name__})
        print(
            "Operation failed. Check the input and database access. "
            "Earlier JSONL lines may already be committed; retrying them is safe.",
            file=sys.stderr,
        )
        return 2
    return 0
