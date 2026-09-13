"""Generate a synthetic recovery recording by running the actual inbox implementation."""

import argparse
import tempfile
from pathlib import Path

from redelivery.store import Inbox, LostLease
from redelivery.trace import export_trace


def generate(destination: Path) -> dict[str, int]:
    now = [1_800_000_000.0]
    with tempfile.TemporaryDirectory() as directory:
        inbox = Inbox(Path(directory) / "demo.db", clock=lambda: now[0])
        for i in range(12):
            inbox.enqueue("synthetic-recovery", f"posting-{i}", {"example": i}, max_attempts=2)
            now[0] += 0.06
        abandoned = inbox.claim("worker-1", lease_seconds=2)
        assert abandoned
        for i in range(11):
            now[0] += 0.09
            lease = inbox.claim(f"worker-{i % 2 + 2}")
            assert lease
            now[0] += 0.2
            if i == 0:
                inbox.fail(lease, code="upstream_timeout")
            elif i == 1:
                inbox.fail(lease, code="invalid_posting")
            else:
                inbox.complete(lease, {"processed": True})
        now[0] += 3
        while lease := inbox.claim("worker-3"):
            now[0] += 0.3
            if lease.job_id == 3:
                inbox.fail(lease, code="invalid_posting")
            else:
                inbox.complete(lease, {"processed": True})
        try:
            inbox.complete(abandoned, {"stale": True})
        except LostLease:
            pass  # Rejected writes are absent from the committed audit export.
        return export_trace(inbox, destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    print(generate(parser.parse_args().output))
