"""An isolated logical-clock explanation; real process crashes are tested separately."""

from pathlib import Path
from tempfile import TemporaryDirectory

from .store import Inbox, LostLease, Payload
from .worker import normalize_posting


def run_demo() -> Payload:
    with TemporaryDirectory(prefix="redelivery-demo-") as temporary:
        now = [1000.0]
        inbox = Inbox(Path(temporary) / "demo.db", clock=lambda: now[0])
        data: Payload = {
            "company": "Example Labs",
            "title": "  SWE   Intern ",
            "location": "Chicago",
        }
        first = inbox.enqueue("synthetic", "posting-42-v1", data)
        duplicate = inbox.enqueue("synthetic", "posting-42-v1", data)
        abandoned = inbox.claim("worker-A", lease_seconds=10)
        assert abandoned is not None
        # Simulate abandonment with a logical clock. No physical process is killed here.
        now[0] = 1010.0
        recovered = inbox.claim("worker-B", lease_seconds=10)
        assert recovered is not None
        rejected = False
        try:
            inbox.complete(abandoned, {"incorrect": True})
        except LostLease:
            rejected = True
        inbox.complete(recovered, normalize_posting(recovered.payload))
        return {
            "scenario": "logical-clock duplicate / abandoned lease / stale worker",
            "duplicate_same_job": duplicate.duplicate and duplicate.job_id == first.job_id,
            "old_generation": abandoned.generation,
            "new_generation": recovered.generation,
            "stale_worker_rejected": rejected,
            "stats": dict(inbox.stats()),
            "job": inbox.get(first.job_id),
            "history": list(inbox.history(first.job_id)),
            "physical_crash_test": "python -m pytest tests/test_processes.py -v",
        }
