"""One bounded worker step and a pure, synthetic job-posting normalizer."""

from collections.abc import Callable
from typing import Literal

from .store import Inbox, LostLease, Payload, log

Outcome = Literal["idle", "succeeded", "pending", "dead", "lost"]


def normalize_posting(payload: Payload) -> Payload:
    result: Payload = {}
    for name in ("company", "title", "location"):
        value = payload.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Posting fields must be nonempty strings")
        result[name] = " ".join(value.split())
    result["schema_version"] = 1
    return result


def run_one(
    inbox: Inbox,
    owner: str,
    handler: Callable[[Payload], Payload] = normalize_posting,
    *,
    lease_seconds: float = 30,
) -> Outcome:
    lease = inbox.claim(owner, lease_seconds=lease_seconds)
    if lease is None:
        return "idle"
    try:
        result = handler(lease.payload)
    except Exception:
        try:
            status = inbox.fail(lease)
            return "dead" if status == "dead" else "pending"
        except LostLease:
            log.warning("lost_lease", extra={"job_id": lease.job_id})
            return "lost"
    try:
        inbox.complete(lease, result)
    except LostLease:
        log.warning("lost_lease", extra={"job_id": lease.job_id})
        return "lost"
    except (ValueError, TypeError):
        # Invalid handler output is a handler failure; storage errors propagate.
        try:
            status = inbox.fail(lease, code="invalid_result")
            return "dead" if status == "dead" else "pending"
        except LostLease:
            return "lost"
    return "succeeded"
