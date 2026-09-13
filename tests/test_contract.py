import json
import sqlite3
from dataclasses import replace

import pytest

from redelivery import Conflict, Inbox, LostLease
from redelivery.store import canonical, parse_object
from redelivery.worker import run_one


def test_duplicate_canonical_object_order_and_restart(inbox):
    first = inbox.enqueue("board", "1", {"a": 1, "b": 2})
    restarted = Inbox(inbox.path)
    same = restarted.enqueue("board", "1", {"b": 2, "a": 1})
    assert first.job_id == same.job_id and same.duplicate
    assert restarted.stats()["pending"] == 1
    with pytest.raises(Conflict):
        restarted.enqueue("board", "1", {"a": 999})
    with pytest.raises(Conflict):
        restarted.enqueue("board", "1", {"a": 1, "b": 2}, max_attempts=9)


def test_identity_is_scoped_by_source(inbox):
    assert inbox.enqueue("a", "1", {}).job_id != inbox.enqueue("b", "1", {}).job_id


def test_complete_persists_one_result_and_audit(inbox):
    receipt = inbox.enqueue("board", "1", {})
    lease = inbox.claim("one")
    inbox.complete(lease, {"answer": 42})
    with pytest.raises(LostLease):
        inbox.complete(lease, {"answer": 99})
    assert Inbox(inbox.path).get(receipt.job_id)["result"] == {"answer": 42}
    assert [t["event"] for t in inbox.history(receipt.job_id)] == [
        "enqueued",
        "claimed",
        "succeeded",
    ]
    assert inbox.stats() == {"pending": 0, "leased": 0, "succeeded": 1, "dead": 0, "results": 1}


@pytest.mark.parametrize("operation", ["complete", "fail", "renew"])
def test_expiry_is_exclusive_even_before_reclaim(inbox, clock, operation):
    inbox.enqueue("board", "1", {})
    lease = inbox.claim("one", lease_seconds=10)
    clock.now += 10
    with pytest.raises(LostLease):
        getattr(inbox, operation)(lease, {}) if operation == "complete" else getattr(
            inbox, operation
        )(lease)
    assert inbox.stats()["results"] == 0


@pytest.mark.parametrize("operation", ["complete", "fail", "renew"])
def test_reclaimed_generation_fences_old_owner(inbox, clock, operation):
    inbox.enqueue("board", "1", {})
    old = inbox.claim("same-name", lease_seconds=10)
    clock.now += 10
    current = inbox.claim("same-name")
    assert current.generation == old.generation + 1
    with pytest.raises(LostLease):
        getattr(inbox, operation)(old, {}) if operation == "complete" else getattr(
            inbox, operation
        )(old)
    inbox.complete(current, {"valid": True})


def test_wrong_worker_cannot_complete(inbox):
    inbox.enqueue("a", "1", {})
    lease = inbox.claim("owner")
    with pytest.raises(LostLease):
        inbox.complete(replace(lease, owner="different"), {})


def test_renewal_extends_current_lease_and_does_not_shorten_it(inbox, clock):
    inbox.enqueue("a", "1", {})
    lease = inbox.claim("one", lease_seconds=10)
    assert inbox.renew(lease, lease_seconds=1) == 1010
    clock.now += 9
    assert inbox.renew(lease, lease_seconds=10) == 1019
    clock.now += 2
    assert inbox.claim("two") is None
    inbox.complete(lease, {})


def test_backoff_budget_and_explicit_replay_preserve_fencing(inbox, clock):
    receipt = inbox.enqueue("a", "1", {}, max_attempts=2)
    first = inbox.claim("one")
    assert inbox.fail(first) == "pending"
    assert inbox.claim("one") is None
    clock.now += 1
    second = inbox.claim("one")
    assert inbox.fail(second) == "dead"
    inbox.replay(receipt.job_id)
    third = inbox.claim("one")
    assert third.attempt == 1 and third.generation == 3
    with pytest.raises(LostLease):
        inbox.complete(first, {})
    inbox.complete(third, {})
    assert "replayed" in [row["event"] for row in inbox.history(receipt.job_id)]
    with pytest.raises(ValueError):
        inbox.replay(receipt.job_id)


def test_abandoned_last_attempt_becomes_dead(inbox, clock):
    inbox.enqueue("a", "1", {}, max_attempts=1)
    inbox.claim("one", lease_seconds=1)
    clock.now += 1
    assert inbox.claim("two") is None
    assert inbox.stats()["dead"] == 1


def test_retry_delay_is_exponential_and_capped(tmp_path, clock):
    inbox = Inbox(tmp_path / "retry.db", clock=clock, retry_base=2, retry_cap=3)
    inbox.enqueue("a", "1", {}, max_attempts=4)
    for delay in (2, 3, 3):
        lease = inbox.claim("one")
        inbox.fail(lease)
        clock.now += delay - 0.1
        assert inbox.claim("two") is None
        clock.now += 0.1
    assert inbox.claim("one").attempt == 4


def test_invalid_result_leaves_no_partial_publication(inbox):
    inbox.enqueue("a", "1", {})
    lease = inbox.claim("one")
    with pytest.raises(ValueError):
        inbox.complete(lease, {"bad": float("nan")})
    assert inbox.stats()["leased"] == 1 and inbox.stats()["results"] == 0
    inbox.complete(lease, {"good": True})


def test_audit_failure_rolls_back_result_and_status(inbox, monkeypatch):
    inbox.enqueue("a", "1", {})
    lease = inbox.claim("one")

    def fail_audit(*args):
        raise sqlite3.OperationalError("injected disk write failure")

    monkeypatch.setattr(inbox, "_record", fail_audit)
    with pytest.raises(sqlite3.OperationalError):
        inbox.complete(lease, {"answer": 1})
    assert inbox.stats()["leased"] == 1
    assert inbox.stats()["results"] == 0


@pytest.mark.parametrize(
    "value", [[], {"x": float("nan")}, {"x": float("inf")}, {1: "a"}, {"x": (1, 2)}]
)
def test_strict_json(value):
    with pytest.raises(ValueError):
        canonical(value)


@pytest.mark.parametrize("text", ['{"x":1,"x":2}', '{"x":NaN}', "[]", '{"x":Infinity}'])
def test_strict_json_parser(text):
    with pytest.raises(ValueError):
        parse_object(text)


def test_size_and_depth_limits():
    with pytest.raises(ValueError):
        canonical({"x": "a" * 65_536})
    value = {}
    for _ in range(34):
        value = {"x": value}
    with pytest.raises(ValueError):
        parse_object(json.dumps(value))


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True, 86401])
def test_invalid_lease_duration(inbox, value):
    with pytest.raises(ValueError):
        inbox.claim("one", lease_seconds=value)


@pytest.mark.parametrize("value", [0, 101, True, 1.5])
def test_invalid_attempt_budget(inbox, value):
    with pytest.raises(ValueError):
        inbox.enqueue("a", "1", {}, max_attempts=value)


def test_bounded_inspection_and_history(inbox):
    for key in range(3):
        inbox.enqueue("a", str(key), {})
    page = inbox.list_jobs(limit=2)
    assert len(page) == 2 and len(inbox.list_jobs(after=page[-1]["id"])) == 1
    assert inbox.history(1, limit=1)[0]["event"] == "enqueued"
    assert inbox.history(1, after=1) == []
    assert inbox.get(999) is None
    with pytest.raises(ValueError):
        inbox.list_jobs(limit=1001)
    with pytest.raises(ValueError):
        inbox.list_jobs(status="invalid")


def test_schema_guard_and_integrity(tmp_path, inbox):
    path = tmp_path / "future.db"
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=99")
    with pytest.raises(ValueError, match="schema"):
        Inbox(path)
    with sqlite3.connect(inbox.path) as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_worker_failure_does_not_log_payload_or_exception(inbox, caplog):
    inbox.enqueue("a", "1", {"secret": "PRIVATE-PAYLOAD"}, max_attempts=1)

    def bad_handler(payload):
        raise RuntimeError("PRIVATE-EXCEPTION")

    with caplog.at_level("INFO", logger="redelivery"):
        assert run_one(inbox, "one", bad_handler) == "dead"
    assert "PRIVATE" not in caplog.text


def test_worker_success_idle_and_invalid_result(inbox):
    inbox.enqueue("a", "1", {"company": " Demo ", "title": " SWE  Intern ", "location": "NY"})
    assert run_one(inbox, "one") == "succeeded"
    assert inbox.get(1)["result"]["title"] == "SWE Intern"
    assert run_one(inbox, "one") == "idle"
    inbox.enqueue("a", "2", {}, max_attempts=1)
    assert run_one(inbox, "one", lambda _: {"invalid": float("nan")}) == "dead"


@pytest.mark.parametrize("handler_end", ["result", "exception", "invalid_result"])
def test_worker_discards_expired_computation(inbox, clock, handler_end):
    inbox.enqueue("a", "1", {})

    def slow_handler(payload):
        clock.now += 31
        if handler_end == "exception":
            raise RuntimeError("computation failed after lease expired")
        return {"answer": float("nan") if handler_end == "invalid_result" else 42}

    assert run_one(inbox, "slow", slow_handler) == "lost"
    assert inbox.stats()["results"] == 0
    replacement = inbox.claim("replacement")
    assert replacement.generation == 2
    inbox.complete(replacement, {"answer": "current"})
    assert inbox.stats()["results"] == 1
