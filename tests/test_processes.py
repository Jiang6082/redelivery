"""Exercise independent OS processes, not mocked database connections."""

import multiprocessing as mp
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from redelivery import Inbox, LostLease


def producer(path, barrier, results):
    inbox = Inbox(path)
    barrier.wait(timeout=30)
    receipt = inbox.enqueue("same-source", "same-key", {"x": 1})
    results.put((receipt.job_id, receipt.duplicate))


def consumer(path, barrier, results, owner):
    inbox = Inbox(path)
    barrier.wait(timeout=30)
    completed = []
    while lease := inbox.claim(owner):
        inbox.complete(lease, {"input": lease.payload["n"]})
        completed.append(lease.job_id)
    results.put(completed)


def crash_worker(path, stage):
    class CrashingInbox(Inbox):
        @staticmethod
        def _record(db, job_id, now, event, generation, detail=""):
            if stage == "during_commit" and event == "succeeded":
                # The actual result INSERT and job UPDATE have executed, without COMMIT.
                os._exit(23)
            Inbox._record(db, job_id, now, event, generation, detail)

    inbox = CrashingInbox(path, clock=lambda: 1000.0)
    lease = inbox.claim("crasher", lease_seconds=10)
    if stage == "after_claim":
        os._exit(23)
    inbox.complete(lease, {"worker": "crasher"})
    os._exit(23)


def join(processes):
    try:
        for process in processes:
            process.join(timeout=30)
            assert not process.is_alive(), "Child process timed out"
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)


def test_four_producers_race_on_one_identity(inbox):
    ctx = mp.get_context("spawn")
    barrier, results = ctx.Barrier(4), ctx.Queue()
    processes = [
        ctx.Process(target=producer, args=(inbox.path, barrier, results)) for _ in range(4)
    ]
    for process in processes:
        process.start()
    join(processes)
    receipts = [results.get(timeout=5) for _ in processes]
    results.close()
    assert {receipt[0] for receipt in receipts} == {1}
    assert sum(receipt[1] for receipt in receipts) == 3
    assert inbox.stats()["pending"] == 1


def test_four_processes_commit_each_job_once(inbox):
    # Real-time consumer clocks can claim these older, injected-clock enqueues.
    for n in range(40):
        inbox.enqueue("load", str(n), {"n": n})
    ctx = mp.get_context("spawn")
    barrier, results = ctx.Barrier(4), ctx.Queue()
    processes = [
        ctx.Process(target=consumer, args=(inbox.path, barrier, results, f"worker-{n}"))
        for n in range(4)
    ]
    for process in processes:
        process.start()
    join(processes)
    ids = [job_id for _ in processes for job_id in results.get(timeout=5)]
    results.close()
    assert len(ids) == len(set(ids)) == 40
    assert inbox.stats()["results"] == inbox.stats()["succeeded"] == 40
    for job_id in ids:
        job = inbox.get(job_id)
        assert job["result"]["input"] == job["payload"]["n"]


@pytest.mark.parametrize("stage", ["after_claim", "during_commit", "after_commit"])
def test_forced_process_exit_and_recovery(inbox, clock, stage):
    inbox.enqueue("a", "1", {})
    process = mp.get_context("spawn").Process(target=crash_worker, args=(inbox.path, stage))
    process.start()
    try:
        process.join(timeout=30)
        assert not process.is_alive()
        assert process.exitcode == 23
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
    if stage == "after_commit":
        assert inbox.stats()["results"] == 1
        assert inbox.claim("replacement") is None
    else:
        assert inbox.stats()["results"] == 0
        assert inbox.get(1)["status"] == "leased"
        clock.now = 1010.0
        replacement = inbox.claim("replacement")
        assert replacement.generation == 2
        inbox.complete(replacement, {"worker": "replacement"})
    assert inbox.stats()["results"] == inbox.stats()["succeeded"] == 1
    assert len([t for t in inbox.history(1) if t["event"] == "succeeded"]) == 1
    with sqlite3.connect(inbox.path) as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_time_is_sampled_after_acquiring_write_lock(inbox, clock, monkeypatch):
    inbox.enqueue("a", "1", {})
    lease = inbox.claim("one", lease_seconds=10)
    connection_ready = Event()
    original = inbox._connect

    def observed_connect():
        db = original()
        connection_ready.set()
        return db

    monkeypatch.setattr(inbox, "_connect", observed_connect)
    blocker = sqlite3.connect(inbox.path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(inbox.complete, lease, {"late": True})
            try:
                assert connection_ready.wait(timeout=5)
                clock.now = 1010.0
            finally:
                blocker.rollback()
            with pytest.raises(LostLease):
                future.result(timeout=10)
    finally:
        blocker.close()
    assert inbox.stats()["results"] == 0
