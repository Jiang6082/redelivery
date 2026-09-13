import json
import subprocess
import sys

from redelivery import Inbox
from redelivery.demo import run_demo


def cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "redelivery", *map(str, args)],
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_installed_cli_roundtrip(tmp_path):
    db = tmp_path / "inbox.db"
    result = cli("--db", db, "ingest", "examples/jobs.jsonl")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"accepted": 2, "duplicates": 1}
    for line in result.stderr.splitlines():
        record = json.loads(line)
        assert record["event"] == "enqueued"
        assert "Example Labs" not in line
    result = cli("--db", db, "work")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["stats"]["results"] == 2
    assert json.loads(cli("--db", db, "ingest", "examples/jobs.jsonl").stdout)["duplicates"] == 3
    result = cli("--db", db, "show", 1)
    assert json.loads(result.stdout)["job"]["result"]["title"] == "Software Engineering Intern"
    assert len(json.loads(cli("--db", db, "list", "--limit", 1).stdout)) == 1
    assert len(json.loads(cli("--db", db, "history", 1, "--limit", 1).stdout)) == 1
    assert cli("--db", db, "show", 999).returncode == 2


def test_partial_ingestion_is_durable_and_safe_to_retry(tmp_path):
    db, source = tmp_path / "inbox.db", tmp_path / "input.jsonl"
    good = {"source": "a", "key": "1", "payload": {}}
    source.write_text(json.dumps(good) + '\n{"broken":\n', encoding="utf-8")
    result = cli("--db", db, "ingest", source)
    assert result.returncode == 2 and '"line":2' in result.stderr
    assert Inbox(db).stats()["pending"] == 1
    source.write_text(json.dumps(good) + "\n", encoding="utf-8")
    result = cli("--db", db, "ingest", source)
    assert json.loads(result.stdout) == {"accepted": 0, "duplicates": 1}


def test_demo_is_isolated_and_reproducible():
    first, second = run_demo(), run_demo()
    assert first == second
    assert first["stale_worker_rejected"] and first["duplicate_same_job"]
    assert first["stats"]["results"] == 1
    assert first["new_generation"] == 2


def test_cli_failure_and_replay(tmp_path):
    db = tmp_path / "inbox.db"
    Inbox(db).enqueue("a", "1", {}, max_attempts=1)
    assert cli("--db", db, "work").returncode == 1
    assert cli("--db", db, "replay", 1).returncode == 0
    assert json.loads(cli("--db", db, "stats").stdout)["pending"] == 1
