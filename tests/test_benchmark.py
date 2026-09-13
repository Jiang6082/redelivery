import json
import subprocess
import sys


def test_benchmark_finishes_and_closes_database_before_cleanup():
    # Regression: sqlite3's transaction context manager left a handle open, so
    # TemporaryDirectory cleanup failed on Windows after an otherwise successful run.
    result = subprocess.run(
        [sys.executable, "-m", "redelivery.benchmark", "--events", "7", "--workers", "2"],
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["unique_inputs"] == 7
    assert report["duplicate_deliveries"] == 2
    assert report["total_deliveries"] == 9
    assert sum(report["per_worker_completed"]) == report["counts"]["results"] == 7
    assert report["counts"]["leased"] == report["counts"]["pending"] == 0
    assert report["integrity_check"] == "ok"
    assert report["journal_mode"] == "delete" and report["synchronous"] == "FULL"
