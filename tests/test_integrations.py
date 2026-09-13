import json

import pytest

from redelivery.greenhouse import ingest_board, parse_board
from redelivery.store import Inbox
from redelivery.trace import export_trace


def feed(*rows):
    return json.dumps({"jobs": rows, "meta": {"total": len(rows)}}).encode()


def posting(**changes):
    return {
        "id": 71,
        "title": " Software   Engineer ",
        "company_name": "Example",
        "location": {"name": "New York"},
        "absolute_url": "https://example.test/jobs/71",
        **changes,
    }


def test_feed_retry_and_content_change(tmp_path):
    inbox = Inbox(tmp_path / "test.db")
    assert ingest_board(inbox, "example", feed(posting())) == {"accepted": 1, "duplicates": 0}
    # Whitespace and unrelated metadata updates do not create another event.
    assert ingest_board(
        inbox, "example", feed(posting(title="Software Engineer", updated_at="new"))
    ) == {
        "accepted": 0,
        "duplicates": 1,
    }
    assert ingest_board(inbox, "example", feed(posting(title="Senior Engineer")))["accepted"] == 1
    assert ingest_board(inbox, "example", feed()) == {"accepted": 0, "duplicates": 0}
    assert inbox.stats()["pending"] == 2  # No deletion or closure inference from absence.


@pytest.mark.parametrize(
    "bad",
    [
        feed(posting(), posting(id=72, title=None)),
        feed(posting(), posting()),
        b'{"jobs":[],"meta":{"total":1}}',
        b'{"jobs":[],"jobs":[],"meta":{"total":0}}',
        feed(posting(id=True)),
        b"{broken",
        b'{"jobs":[],"meta":{"total":false}}',
    ],
)
def test_bad_feed_writes_nothing(tmp_path, bad):
    inbox = Inbox(tmp_path / "test.db")
    with pytest.raises(ValueError):
        ingest_board(inbox, "example", bad)
    assert inbox.stats()["pending"] == 0


def test_board_cannot_be_a_url_or_path():
    with pytest.raises(ValueError):
        parse_board("../other?url=https://example.test", feed())


def test_export_is_bounded_prefix_and_attributes_worker(tmp_path):
    now = [100.0]
    inbox = Inbox(tmp_path / "test.db", clock=lambda: now[0])
    inbox.enqueue("source", "secret-key", {"secret": "input"})
    first = inbox.claim("alpha", lease_seconds=1)
    assert first
    now[0] += 2
    second = inbox.claim("beta")
    assert second
    high_water = inbox.audit_high_water()
    inbox.complete(second, {"secret": "output"})
    prefix = inbox.audit_page(after=0, through=high_water, limit=100)
    assert [r["event"] for r in prefix] == ["enqueued", "claimed", "reclaimed"]
    assert [r["worker"] for r in prefix] == ["producer", "alpha", "beta"]
    path = tmp_path / "trace.jsonl"
    assert export_trace(inbox, path) == {"events": 4, "through": 4}
    text = path.read_text()
    assert "secret" not in text
    assert json.loads(text.splitlines()[-1])["worker"] == "beta"
    with pytest.raises(ValueError):
        export_trace(inbox, inbox.path)


def test_failed_export_preserves_previous_file_and_cleans_temp(tmp_path, monkeypatch):
    inbox = Inbox(tmp_path / "test.db")
    path = tmp_path / "trace.jsonl"
    path.write_text("old recording\n")
    before = set(tmp_path.iterdir())

    def broken(**kwargs):
        raise OSError("simulated read failure")

    monkeypatch.setattr(inbox, "audit_page", broken)
    with pytest.raises(OSError):
        export_trace(inbox, path)
    assert path.read_text() == "old recording\n"
    assert set(tmp_path.iterdir()) == before


def test_export_continues_across_page_boundary(tmp_path):
    inbox = Inbox(tmp_path / "test.db")
    # Insert audit rows inside one test transaction to exercise paging cheaply.
    inbox.enqueue("source", "one", {})
    with inbox._transaction() as db:
        db.executemany(
            "INSERT INTO transitions(job_id,at,event,generation,detail) VALUES(1,?,'renewed',1,'')",
            [(float(i),) for i in range(1005)],
        )
    path = tmp_path / "many.jsonl"
    assert export_trace(inbox, path)["events"] == 1006
    ids = [json.loads(line)["id"] for line in path.read_text().splitlines()]
    assert ids == list(range(1, 1007))


def test_cli_collector_and_export_without_network(tmp_path, monkeypatch, capsys):
    from redelivery.cli import main

    monkeypatch.setattr("redelivery.greenhouse.fetch_board", lambda board: feed(posting()))
    common = ["--db", str(tmp_path / "test.db")]
    assert main([*common, "collect", "example"]) == 0
    assert json.loads(capsys.readouterr().out)["accepted"] == 1
    assert main([*common, "export", str(tmp_path / "trace.jsonl")]) == 0
    assert json.loads(capsys.readouterr().out)["events"] == 1


def test_fetch_uses_fixed_endpoint_and_bounded_read(monkeypatch):
    from redelivery.greenhouse import MAX_FEED_BYTES, fetch_board

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self, count):
            assert count == MAX_FEED_BYTES + 1
            return b"{}"

    def request(req, timeout):
        assert req.full_url == "https://boards-api.greenhouse.io/v1/boards/example/jobs"
        assert timeout == 20
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", request)
    assert fetch_board("example") == b"{}"


def test_replay_is_an_operator_transition(tmp_path):
    inbox = Inbox(tmp_path / "test.db")
    receipt = inbox.enqueue("source", "one", {}, max_attempts=1)
    lease = inbox.claim("worker")
    assert lease
    inbox.fail(lease)
    inbox.replay(receipt.job_id)
    page = inbox.audit_page(after=0, through=inbox.audit_high_water())
    assert page[-1]["event"] == "replayed"
    assert page[-1]["worker"] == "operator"
