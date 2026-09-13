# Redelivery

[![CI](https://github.com/Jiang6082/redelivery/actions/workflows/ci.yml/badge.svg)](https://github.com/Jiang6082/redelivery/actions/workflows/ci.yml)

A small, durable event inbox for Python ingestion jobs. The interesting part is
what happens after a worker crashes: a replacement can finish the job while an
expired worker is prevented from publishing an old result.

Python + SQLite. No runtime dependencies, service accounts, or cloud setup.

Explore its recovery decisions in [TraceScope](https://github.com/Jiang6082/tracescope),
a browser-local timeline and job inspector built around these audit events.

## Quick start

```sh
python -m pip install -e ".[dev]"
python -m redelivery demo
python -m redelivery --db runtime/inbox.db ingest examples/jobs.jsonl
python -m redelivery --db runtime/inbox.db work --worker first
python -m redelivery --db runtime/inbox.db stats
python -m redelivery --db runtime/inbox.db show 1
```

Run ingestion again to see duplicate suppression. Examples are synthetic.
The `work` command drains jobs currently eligible; delayed retries require a later
invocation. Run separate worker commands to use independent processes.

## Public collector and visible audit history

```sh
python -m redelivery --db runtime/pilot.db collect stripe
python -m redelivery --db runtime/pilot.db work --worker collector-1
python -m redelivery --db runtime/pilot.db export runtime/pilot.jsonl
```

Open the JSONL file in TraceScope. The Greenhouse adapter validates the entire
response first, then enqueues each posting with a content-derived identity. Repeated
content is suppressed; meaningful posting changes produce a new event. Missing
postings never trigger deletion. It reads a public board without credentials.

The first [disposable collector pilot](docs/COLLECTOR_AND_TRACES.md) accepted 633
postings, suppressed 633 identical redeliveries and persisted 633 results. The
exporter captures a paged, immutable audit prefix and replaces its output atomically.

## Guarantees worth inspecting

- A unique `(source, key)` identifies immutable canonical JSON. Conflicting reuse
  is an error rather than silently changing the work.
- Claim, renewal, failure, and completion are transactional.
- Monotonic generations fence expired/replaced workers, including after replay.
- A result and successful completion commit together in the same SQLite file.
- Crashes and failures consume bounded attempts, with explicit dead-letter replay.
- SQL audit transitions are durable; JSON logs omit payloads and exception text.

**The boundary:** handlers may execute repeatedly. One committed local result is
enforced per retained identity. Arbitrary HTTP calls, emails, or other external
effects are outside that transaction. Multiple processes work on one host; this
does not provide replication or multi-host availability.

## Verification

```sh
python -m pytest --cov=redelivery --cov-report=term-missing
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m build
python -m redelivery.benchmark --events 1000 --workers 4
```

Tests include actual independent processes and a forced process exit inside a
publication transaction. Physical disk failures and power loss are not simulated.

## Read the design

- [Project contract](PROJECT_SPEC.md)
- [Architecture and failure model](ARCHITECTURE.md)
- [Milestones](MILESTONES.md)
- [Interview questions and learning exercises](INTERVIEW_VALUE.md)
- [Build status](docs/BUILD_STATUS.md)
- [Python API and operating notes](docs/OPERATIONS.md)
- [Measured contention and benchmark protocol](docs/MEASUREMENTS.md)

Keep the live database on a local disk. The default is SQLite rollback journal
mode with FULL synchronization and bounded lock waits. See the architecture for
the concurrency and performance tradeoffs. The code is an early library release,
with local and CI validation documented separately from operational use.
