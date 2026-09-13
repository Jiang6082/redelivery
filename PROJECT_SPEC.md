# Redelivery

A durable event inbox for small ingestion jobs, with a reproducible crash-and-recovery demonstration.

## Problem and intended user

Small data collectors commonly run on a workstation or one server. Inputs repeat,
workers die, and a timeout does not tell you whether work committed. A Python
developer should be able to retry ingestion without silently duplicating local
results or allowing an expired worker to overwrite its replacement.

The initial example normalizes **synthetic job-posting events**. It uses no scraped
records, employer code, credentials, or clinical data. The library is reusable by
a collector, but no existing repository or production pipeline is integrated yet.

## Scope

Python 3.11+, the standard library, one local SQLite database, a CLI, and a small
library API. Multiple independent worker processes share that local file.
Development dependencies: pytest, coverage, Ruff, mypy, build.

This is a single-host system demonstrating distributed-systems concepts. It is
not a replicated database, network queue, scheduler platform, or exactly-once
executor for arbitrary external effects. Handler computations may run repeatedly.

## Contract

1. `enqueue(source, key, payload)` commits an immutable JSON input before success.
2. Identity is `(source, key)`. Same identity and canonical JSON is a duplicate;
   different content or attempt budget is a conflict, not an update. Callers must use a new key for
   a new upstream revision. Key retention equals database retention.
3. `claim(worker, lease_seconds)` atomically assigns at most one eligible job.
   Each assignment increments a per-job fencing token (`generation`).
4. `complete(lease, result)` verifies job, owner, generation, and unexpired lease
   inside the write transaction. Result insertion, completion, and audit history
   commit together. One result row per input identity.
5. `fail` and `renew` use the same fencing condition. An expired worker cannot
   publish, fail, or renew its old assignment, even before another worker claims.
6. Expired jobs become claimable; both failures and abandoned claims consume the
   configured attempt budget. Exhausted jobs become dead letters. A deliberate
   `replay` resets the budget, preserves history, and never resets the generation.
7. Retries use bounded exponential delay. No sleeping while holding a transaction.
8. Payload/result limits and bounded inspection pages constrain accidental memory
   usage. Inputs must be strict JSON objects; NaN and Infinity are rejected.
9. Logging contains identifiers and transitions, never payloads or raw exceptions.
   The transactional audit is authoritative; stderr logging is best effort.

## Acceptance criteria

- Fresh install can ingest an example JSONL file, run a worker, and inspect results.
- Replaying identical input yields exactly the same count of persisted results.
- Independent processes cannot successfully publish for the same assignment.
- A killed process leaves no half-published result and can be recovered.
- Old owners and old generations are rejected after reassignment or replay.
- Fake-clock tests cover exact expiry, retries, renewal, and attempt exhaustion.
- An actual child-process crash inside the completion transaction rolls back.
- Formatting, lint, strict type checks, and tests run in CI on Linux and Windows.
- A benchmark reports its workload, Python/SQLite/platform, durability settings,
  and measured counts/latencies. It never substitutes a target for a measurement.

## Deliberate limits

No HTTP API, authentication service, remote database filesystem, Kafka, Redis,
Kubernetes, LLM, automatic trading, or user-supplied executable handler code in the
CLI. No arbitrary SQL callback within the public commit API. No external delivery
guarantee. The trusted Python API can register handlers in its own process.

Default database mode is rollback journal (`DELETE`) with `synchronous=FULL`.
This keeps deployment compatible with bundled SQLite versions and avoids assuming
WAL safety on older runtimes. Only one writer can commit at a time; short read and
write transactions matter. See the journal decision in ARCHITECTURE.md.

## Finish line

The compelling demo is: submit duplicates, kill a worker, reclaim the job, reject
the old lease, and show one committed result plus its attempt history. A real
collector integration and external user feedback follow only after this contract
is understood and measured.
