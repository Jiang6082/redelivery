# Build status

September 13, 2026. Built with coding-assistant support. This is a working early
library release; personal mastery and production usage are separate milestones.

| Milestone | Actual status |
|---|---|
| M0 — skeleton | Complete: installable package, CLI, schema, four specification documents |
| M1 — MVP | Complete: durable receipt, duplicate/conflict handling, claim, local atomic results, JSONL example, inspection |
| M2 — recovery | Complete: generation fencing, renewal, expiry/reclaim, backoff, dead letters and explicit replay |
| M3 — engineering | Implemented and locally verified: real process races/crashes, validation, structured logging, strict typing, lint, packaging and CI definition; hosted CI pending publication |
| M4 — demo | Local deterministic demo works; public repository publication pending |
| M5 — measurements | Reproducible benchmark implemented; measurements running. Real collector pilot and human review remain future work |

## Local verification

Python 3.12.10, SQLite 3.49.1, Windows. Latest full test run: **51 passed in
27.58 seconds**, with **91% statement coverage** after enabling coverage in
subprocesses. Formatting, Ruff and strict mypy pass. Initial sdist/wheel build
passed; final packaging verification follows the measurement update.

Coverage is an aid to finding untested branches, not a correctness guarantee.
Forced `os._exit` processes do not flush coverage, by design. Their externally
observed database state is checked by the parent process after restart.

Failure experiments include four producers racing on the same identity, four
independent consumers processing 40 jobs, stale leases after expiry, and forced
child-process exits after claim, inside publication, and after commit. The tests
assert durable state and database integrity after restart. Physical power loss,
storage corruption, external service idempotency and multi-host failure are outside
the tested boundary.

The first benchmark exposed a Windows resource-lifetime bug in the benchmark
itself: `with sqlite3.connect(...)` commits/rolls back but does not close the
handle. This prevented temporary-directory cleanup after a successful workload.
The benchmark now closes explicitly, and a subprocess regression test exercises
the complete run and cleanup. Initial failed runs produced no valid measurement
report and are not used as performance evidence.

## Next evidence to earn

- Explain and reimplement claim/complete from the specification.
- Repeat benchmark runs on a known local disk, recording hardware and workload.
- Compare cheap handlers against compute-heavy handlers before changing storage.
- Integrate a disposable collector pilot, then record observed retries and recovery.
- Obtain human review and record actual usage; neither is established by this build.
