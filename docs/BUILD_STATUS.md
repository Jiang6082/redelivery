# Build status

September 13, 2026. Built with coding-assistant support. This is a working early
library release; personal mastery and production usage are separate milestones.

| Milestone | Actual status |
|---|---|
| M0 — skeleton | Complete: installable package, CLI, schema, four specification documents |
| M1 — MVP | Complete: durable receipt, duplicate/conflict handling, claim, local atomic results, JSONL example, inspection |
| M2 — recovery | Complete: generation fencing, renewal, expiry/reclaim, backoff, dead letters and explicit replay |
| M3 — engineering | Complete for the defined library scope: real process races/crashes, validation, logging, strict typing, lint, packaging; all four hosted Linux/Windows CI jobs passed |
| M4 — demo | Complete: public repository at https://github.com/Jiang6082/redelivery and one-command deterministic local demo; no hosted service required |
| M5 — measurements | Initial 1/2/4-worker measurements complete; real collector pilot, repeated measurements and human review remain future work |

## Local verification

Python 3.12.10, SQLite 3.49.1, Windows. Latest full test run: **54 passed in
17.09 seconds**, with **93% statement coverage** after enabling coverage in
subprocesses. Formatting, Ruff and strict mypy pass. Final sdist/wheel build
passed with source, tests, fixtures, documentation and the library typing marker.
The wheel was installed into a separate empty virtual environment; the isolated
demo passed there and the wheel contains `py.typed`. No runtime packages were
installed beyond Redelivery itself.

Hosted CI passed for Python 3.11 and 3.12 on both Ubuntu and Windows at source
commit `ccf80e5d949e170c458df285611ed4b1d45592d2`. All four jobs completed lint,
format checks, strict types, tests, demo and package builds successfully.
[Verified CI run](https://github.com/Jiang6082/redelivery/actions/runs/34738575111).
The following documentation-only commit records that outcome without changing
the verified runtime, tests, dependencies or workflow.

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

## Observed measurements

Each run accepted 1,000 unique inputs, suppressed 200 repeated deliveries and
persisted exactly 1,000 local results. One/two/four workers delivered 48.40/46.77/
45.67 results per second; corresponding p95 claim/complete latencies were 24.046/
141.830/219.093 ms. These are single local runs of tiny handlers, with FULL
synchronization. [Protocol, raw reports and interpretation](MEASUREMENTS.md) explain
why this is evidence of a contention tradeoff rather than a speedup claim.

## Next evidence to earn

- Explain and reimplement claim/complete from the specification.
- Repeat benchmark runs on a known local disk, recording hardware and workload.
- Compare cheap handlers against compute-heavy handlers before changing storage.
- Integrate a disposable collector pilot, then record observed retries and recovery.
- Obtain human review and record actual usage; neither is established by this build.
