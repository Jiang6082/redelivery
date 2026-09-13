# Build status

September 13, 2026. Built with coding-assistant support. This is a working early
library release; personal mastery and production usage are separate milestones.

| Milestone | Actual status |
|---|---|
| M0 — skeleton | Complete: installable package, CLI, schema, four specification documents |
| M1 — MVP | Complete: durable receipt, duplicate/conflict handling, claim, local atomic results, JSONL example, inspection |
| M2 — recovery | Complete: generation fencing, renewal, expiry/reclaim, backoff, dead letters and explicit replay |
| M3 — engineering | Implemented for the defined library scope: process races/crashes, validation, logging, strict typing, lint and packaging; current hosted status is linked in the README badge |
| M4 — demo | Complete: public repository at https://github.com/Jiang6082/redelivery and one-command deterministic local demo; no hosted service required |
| M5 — measurements | Initial 1/2/4-worker comparison and a real public collector pilot complete; repeated workload measurements and human review remain |

## Local verification

Python 3.12.10, SQLite 3.49.1, Windows. Latest full test run: **69 passed in
12.43 seconds**, with **93% statement coverage** after enabling coverage in
subprocesses. Formatting, Ruff and strict mypy pass. Final sdist/wheel build
passed with source, tests, fixtures, documentation and the library typing marker.
The wheel was installed into a separate empty virtual environment; the isolated
demo passed there and the wheel contains `py.typed`. No runtime packages were
installed beyond Redelivery itself.

For the original 0.1 release, hosted CI passed on Python 3.11 and 3.12 on Ubuntu and Windows at source
commit `ccf80e5d949e170c458df285611ed4b1d45592d2`. All four jobs completed lint,
format checks, strict types, tests, demo and package builds successfully.
[Verified CI run](https://github.com/Jiang6082/redelivery/actions/runs/34738575111).
That run predates the 0.2 collector/export integration. The current README badge
links to the latest workflow status; do not treat the old run as validation of new code.

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

The 0.2 continuation adds a public Greenhouse collector, atomic JSONL audit export,
and a visible [TraceScope inspector](https://github.com/Jiang6082/tracescope). One
disposable live response contained 633 postings; identical redelivery suppressed
633 duplicates, and one worker persisted 633 results. The 1,899-event audit recording
is included in TraceScope. [Pilot protocol and limits](COLLECTOR_AND_TRACES.md).

- Explain and reimplement claim/complete from the specification.
- Repeat benchmark runs on a known local disk, recording hardware and workload.
- Compare cheap handlers against compute-heavy handlers before changing storage.
- Repeat the collector pilot over time, then record observed retries and recovery.
- Obtain human review and record actual usage; neither is established by this build.
