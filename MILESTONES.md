# Milestones

Effort estimates below are focused learner hours, including understanding and
verification. They are not estimates of automated generation time.

| Stage | Deliverable / acceptance gate | Estimate |
|---|---|---:|
| M0 — skeleton | Package, CLI entry point, schema, design docs, dev tools, CI definition | 3–5 h |
| M1 — functioning MVP | Durable enqueue/dedup, claim, local atomic result, CLI ingest/work/show; restart and identity tests | 8–12 h |
| M2 — technically interesting feature | Fenced leases, expiry/reclaim, renewal, retry budget, dead-letter replay; exact-boundary tests | 10–15 h |
| M3 — production-quality engineering | Process races and crash-in-transaction test, strict validation, structured logging, bounded reads, schema guard, cross-platform CI | 8–12 h |
| M4 — deploy/demo | Public repository and one-command deterministic demo; optional recorded terminal walkthrough | 3–5 h |
| M5 — measurements + resume evidence | Repeatable benchmark, workload metadata, 1/2/4-worker comparison, honest bullet, external feedback and collector pilot | 5–10 h plus observation |

Total: roughly 37–59 focused hours, or 4–6 weeks at 10 hours/week.

## Recommended commit boundaries

1. M0/M1: durable happy path and duplicate contract.
2. M2: recovery, fencing, retry, replay.
3. M3: realistic failure tests and validation improvements.
4. M4/M5: runnable demo, benchmark evidence, release documentation.

The accompanying implementation is generated with coding assistance. Milestone
completion establishes repository behavior, not the candidate's personal mastery.
Reimplement claim/complete from the specification, predict failing schedules,
and explain the tests before using this as an interview project.

## After this build

A disposable public Greenhouse collector pilot is complete, and TraceScope now
provides a separate interactive inspector for its audit history. Next, record
observed behavior over repeated real runs and request a human review of the lease
contract. See docs/BUILD_STATUS.md for actual validation and unfinished evidence.
