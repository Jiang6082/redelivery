# Interview value

## Why this project belongs beside the existing work

Research pipelines already provide evidence of mathematics, Python, reproducibility,
and careful validation. Full-stack work provides API and SQL experience. This
project adds a compact system whose correctness can be challenged by a concrete
schedule of interleaving processes and crashes. The example domain is familiar;
the new learning lies in ownership, transactions, and recovery.

## Two-minute explanation to rehearse

“I wanted a small ingestion job to survive duplicate delivery and worker crashes.
I separated durable receipt, temporary ownership, and committing the result. A
SQLite transaction claims a job and increments its generation. Computation runs
outside the transaction. Completion accepts only the current unexpired generation,
then stores the result and marks success atomically. I can kill a worker during
publication and show that the partial result rolls back. This guarantees one
committed local result per identity, not one execution of arbitrary side effects.”

Use this explanation only after you can derive it and reproduce the behavior.

## Questions worth being able to answer

| Question | What a strong answer should establish |
|---|---|
| Why doesn't SELECT then UPDATE suffice? | Two workers can read pending before either updates; write reservation or conditional update is needed. |
| Why both expiry and generation? | Expiry allows recovery; generation rejects obsolete owners after reassignment and replay. |
| Why reject a worker at exact expiry? | The contract uses a half-open validity interval; no ambiguous shared boundary. |
| Why sample time after lock acquisition? | Waiting for a lock may consume the lease; old time would accept a stale completion. |
| Is this exactly once? | Computation is at least once subject to retry budget; one local committed result is enforced. External effects need a separate design. |
| Why SQLite rather than Redis/Kafka? | A same-file transaction joins receipt, result and audit; one-host constraints match the initial problem. |
| What does FULL synchronization buy? | Stronger commit durability subject to filesystem/hardware behavior; tests do not simulate physical power loss. |
| What is the first scaling limit? | A single writer, open/close and fsync overhead; show measured contention instead of guessing. |
| Can four workers be slower than one? | Yes, for tiny jobs where database contention dominates useful computation. |
| What happens when the last attempt crashes? | Expiry consumes that attempt; the next claim sweep dead-letters it rather than leaving it stuck. |
| What happens after deleting old identities? | An old producer retry can become new work; retention is a semantics decision. |
| What about wall-clock jumps? | Availability and duplicate computation can change; ownership checks preserve accepted state. |
| Why keep handler code out of the transaction? | Avoid serializing long work and blocking all other producers/workers. |
| How would external delivery work? | Transactional outbox + destination idempotency; do not claim the database can atomically commit an HTTP request. |

## Learning exercises

1. Draw two workers reading the same pending row in a naive queue and reproduce
   the bug in a throwaway implementation.
2. Remove the generation condition locally; write a stale-owner counterexample.
3. Predict the database after a crash at each line of completion, then run the
   fault test. Explain rollback before looking at its assertions.
4. Measure 1, 2, and 4 workers on cheap and compute-heavy handlers. Explain any
   slowdown, including connection overhead and synchronization cost.
5. Extend the example to an outbox on paper. List the exact external duplicate
   case that a local transaction cannot prevent.

## Evidence progression

Implemented behavior → repeatable failure experiment → measurements with settings
→ independent review → a real collector pilot. Stars and technology counts are
not substitutes for the later steps.

## Resume language

Initial truthful template after understanding and verifying the code:

> Built a Python/SQLite ingestion library with transactional deduplication,
> generation-fenced worker leases, bounded retries, and crash recovery; verified
> atomic result publication with competing-process and forced-crash tests.

Measured template after collecting your own data:

> Engineered a durable ingestion library processing [N] synthetic deliveries with
> [D]% duplicates across [W] workers, committing [U] unique results with zero
> duplicate result rows; measured [p95] ms completion latency under [settings] and
> recovered from [K] injected worker failures.

Never combine independent benchmark and failure workloads into one unsupported
claim. Do not claim production users, multi-host availability, latency improvement,
or personal learning merely because generated code passes tests. Record assistance
honestly if asked and be able to explain the implementation without the assistant.
