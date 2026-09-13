# Using and operating Redelivery

## Python API

```python
from redelivery import Inbox, LostLease

inbox = Inbox("runtime/inbox.db")
receipt = inbox.enqueue("my-collector", "record-42:revision-1", {"value": 42})
lease = inbox.claim("local-worker", lease_seconds=30)
if lease is not None:
    value = lease.payload.get("value")
    if not isinstance(value, int):
        raise ValueError("Expected an integer value")
    try:
        inbox.complete(lease, {"doubled": value * 2})
    except LostLease:
        # Discard an obsolete local result; another worker may own the job now.
        pass
```

The minimal example above assumes the collector's numeric schema. Long-running
trusted handlers must renew their lease before expiry or choose an appropriate
duration. The CLI's normalizer is intentionally short and does not auto-renew.

## Input and identity

One JSON object per line: `source`, `key`, `payload`, optional `max_attempts`.
Source/key are nonblank strings of at most 200 characters; max_attempts is 1–100.
Payloads/results are JSON objects limited to 64 KiB after canonical encoding and
32 nested levels. CLI envelopes also have a 64 KiB limit. Unknown envelope fields,
duplicate JSON keys and nonfinite numbers are rejected.

Canonicalization sorts object keys; array order, strings and numeric
representations such as `1` versus `1.0` retain their distinct meaning. Producer
keys must include upstream revision when a changed input represents new work.
Reusing an identity with different content or attempt budget is a conflict.

Each valid JSONL line commits independently. After a later line fails, fix the
input and ingest again; prior identities are duplicates. Enqueue acknowledges
only committed input. Back up the database if preserving those identities matters.

## Commands and exit codes

`--db PATH` precedes the subcommand. `init`, `ingest`, `work`, `stats`, `show`,
`list`, `history`, `replay` and `demo` are available with `--help`.

- Exit 0: command succeeded; work may simply have found no eligible job.
- Exit 1: work encountered a retried, dead-lettered or lost-lease attempt.
- Exit 2: bad arguments/input or a storage/IO failure. Earlier input lines may
  already be durable. Inspect state before deciding whether processing is done.

`work --max-jobs N` bounds attempts in one invocation. It stops when no job is
currently eligible, including when retries are scheduled for later. It is a batch
command, not a background daemon. `stats` reveals waiting/leased/dead work. Each
claim sweeps at most 100 exhausted leases; subsequent invocations finish a larger
expired backlog. `history --after ID --limit N` pages through the audit trail.

## Failure and observability

Storage errors propagate from the Python API. Do not convert them to successful
acknowledgments. The worker records a bounded error code rather than arbitrary
exception text. Attach your own trusted debugger for an individual failure; do not
turn on payload logging for a real sensitive workload.

CLI logs use JSON on stderr. Every accepted state transition also writes a durable
SQL audit record in the same transaction. Duplicate enqueue attempts do not append
audit rows; count duplicates at the producer if you need that metric.

On a dead letter, first inspect the input and code defect. `replay ID` resets its
attempt budget but preserves the original input and fencing generation. To correct
the input, enqueue a new revision identity. Replay is for reprocessing after fixing
a handler or transient environment, not editing historical data.

## Storage and scaling

Keep the database on local disk with normal filesystem permissions. No encryption,
multi-tenant access control, automatic retention, replication, or remote workers
are included. Avoid a network filesystem or cloud-sync directory. Use SQLite's
backup API for a live backup; otherwise stop writers before copying the database.

One writer can progress at a time. More workers can make tiny jobs slower. Use
the benchmark to separate useful computation from serialization/fsync overhead.
Changing FULL synchronization to get a prettier number changes the durability
contract; report that explicitly in any experiment.

## Contribution scope

A useful first review is a counterexample to a stated invariant, a minimized crash
schedule, or a measured bottleneck. Supply a failing test and explain which
assumption changed. A new adapter should use synthetic or appropriately shareable
fixtures. No production source or clinical records are needed for this repository.
