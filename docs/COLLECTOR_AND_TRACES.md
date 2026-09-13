# Public collector and TraceScope integration

The adapter reads Greenhouse's documented public board endpoint. The board token
is restricted to a short ASCII label and the host/path are fixed. Requests have a
20-second timeout and an 8 MiB response cap. Every row and the declared total are
validated before enqueuing anything. A malformed later row does not partially import
the earlier valid rows. Database failures during enqueue may leave a committed prefix;
repeating the response safely resumes by identity.

Identity is `(greenhouse:{board}:v1, posting_id:sha256(canonical_fields))`. Canonical
fields are upstream ID, company, title, location and URL. Whitespace is normalized.
Unrelated metadata/timestamp changes do not create another event. Meaningful changes
create a new immutable event. Source absence never implies closure or deletion.
The normalizer currently persists company/title/location; original URL and upstream
ID remain in the inbox input for local inspection.

Source: [official Greenhouse Job Board API](https://docs.greenhouse.io/job-board.html).
The endpoint is read-only and unauthenticated; no application submission is involved.

## One-time pilot

[Raw result](measurements/collector-pilot.json), September 13, 2026:

- One live Stripe board response: 633 postings, 395,340 bytes.
- First delivery: 633 accepted. Exact same captured response delivered again: 633 duplicates.
- One local worker persisted 633 results; zero pending, leased or dead jobs remained.
- Export contained 1,899 committed transitions, included as TraceScope's collector sample.
- Ingestion/redelivery took 4.9387 seconds; processing took 9.7499 seconds on this machine.

This proves one adapter/inbox/worker/export integration run. It is not a sustained-use
study or a fault-injected benchmark. The trace omits duplicate receipt attempts, so
duplicate counts come from the pilot report, not inference from missing trace rows.

Reproduce in a disposable database:

```sh
python scripts/collector_pilot.py stripe --report runtime/pilot-report.json --trace runtime/pilot.jsonl
```

The script captures one response and redelivers the same bytes. A later live board
will have different content/counts. The report includes its digest and observation time.

## Export contract

`export` captures the maximum committed transition ID, then reads pages of at most
1,000 events through that ID. Because the public API never edits/deletes transitions,
this yields a stable committed prefix without holding a read transaction throughout
file writing. New transitions are left for a later export. Direct SQL mutation of
the audit table is outside this contract.

Source, sequence, time, job, event, generation, last owner for that generation and
controlled detail labels are exported. Unclaimed events use `producer`; operator
replay uses `operator`. Handler inputs, event keys and result objects are omitted.
Metadata may still identify a source/worker; the export is not anonymization.

The exporter writes a temporary sibling, flushes/fsyncs and atomically replaces the
destination after success. Failures preserve the old file and clean up the temporary.
It requires a .jsonl/.ndjson output distinct from the database. Parent-directory fsync
is not implemented; replacement persistence under physical power loss is not claimed.

## Recovery recording

```sh
python scripts/generate_trace_demo.py runtime/recovery.jsonl
```

The generated scenario calls the actual inbox API with a deterministic fake clock.
Its 41 transitions include one reclaimed lease, 11 results and one dead-letter job.
A stale worker's attempted publication is rejected; rejected transactions do not
appear in committed audit history. This recording complements the existing tests
that terminate real OS processes; the fake-clock demo is not itself a process kill.
