# Measurement protocol

Run on a local disk after installing the package:

```sh
python -m redelivery.benchmark --events 1000 --workers 1
python -m redelivery.benchmark --events 1000 --workers 2
python -m redelivery.benchmark --events 1000 --workers 4
```

Each invocation creates an isolated temporary database and uses fresh spawned
worker processes. It loads 1,000 unique integer payloads followed by 200 duplicate
deliveries, then drains 1,000 jobs and verifies persisted result counts and SQLite
integrity. Duplicate deliveries are 200/1,200 = 16.7% of total deliveries.

Settings: rollback journal (`DELETE`), `synchronous=FULL`, five-second busy timeout,
one connection per operation, default 30-second leases. Computation is deliberately
tiny. Throughput includes worker process creation, shutdown, and database work;
claim/complete latency excludes time sitting in the preloaded queue. Enqueue p95
measures unique enqueue operations; ingestion duration also includes duplicates.

Raw results live in `measurements/windows-1000-w{1,2,4}.json`. The initial comparison
uses one run per condition, with a warm operating-system cache. These are local
observations, not production capacity or statistically established speedups.

## Initial results — September 13, 2026

Code: `e248a5292be42cdd8f92ca53c1e9cfb8736745df`. Windows 11, Python 3.12.10,
SQLite 3.49.1, Intel Core i7-9700 with eight logical processors, approximately
32 GiB RAM, Samsung SSD 990 PRO 1TB. One run per row.

| Workers | Unique results | Duplicate deliveries suppressed | Drain results/s | Claim/complete p95 | Claim/complete p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,000 | 200 | 48.40 | 24.046 ms | 39.469 ms |
| 2 | 1,000 | 200 | 46.77 | 141.830 ms | 263.113 ms |
| 4 | 1,000 | 200 | 45.67 | 219.093 ms | 1,537.755 ms |

Every run ended with 1,000 succeeded jobs, 1,000 result rows, no pending/leased/dead
jobs, and `integrity_check=ok`. Four workers completed 284, 167, 201 and 348 jobs,
confirming that all four contributed in that run. No worker crashes were injected
in this benchmark; crash correctness is established by the separate test suite.

The observed direction is consistent with contention dominating tiny handlers:
more workers did not improve throughput and increased tail latency. Because the
comparison has one run per condition and no controlled CPU-heavy workload, it
does not establish a general scaling law or isolate fsync cost from lock wait.
Do not claim a speedup. The result is useful precisely because it exposes a limit.

## What to collect next

| Metric | Why it matters | Measurement design |
|---|---|---|
| Accepted/duplicate/conflicting inputs | Proves producer identity behavior | Count at the producer; do not infer duplicate count from final rows |
| Unique results and duplicate result rows | Tests the actual local commit contract | Compare expected IDs with persisted results after each workload |
| p50/p95/p99 claim-to-complete | Shows contention and worker responsiveness | Fixed payload/handler/durability; repeat each condition at least five times |
| Enqueue-to-result latency | Captures backlog delay omitted by the first benchmark | Persist/compare arrival and completion timestamps under bounded arrival rates |
| Crash recovery latency | Shows lease duration versus recovery tradeoff | Kill after claim; time from failure to replacement's committed result |
| Lost-lease rejections | Demonstrates stale-worker protection | Delay a worker through expiry, then make both workers attempt publication |
| Busy errors and transaction wait | Identifies write contention | Vary worker count and injected handler duration; count errors rather than dropping them |
| Database growth per input/attempt | Quantifies audit/identity retention cost | Vary retries and payload sizes; include identities retained for deduplication |
| Real collector outcomes | Establishes practical use | One-week disposable pilot, source failure cases, human review notes |

Record CPU model, disk type, free space, OS/Python/SQLite versions, source commit,
payload size, arrival pattern, journal mode and synchronization for each published
comparison. Keep process-crash experiments separate from throughput runs unless
the combined workload actually injects and records those crashes.

The first benchmark does not compare an alternative queue and does not demonstrate
a latency improvement over another implementation. A more valuable next experiment
is to add 5–20 ms of real CPU work per job and determine when parallel computation
starts to outweigh write serialization.
