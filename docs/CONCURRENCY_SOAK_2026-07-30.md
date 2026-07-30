# Concurrent writer/maintenance soak — 2026-07-30

The follow-up work and clean retest are documented in
[Concurrency retest](CONCURRENCY_RETEST_2026-07-30.md).

## Verdict

Data correctness passed. Operational reliability did not pass cleanly.

Three LakeDucktor workers maintained three actively written tables without
losing or duplicating acknowledged rows. All writers completed without an
error, all advisory claims were released, all workers stopped cleanly, and the
lake eventually reached zero reported merge debt.

The same run exposed two production blockers:

1. Native compaction returned 14 failures, including one after 513 seconds.
   Retrying eventually converged, but the amount of wasted or partially
   committed work is not currently observable.
2. Health and metrics requests sometimes received an empty HTTP response while
   a worker was busy. Kubernetes must be able to trust these endpoints.

The final file counts are not a clean LakeDucktor throughput measurement.
During the run, the medium table's persisted `target_file_size` changed from
5 MB to 128 MB and the large table's changed through 32 MB and 128 MB to
512 MB. Neither LakeDucktor nor the soak harness writes these changes. The
actor is unknown, so the final drain result is contaminated and must not be
presented as LakeDucktor implementing tiered compaction.

## Workload

Run ID: `20260729a`

Metadata catalog:
`ducklake_b545b95f250a49f28b753109d95b13ed` on PostgreSQL.

Three maintainers ran with four DuckDB threads and a 4 GB memory limit each.
The large table was sorted by `id`; its treatment admission therefore reserved
2 GB of headroom rather than the 1 GB used for the unsorted tables. Every
writer and every treatment owned a separate DuckDB connection. All connections
disabled checkpoint-on-shutdown and none detached the lake.

| Table | Initial files | Initial rows | Initial bytes | Typical input file |
| --- | ---: | ---: | ---: | ---: |
| Tiny | 10,000 | 10,000 | 8,889,846 | 889 B |
| Medium | 100 | 1,000,000 | 73,682,064 | 736,808 B |
| Large, sorted | 500 | 20,000,000 | 1,473,711,812 | 2,947,620 B |

For ten minutes, one dedicated process per table inserted random batches of
1–20 rows every 0.25–1 second. Maintenance then drained until three consecutive
samples reported zero merge debt. The final sample was 872 seconds after
startup, approximately 272 seconds after the write window.

Fixture preparation was not part of the measured ten-minute window. During
parallel preparation, two seed transactions exhausted DuckLake's ten metadata
commit retries on snapshot primary-key conflicts. Both were safely resumed;
the measured writers did not reproduce that error at the lower randomized
write rate.

## Writer results

| Table | Commits | Rows acknowledged | Errors | p50 | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Tiny | 352 | 3,590 | 0 | 0.708 s | 3.337 s | 5.123 s | 10.655 s |
| Medium | 329 | 3,405 | 0 | 0.697 s | 3.440 s | 6.495 s | 14.879 s |
| Large | 355 | 3,665 | 0 | 0.698 s | 2.815 s | 5.521 s | 9.195 s |
| **Total** | **1,036** | **10,660** | **0** | | | | |

These are end-to-end insert latencies observed by the writer process, not
isolated PostgreSQL or object-store timings.

## Correctness validation

Validation scanned each table after maintenance and compared it with every
acknowledged writer ID.

| Table | Seed rows expected/found | Writer rows expected/found | Missing acknowledged IDs | Duplicate IDs | Final rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| Tiny | 10,000 / 10,000 | 3,590 / 3,590 | 0 | 0 | 13,590 |
| Medium | 1,000,000 / 1,000,000 | 3,405 / 3,405 | 0 | 0 | 1,003,405 |
| Large | 20,000,000 / 20,000,000 | 3,665 / 3,665 | 0 | 0 | 20,003,665 |

This establishes preservation of seeded and acknowledged inserted rows for
this workload. It does not test updates, deletes, schema evolution, process
crashes, or object-store faults.

## Maintenance behavior

The workers made 115 claims and encountered 194 busy claims. Busy claims are
expected and show that PostgreSQL advisory locking serialized LakeDucktor
workers by table.

| Table | Successful calls | Failed calls | Longest success | Longest failure |
| --- | ---: | ---: | ---: | ---: |
| Tiny | 17 | 1 | 2.105 s | 512.965 s |
| Medium | 59 | 7 | 19.358 s | 29.781 s |
| Large, sorted | 24 | 6 | 38.468 s | 244.299 s |
| **Total** | **100** | **14** | | |

All 14 failed claims were released and later cycles continued. Thirteen traces
said that another transaction had compacted the same table. One medium-table
call exhausted DuckLake's ten metadata-commit retries on a duplicate snapshot
ID.

The catalog contains 140 `merge_adjacent` snapshots for the test tables while
LakeDucktor logged 100 successful calls, including two successful no-ops.
Together with file-count changes observed during long calls that eventually
failed, this shows that a returned failure cannot safely be interpreted as
zero physical progress. Because an unknown actor also changed table options,
this run cannot determine how much of the additional work came from failed
calls versus that actor.

DuckLake documents that concurrent metadata commits are retried and that some
logical conflicts abort rather than retry. LakeDucktor should classify these
conflicts explicitly, retain the native cause in its concise error log, and
expose retry/conflict counters instead of treating all of them as an opaque
cycle failure.

## Final physical state

| Table | Active files | Active bytes | Final file sizes |
| --- | ---: | ---: | --- |
| Tiny | 1 | 1,062,287 | 1,062,287 B |
| Medium | 1 | 73,841,488 | 73,841,488 B |
| Large | 3 | 1,478,353,651 | 436–521 MB |

The last three observations reported zero merge debt. The run also left
11,842 test files scheduled for deletion: 10,380 tiny, 516 medium, and 946
large. No cleanup was run. DuckLake intentionally schedules superseded files
before a later cleanup operation, so this is expected catalog state, but it is
a material storage side effect of the test.

## Health and progress

No worker exited early. All three logged `worker_stopped` after a graceful
interrupt.

Every HTTP response that arrived had status 200, but not every request produced
a response:

| Endpoint | Requests | Empty-response errors |
| --- | ---: | ---: |
| `/readyz` | 345 | 20 (5.8%) |
| `/metrics` | 492 | 45 (9.1%) |

The client error was
`RemoteDisconnected('Remote end closed connection without response')`.
Forty-five metrics failures were concentrated on worker 1 during long sorted
large-table treatments. No server-side traceback explained them.

The test configured `TREATMENT_STUCK_AFTER_SECONDS=900`; the longest treatment
was 513 seconds, so readiness remaining healthy was consistent with that
configuration. This run therefore did not exercise the 503/stuck transition.
It did show that the HTTP serving path itself is unreliable under load, which
must be fixed and retested with a deliberately low stuck threshold.

## Interpretation

The important positive result is narrow but real: with PostgreSQL metadata,
three LakeDucktor processes coordinated their own table treatments, concurrent
inserts stayed available, and acknowledged data was correct afterward.

It is not yet evidence that three workers compact faster than one, or that the
current treatment size is operationally safe. A single native call spent more
than eight minutes before failing, and the run's target-size mutation prevents
a clean throughput comparison.

DuckLake's documented tiered strategy requires the caller to set a target and
size bounds for each tier. LakeDucktor currently calls
`ducklake_merge_adjacent_files` with a table and `max_compacted_files` only; it
does not implement those tier transitions. DuckLake also documents that
`auto_compact` does not compact on insert, so the same-table compaction
conflicts and option mutation require an isolated reproduction.

## Required follow-up

Before calling this production-ready:

1. Make the harness record effective table options in every observation and
   fail the run if an unplanned option mutation occurs.
2. Reproduce one actively written table with exactly one maintainer, then
   repeat with three. This separates native writer/compactor behavior from
   LakeDucktor-to-LakeDucktor coordination.
3. Bound work by actual input groups, not only projected output count, so a
   10,000-file treatment cannot become one eight-minute opaque call.
4. Add conflict-specific retry/backoff metrics and report committed progress
   from post-failure inventory.
5. Fix the empty HTTP responses and test that `/readyz` becomes 503 while an
   intentionally over-threshold treatment is still running.
6. Run a crash test that kills a maintainer mid-treatment and verifies advisory
   lock recovery, writer correctness, and eventual convergence.

## Evidence

The reusable harness is `tests/stress/concurrent_soak.py`.

Raw artifacts are in `/tmp/lakeducktor-soak-20260729a-run/`:

- `maintainer-{1,2,3}.log`
- `writer-{tiny,medium,large}.jsonl`
- `observations.jsonl`
- `analysis.json`
- `validation.json`

Fixture-preparation logs are in
`/tmp/lakeducktor-soak-20260729a-seed/`.

Relevant upstream behavior is documented in DuckLake's
[merge-adjacent-files documentation](https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files),
[conflict-resolution documentation](https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution),
and
[cleanup documentation](https://ducklake.select/docs/stable/duckdb/maintenance/cleanup_of_files).
