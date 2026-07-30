# Concurrency retest — 2026-07-30

## Verdict

The controlled one-worker, three-worker, crash, and full concurrent-writer
tests passed correctness and convergence without data loss, duplicate IDs,
table-option drift, writer errors, or health-response failures.

The full run recovered from three classified storage failures: two on the
target sorted table and one on an older test table in the same lake. Each
post-failure inventory showed zero committed progress, every claim released,
and later cycles converged. This is a pass with recovered environmental errors,
not an error-free performance run.

PostgreSQL coordination is intentionally lake-scoped. DuckLake uses one
snapshot sequence for every table in a lake, so table-scoped maintenance locks
still allow catalog commit collisions. Replicas safely parallelize independent
lakes; they do not run simultaneous maintenance transactions within one lake.

## Changes under test

- Every observation records `target_file_size`, `auto_compact`,
  `data_inlining_row_limit`, `sort_on_insert`, and sorting state. Any mutation
  fails the run.
- Merge admission uses a session-only target and output-group count bounded by
  an estimated 512 total inputs and the treatment's usable-memory budget.
- Sorted tables reserve 50% memory headroom; unsorted tables reserve the larger
  of 25% or 125 MB per DuckDB thread.
- Native failures are classified. Concurrent compaction and snapshot retry
  exhaustion receive exponential backoff and dedicated metrics.
- A failed native call is inventoried while its claim is still held, and
  observed snapshot/file progress is logged and counted.
- Every treatment runs in an isolated child process with a private DuckDB
  connection.
- The health server uses independent request threads. Readiness changes to 503
  when a cycle or treatment exceeds its configured threshold.
- The stress harness treats malformed/empty health responses, premature
  process exits, writer errors, option drift, non-convergence, lost rows, and
  duplicate IDs as hard failures.

## Controlled one-table runs

The same actively written 100-file medium table was run for 120 seconds first
with one maintainer and then with three.

| Maintainers | Successful treatments | Writer rows | Writer errors | Drain | Final files |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 43 | 1,363 | 0 | 84.221 s | 15 |
| 3 | 43 | 1,357 | 0 | 85.354 s | 15 |

Both runs preserved exactly 1,000,000 seed rows and every acknowledged writer
row, with zero duplicates, option changes, treatment failures, or health
request failures. Three workers did not speed up one table, which is the
expected coordination behavior.

## Crash recovery

A maintainer was killed with `SIGKILL` after treatment started, including its
isolated treatment process. PostgreSQL released the session advisory lock in
0.129 seconds and a replacement claimed work 1.850 seconds after the kill.

The replacement converged the table to 15 files. Its active writer committed
1,324 rows with zero errors; all 1,000,000 seed rows and acknowledged writer
rows were present exactly once. Effective table options remained unchanged.

## Full workload

The final workload used three writers and three maintainers:

| Table | Initial files | Initial rows | Initial bytes | Sorting |
| --- | ---: | ---: | ---: | --- |
| Tiny | 10,000 | 10,000 | 8,889,787 | disabled |
| Medium | 100 | 1,000,000 | 73,702,143 | disabled |
| Large | 500 | 20,000,000 | 1,473,867,480 | enabled |

Each writer inserted random 1–20 row batches for ten minutes using its own
process and DuckDB connection. Each maintainer treatment also used its own
process and connection. Maintenance then drained until three consecutive
samples reported zero merge debt.

The ten-minute write window and subsequent drain completed as follows:

| Table | Commits | Rows acknowledged | Writer errors | p50 | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Tiny | 496 | 5,074 | 0 | 0.448 s | 1.402 s | 2.291 s | 4.639 s |
| Medium | 474 | 4,942 | 0 | 0.502 s | 1.556 s | 2.759 s | 4.354 s |
| Large | 498 | 5,298 | 0 | 0.529 s | 1.275 s | 2.138 s | 3.848 s |

All acknowledged IDs were present exactly once. Seed rows were unchanged:
10,000 tiny, 1,000,000 medium, and 20,000,000 large.

| Table | Successful calls | Files processed | Files created | Largest input | p50 duration | p95 duration | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Tiny | 93 | 10,588 | 93 | 511 | 2.895 s | 9.899 s | 11.990 s |
| Medium | 4 | 577 | 18 | 280 | 3.973 s | 6.564 s | 11.885 s |
| Large, sorted | 10 | 1,000 | 253 | 275 | 37.988 s | 41.675 s | 41.981 s |

No successful treatment exceeded the 512-input admission guard. Two sorted
large treatments failed with `storage_error` after 28.140 and 51.167 seconds;
post-failure inventory found no snapshot or file-count progress. Later calls
completed the work.

Drain took 1,058.728 seconds after writers stopped. Final active files were:

| Table | Final files | Final rows | Final bytes |
| --- | ---: | ---: | ---: |
| Tiny | 1 | 15,074 | 1,158,041 |
| Medium | 15 | 1,004,942 | 74,016,005 |
| Large, sorted | 251 | 20,005,298 | 1,474,662,287 |

The workers made 117 claims and encountered 348 busy lake claims. All three
workers exited cleanly. The harness observed 348 successful readiness
responses and 942 successful metrics responses, with no empty, malformed, or
failed HTTP responses.

The lake also contained unfinished debt from an earlier rejected fixture.
LakeDucktor correctly maintained it because selection is lake-wide: six
successful calls and one recovered storage failure were spent on that older
table. Therefore the 1,058-second drain is a conservative whole-lake result,
not a pure three-table throughput benchmark.

## Findings from rejected fixture/run attempts

The harness rejected intermediate attempts rather than incorporating them:

- A storage `RequestTimeTooSkewed` exception was followed by evidence that a
  five-file seed transaction had nevertheless committed. This confirms that
  post-failure inventory is required; an exception cannot imply zero progress.
- Concurrent fixture writers on different tables exhausted DuckLake's ten
  snapshot retries. An earlier measured attempt similarly produced one large
  writer snapshot conflict under table-scoped maintainer locks. These results
  motivated and validate the lake-scoped claim.
- Limiting treatment to one output group was safe but made the 500-file sorted
  table require roughly 250 calls. The final bound instead permits multiple
  groups while limiting estimated total inputs and bytes. A live validation
  admitted 36 groups, processed 72 files into 36 in 48.447 seconds, and stayed
  within the 2 GB sorted-treatment budget.

Rejected attempts are not counted in the final performance or correctness
result.

The definitive run's three failures were classified only as `storage_error`
because the isolated child discarded the bounded native message. The executor
now retains a single-line, 500-character native cause for future failures. The
same environment produced explicit `RequestTimeTooSkewed` errors during
fixture construction, but attributing the measured failures to that cause
would be an inference rather than direct log evidence.

## Automated verification

Formatting and lint pass, and all 119 unit tests pass. Unit coverage includes
failure classification, retry/backoff metrics, post-failure committed progress,
bounded input admission, lake-scoped claims, isolated execution, and complete
HTTP responses with `/readyz` returning 503 during an intentionally
over-threshold treatment.

The definitive artifacts are in
`/tmp/lakeducktor-full-final-20260730/`; controlled-run artifacts are under
`/tmp/lakeducktor-{iso1,iso3,crash}-20260730/`.
