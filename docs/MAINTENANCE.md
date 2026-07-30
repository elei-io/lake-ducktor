# Maintenance learnings

This document distills the maintenance lessons encoded in DuckBasin's
compaction implementation, tests, operational metrics, and revision history.
They are constraints and observations for LakeDucktor, not an implementation
plan.

DuckBasin exercised file merging and delete-heavy file rewriting. It did not
establish equivalent experience for snapshot expiration, obsolete-file
cleanup, orphan removal, or flushing inlined data. LakeDucktor therefore keeps
its inline-flush rule deliberately small and derived from DuckLake state.

## Ground truth and derived state

The current DuckLake catalogue is the authority for both physical condition and
effective maintenance policy.

- Active files, active delete files, table identity, schema versions,
  partitions, and effective settings must be read from the current catalogue.
- Notifications, dirty flags, rankings, claims, and run histories are
  disposable accelerators. Losing them must cause rediscovery, not lost work.
- Stable table identifiers should drive coordination. Names are useful for
  display but may change.
- Cached observations become stale as soon as writers or maintenance commit.
  Every treatment therefore ends in a fresh diagnosis.
- Loss of derived state, upgrades, strategy changes, and explicit wakes require
  a full reconciliation path. Incremental signals alone cannot prove that no
  debt was missed.

Event-driven discovery reduced unnecessary catalogue scans in DuckBasin, but
correctness depended on persisting a coalesced dirty-table signal before
acknowledging its source. Maintenance-generated changes also had to be
distinguished from new application work to avoid a feedback loop. The general
lesson is to treat events as hints backed by level-triggered, reconstructable
state.

## Correctness

### Respect native semantics

- Maintenance should invoke native DuckLake operations rather than edit
  metadata tables or delete objects itself.
- Lake-, schema-, and table-scoped settings must be resolved with DuckLake's
  own precedence. An exclusion such as `auto_compact` remains authoritative.
- Execution controls may pause or throttle work, but must not silently create
  different retention, rewrite, or layout policy.
- DuckBasin's fixed size tiers and its executor-selected writes to
  `target_file_size` are useful experiments, not policy to inherit. Persisting
  an execution strategy as lake policy crosses LakeDucktor's boundary.

### Never checkpoint a lake

LakeDucktor must not issue `CHECKPOINT` or trigger it implicitly by detaching
or shutting down an attached DuckLake. DuckLake maps a checkpoint to its full
maintenance suite, which would bypass LakeDucktor's selected table, resource
admission, and coordination claim. DuckDB connections therefore disable
checkpoint-on-shutdown before attaching a lake and close without `DETACH`.

### Diagnose compatible file groups

Files cannot be treated as one table-wide merge pool. Expected compaction
output must respect every boundary across which DuckLake will not combine
files, including partition identity and values and schema version.

A table-wide estimate can otherwise claim that many files will disappear while
the native operation correctly produces one output for each compatible group.
DuckBasin caught this with real tests covering partitioned tables and schema
evolution.

### Count only live work

- Candidate discovery must use active data and delete files.
- Delete files attached to inactive data files are not rewrite input. They are a
  separate cleanup or consistency signal.
- Rewrite pressure is measured per live data file using deleted rows relative
  to its original row count.
- A candidate is useful only when the expected result improves physical
  health. Merely finding small files is insufficient if no file can be
  eliminated.

### Order treatments to avoid repeated work

When the same table needs a delete rewrite and a merge, rewriting first avoids
copying rows that are already logically dead and then rewriting the merged
output again.

When accumulated inlined rows need flushing, that flush blocks a merge of the
same table. The resulting Parquet file can then participate in the next merge
diagnosis rather than being left as immediate new merge debt.

An oversized rewrite that cannot be admitted must remain visible, but it must
not prevent independent, safe merge work from proceeding.

### Flush accumulated inlined data proportionally

DuckLake's effective `data_inlining_row_limit` controls whether one write is
stored inline. LakeDucktor resolves that persisted option with table, schema,
global, then native-default precedence and diagnoses flush pressure when:

```text
active inlined rows >= max(1, data_inlining_row_limit × 5)
```

The multiplier is LakeDucktor admission policy, not lake state. A zero limit
therefore flushes any rows left from an earlier policy. `auto_compact=false`
remains authoritative. Active inline rows and their approximate serialized
bytes are rediscovered on every inventory; no flush watermark or task record
is persisted by LakeDucktor.

### Drain to verified health

A successful bounded call does not mean the table is healthy. It may have
processed only part of the eligible file groups, and its outputs may become
inputs to a later size class.

- Productive work keeps the table eligible for immediate reinspection.
- Normal write bursts may be quieted; an already-started drain should continue
  without repeatedly paying the normal cooldown.
- Completion means a new inspection found no actionable debt, not merely that
  a native call returned successfully.
- A no-op and a failure must not accidentally clear known debt.

This behavior was added to DuckBasin after bounded compaction initially stopped
after one productive pass.

### Coordination must recover

- Concurrent treatment of the same table must be prevented.
- Independent tables may be maintained concurrently only when the catalogue
  and native operations support it.
- Failure to acquire ownership is a waiting state, not a reason for a worker to
  exit permanently.
- Ownership must be released automatically after process death.
- Notifications that arrive during a scheduling cycle must not be erased by a
  wake/reset race.
- Failed work remains discoverable and receives a retry delay so a persistent
  fault cannot create a tight loop.
- Coordination records need storage-level defaults because not every writer
  necessarily passes through the same application model.

A coarse lake-wide lock gave DuckBasin safe sidecar rollover, while table locks
prevented duplicate local jobs. The durable lesson is the collision scope, not
those particular locking mechanisms. Coarse ownership also limits horizontal
planning and should not be mistaken for a requirement.

### Validate with the real engine

Mocks were useful for decisions and failure paths, but the important
correctness checks required DuckLake itself:

- rows survive repeated movement through file-size classes;
- partitions and schema versions remain isolated correctly;
- bounded calls converge over repeated passes;
- distinct tables can be maintained concurrently;
- delete rewrites remove delete debt without changing query results.

Catalogue inspection SQL also needs tests against the real catalogue database,
not only string or mock assertions.

## Prioritization

Maintenance has two separate decisions:

1. Is this work eligible to run?
2. If eligible, which work creates the most value now?

Conflating them makes deferred work disappear from observations or allows
high-scoring but unsafe work to block the queue.

### Eligibility

DuckBasin found value in combining several pressure clocks:

- a quiet period coalesces a burst of writes;
- a change-count threshold reacts quickly to sustained ingestion;
- a maximum delay eventually services low-volume tables;
- a minimum interval prevents repeated churn;
- a retry deadline backs off failures;
- active draining bypasses the ordinary cooldown.

These are scheduling concerns, not lake policy. A forced wake may skip waiting
windows, but should never bypass DuckLake exclusions or resource safety.

### Benefit

Small-file pressure is better represented by expected improvement than by raw
file count. DuckBasin ranked merge candidates using two signals:

- expected files eliminated; and
- severity of undersizing relative to the desired output size.

The undersize contribution was capped so pathological tiny files could not
produce unbounded scores. Output estimates were calculated per compatible
partition and schema group.

The exact formula is not a contract. The reusable principle is to rank expected
benefit using information that the native operation will actually honor.

Recent writer activity is a small negative merge signal. LakeDucktor counts
active data files created by insertion snapshots during the previous minute,
then subtracts one expected file elimination per four recent files, capped at
eight. This lets similarly valuable quiet tables go first without making a
continuously written table ineligible or allowing the penalty to grow without
bound. Delete rewrites are unaffected because leaving delete-heavy data in
place has a different correctness and cost profile.

Delete rewrites have a different benefit model: live input bytes describe
cost, while eligible delete files, deleted rows, and deleted fraction describe
benefit. Different treatments should not be forced into a misleading common
unit merely to share a queue.

### Fairness and explainability

- Dirty age must be retained even when newer events are coalesced.
- Age provides starvation protection and should be observable independently of
  priority.
- Deterministic tie-breaking makes decisions reproducible.
- Every candidate should explain why it is healthy, waiting, runnable,
  deferred, running, or failed.
- Deferred debt remains in backlog totals even when runnable work is zero.

DuckBasin used age only as a tie-break after its benefit score. That does not
prove starvation freedom when high-pressure work arrives continuously.
LakeDucktor should treat long-term fairness as an explicit property rather than
assuming a scoring heuristic provides it.

### Select one treatment

Each worker has a fixed `DUCKDB_THREADS` and `DUCKDB_MEMORY` envelope and
selects at most one treatment. Selection scans native-policy scheduled-file
cleanup, then the ranked delete-rewrite lane, the inline-flush lane, and the
ranked runnable-merge lane. It does not manufacture a score that compares
unlike treatments.

Scheduled-file cleanup delegates eligibility to DuckLake and needs no DuckDB
memory admission. A delete rewrite is admitted only when the complete active
table footprint fits the memory envelope. An inline flush is admitted only
when its current serialized inline input fits. A merge is admitted when at
least one target-sized output fits. Its native `max_compacted_files` bound is
derived from the number of target-sized outputs that fit, capped by the
diagnosed output count. Oversized work remains reported as memory-deferred and
does not block an independent treatment.

Admission reads the table's current active sort configuration from
`ducklake_sort_info`. This is refreshed after claiming because DuckLake applies
the sort order active when compaction runs, rather than the order used when the
input files were written.

The output budget retains explicit memory headroom:

- unsorted treatments reserve 25% of `DUCKDB_MEMORY`;
- sorted treatments reserve 50%; and
- every treatment reserves at least 125 MB per configured DuckDB thread.

The largest applicable reserve wins. The remaining memory determines merge
batch size and whether a full-table delete rewrite fits. Sorting state,
headroom, usable memory, and the resulting native bound are logged with the
selection and refreshed treatment.

`lakeducktor select` performs this decision without claiming or changing lake
state. Thread count is part of the eventual execution envelope; it does not
alter treatment priority.

### Treat one selection

`lakeducktor maintain` performs at most one native treatment. PostgreSQL-backed
lakes use a non-blocking session advisory lock scoped to the metadata schema.
DuckLake metadata commits share one snapshot sequence across a lake, so only
one LakeDucktor treatment may commit to a lake at a time; independent lakes can
still be treated concurrently. The dedicated PostgreSQL session holds
ownership for the duration of treatment. Explicit release happens in all
normal and failure paths, while connection loss releases the lock after worker
death.

After claiming, LakeDucktor inventories and diagnoses that lake again. It
abandons work that was removed, healed, excluded, or no longer fits, and
refreshes renamed tables by stable ID. It then opens a fresh writable DuckDB
context with the configured memory and threads:

- merges call DuckLake's table-scoped `merge_adjacent_files` with a
  session-only execution target and output-group count selected to keep the
  estimated treatment at no more than 512 input files and within usable
  memory;
- delete rewrites call DuckLake's table-scoped `rewrite_data_files` without
  overriding the lake's effective threshold;
- inline flushes call DuckLake's table-scoped `ducklake_flush_inlined_data` and
  report the returned flushed-row count and the observed active-file increase
  around the flush.

DuckLake chooses the current eligible files and owns the metadata and object
storage changes. LakeDucktor records the returned processed/created file
counts, diagnoses the table once more, and releases the claim. A successful
bounded treatment may remain actionable for the next invocation.

### Clean up scheduled files

Scheduled-file cleanup is a lake-scoped lifecycle, not a table treatment.
Compaction and snapshot expiry deliberately schedule obsolete objects instead
of deleting them immediately because existing readers may still need them.

Inventory calls `ducklake_cleanup_old_files(..., dry_run => true)` through a
read-only DuckLake attachment, without supplying `older_than`. DuckLake
therefore resolves its persisted global `delete_older_than` option or its
native default. The current pinned extension defaults to two days. The
persisted `expire_older_than` option is also recorded for visibility, but it
governs the separate snapshot-expiration lifecycle.

When the dry run returns files, LakeDucktor creates one lake-scoped cleanup
candidate. Treatment acquires the same lake-wide claim as other maintenance,
repeats native dry-run diagnosis during revalidation, and calls:

```sql
CALL ducklake_cleanup_old_files('lakeducktor_treatment');
```

No explicit `older_than` and no `cleanup_all` are supplied. DuckLake owns
policy resolution, object deletion, and schedule-row removal. LakeDucktor
reports deleted files, current eligible files, and the stored policy source.
Because the native function has no file-count bound, health and stuck-treatment
signals remain important for unusually large or slow object-store cleanups.

Orphan cleanup is a separate, more dangerous operation. Untracked objects are
not equivalent to files DuckLake explicitly scheduled for deletion and must
not share this treatment lane.

Merge treatment is incremental. LakeDucktor never mutates the lake's persisted
`target_file_size`; when the smallest eligible inputs would make one native
call too broad, it lowers DuckDB's target for that treatment session only. The
temporary target is capped by the lake target and available memory. This
creates bounded intermediate groups that remain eligible for later passes
until the lake's own target is reached.

Long treatments run in an isolated child process with its own DuckDB
connection. They emit start and completion or classified failure events.
Running state, start time, elapsed time, and outcomes are meaningful
operational signals; DuckLake does not expose a reliable percentage complete,
so LakeDucktor does not manufacture one. PostgreSQL session ownership needs no
application heartbeat.

PostgreSQL coordination uses the optional dependency:

```sh
uv sync --extra postgres
uv run lakeducktor maintain
```

## Performance and resource safety

### Understand what a native bound really bounds

DuckLake's merge limit bounds output compaction groups, not input files.
LakeDucktor therefore estimates inputs per group from the smallest eligible
file, derives a session-only target, and admits as many groups as fit both the
512-input guard and usable memory. Consequently:

- the guard is an estimate from inventory, not a native hard input limit;
- the likely output working set and sorting headroom still matter;
- a productive pass normally needs several calls to drain the same table.

DuckBasin eventually sized a maintenance execution context from the largest
possible bounded output batch rather than from the apparent file-count limit.
This fixed a case where the overall pod had spare memory but an individual
DuckDB context could still run out of memory.

Delete rewriting was harder: the native operation did not expose a comparable
batch limit and could include adjacent files. DuckBasin conservatively admitted
automatic rewrites only when the complete active table footprint fit within one
execution context. This is safe but can defer large tables indefinitely; it is
a known limitation, not a complete solution.

### Budget every execution context

- Keep headroom outside DuckDB for the process, extensions, catalogue clients,
  and transient allocations.
- Give every concurrent DuckDB context its own enforceable memory limit.
- Derive concurrency from the resource envelope left after non-maintenance
  workloads, not from CPU count alone.
- Do not start additional work when observed CPU or memory pressure is already
  high.
- Re-evaluate pressure between admissions rather than launching the entire
  queue at once.
- Report resource-blocked work as debt, not health.

Static limits and live admission solve different problems. Static per-context
limits contain a single operation; live process or container observations
prevent aggregate overcommit.

The concrete headroom ratios and memory sizes used by DuckBasin were local
operational choices. They are not portable defaults.

### Allocate parallelism deliberately

DuckBasin exposed two useful dimensions:

- more threads can accelerate one large native operation;
- more independent execution contexts can process distinct tables.

A lone job can use available thread capacity, while concurrent jobs must share
it. One table should not be scheduled twice merely because executor capacity
remains.

Concurrent work should use isolated DuckDB contexts. Sharing or duplicating a
context allowed configuration and extension behavior from CDC workloads to
leak into maintenance. A finite pool both isolates workloads and makes maximum
concurrency explicit. Execution-time settings must be applied at the point of
use because extensions or earlier operations may have changed connection
configuration.

DuckBasin explored both widening a merge batch and increasing DuckDB threads as
ways to spend spare CPU. The repository does not establish a universal winner.
That choice requires workload measurements rather than treating thread count as
an abstract capacity token.

### Keep diagnosis cheaper than treatment

The first inventory query performed a correlated partition-value aggregation
for every active file. It was replaced by:

- materializing the active-file set once;
- preaggregating partition values once; and
- joining the two linear intermediate sets.

Continuous diagnosis can otherwise become the dominant catalogue workload,
especially for the unhealthy tables it is meant to repair. Inspection cost
must be measured and should scale approximately with the metadata actually
examined.

Signals can limit inspections to touched tables, but periodic reconciliation is
still needed for correctness. The two paths should share the same diagnosis
semantics.

### Measure convergence, not activity

Files processed per second can rise while backlog grows faster. The performance
question is whether the service restores and maintains physical health within
an acceptable time and resource envelope.

Useful measurements include:

- actionable and deferred debt in files and bytes;
- age of the oldest debt;
- expected files eliminated;
- files processed and created;
- operation duration and outcomes;
- running and waiting work;
- CPU and memory usage and saturation;
- time since the last successful treatment while debt exists.

Alerts should combine staleness with actual debt. Time since last success alone
is not a failure when the lake is already healthy.

Per-table diagnostics are valuable, but metric labels must remain bounded.
Paths, snapshot IDs, run IDs, SQL, errors, credentials, and other unbounded or
sensitive values belong in logs or bounded status records, not labels. Run
history must also be retained within a fixed bound.

## Evidence limits

The DuckBasin repository contains strong behavioral and integration tests but
no compaction throughput benchmark for a semi-large lake. Its prioritization
formula, resource thresholds, fixed size classes, and concurrency choices are
informed heuristics rather than demonstrated universal optima.

It also relied on PostgreSQL-specific coordination and a CDC trigger. Those
choices provide evidence for recoverable ownership and efficient discovery,
but not for a portable LakeDucktor architecture.

The remaining maintenance operations have additional safety questions:

- snapshot expiration changes available history;
- obsolete-file cleanup must respect active readers and configured age;
- orphan detection must distinguish genuinely untracked objects from recent or
  in-flight writes;
- high-volume concurrent inline flushing still needs broader soak evidence.

Those areas require their own evidence before conclusions from compaction are
generalized to them.
