# Scaling

LakeDucktor is horizontally scale-safe for DuckLakes backed by PostgreSQL.
Identical workers discover work from the lake and coordinate through
recoverable claims, preventing the same maintenance scope from being treated
concurrently. Workers hold no authoritative local state, so replicas may be
added, removed, restarted, or briefly overlapped.

Horizontal scaling increases the number of independent lakes that can be
treated concurrently. Treatments within one lake are serialized because
DuckLake metadata commits share its snapshot sequence. Vertical scaling gives
each treatment more CPU and memory. A single busy lake benefits from an
appropriately sized worker, not additional replicas competing for its claim.

## Scaling signals

LakeDucktor exposes metrics that distinguish demand from capacity:

- actionable, runnable, blocked, and memory-deferred work
- expiring snapshots, scheduled/cleanup-eligible files, orphan files, and
  dangling delete files
- recent insertion-file activity
- accumulated inlined rows and serialized bytes
- running and stuck treatment state
- treatment throughput, duration, outcomes, files eliminated, rows flushed,
  and snapshots expired
- estimated merge/rewrite file debt and claim contention
- worker readiness, liveness, stuck state, and cycle outcomes

Scale out when runnable work remains high despite sustained treatment
throughput. Scale up when work is memory-deferred or individual treatments need
a larger resource envelope. Scale in after runnable work remains low.

Backlog size alone is insufficient: deferred work may require a larger worker
rather than more workers, and a healthy lake may correctly have no recent
treatment.

## Deployment

LakeDucktor does not manage its own replicas or require a controller. Run
`lakeducktor run` in an ordinary Kubernetes Deployment and use the existing
autoscaling stack, exporting LakeDucktor's Prometheus metrics through the
preferred metrics adapter. See [Running](RUNNING.md) for probes and shutdown
behavior.

Scaling down is safe: terminating workers stop admitting new work, and
recoverable claims allow unfinished work to be rediscovered. The DuckLake
catalogue remains the source of truth throughout.

Local DuckDB- or SQLite-backed lakes do not share PostgreSQL's distributed
coordination model and should not be assumed to support horizontal workers.
