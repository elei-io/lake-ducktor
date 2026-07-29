# Scaling

LakeDucktor is horizontally scale-safe for DuckLakes backed by PostgreSQL.
Identical workers discover work from the lake and coordinate through
recoverable claims, preventing the same maintenance scope from being treated
concurrently. Workers hold no authoritative local state, so replicas may be
added, removed, restarted, or briefly overlapped.

Horizontal scaling increases the number of independent tables or lakes that can
be treated concurrently. Vertical scaling gives each treatment more CPU and
memory. A single large maintenance operation may benefit more from a larger
worker than from additional replicas.

## Scaling signals

LakeDucktor exposes metrics that distinguish demand from capacity:

- actionable and deferred maintenance debt
- age of the oldest debt
- running and waiting work
- treatment throughput, duration, and outcomes
- CPU and memory usage and saturation
- time since successful treatment while debt remains

Scale out when runnable work and debt age grow despite sustained worker
utilization. Scale up when individual treatments are resource-bound or cannot
be admitted safely. Scale in only after runnable work and debt age remain low.

Backlog size alone is insufficient: deferred work may require a larger worker
rather than more workers, and a healthy lake may correctly have no recent
treatment.

## Deployment

LakeDucktor does not manage its own replicas or require a controller. Operators
can use their existing Kubernetes Deployment and autoscaling stack, exporting
LakeDucktor's Prometheus metrics through their preferred metrics adapter.

Scaling down is safe: terminating workers stop admitting new work, and
recoverable claims allow unfinished work to be rediscovered. The DuckLake
catalogue remains the source of truth throughout.

Local DuckDB- or SQLite-backed lakes do not share PostgreSQL's distributed
coordination model and should not be assumed to support horizontal workers.
