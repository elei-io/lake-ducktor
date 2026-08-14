# Running

Run one long-lived worker per pod:

```sh
uv run lakeducktor run
```

Each cycle discovers the selected lakes afresh, diagnoses and ranks their
current state, claims one fitting treatment, revalidates it, and invokes the
native DuckLake operation. Useful work drains immediately. No work, contention,
resource deferral, native no-progress, and failures wait
`POLL_INTERVAL_SECONDS` before retrying.

Merge debt on an actively written table waits until a compatible group has 32
candidate files or one target file's worth of candidate bytes. The debt
remains observable while waiting. After one minute without inserted files,
productive merge tails drain without the active-writer threshold.
Once the table-level gate opens, admission includes all productive compatible
groups within the normal 512-input and memory bounds because DuckLake chooses
groups at table scope.
For a sorted table, admission is intentionally stricter: one compacted output
per call and an eight-times compressed-input memory allowance.

Scheduled-file eligibility is obtained from DuckLake's own read-only cleanup
dry run. The worker does not override `delete_older_than`; a cleanup treatment
uses the lake's persisted policy or the extension's native default.

Snapshot-expiration eligibility is obtained from DuckLake's native dry run
without overriding `expire_older_than`. Orphan eligibility requires a storage
walk, so the worker performs it at startup and then every
`ORPHAN_SCAN_INTERVAL_SECONDS` instead of every poll. Orphan deletion likewise
uses DuckLake's stored/default `delete_older_than`. A storage race or transient
file disappearance can fail the diagnostic walk; that failure disables only
orphan cleanup until the next scan interval. Other maintenance continues, and
`lakeducktor_orphan_probe_failures_total` reports the degraded housekeeping.

A pod performs one treatment at a time. Each treatment runs in an isolated
child process and creates its own DuckDB connection. Connections are never
shared with the parent loop, health server, watchdog, or another treatment.

Classified concurrent-compaction and snapshot-commit conflicts are retried with
exponential backoff bounded by `CONFLICT_BACKOFF_BASE_SECONDS` and
`CONFLICT_BACKOFF_MAX_SECONDS`. After any failed native call, LakeDucktor
re-inventories under the still-held claim and reports observed committed
progress before deciding the next cycle.

Each treatment connection also gives DuckLake up to 20 snapshot-commit
attempts, beginning at a 100 ms wait with a 1.2× backoff. This absorbs short
writer bursts before LakeDucktor's slower outer retry takes over.

## Health

The server listens on `METRICS_HOST:METRICS_PORT` and exposes:

- `/livez` — the process watchdog is responding
- `/readyz` — the worker is accepting work and making timely progress
- `/metrics` — Prometheus metrics

Readiness returns `503` after a failed cycle and remains failed throughout
subsequent attempts until one completes successfully. Inventory cycles are
considered stuck after the smaller of `TREATMENT_STUCK_AFTER_SECONDS` and a
poll-derived bound of at least 60 seconds. Treatments retain the configured
threshold. The worker is also unready when the loop fails to wake after its
idle deadline. A non-transient treatment failure with verified zero progress
blocks that table for the worker lifetime and also keeps readiness at `503`; it
is never retried automatically. An overdue treatment remains live so
Kubernetes does not repeatedly kill potentially useful work; alert on failed
readiness and the stuck metrics.

On `SIGTERM` or `SIGINT`, the worker becomes unready, admits no new treatment,
terminates an active isolated treatment, re-inventories any committed progress,
releases its claim, and exits. A hard process or pod kill is also recoverable
for PostgreSQL-backed lakes because PostgreSQL releases the session advisory
lock when the connection disappears.

Example probes:

```yaml
livenessProbe:
  httpGet: {path: /livez, port: 8000}
readinessProbe:
  httpGet: {path: /readyz, port: 8000}
```

Operational settings are documented in [`.env.example`](../.env.example).

## Local filesystem lakes

For a filesystem-backed lake, expose its data directory to the LakeDucktor
process and configure an absolute path:

```sh
CATALOG_STORAGE=filesystem
CATALOG_DATA_PATH=/absolute/path/to/lake/
```

LakeDucktor attaches with that path as a connection-local DuckLake data-path
override. This lets a producer use a different container path for the same
bind-mounted directory while LakeDucktor runs on the host. The catalogue's
schema, table, and active file paths must be relative to the DuckLake data
root; absolute file registrations cannot be relocated this way.

Build the LakeDucktor image and run a read-only inventory smoke test:

```sh
docker compose build
docker compose run --rm lakeducktor inventory
```

The image currently includes a source-pinned DuckLake compatibility backport;
see [DuckLake compatibility pin](DUCKLAKE_COMPATIBILITY.md).

The Compose project is LakeDucktor and is not tied to a particular producer.
Configure its metadata network and shared lake path in `.env`. For example, a
local Atlas deployment that stores its state in `/Users/me/Code/atlas/.atlas`
and uses the `atlas_default` network needs:

```sh
CONTAINER_METADATA_DATABASE_HOST=atlas-test-postgres
CONTAINER_METADATA_DATABASE_PORT=5432
CONTAINER_CATALOG_DATA_PATH=/app/.atlas/lake/
LAKE_HOST_PATH=/Users/me/Code/atlas/.atlas
LAKE_CONTAINER_PATH=/app/.atlas
LAKEDUCKTOR_DOCKER_NETWORK=atlas_default
```

The producer and LakeDucktor must mount the same host data. Preserve the
producer's container path when the catalogue contains absolute registered file
paths. Do not mount the producer's private PostgreSQL data volume; LakeDucktor
connects to PostgreSQL over the configured Docker network.

The image and Compose service default to `lakeducktor run`. Use explicit
one-shot commands for smoke tests, and start the worker with:

```sh
docker compose up lakeducktor
```
