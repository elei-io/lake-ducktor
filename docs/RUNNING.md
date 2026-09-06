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
The example configuration sets `ORPHAN_CLEANUP_ENABLED=false`. Keep it disabled when the configured DuckLake data path shares
its root with objects owned by another application. This skips both orphan
detection and deletion while leaving all other maintenance enabled.

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

Build the LakeDucktor image and run a read-only inventory smoke test with the
filesystem Compose override:

```sh
docker compose -f compose.yml -f compose.filesystem.yml build
docker compose -f compose.yml -f compose.filesystem.yml run --rm lakeducktor inventory
```

The image currently includes a source-pinned DuckLake compatibility backport;
see [DuckLake compatibility pin](DUCKLAKE_COMPATIBILITY.md).

Configure the external metadata network and shared data path in `.env` using
values from your own deployment. The worker defaults to UID/GID 10001; grant
that identity access to the mounted lake directory, or configure an appropriate
non-root identity for your environment.

The producer and LakeDucktor must mount the same host data. Preserve the
producer's container path when the catalogue contains absolute registered file
paths. Do not mount the producer's private PostgreSQL data volume; LakeDucktor
connects to PostgreSQL over the configured Docker network.

The image and Compose service default to `lakeducktor run`. Use explicit
one-shot commands for smoke tests, and start the worker with:

```sh
docker compose -f compose.yml -f compose.filesystem.yml up lakeducktor
```

## Alluxio S3 proxy

LakeDucktor can maintain a lake through the same Alluxio S3 proxy used by its
writers. It must connect to both the existing DuckLake metadata database and
the S3 proxy; access to Alluxio or its under-store alone is not enough to
identify the lake.

Join the network shared by the metadata database and S3 proxy. Configure
`METADATA_DATABASE_*` for the existing PostgreSQL database and
`CATALOG_STORAGE_*` for the proxy's endpoint, bucket, and credentials. Set
`MAINTAIN_LAKES` to the intended schema and `LAKEDUCKTOR_DOCKER_NETWORK` to that
network's name. See `.env.example` for the complete variable names.

No lake volume is mounted for S3-compatible storage. Run `inventory` first to
verify the attachment without mutating the lake, then start the worker:

```sh
docker compose run --rm lakeducktor inventory
docker compose up lakeducktor
```

All maintenance reads, compaction outputs, and file deletions use that proxy.
Consequently, Alluxio's write type, replication, persistence, and under-store
settings remain the storage authority for LakeDucktor operations too.

## First-run scope and policy

Use `inventory` and `select` before `maintain` or `run`. Explicitly set
`MAINTAIN_LAKES` or `METADATA_DATABASE_SCHEMA`. Without either, `maintain` and
`run` require the explicit opt-in `MAINTAIN_ALL_LAKES=true`. Read-only discovery
can still inspect all schemas. Orphan cleanup
is disabled by default. Enable it explicitly only after reviewing the storage
root. Existing deployments that need orphan cleanup must now set
`ORPHAN_CLEANUP_ENABLED=true`.
The setting applies to one-shot commands as well as the service.

Disabling orphan cleanup does not disable snapshot expiration or deletion of
scheduled obsolete files. Those operations follow persisted/native DuckLake
policy. Inspect that policy before enabling maintenance.
