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

A pod performs one treatment at a time. Each treatment runs in an isolated
child process and creates its own DuckDB connection. Connections are never
shared with the parent loop, health server, watchdog, or another treatment.

Classified concurrent-compaction and snapshot-commit conflicts are retried with
exponential backoff bounded by `CONFLICT_BACKOFF_BASE_SECONDS` and
`CONFLICT_BACKOFF_MAX_SECONDS`. After any failed native call, LakeDucktor
re-inventories under the still-held claim and reports observed committed
progress before deciding the next cycle.

## Health

The server listens on `METRICS_HOST:METRICS_PORT` and exposes:

- `/livez` — the process watchdog is responding
- `/readyz` — the worker is accepting work and making timely progress
- `/metrics` — Prometheus metrics

Readiness returns `503` after a failed cycle, when a cycle or treatment runs
longer than `TREATMENT_STUCK_AFTER_SECONDS`, or when the loop fails to wake
after its idle deadline. An overdue treatment remains live so Kubernetes does
not repeatedly kill potentially useful work; alert on failed readiness and the
stuck metrics.

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
