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

A pod performs one treatment at a time. DuckDB connections stay local to the
main worker thread and are never shared with the health server, watchdog, or
another treatment.

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
finishes the current native operation, releases its claim, and exits. Set the
pod termination grace period long enough for expected treatments.

Example probes:

```yaml
livenessProbe:
  httpGet: {path: /livez, port: 8000}
readinessProbe:
  httpGet: {path: /readyz, port: 8000}
```

Operational settings are documented in [`.env.example`](../.env.example).
