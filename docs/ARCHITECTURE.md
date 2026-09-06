# Architecture and tradeoffs

LakeDucktor is a stateless maintenance worker for an existing DuckLake. It reads
current lake state and persisted policy, then delegates mutations to native
DuckLake operations.

```text
configuration → discover → inventory → diagnose → prioritize → admit
                                                            ↓
                                                claim → revalidate
                                                            ↓
                                                execute → verify
                                                            ↓
                                                      release → repeat
```

## Responsibilities

| Module | Responsibility |
| --- | --- |
| `lake.py`, `inventory.py` | Discover catalogs and collect physical state |
| `diagnosis.py`, `priority.py` | Identify and rank maintenance needs |
| `resources.py`, `selection.py` | Admit work within estimated resource limits |
| `coordination.py` | Hold PostgreSQL session advisory claims |
| `executor.py` | Invoke native maintenance in an isolated process |
| `daemon.py`, `telemetry.py` | Run the loop, recover, and expose progress |

## Why lake-scoped claims?

Tables in a DuckLake share the catalog snapshot sequence. Serializing maintenance
within a lake avoids maintenance workers competing for those commits. Replicas
parallelize different lakes; more workers do not accelerate one lake. Writers
still operate concurrently, so native conflict handling remains necessary.

## Why isolate treatments?

Each treatment owns a child process and DuckDB connection. Health serving and
shutdown control remain separate from the native call. PostgreSQL session
claims are disposable; a lost connection releases its advisory lock. No durable
work queue needs recovery because work is derived from the lake again.

## Why verify after failure?

A native operation can commit progress before reporting an error. The worker
re-inventories while holding its claim rather than interpreting an exception
as proof that nothing changed. The concurrency reports document the experiments
that motivated this behavior.

## Limits of admission

File counts and compressed input sizes inform admission, with extra headroom
for sorted tables. These estimates are not a hard process-wide memory or I/O
limit. Deployments still need container resource limits and monitoring.

## Evidence and limits

[Concurrency retest](CONCURRENCY_RETEST_2026-07-30.md) records historical tests
of concurrent inserts, crash recovery, and convergence. Those results are not
fresh validation of every later commit. The small demo verifies exact row
preservation and file reduction; it does not cover updates, deletes, schema
changes, object-store faults, or long-running production workloads.
