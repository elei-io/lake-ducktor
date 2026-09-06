# LakeDucktor

Automatic physical maintenance for [DuckLake](https://ducklake.select/).
LakeDucktor discovers maintenance debt, admits bounded work, and invokes native
DuckLake operations using the lake's persisted settings.

**Status: experimental.** Validate against disposable data before using it on
important lakes. Snapshot expiration and file cleanup can delete data according
to native DuckLake policy. See [operating boundaries](SECURITY.md).

## What it does

- Flushes inlined data, merges small files, and rewrites delete-heavy files.
- Expires snapshots and cleans eligible obsolete or orphaned files.
- Coordinates PostgreSQL-backed workers with lake-scoped advisory claims.
- Exposes readiness, liveness, and Prometheus maintenance metrics.

It does not provision lakes, proxy queries, manage users, or define retention
policy. Work is derived from lake state; there is no authoritative local queue.

## Try it

Run the [disposable demo](examples/demo/README.md) with Docker Compose:

```sh
docker compose -f examples/demo/compose.yml run --build --rm demo
```

It seeds synthetic data, invokes maintenance, and verifies that active files
are reduced while every row remains unchanged. The first build compiles a
pinned native extension. Cleanup instructions are in the demo guide.

## Run against an existing lake

Requires Python 3.14 and uv:

```sh
uv sync --frozen --extra postgres
cp .env.example .env
# Edit .env: choose the lake, storage path, credentials, and resource budget.
uv run --frozen lakeducktor inventory
uv run --frozen lakeducktor select
# Only after reviewing the selected lake and its retention/deletion policy:
uv run --frozen lakeducktor maintain
# For continuous maintenance:
uv run --frozen lakeducktor run
```

`inventory`, `diagnose`, `prioritize`, and `select` do not execute maintenance.
`maintain` executes at most one treatment; `run` continues until stopped.
Read-only commands can discover all lakes. Mutating commands require
`MAINTAIN_LAKES`, `METADATA_DATABASE_SCHEMA`, or an explicit
`MAINTAIN_ALL_LAKES=true`. Orphan cleanup is disabled by default; keep it disabled
for shared data roots.

## Supported setup

| Area | Current implementation |
| --- | --- |
| Metadata | PostgreSQL; SQLite/DuckDB capability classes are not supported attachment paths |
| Storage | Local filesystem or S3-compatible endpoints |
| Replicas | PostgreSQL claims serialize each lake; separate lakes can run concurrently |
| Python / DuckDB | Python 3.14+, DuckDB 1.5.5 pinned in the lockfile |
| Container | Builds a pinned, patched DuckLake extension; unsigned loading is enabled |
| Host install | Uses available signed extensions; does not automatically include the container patch |

See [compatibility details](docs/DUCKLAKE_COMPATIBILITY.md) before using external
Hive-partitioned registrations. Storage support is not a certification of every
S3 gateway or catalog/extension combination.

## Design and evidence

- [Architecture and tradeoffs](docs/ARCHITECTURE.md)
- [Vision and scope](docs/VISION.md)
- [Running and configuration](docs/RUNNING.md) · [Scaling](docs/SCALING.md)
- [Historical concurrency retest](docs/CONCURRENCY_RETEST_2026-07-30.md)
- [Contributing and local checks](CONTRIBUTING.md)
- [Release readiness and remaining work](docs/RELEASE_READINESS.md)

## License

[MIT](LICENSE), copyright (c) 2026 elei.io.
See [third-party notices](THIRD_PARTY_NOTICES.md) for vendored material.
