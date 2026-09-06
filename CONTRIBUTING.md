# Contributing

LakeDucktor is experimental. Small, focused issues and pull requests are welcome.
Describe the problem, expected behavior, and how the change was verified.

## Development

Install Python 3.14 and uv, then run:

```sh
uv sync --frozen --extra postgres
uv run --frozen pytest tests/unit -q
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv build
```

Unit tests do not need a live lake. The disposable integration example is in
[examples/demo](examples/demo/README.md). Never run stress tests against a
production lake: they create tables and write substantial data.

For maintenance changes, test row preservation and failure behavior, not just
successful return values. Keep native DuckLake mutations separate from
inventory, admission, and coordination. Do not change persisted lake policy
as a shortcut for making maintenance converge.

Include versions, reproduction steps, and sanitized logs in bug reports.
Never include `.env`, credentials, customer data, or private infrastructure
addresses. See [security reporting](SECURITY.md) for suspected vulnerabilities.

Contributions are provided under this repository's MIT license. Preserve
third-party notices when adapting upstream code.

## Historical stress harness

The stress harness requires an existing **disposable** PostgreSQL/S3-backed
DuckLake. Set its connection/storage environment and explicitly set
`SOAK_METADATA_SCHEMA` to that lake's metadata schema. It no longer has a
personal catalog identifier as a default. Inspect available commands with:

```sh
uv run --frozen python tests/stress/concurrent_soak.py --help
```

Keep raw observations, validation output, image/extension versions, hardware,
and the tested Git commit together when publishing results. The small demo is
the supported self-contained starting point; the larger harness still requires
manual fixture provisioning.
