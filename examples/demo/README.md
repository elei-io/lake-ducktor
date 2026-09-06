# Disposable demo

Requires Docker with Compose and internet access for the first build. The build
compiles the pinned DuckLake extension and can take several minutes. No existing
lake, host credentials, or external Docker network is used. PostgreSQL is not
published on a host port; the sample password is for this isolated example only.

From the repository root:

```sh
docker compose -f examples/demo/compose.yml run --build --rm demo
```

The demo creates 4,000 synthetic rows in 40 batches, runs the real `inventory`,
`select`, and `maintain` commands, and asserts exact before/after row equality
and a reduction in active file count. It prints a JSON result on success.
The lake and database use dedicated Docker volumes. The worker runs as the
image's non-root user. A short Alpine setup container assigns ownership of only
the demo's named lake volume before the worker starts.

Remove this demo's data after the run (also required before repeating it):

```sh
docker compose -f examples/demo/compose.yml down --volumes
```

This is a compaction smoke test, not a performance benchmark or a test of
retention and orphan deletion. The demo explicitly disables orphan cleanup.

Example result from the local verification run:

```json
{
  "rows_before": 4000,
  "rows_after": 4000,
  "files_before": 40,
  "files_after": 1,
  "exact_row_match": true
}
```
