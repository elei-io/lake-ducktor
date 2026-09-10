# DuckLake compatibility pin

The container image temporarily carries DuckLake's upstream fix for compacting
Hive-partitioned files registered from external directories with
`ducklake_add_data_files`.

The reproducible build pins:

- DuckDB `v1.5.5`
- DuckLake `d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4`
- CRoaring `v4.5.0`
- the logic from DuckLake
  [PR #1181](https://github.com/duckdb/ducklake/pull/1181), commit
  [`9ef79528`](https://github.com/duckdb/ducklake/commit/9ef79528c09ad598a1913c6bc84b16c885a79059)

The adapted patch is vendored at
[`vendor/ducklake/0001-external-hive-compaction.patch`](../vendor/ducklake/0001-external-hive-compaction.patch).
The bounded-compaction selection patch is vendored at
[`vendor/ducklake/0002-productive-compaction-batches.patch`](../vendor/ducklake/0002-productive-compaction-batches.patch).
It ranks native merge batches by files eliminated before applying
`max_compacted_files`, with oldest input first as a deterministic tie-breaker.
The native partition/schema boundaries, size filters, batch-size construction and
Ducktor memory admission remain unchanged. Only selected batches are bound for
execution. This prevents newly arriving pairs from repeatedly winning over larger
backlogs through hash-map iteration order.

The image builds a self-contained extension against the pinned DuckDB headers.
This avoids expiring CI artifact URLs and mismatched DuckDB extension ABIs.

Locally built DuckDB extensions do not carry DuckDB's release signature. The
container therefore enables `allow_unsigned_extensions` for each of its own
DuckDB connections. Normal host installs remain signature-enforcing by
default.

The pin was verified against Atlas's external, Hive-partitioned files on
2026-07-30. A treatment compacted six registered files into one, advanced the
lake from snapshot 611 to 612, reduced active files from 2,039 to 2,034, and
wrote the result under DuckLake's canonical table/partition path.

## Retiring the compatibility patch

Do not remove the patch merely because PR #1181 is merged into DuckLake's
`main` branch. The extension installed by `INSTALL ducklake` is built for a
specific DuckDB release and can come from a release branch that does not contain
the fix. Retire the patch only when all of these conditions are true:

1. A stable DuckDB release supported by LakeDucktor has a signed `ducklake`
   extension in the `core` repository.
2. The source revision reported by that installed extension contains commit
   `9ef79528c09ad598a1913c6bc84b16c885a79059` or an equivalent upstream fix.
   Confirm this from the DuckLake source history; the existence of the merged
   pull request on another branch is not sufficient.
3. A disposable regression run passes with that signed extension. The fixture
   must register Hive-partitioned Parquet files from a directory outside the
   DuckLake table's `DATA_PATH` using `ducklake_add_data_files`, run
   `ducklake_merge_adjacent_files`, and verify all of the following:
   - the complete row set is unchanged;
   - the number of active files decreases;
   - the new active file is under DuckLake's canonical table and Hive-partition
     path; and
   - compaction does not create a path containing a nested copy of the external
     source path.

The ordinary disposable demo is not sufficient for this decision because it
creates its source files inside DuckLake's managed data path.

The productive-batch patch has an independent retirement gate: the signed extension
must also pass `tests/native_sorted_batches.py`, including the 40-file group versus
new two-file groups and the bounded 600-file regression. Preserve this patch and
the source build until both fixes are present and verified.

After those checks pass:

1. Replace the source-built extension installation in the container with
   `INSTALL ducklake` from the default signed `core` repository.
2. Remove the DuckLake/CRoaring builder stage, the vendored patch, and the
   vendored DuckLake license if no other vendored DuckLake material remains.
3. Remove `DUCKDB_ALLOW_UNSIGNED_EXTENSIONS` from the container. Remove the
   corresponding connection configuration and tests if no other supported
   deployment needs unsigned extensions.
4. Update `README.md`, `SECURITY.md`, `THIRD_PARTY_NOTICES.md`, this document,
   and the recorded demo metadata so none describes the retired custom build.
5. Rebuild the image from a clean checkout and run the full unit, lint, package,
   disposable-demo, and external-Hive regression checks before publishing it.
