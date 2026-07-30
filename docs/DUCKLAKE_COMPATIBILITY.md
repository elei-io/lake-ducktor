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

Remove the patch and unsigned-extension exception once a stable DuckLake build
for the selected DuckDB release contains the upstream regression fix.
