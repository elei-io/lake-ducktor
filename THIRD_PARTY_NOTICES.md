# Third-party notices

LakeDucktor's original code is licensed under the [MIT License](LICENSE).
Third-party code retains its own copyright and license terms.

## DuckLake compatibility patch

`vendor/ducklake/0001-external-hive-compaction.patch` adapts upstream DuckLake
compaction changes from [PR #1181](https://github.com/duckdb/ducklake/pull/1181),
commit `9ef79528c09ad598a1913c6bc84b16c885a79059`, for base commit
`d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4`.

Copyright 2018-2025 Stichting DuckDB Foundation.

The full upstream MIT notice is preserved in
[vendor/ducklake/LICENSE](vendor/ducklake/LICENSE). It applies to the upstream
material in the patch and the DuckLake extension built by the Dockerfile.
See [compatibility details](docs/DUCKLAKE_COMPATIBILITY.md) for the build pins
and the reason for the adaptation.

## Dependencies and container distributions

Python dependencies, DuckDB extensions, native libraries, and operating-system
packages retain their respective licenses. This document records the vendored
DuckLake material; it is not a complete license inventory of the container's
transitive dependencies. Review those dependencies and preserve their required
notices before publishing container images.
