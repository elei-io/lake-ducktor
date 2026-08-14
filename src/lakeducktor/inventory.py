"""Read-only inventory of physical DuckLake catalog state."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from time import monotonic
from typing import Protocol

import duckdb

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.duckdb_config import connection_config
from lakeducktor.model import (
    BackendDetection,
    CatalogInventory,
    CompatibleFileGroup,
    FileSizeDistribution,
    LakeInventory,
    MetadataBackend,
    TableInventory,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)

_INVENTORY_ALIAS = "lakeducktor_inventory"
_CLEANUP_PROBE_ALIAS = "lakeducktor_cleanup_probe"
_EXPIRATION_PROBE_ALIAS = "lakeducktor_expiration_probe"
_ORPHAN_PROBE_ALIAS = "lakeducktor_orphan_probe"
_PROBE_STORAGE_SECRET = "lakeducktor_probe_storage"

_LOGGER = logging.getLogger("lakeducktor")

type LakeSummaryRow = tuple[
    int | None,
    int | None,
    int,
    int | None,
    str | None,
    str | None,
]
type TableInventoryRow = tuple[
    int,
    str,
    str,
    bool,
    int,
    float,
    bool,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
]
type CompatibleFileGroupRow = tuple[
    int,
    int | None,
    int | None,
    int,
    int,
    int,
    int,
]
type InlinedDataRow = tuple[int, int, int]


class InventoryError(RuntimeError):
    """The catalog could not be inventoried safely."""


class InventorySource(Protocol):
    """Minimal query boundary used by the pure inventory collector."""

    def lake_summary(self, metadata_schema: str) -> LakeSummaryRow:
        """Return snapshot and scheduled-deletion facts for one lake."""

    def tables(self, metadata_schema: str) -> Iterable[TableInventoryRow]:
        """Return current physical facts for every active table."""

    def compatible_file_groups(
        self,
        metadata_schema: str,
    ) -> Iterable[CompatibleFileGroupRow]:
        """Return native merge compatibility groups for active data files."""

    def inlined_data(self, metadata_schema: str) -> Iterable[InlinedDataRow]:
        """Return active inlined rows and their serialized size by table."""


def _identifier(value: str) -> str:
    return f'"{value.replace('"', '""')}"'


def _utc_from_milliseconds(value: int | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1_000, tz=UTC)


class DuckDBInventorySource:
    """Query a directly attached DuckLake metadata database through DuckDB."""

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        catalog_alias: str,
    ) -> None:
        self._connection = connection
        self._catalog_alias = _identifier(catalog_alias)

    def _relation(self, metadata_schema: str, table: str) -> str:
        return (
            f"{self._catalog_alias}.{_identifier(metadata_schema)}.{_identifier(table)}"
        )

    def lake_summary(self, metadata_schema: str) -> LakeSummaryRow:
        snapshots = self._relation(metadata_schema, "ducklake_snapshot")
        metadata = self._relation(metadata_schema, "ducklake_metadata")
        scheduled = self._relation(
            metadata_schema,
            "ducklake_files_scheduled_for_deletion",
        )
        row = self._connection.execute(
            f"""
            SELECT
                max(snapshot_id),
                epoch_ms(max(snapshot_time)),
                (SELECT count(*) FROM {scheduled}),
                (SELECT epoch_ms(min(schedule_start)) FROM {scheduled}),
                (
                    SELECT value
                    FROM {metadata}
                    WHERE key = 'delete_older_than'
                      AND scope IS NULL
                ),
                (
                    SELECT value
                    FROM {metadata}
                    WHERE key = 'expire_older_than'
                      AND scope IS NULL
                )
            FROM {snapshots}
            """
        ).fetchone()
        if row is None:
            raise InventoryError(
                f"metadata schema returned no inventory summary: {metadata_schema}"
            )
        return row

    def tables(self, metadata_schema: str) -> Iterable[TableInventoryRow]:
        tables = self._relation(metadata_schema, "ducklake_table")
        schemas = self._relation(metadata_schema, "ducklake_schema")
        metadata = self._relation(metadata_schema, "ducklake_metadata")
        data_files = self._relation(metadata_schema, "ducklake_data_file")
        delete_files = self._relation(metadata_schema, "ducklake_delete_file")
        sort_info = self._relation(metadata_schema, "ducklake_sort_info")
        snapshots = self._relation(metadata_schema, "ducklake_snapshot")
        snapshot_changes = self._relation(
            metadata_schema,
            "ducklake_snapshot_changes",
        )
        return self._connection.execute(
            f"""
            WITH active_tables AS (
                SELECT
                    tables.table_id,
                    schemas.schema_id,
                    schemas.schema_name,
                    tables.table_name,
                    coalesce(
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'auto_compact'
                              AND scope = 'table'
                              AND scope_id = tables.table_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'auto_compact'
                              AND scope = 'schema'
                              AND scope_id = schemas.schema_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'auto_compact'
                              AND scope IS NULL
                        ),
                        'true'
                    )::BOOLEAN AS auto_compact,
                    coalesce(
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'target_file_size'
                              AND scope = 'table'
                              AND scope_id = tables.table_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'target_file_size'
                              AND scope = 'schema'
                              AND scope_id = schemas.schema_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'target_file_size'
                              AND scope IS NULL
                        ),
                        '536870912'
                    )::BIGINT AS target_file_size_bytes,
                    coalesce(
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'rewrite_delete_threshold'
                              AND scope = 'table'
                              AND scope_id = tables.table_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'rewrite_delete_threshold'
                              AND scope = 'schema'
                              AND scope_id = schemas.schema_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'rewrite_delete_threshold'
                              AND scope IS NULL
                        ),
                        '0.95'
                    )::DOUBLE AS rewrite_delete_threshold
                    ,
                    coalesce(
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'data_inlining_row_limit'
                              AND scope = 'table'
                              AND scope_id = tables.table_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'data_inlining_row_limit'
                              AND scope = 'schema'
                              AND scope_id = schemas.schema_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'data_inlining_row_limit'
                              AND scope IS NULL
                        ),
                        '10'
                    )::BIGINT AS data_inlining_row_limit
                FROM {tables} AS tables
                JOIN {schemas} AS schemas
                  ON schemas.schema_id = tables.schema_id
                 AND schemas.end_snapshot IS NULL
                WHERE tables.end_snapshot IS NULL
            ),
            active_sorts AS (
                SELECT DISTINCT table_id
                FROM {sort_info}
                WHERE end_snapshot IS NULL
            ),
            active_data_files AS MATERIALIZED (
                SELECT
                    data_file_id,
                    table_id,
                    begin_snapshot,
                    file_size_bytes,
                    record_count
                FROM {data_files}
                WHERE end_snapshot IS NULL
            ),
            data_file_summary AS (
                SELECT
                    table_id,
                    count(*) AS file_count,
                    coalesce(sum(file_size_bytes), 0) AS file_bytes,
                    coalesce(sum(record_count), 0) AS row_count,
                    coalesce(min(file_size_bytes), 0) AS minimum_bytes,
                    coalesce(
                        cast(approx_quantile(file_size_bytes, 0.5) AS BIGINT),
                        0
                    ) AS median_bytes,
                    coalesce(
                        cast(approx_quantile(file_size_bytes, 0.9) AS BIGINT),
                        0
                    ) AS p90_bytes,
                    coalesce(max(file_size_bytes), 0) AS maximum_bytes
                FROM active_data_files
                GROUP BY table_id
            ),
            recent_insert_snapshots AS MATERIALIZED (
                SELECT
                    snapshots.snapshot_id,
                    changes.changes_made
                FROM {snapshots} AS snapshots
                JOIN {snapshot_changes} AS changes USING (snapshot_id)
                WHERE snapshots.snapshot_time
                    >= current_timestamp - INTERVAL '1 minute'
            ),
            recent_data_file_summary AS (
                SELECT
                    data.table_id,
                    count(*) AS file_count
                FROM active_data_files AS data
                JOIN recent_insert_snapshots AS snapshots
                  ON snapshots.snapshot_id = data.begin_snapshot
                WHERE list_contains(
                    string_split(snapshots.changes_made, ','),
                    concat('inserted_into_table:', data.table_id::VARCHAR)
                )
                GROUP BY data.table_id
            ),
            active_delete_files AS MATERIALIZED (
                SELECT
                    deletes.delete_file_id,
                    deletes.table_id,
                    deletes.data_file_id,
                    deletes.file_size_bytes AS delete_file_bytes,
                    deletes.delete_count,
                    data.record_count,
                    data.file_size_bytes AS data_file_bytes,
                    data.data_file_id IS NOT NULL AS data_file_active
                FROM {delete_files} AS deletes
                LEFT JOIN active_data_files AS data USING (data_file_id)
                WHERE deletes.end_snapshot IS NULL
            ),
            delete_file_summary AS (
                SELECT
                    table_id,
                    count(*) FILTER (WHERE data_file_active)
                      AS file_count,
                    coalesce(
                        sum(delete_file_bytes) FILTER (WHERE data_file_active),
                        0
                    ) AS file_bytes,
                    coalesce(
                        sum(delete_count) FILTER (WHERE data_file_active),
                        0
                    ) AS row_count,
                    count(*) FILTER (WHERE NOT data_file_active)
                      AS dangling_count
                FROM active_delete_files
                GROUP BY table_id
            ),
            deletes_per_data_file AS (
                SELECT
                    table_id,
                    data_file_id,
                    count(*) AS delete_file_count,
                    coalesce(sum(delete_file_bytes), 0) AS delete_file_bytes,
                    coalesce(sum(delete_count), 0) AS deleted_rows,
                    max(record_count) AS original_rows,
                    max(data_file_bytes) AS data_file_bytes
                FROM active_delete_files
                WHERE data_file_active
                GROUP BY table_id, data_file_id
            ),
            rewrite_summary AS (
                SELECT
                    deletes.table_id,
                    count(*) AS data_file_count,
                    coalesce(sum(deletes.data_file_bytes), 0) AS input_bytes,
                    coalesce(sum(deletes.delete_file_count), 0)
                      AS delete_file_count,
                    coalesce(sum(deletes.delete_file_bytes), 0)
                      AS delete_file_bytes,
                    coalesce(sum(deletes.deleted_rows), 0) AS deleted_rows,
                    coalesce(sum(deletes.original_rows), 0) AS original_rows
                FROM deletes_per_data_file AS deletes
                JOIN active_tables AS tables USING (table_id)
                WHERE deletes.original_rows > 0
                  AND deletes.deleted_rows::DOUBLE / deletes.original_rows
                      > tables.rewrite_delete_threshold
                GROUP BY deletes.table_id
            )
            SELECT
                tables.table_id,
                tables.schema_name,
                tables.table_name,
                tables.auto_compact,
                tables.target_file_size_bytes,
                tables.rewrite_delete_threshold,
                sorts.table_id IS NOT NULL AS sorting_enabled,
                coalesce(data.file_count, 0),
                coalesce(data.file_bytes, 0),
                coalesce(data.row_count, 0),
                coalesce(data.minimum_bytes, 0),
                coalesce(data.median_bytes, 0),
                coalesce(data.p90_bytes, 0),
                coalesce(data.maximum_bytes, 0),
                coalesce(deletes.file_count, 0),
                coalesce(deletes.file_bytes, 0),
                coalesce(deletes.row_count, 0),
                coalesce(deletes.dangling_count, 0),
                coalesce(rewrite.data_file_count, 0),
                coalesce(rewrite.input_bytes, 0),
                coalesce(rewrite.delete_file_count, 0),
                coalesce(rewrite.delete_file_bytes, 0),
                coalesce(rewrite.deleted_rows, 0),
                coalesce(rewrite.original_rows, 0),
                coalesce(recent.file_count, 0),
                tables.data_inlining_row_limit
            FROM active_tables AS tables
            LEFT JOIN active_sorts AS sorts USING (table_id)
            LEFT JOIN data_file_summary AS data USING (table_id)
            LEFT JOIN recent_data_file_summary AS recent USING (table_id)
            LEFT JOIN delete_file_summary AS deletes USING (table_id)
            LEFT JOIN rewrite_summary AS rewrite USING (table_id)
            ORDER BY tables.table_id
            """
        ).fetchall()

    def inlined_data(self, metadata_schema: str) -> Iterable[InlinedDataRow]:
        mapping = self._relation(metadata_schema, "ducklake_inlined_data_tables")
        rows = self._connection.execute(
            f"""
            SELECT table_id, table_name
            FROM {mapping}
            ORDER BY table_id, schema_version
            """
        ).fetchall()
        if not rows:
            return ()
        summaries = "\nUNION ALL\n".join(
            f"""
            SELECT
                {int(row[0])}::BIGINT AS table_id,
                count(*)::BIGINT AS active_rows,
                coalesce(
                    sum(length(to_json(inlined_row)::VARCHAR)),
                    0
                )::BIGINT AS active_bytes
            FROM {self._relation(metadata_schema, str(row[1]))} AS inlined_row
            WHERE end_snapshot IS NULL
            """
            for row in rows
        )
        return self._connection.execute(
            f"""
            SELECT
                table_id,
                sum(active_rows)::BIGINT,
                sum(active_bytes)::BIGINT
            FROM ({summaries}) AS summaries
            GROUP BY table_id
            ORDER BY table_id
            """
        ).fetchall()

    def compatible_file_groups(
        self,
        metadata_schema: str,
    ) -> Iterable[CompatibleFileGroupRow]:
        tables = self._relation(metadata_schema, "ducklake_table")
        schemas = self._relation(metadata_schema, "ducklake_schema")
        metadata = self._relation(metadata_schema, "ducklake_metadata")
        data_files = self._relation(metadata_schema, "ducklake_data_file")
        schema_versions = self._relation(
            metadata_schema,
            "ducklake_schema_versions",
        )
        partition_values = self._relation(
            metadata_schema,
            "ducklake_file_partition_value",
        )
        delete_files = self._relation(metadata_schema, "ducklake_delete_file")
        inlined_delete_tables = self._connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = ?
              AND table_schema = ?
              AND regexp_matches(
                    table_name,
                    '^ducklake_inlined_delete_[0-9]+$'
                  )
            ORDER BY table_name
            """,
            [self._catalog_alias.strip('"'), metadata_schema],
        ).fetchall()
        if inlined_delete_tables:
            inlined_deletions = "\nUNION ALL\n".join(
                f"SELECT file_id FROM {self._relation(metadata_schema, str(row[0]))}"
                for row in inlined_delete_tables
            )
        else:
            inlined_deletions = "SELECT NULL::BIGINT AS file_id WHERE false"
        return self._connection.execute(
            f"""
            WITH active_tables AS (
                SELECT
                    tables.table_id,
                    coalesce(
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'target_file_size'
                              AND scope = 'table'
                              AND scope_id = tables.table_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'target_file_size'
                              AND scope = 'schema'
                              AND scope_id = schemas.schema_id
                        ),
                        (
                            SELECT value
                            FROM {metadata}
                            WHERE key = 'target_file_size'
                              AND scope IS NULL
                        ),
                        '536870912'
                    )::BIGINT AS target_file_size_bytes
                FROM {tables} AS tables
                JOIN {schemas} AS schemas
                  ON schemas.schema_id = tables.schema_id
                 AND schemas.end_snapshot IS NULL
                WHERE tables.end_snapshot IS NULL
            ),
            active_data_files AS MATERIALIZED (
                SELECT
                    data.data_file_id,
                    data.table_id,
                    data.begin_snapshot,
                    data.partition_id,
                    data.file_size_bytes
                FROM {data_files} AS data
                WHERE data.end_snapshot IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {delete_files} AS deletes
                      WHERE deletes.table_id = data.table_id
                        AND deletes.data_file_id = data.data_file_id
                  )
                  AND data.data_file_id NOT IN (
                      {inlined_deletions}
                  )
            ),
            snapshot_ranges AS (
                SELECT
                    table_id,
                    begin_snapshot,
                    coalesce(
                        lead(begin_snapshot) OVER (
                            PARTITION BY table_id ORDER BY begin_snapshot
                        ),
                        9223372036854775807
                    ) AS end_snapshot,
                    schema_version
                FROM {schema_versions}
            ),
            file_partitions AS (
                SELECT
                    values.data_file_id,
                    array_agg(
                        values.partition_value
                        ORDER BY values.partition_key_index
                    ) AS partition_values
                FROM {partition_values} AS values
                JOIN active_data_files AS data USING (data_file_id)
                GROUP BY values.data_file_id
            )
            SELECT
                data.table_id,
                ranges.schema_version,
                data.partition_id,
                count(*) AS active_files,
                coalesce(sum(data.file_size_bytes), 0) AS active_bytes,
                count(*) FILTER (
                    WHERE data.file_size_bytes < tables.target_file_size_bytes
                ) AS merge_candidate_files,
                coalesce(
                    sum(data.file_size_bytes) FILTER (
                        WHERE data.file_size_bytes
                            < tables.target_file_size_bytes
                    ),
                    0
                ) AS merge_candidate_bytes
            FROM active_data_files AS data
            JOIN active_tables AS tables USING (table_id)
            LEFT JOIN snapshot_ranges AS ranges
              ON ranges.table_id = data.table_id
             AND data.begin_snapshot >= ranges.begin_snapshot
             AND data.begin_snapshot < ranges.end_snapshot
            LEFT JOIN file_partitions AS partitions USING (data_file_id)
            GROUP BY
                data.table_id,
                ranges.schema_version,
                data.partition_id,
                partitions.partition_values
            ORDER BY data.table_id, ranges.schema_version, data.partition_id
            """
        ).fetchall()


def collect_inventory(
    source: InventorySource,
    metadata_schemas: Iterable[str],
) -> CatalogInventory:
    """Collect immutable inventory records from a query source."""

    lakes: list[LakeInventory] = []
    for metadata_schema in sorted(set(metadata_schemas)):
        (
            snapshot_id,
            snapshot_ms,
            scheduled_files,
            scheduled_ms,
            delete_older_than,
            expire_older_than,
        ) = source.lake_summary(metadata_schema)
        groups_by_table: dict[int, list[CompatibleFileGroup]] = {}
        for row in source.compatible_file_groups(metadata_schema):
            groups_by_table.setdefault(int(row[0]), []).append(
                CompatibleFileGroup(
                    schema_version=int(row[1]) if row[1] is not None else None,
                    partition_id=int(row[2]) if row[2] is not None else None,
                    active_files=int(row[3]),
                    active_bytes=int(row[4]),
                    merge_candidate_files=int(row[5]),
                    merge_candidate_bytes=int(row[6]),
                )
            )
        inlined_by_table = {
            int(row[0]): (int(row[1]), int(row[2]))
            for row in source.inlined_data(metadata_schema)
        }
        tables = tuple(
            TableInventory(
                metadata_schema=metadata_schema,
                table_id=int(row[0]),
                schema_name=str(row[1]),
                table_name=str(row[2]),
                auto_compact=bool(row[3]),
                target_file_size_bytes=int(row[4]),
                rewrite_delete_threshold=float(row[5]),
                sorting_enabled=bool(row[6]),
                active_data_files=int(row[7]),
                active_data_bytes=int(row[8]),
                active_data_rows=int(row[9]),
                recent_data_files_60s=int(row[24]),
                data_file_sizes=FileSizeDistribution(
                    minimum_bytes=int(row[10]),
                    median_bytes=int(row[11]),
                    p90_bytes=int(row[12]),
                    maximum_bytes=int(row[13]),
                ),
                compatible_file_groups=tuple(groups_by_table.get(int(row[0]), ())),
                active_delete_files=int(row[14]),
                active_delete_bytes=int(row[15]),
                deleted_rows=int(row[16]),
                dangling_delete_files=int(row[17]),
                rewrite_data_files=int(row[18]),
                rewrite_input_bytes=int(row[19]),
                rewrite_delete_files=int(row[20]),
                rewrite_delete_bytes=int(row[21]),
                rewrite_deleted_rows=int(row[22]),
                rewrite_original_rows=int(row[23]),
                data_inlining_row_limit=int(row[25]),
                inlined_data_rows=inlined_by_table.get(int(row[0]), (0, 0))[0],
                inlined_data_bytes=inlined_by_table.get(int(row[0]), (0, 0))[1],
            )
            for row in source.tables(metadata_schema)
        )
        lakes.append(
            LakeInventory(
                metadata_schema=metadata_schema,
                latest_snapshot_id=(
                    int(snapshot_id) if snapshot_id is not None else None
                ),
                latest_snapshot_at=_utc_from_milliseconds(snapshot_ms),
                scheduled_files=int(scheduled_files),
                oldest_scheduled_at=_utc_from_milliseconds(scheduled_ms),
                tables=tables,
                delete_older_than=(
                    str(delete_older_than) if delete_older_than is not None else None
                ),
                expire_older_than=(
                    str(expire_older_than) if expire_older_than is not None else None
                ),
            )
        )
    return CatalogInventory(lakes=tuple(lakes))


def _native_dry_run_count(
    configuration: MetadataConfiguration,
    metadata_schema: str,
    *,
    alias: str,
    function: str,
    diagnosis: str,
    storage: StorageConfiguration | None = None,
    read_only: bool = True,
) -> int:
    connection = duckdb.connect(
        database=":memory:",
        config=connection_config({"threads": "1"}),
    )
    try:
        connection.execute("PRAGMA disable_checkpoint_on_shutdown")
        extensions = ("httpfs", "postgres", "ducklake")
        for extension in extensions:
            connection.execute(f"INSTALL {extension}")
            connection.execute(f"LOAD {extension}")
        if storage is not None and storage.provider == "s3-compatible":
            connection.execute(
                f"""
                CREATE SECRET {_PROBE_STORAGE_SECRET} (
                    TYPE s3,
                    KEY_ID ?,
                    SECRET ?,
                    REGION ?,
                    ENDPOINT ?,
                    URL_STYLE 'path',
                    USE_SSL ?,
                    SCOPE ?
                )
                """,
                [
                    storage.access_key_id,
                    storage.secret_access_key,
                    storage.region,
                    storage.endpoint,
                    storage.use_ssl,
                    f"s3://{storage.bucket}",
                ],
            )
        uri = configuration.postgres_uri().replace("'", "''")
        schema = metadata_schema.replace("'", "''")
        options = [f"METADATA_SCHEMA '{schema}'", "CREATE_IF_NOT_EXISTS false"]
        if read_only:
            options.append("READ_ONLY")
        if storage is not None and storage.provider == "filesystem":
            if storage.data_path is None:
                raise InventoryError("filesystem storage is missing its data path")
            data_path = storage.data_path.replace("'", "''")
            options.extend((f"DATA_PATH '{data_path}'", "OVERRIDE_DATA_PATH true"))
        connection.execute(
            f"""
            ATTACH 'ducklake:postgres:{uri}' AS {alias} (
                {", ".join(options)}
            )
            """
        )
        row = connection.execute(
            f"""
            SELECT count(*)::BIGINT
            FROM {function}(?, dry_run => true)
            """,
            [alias],
        ).fetchone()
        if row is None or len(row) != 1:
            raise InventoryError(
                f"DuckLake returned invalid {diagnosis}: {metadata_schema}"
            )
        return int(row[0])
    except InventoryError:
        raise
    except duckdb.Error as error:
        raise InventoryError(
            f"could not diagnose {diagnosis}: {metadata_schema}"
        ) from error
    finally:
        connection.close()


def _cleanup_eligible_files(
    configuration: MetadataConfiguration,
    metadata_schema: str,
) -> int:
    """Ask DuckLake to resolve its own persisted/default cleanup policy."""

    return _native_dry_run_count(
        configuration,
        metadata_schema,
        alias=_CLEANUP_PROBE_ALIAS,
        function="ducklake_cleanup_old_files",
        diagnosis="scheduled-file cleanup",
    )


def _expiring_snapshots(
    configuration: MetadataConfiguration,
    metadata_schema: str,
) -> int:
    """Ask DuckLake to resolve its persisted snapshot-retention policy."""

    return _native_dry_run_count(
        configuration,
        metadata_schema,
        alias=_EXPIRATION_PROBE_ALIAS,
        function="ducklake_expire_snapshots",
        diagnosis="snapshot expiration",
    )


def _orphan_files(
    configuration: MetadataConfiguration,
    storage: StorageConfiguration,
    metadata_schema: str,
) -> int:
    """Run DuckLake's storage-aware orphan diagnosis without deleting files."""

    # DuckLake currently rejects this dry run on a READ_ONLY attachment.
    # The function is still non-mutating because dry_run remains explicit.
    return _native_dry_run_count(
        configuration,
        metadata_schema,
        alias=_ORPHAN_PROBE_ALIAS,
        function="ducklake_delete_orphaned_files",
        diagnosis="orphan-file cleanup",
        storage=storage,
        read_only=False,
    )


def _with_cleanup_eligibility(
    configuration: MetadataConfiguration,
    inventory: CatalogInventory,
) -> CatalogInventory:
    if not any(lake.scheduled_files for lake in inventory.lakes):
        return inventory
    lakes = tuple(
        replace(
            lake,
            cleanup_eligible_files=_cleanup_eligible_files(
                configuration,
                lake.metadata_schema,
            ),
        )
        if lake.scheduled_files
        else lake
        for lake in inventory.lakes
    )
    return CatalogInventory(lakes=lakes)


def _with_expiration_eligibility(
    configuration: MetadataConfiguration,
    inventory: CatalogInventory,
) -> CatalogInventory:
    if not inventory.lakes:
        return inventory
    lakes = tuple(
        replace(
            lake,
            expiring_snapshots=_expiring_snapshots(
                configuration,
                lake.metadata_schema,
            ),
        )
        for lake in inventory.lakes
    )
    return CatalogInventory(lakes=lakes)


def _with_orphan_eligibility(
    configuration: MetadataConfiguration,
    storage: StorageConfiguration,
    inventory: CatalogInventory,
    schemas: frozenset[str],
) -> CatalogInventory:
    if not schemas:
        return inventory
    lakes = tuple(
        replace(
            lake,
            orphan_files=_orphan_files(
                configuration,
                storage,
                lake.metadata_schema,
            ),
        )
        if lake.metadata_schema in schemas
        else lake
        for lake in inventory.lakes
    )
    return CatalogInventory(lakes=lakes)


def inventory_catalog(
    configuration: MetadataConfiguration,
    detection: BackendDetection,
    storage: StorageConfiguration | None = None,
    *,
    orphan_probe_schemas: frozenset[str] | None = None,
) -> CatalogInventory:
    """Inventory the configured catalog through a read-only metadata attach."""

    if detection.backend is not MetadataBackend.POSTGRES:
        raise InventoryError(
            f"inventory attachment is not implemented for: {detection.backend}"
        )

    connection = duckdb.connect(
        database=":memory:",
        config=connection_config({"threads": "1"}),
    )
    attached = False
    try:
        connection.execute("PRAGMA disable_checkpoint_on_shutdown")
        connection.execute("INSTALL postgres")
        connection.execute("LOAD postgres")
        uri = configuration.postgres_uri().replace("'", "''")
        connection.execute(
            f"ATTACH '{uri}' AS {_INVENTORY_ALIAS} (TYPE postgres, READ_ONLY)"
        )
        attached = True
        inventory = collect_inventory(
            DuckDBInventorySource(connection, _INVENTORY_ALIAS),
            detection.metadata_schemas,
        )
    except InventoryError:
        raise
    except duckdb.Error as error:
        raise InventoryError("could not inventory the configured catalog") from error
    finally:
        if attached:
            connection.execute(f"DETACH {_INVENTORY_ALIAS}")
        connection.close()
    inventory = _with_expiration_eligibility(configuration, inventory)
    inventory = _with_cleanup_eligibility(configuration, inventory)
    if storage is not None:
        schemas = (
            frozenset(detection.metadata_schemas)
            if orphan_probe_schemas is None
            else orphan_probe_schemas
        )
        inventory = _with_orphan_eligibility(
            configuration,
            storage,
            inventory,
            schemas,
        )
    return inventory


@dataclass(slots=True)
class MaintenanceInventory:
    """Inventory all native work while throttling expensive orphan scans."""

    storage: StorageConfiguration
    orphan_scan_interval_seconds: float | None = None
    clock: Callable[[], float] = monotonic
    orphan_probe_observer: Callable[[bool], None] | None = None
    _orphan_files_by_lake: dict[str, int] = field(default_factory=dict)
    _orphan_scanned_at: dict[str, float] = field(default_factory=dict)

    def __call__(
        self,
        configuration: MetadataConfiguration,
        detection: BackendDetection,
    ) -> CatalogInventory:
        now = self.clock()
        due = frozenset(
            schema
            for schema in detection.metadata_schemas
            if self.orphan_scan_interval_seconds is None
            or schema not in self._orphan_scanned_at
            or (
                now - self._orphan_scanned_at[schema]
                >= self.orphan_scan_interval_seconds
            )
        )
        inventory = inventory_catalog(
            configuration,
            detection,
            self.storage,
            orphan_probe_schemas=frozenset(),
        )
        probe_failed = False
        lakes: list[LakeInventory] = []
        for lake in inventory.lakes:
            if lake.metadata_schema in due:
                self._orphan_scanned_at[lake.metadata_schema] = now
                try:
                    orphan_files = _orphan_files(
                        configuration,
                        self.storage,
                        lake.metadata_schema,
                    )
                except InventoryError as error:
                    probe_failed = True
                    self._orphan_files_by_lake[lake.metadata_schema] = 0
                    cause = error.__cause__ or error
                    _LOGGER.warning(
                        "orphan_probe_failed lake=%s error=%s "
                        "continuing_without_orphan_cleanup=true",
                        lake.metadata_schema,
                        cause,
                    )
                else:
                    self._orphan_files_by_lake[lake.metadata_schema] = orphan_files
            lakes.append(
                replace(
                    lake,
                    orphan_files=self._orphan_files_by_lake.get(
                        lake.metadata_schema,
                        0,
                    ),
                )
            )
        if due and self.orphan_probe_observer is not None:
            self.orphan_probe_observer(not probe_failed)
        return CatalogInventory(lakes=tuple(lakes))

    def treatment_completed(
        self,
        selection: TreatmentSelection,
        result: TreatmentResult,
    ) -> None:
        """Update the cached orphan debt after its native cleanup succeeds."""

        if selection.kind is not TreatmentKind.ORPHAN_FILE_CLEANUP:
            return
        previous = self._orphan_files_by_lake.get(selection.metadata_schema, 0)
        self._orphan_files_by_lake[selection.metadata_schema] = max(
            0,
            previous - result.files_processed,
        )
        self._orphan_scanned_at[selection.metadata_schema] = self.clock()
