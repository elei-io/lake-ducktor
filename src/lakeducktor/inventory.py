"""Read-only inventory of physical DuckLake catalog state."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Protocol

import duckdb

from lakeducktor.config import MetadataConfiguration
from lakeducktor.model import (
    BackendDetection,
    CatalogInventory,
    CompatibleFileGroup,
    FileSizeDistribution,
    LakeInventory,
    MetadataBackend,
    TableInventory,
)

_INVENTORY_ALIAS = "lakeducktor_inventory"

type LakeSummaryRow = tuple[int | None, int | None, int, int | None]
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
                (SELECT epoch_ms(min(schedule_start)) FROM {scheduled})
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
                SELECT data_file_id, table_id, file_size_bytes, record_count
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
                coalesce(rewrite.original_rows, 0)
            FROM active_tables AS tables
            LEFT JOIN active_sorts AS sorts USING (table_id)
            LEFT JOIN data_file_summary AS data USING (table_id)
            LEFT JOIN delete_file_summary AS deletes USING (table_id)
            LEFT JOIN rewrite_summary AS rewrite USING (table_id)
            ORDER BY tables.table_id
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
                    data_file_id,
                    table_id,
                    begin_snapshot,
                    partition_id,
                    file_size_bytes
                FROM {data_files}
                WHERE end_snapshot IS NULL
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
        snapshot_id, snapshot_ms, scheduled_files, scheduled_ms = source.lake_summary(
            metadata_schema
        )
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
            )
        )
    return CatalogInventory(lakes=tuple(lakes))


def inventory_catalog(
    configuration: MetadataConfiguration,
    detection: BackendDetection,
) -> CatalogInventory:
    """Inventory the configured catalog through a read-only metadata attach."""

    if detection.backend is not MetadataBackend.POSTGRES:
        raise InventoryError(
            f"inventory attachment is not implemented for: {detection.backend}"
        )

    connection = duckdb.connect(database=":memory:", config={"threads": "1"})
    attached = False
    try:
        connection.execute("INSTALL postgres")
        connection.execute("LOAD postgres")
        uri = configuration.postgres_uri().replace("'", "''")
        connection.execute(
            f"ATTACH '{uri}' AS {_INVENTORY_ALIAS} (TYPE postgres, READ_ONLY)"
        )
        attached = True
        return collect_inventory(
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
