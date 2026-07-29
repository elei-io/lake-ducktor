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


class InventoryError(RuntimeError):
    """The catalog could not be inventoried safely."""


class InventorySource(Protocol):
    """Minimal query boundary used by the pure inventory collector."""

    def lake_summary(self, metadata_schema: str) -> LakeSummaryRow:
        """Return snapshot and scheduled-deletion facts for one lake."""

    def tables(self, metadata_schema: str) -> Iterable[TableInventoryRow]:
        """Return current physical facts for every active table."""


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
        data_files = self._relation(metadata_schema, "ducklake_data_file")
        delete_files = self._relation(metadata_schema, "ducklake_delete_file")
        return self._connection.execute(
            f"""
            WITH active_tables AS (
                SELECT tables.table_id, schemas.schema_name, tables.table_name
                FROM {tables} AS tables
                JOIN {schemas} AS schemas
                  ON schemas.schema_id = tables.schema_id
                 AND schemas.end_snapshot IS NULL
                WHERE tables.end_snapshot IS NULL
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
            delete_file_summary AS (
                SELECT
                    deletes.table_id,
                    count(*) FILTER (WHERE data.data_file_id IS NOT NULL)
                      AS file_count,
                    coalesce(
                        sum(deletes.file_size_bytes)
                          FILTER (WHERE data.data_file_id IS NOT NULL),
                        0
                    ) AS file_bytes,
                    coalesce(
                        sum(deletes.delete_count)
                          FILTER (WHERE data.data_file_id IS NOT NULL),
                        0
                    ) AS row_count,
                    count(*) FILTER (WHERE data.data_file_id IS NULL)
                      AS dangling_count
                FROM {delete_files} AS deletes
                LEFT JOIN active_data_files AS data USING (data_file_id)
                WHERE deletes.end_snapshot IS NULL
                GROUP BY deletes.table_id
            )
            SELECT
                tables.table_id,
                tables.schema_name,
                tables.table_name,
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
                coalesce(deletes.dangling_count, 0)
            FROM active_tables AS tables
            LEFT JOIN data_file_summary AS data USING (table_id)
            LEFT JOIN delete_file_summary AS deletes USING (table_id)
            ORDER BY tables.table_id
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
        tables = tuple(
            TableInventory(
                metadata_schema=metadata_schema,
                table_id=int(row[0]),
                schema_name=str(row[1]),
                table_name=str(row[2]),
                active_data_files=int(row[3]),
                active_data_bytes=int(row[4]),
                active_data_rows=int(row[5]),
                data_file_sizes=FileSizeDistribution(
                    minimum_bytes=int(row[6]),
                    median_bytes=int(row[7]),
                    p90_bytes=int(row[8]),
                    maximum_bytes=int(row[9]),
                ),
                active_delete_files=int(row[10]),
                active_delete_bytes=int(row[11]),
                deleted_rows=int(row[12]),
                dangling_delete_files=int(row[13]),
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
