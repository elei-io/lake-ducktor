"""Small immutable values shared by LakeDucktor's core."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class MetadataBackend(StrEnum):
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    SQLITE = "sqlite"


class CoordinationStrategy(StrEnum):
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    SQLITE = "sqlite"


@dataclass(frozen=True, slots=True)
class DuckDBExtension:
    name: str
    version: str
    install_mode: str
    source: str


@dataclass(frozen=True, slots=True)
class BackendDetection:
    backend: MetadataBackend
    metadata_schemas: tuple[str, ...]
    extension_version: str
    duckdb_extensions: tuple[DuckDBExtension, ...]


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    metadata_backend: MetadataBackend
    coordination: CoordinationStrategy
    horizontal_scale_safe: bool
    coordination_extra: str | None


@dataclass(frozen=True, slots=True)
class FileSizeDistribution:
    minimum_bytes: int
    median_bytes: int
    p90_bytes: int
    maximum_bytes: int


@dataclass(frozen=True, slots=True)
class TableInventory:
    metadata_schema: str
    table_id: int
    schema_name: str
    table_name: str
    active_data_files: int
    active_data_bytes: int
    active_data_rows: int
    data_file_sizes: FileSizeDistribution
    active_delete_files: int
    active_delete_bytes: int
    deleted_rows: int
    dangling_delete_files: int


@dataclass(frozen=True, slots=True)
class LakeInventory:
    metadata_schema: str
    latest_snapshot_id: int | None
    latest_snapshot_at: datetime | None
    scheduled_files: int
    oldest_scheduled_at: datetime | None
    tables: tuple[TableInventory, ...]

    @property
    def table_count(self) -> int:
        return len(self.tables)

    @property
    def active_data_files(self) -> int:
        return sum(table.active_data_files for table in self.tables)

    @property
    def active_data_bytes(self) -> int:
        return sum(table.active_data_bytes for table in self.tables)

    @property
    def active_delete_files(self) -> int:
        return sum(table.active_delete_files for table in self.tables)

    @property
    def active_delete_bytes(self) -> int:
        return sum(table.active_delete_bytes for table in self.tables)

    @property
    def dangling_delete_files(self) -> int:
        return sum(table.dangling_delete_files for table in self.tables)


@dataclass(frozen=True, slots=True)
class CatalogInventory:
    lakes: tuple[LakeInventory, ...]

    @property
    def table_count(self) -> int:
        return sum(lake.table_count for lake in self.lakes)

    @property
    def active_data_files(self) -> int:
        return sum(lake.active_data_files for lake in self.lakes)

    @property
    def active_data_bytes(self) -> int:
        return sum(lake.active_data_bytes for lake in self.lakes)

    @property
    def active_delete_files(self) -> int:
        return sum(lake.active_delete_files for lake in self.lakes)

    @property
    def active_delete_bytes(self) -> int:
        return sum(lake.active_delete_bytes for lake in self.lakes)

    @property
    def scheduled_files(self) -> int:
        return sum(lake.scheduled_files for lake in self.lakes)
