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


class DiagnosisState(StrEnum):
    HEALTHY = "healthy"
    ACTIONABLE = "actionable"
    EXCLUDED = "excluded"
    ATTENTION = "attention"


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
class CompatibleFileGroup:
    schema_version: int | None
    partition_id: int | None
    active_files: int
    active_bytes: int
    merge_candidate_files: int
    merge_candidate_bytes: int


@dataclass(frozen=True, slots=True)
class TableInventory:
    metadata_schema: str
    table_id: int
    schema_name: str
    table_name: str
    auto_compact: bool
    target_file_size_bytes: int
    rewrite_delete_threshold: float
    active_data_files: int
    active_data_bytes: int
    active_data_rows: int
    data_file_sizes: FileSizeDistribution
    compatible_file_groups: tuple[CompatibleFileGroup, ...]
    active_delete_files: int
    active_delete_bytes: int
    deleted_rows: int
    dangling_delete_files: int
    rewrite_data_files: int
    rewrite_input_bytes: int
    rewrite_delete_files: int
    rewrite_delete_bytes: int
    rewrite_deleted_rows: int
    rewrite_original_rows: int


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


@dataclass(frozen=True, slots=True)
class TableDiagnosis:
    metadata_schema: str
    table_id: int
    schema_name: str
    table_name: str
    state: DiagnosisState
    reasons: tuple[str, ...]
    merge_groups: int
    merge_input_files: int
    merge_input_bytes: int
    expected_files_eliminated: int
    rewrite_data_files: int
    rewrite_input_bytes: int
    rewrite_delete_files: int
    rewrite_deleted_rows: int
    dangling_delete_files: int


@dataclass(frozen=True, slots=True)
class LakeDiagnosis:
    metadata_schema: str
    state: DiagnosisState
    scheduled_files: int
    tables: tuple[TableDiagnosis, ...]

    @property
    def actionable_tables(self) -> int:
        return sum(table.state is DiagnosisState.ACTIONABLE for table in self.tables)

    @property
    def excluded_tables(self) -> int:
        return sum(table.state is DiagnosisState.EXCLUDED for table in self.tables)

    @property
    def attention_tables(self) -> int:
        return sum(table.state is DiagnosisState.ATTENTION for table in self.tables)


@dataclass(frozen=True, slots=True)
class CatalogDiagnosis:
    lakes: tuple[LakeDiagnosis, ...]
