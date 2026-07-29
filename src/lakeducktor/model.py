"""Small immutable values shared by LakeDucktor's core."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class MetadataBackend(StrEnum):
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
