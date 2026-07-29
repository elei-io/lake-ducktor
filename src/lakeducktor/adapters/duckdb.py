"""Capabilities of a DuckDB-backed DuckLake."""

from __future__ import annotations

from lakeducktor.model import (
    BackendCapabilities,
    CoordinationStrategy,
    MetadataBackend,
)


class DuckDBCapabilitiesAdapter:
    backend = MetadataBackend.DUCKDB

    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            metadata_backend=self.backend,
            coordination=CoordinationStrategy.DUCKDB,
            horizontal_scale_safe=False,
            coordination_extra=None,
        )
