"""Capabilities of a PostgreSQL-backed DuckLake."""

from __future__ import annotations

from lakeducktor.model import (
    BackendCapabilities,
    CoordinationStrategy,
    MetadataBackend,
)


class PostgresCapabilitiesAdapter:
    backend = MetadataBackend.POSTGRES

    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            metadata_backend=self.backend,
            coordination=CoordinationStrategy.POSTGRES,
            horizontal_scale_safe=True,
            coordination_extra="postgres",
        )
