"""Capabilities of a SQLite-backed DuckLake."""

from __future__ import annotations

from lakeducktor.model import (
    BackendCapabilities,
    CoordinationStrategy,
    MetadataBackend,
)


class SQLiteCapabilitiesAdapter:
    backend = MetadataBackend.SQLITE

    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            metadata_backend=self.backend,
            coordination=CoordinationStrategy.SQLITE,
            horizontal_scale_safe=False,
            coordination_extra=None,
        )
