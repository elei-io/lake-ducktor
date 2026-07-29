"""Capability adapter contract and backend selection."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from lakeducktor.adapters.duckdb import DuckDBCapabilitiesAdapter
from lakeducktor.adapters.postgres import PostgresCapabilitiesAdapter
from lakeducktor.adapters.sqlite import SQLiteCapabilitiesAdapter
from lakeducktor.model import BackendCapabilities, BackendDetection, MetadataBackend


@runtime_checkable
class CapabilitiesAdapter(Protocol):
    """Translate one metadata backend into runtime capabilities."""

    backend: MetadataBackend

    def capabilities(self) -> BackendCapabilities:
        """Return the backend's immutable runtime capabilities."""


class CapabilitiesError(RuntimeError):
    """Detected runtime capabilities are unsupported or incomplete."""


class UnsupportedBackendError(CapabilitiesError):
    """No capability adapter exists for the detected backend."""


class MissingExtensionError(CapabilitiesError):
    """A required DuckDB extension was not detected."""


_ADAPTERS: tuple[CapabilitiesAdapter, ...] = (
    DuckDBCapabilitiesAdapter(),
    PostgresCapabilitiesAdapter(),
    SQLiteCapabilitiesAdapter(),
)


def adapter_for(detection: BackendDetection) -> CapabilitiesAdapter:
    """Validate a detection and select its backend capability adapter."""

    for adapter in _ADAPTERS:
        if adapter.backend is detection.backend:
            break
    else:
        raise UnsupportedBackendError(
            f"unsupported metadata backend: {detection.backend}"
        )

    if not any(
        extension.name == "ducklake" for extension in detection.duckdb_extensions
    ):
        raise MissingExtensionError(
            "required DuckDB extension was not detected: ducklake"
        )
    return adapter
