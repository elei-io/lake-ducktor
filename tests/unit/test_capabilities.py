from typing import cast

import pytest

from lakeducktor.adapters import (
    DuckDBCapabilitiesAdapter,
    PostgresCapabilitiesAdapter,
    SQLiteCapabilitiesAdapter,
)
from lakeducktor.capabilities import (
    MissingExtensionError,
    UnsupportedBackendError,
    adapter_for,
)
from lakeducktor.model import (
    BackendDetection,
    CoordinationStrategy,
    DuckDBExtension,
    MetadataBackend,
)


def detection_for(
    backend: MetadataBackend,
    extension_names: tuple[str, ...] = ("ducklake",),
) -> BackendDetection:
    return BackendDetection(
        backend=backend,
        metadata_schemas=("lake",),
        extension_version="test",
        duckdb_extensions=tuple(
            DuckDBExtension(
                name=name,
                version="test",
                install_mode="repository",
                source="test",
            )
            for name in extension_names
        ),
    )


@pytest.mark.parametrize(
    (
        "backend",
        "adapter_type",
        "coordination",
        "horizontal_scale_safe",
        "coordination_extra",
    ),
    [
        (
            MetadataBackend.DUCKDB,
            DuckDBCapabilitiesAdapter,
            CoordinationStrategy.DUCKDB,
            False,
            None,
        ),
        (
            MetadataBackend.POSTGRES,
            PostgresCapabilitiesAdapter,
            CoordinationStrategy.POSTGRES,
            True,
            "postgres",
        ),
        (
            MetadataBackend.SQLITE,
            SQLiteCapabilitiesAdapter,
            CoordinationStrategy.SQLITE,
            False,
            None,
        ),
    ],
)
def test_backend_capabilities_are_resolved(
    backend: MetadataBackend,
    adapter_type: type,
    coordination: CoordinationStrategy,
    horizontal_scale_safe: bool,
    coordination_extra: str | None,
) -> None:
    adapter = adapter_for(detection_for(backend))
    capabilities = adapter.capabilities()

    assert isinstance(adapter, adapter_type)
    assert capabilities.metadata_backend is backend
    assert capabilities.coordination is coordination
    assert capabilities.horizontal_scale_safe is horizontal_scale_safe
    assert capabilities.coordination_extra == coordination_extra


def test_unsupported_backend_is_rejected_without_a_connection() -> None:
    unsupported = cast(MetadataBackend, "mysql")

    with pytest.raises(UnsupportedBackendError, match="mysql"):
        adapter_for(detection_for(unsupported))


def test_missing_ducklake_extension_is_rejected_without_a_connection() -> None:
    detection = detection_for(
        MetadataBackend.POSTGRES,
        extension_names=("core_functions",),
    )

    with pytest.raises(MissingExtensionError, match="ducklake"):
        adapter_for(detection)
