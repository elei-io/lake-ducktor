import duckdb
import pytest

from lakeducktor.lake import (
    BackendDetectionError,
    _loaded_duckdb_extensions,
    _select_metadata_schemas,
)


def test_unique_metadata_schema_is_discovered() -> None:
    assert _select_metadata_schemas(["ducklake"], configured=None) == ("ducklake",)


def test_configured_metadata_schema_must_be_a_ducklake() -> None:
    with pytest.raises(BackendDetectionError, match="does not contain"):
        _select_metadata_schemas(["ducklake"], configured="other")


def test_all_metadata_schemas_are_discovered_without_an_override() -> None:
    assert _select_metadata_schemas(["lake_b", "lake_a"], configured=None) == (
        "lake_a",
        "lake_b",
    )


def test_configured_metadata_schema_narrows_discovery() -> None:
    assert _select_metadata_schemas(["lake_a", "lake_b"], configured="lake_b") == (
        "lake_b",
    )


def test_missing_metadata_schema_is_rejected() -> None:
    with pytest.raises(BackendDetectionError, match="no DuckLake"):
        _select_metadata_schemas([], configured=None)


def test_loaded_duckdb_extensions_are_detected() -> None:
    connection = duckdb.connect(":memory:")
    try:
        extensions = _loaded_duckdb_extensions(connection)
    finally:
        connection.close()

    names = {extension.name for extension in extensions}
    assert "core_functions" in names
    assert all(extension.version for extension in extensions)
