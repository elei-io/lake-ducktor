from unittest.mock import Mock

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


def test_maintain_lakes_filters_discovered_schemas() -> None:
    assert _select_metadata_schemas(
        ["lake_a", "lake_b", "lake_c"],
        configured=None,
        maintained=("lake_c", "lake_a"),
    ) == ("lake_a", "lake_c")


def test_unknown_maintained_lake_is_rejected() -> None:
    with pytest.raises(BackendDetectionError, match="unknown.*lake_c"):
        _select_metadata_schemas(
            ["lake_a", "lake_b"],
            configured=None,
            maintained=("lake_a", "lake_c"),
        )


def test_maintain_lakes_must_respect_explicit_schema() -> None:
    with pytest.raises(BackendDetectionError, match="excluded.*lake_b"):
        _select_metadata_schemas(
            ["lake_a", "lake_b"],
            configured="lake_a",
            maintained=("lake_b",),
        )


def test_missing_metadata_schema_is_rejected() -> None:
    with pytest.raises(BackendDetectionError, match="no DuckLake"):
        _select_metadata_schemas([], configured=None)


def test_loaded_duckdb_extensions_are_detected() -> None:
    connection = Mock()
    connection.execute.return_value.fetchall.return_value = [
        ("core_functions", "v1", "STATICALLY_LINKED", ""),
        ("ducklake", "abc123", "REPOSITORY", "core"),
    ]

    extensions = _loaded_duckdb_extensions(connection)

    assert [(extension.name, extension.version) for extension in extensions] == [
        ("core_functions", "v1"),
        ("ducklake", "abc123"),
    ]
    assert extensions[0].source == "built-in"
    connection.execute.assert_called_once()
