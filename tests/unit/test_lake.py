from unittest.mock import Mock, patch

import pytest

from lakeducktor.config import MetadataConfiguration
from lakeducktor.lake import (
    BackendDetectionError,
    _loaded_duckdb_extensions,
    _select_metadata_schemas,
    detect_metadata_backend,
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


def test_detection_never_detaches_a_ducklake_or_checkpoints_on_shutdown() -> None:
    connection = Mock()

    def execute(query: str, parameters: object = None) -> Mock:
        result = Mock()
        if "information_schema.tables" in query:
            result.fetchall.return_value = [("lake_a",), ("lake_b",)]
        elif "ducklake_settings" in query:
            result.fetchone.return_value = ("postgres", "ducklake-version")
        elif "duckdb_extensions()" in query:
            result.fetchall.return_value = []
        return result

    connection.execute.side_effect = execute
    configuration = MetadataConfiguration(
        backend_hint="postgres",
        host="catalog.example",
        port=5432,
        username="user",
        password="password",
        database="lake",
    )

    with patch("lakeducktor.lake.duckdb.connect", return_value=connection):
        detection = detect_metadata_backend(configuration)

    assert detection.metadata_schemas == ("lake_a", "lake_b")
    queries = [call.args[0] for call in connection.execute.call_args_list]
    assert queries[0] == "PRAGMA disable_checkpoint_on_shutdown"
    assert "INSTALL httpfs" in queries
    assert "LOAD httpfs" in queries
    assert sum("ATTACH 'ducklake:" in query for query in queries) == 2
    assert not any("DETACH lakeducktor_lake" in query for query in queries)
    assert connection.close.call_count == 1
