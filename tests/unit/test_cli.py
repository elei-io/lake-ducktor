import logging
from types import SimpleNamespace

from lakeducktor import __version__
from lakeducktor.cli import main
from lakeducktor.model import BackendDetection, DuckDBExtension, MetadataBackend

_EXTENSIONS = (
    DuckDBExtension(
        name="core_functions",
        version="v1",
        install_mode="statically_linked",
        source="built-in",
    ),
    DuckDBExtension(
        name="ducklake",
        version="abc123",
        install_mode="repository",
        source="core",
    ),
)


def test_entry_point_reports_detected_backend(
    monkeypatch,
    caplog,
    capsys,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: BackendDetection(
            backend=MetadataBackend.POSTGRES,
            metadata_schemas=("ducklake",),
            extension_version="v1",
            duckdb_extensions=_EXTENSIONS,
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "detect-backend"])

    assert result == 0
    assert capsys.readouterr().out == ""
    messages = [record.getMessage() for record in caplog.records]
    assert f"starting version={__version__}" in messages
    assert "detected metadata_backend=postgres" in messages
    assert "detected lakes=1" in messages
    assert "selected adapter=PostgresCapabilitiesAdapter" in messages
    assert not any(message.startswith("detected extension=") for message in messages)


def test_entry_point_summarizes_multiple_lakes(
    monkeypatch,
    caplog,
    capsys,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: BackendDetection(
            backend=MetadataBackend.POSTGRES,
            metadata_schemas=("lake_a", "lake_b"),
            extension_version="v1",
            duckdb_extensions=_EXTENSIONS,
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "detect-backend"])

    assert result == 0
    assert capsys.readouterr().out == ""
    messages = [record.getMessage() for record in caplog.records]
    assert "detected metadata_backend=postgres" in messages
    assert "detected lakes=2" in messages


def test_inventory_command_logs_aggregate_physical_facts(
    monkeypatch,
    caplog,
    capsys,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake_a", "lake_b"),
        extension_version="v1",
        duckdb_extensions=_EXTENSIONS,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: detection,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.inventory_catalog",
        lambda _configuration, _detection: SimpleNamespace(
            lakes=(
                SimpleNamespace(
                    metadata_schema="lake_a",
                    latest_snapshot_id=11,
                    table_count=4,
                    active_data_files=12,
                    active_data_bytes=1_024,
                    active_delete_files=3,
                    active_delete_bytes=128,
                    dangling_delete_files=1,
                    scheduled_files=2,
                ),
                SimpleNamespace(
                    metadata_schema="lake_b",
                    latest_snapshot_id=None,
                    table_count=0,
                    active_data_files=0,
                    active_data_bytes=0,
                    active_delete_files=0,
                    active_delete_bytes=0,
                    dangling_delete_files=0,
                    scheduled_files=0,
                ),
            ),
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "inventory"])

    assert result == 0
    assert capsys.readouterr().out == ""
    assert (
        "detected lake=lake_a snapshot=11 tables=4 active_data_files=12 "
        "active_data_bytes=1024 active_delete_files=3 active_delete_bytes=128 "
        "dangling_delete_files=1 scheduled_files=2"
    ) in [record.getMessage() for record in caplog.records]
