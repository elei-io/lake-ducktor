import logging

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
    assert "detected schema=ducklake" in messages
    assert "selected adapter=PostgresCapabilitiesAdapter" in messages
    assert "detected extension=core_functions version=v1" in messages
    assert "detected extension=ducklake version=abc123" in messages


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
    assert "detected schemas=2" in messages
