from lakeducktor.duckdb_config import connection_config


def test_unsigned_extensions_are_rejected_by_default(monkeypatch) -> None:
    monkeypatch.delenv("DUCKDB_ALLOW_UNSIGNED_EXTENSIONS", raising=False)

    assert connection_config({"threads": "1"}) == {"threads": "1"}


def test_unsigned_extensions_can_be_enabled_explicitly(monkeypatch) -> None:
    monkeypatch.setenv("DUCKDB_ALLOW_UNSIGNED_EXTENSIONS", "true")

    assert connection_config({"threads": "1"}) == {
        "threads": "1",
        "allow_unsigned_extensions": "true",
    }
