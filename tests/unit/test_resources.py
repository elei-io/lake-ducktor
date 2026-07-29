import pytest

from lakeducktor.config import ConfigurationError
from lakeducktor.resources import (
    parse_memory_bytes,
    resource_envelope_from_environment,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("4GB", 4_000_000_000),
        ("4096MiB", 4_294_967_296),
        ("1.5 GB", 1_500_000_000),
        ("1024B", 1_024),
    ],
)
def test_memory_units_match_duckdb_conventions(value: str, expected: int) -> None:
    assert parse_memory_bytes(value) == expected


def test_resource_envelope_is_read_from_environment() -> None:
    envelope = resource_envelope_from_environment(
        {"DUCKDB_THREADS": "4", "DUCKDB_MEMORY": "4GB"}
    )

    assert envelope.duckdb_threads == 4
    assert envelope.duckdb_memory == "4GB"
    assert envelope.duckdb_memory_bytes == 4_000_000_000


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"DUCKDB_MEMORY": "4GB"}, "DUCKDB_THREADS is required"),
        (
            {"DUCKDB_THREADS": "auto", "DUCKDB_MEMORY": "4GB"},
            "positive integer",
        ),
        ({"DUCKDB_THREADS": "4"}, "DUCKDB_MEMORY is required"),
        (
            {"DUCKDB_THREADS": "4", "DUCKDB_MEMORY": "lots"},
            "size such as",
        ),
    ],
)
def test_invalid_resource_envelope_is_rejected(
    environment: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(ConfigurationError, match=message):
        resource_envelope_from_environment(environment)
