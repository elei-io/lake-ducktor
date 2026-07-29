from pathlib import Path

import pytest

from lakeducktor.config import (
    ConfigurationError,
    MetadataConfiguration,
    load_env_file,
)


def postgres_environment(**overrides: str) -> dict[str, str]:
    environment = {
        "METADATA_DATABASE": "postgres",
        "METADATA_DATABASE_HOST": "catalog.example",
        "METADATA_DATABASE_PORT": "5432",
        "METADATA_DATABASE_USERNAME": "lake user",
        "METADATA_DATABASE_PASSWORD": "p@ss:/word",
        "METADATA_DATABASE_NAME": "lake/db",
    }
    environment.update(overrides)
    return environment


def test_postgres_uri_encodes_credentials_and_database() -> None:
    configuration = MetadataConfiguration.from_environment(postgres_environment())

    assert (
        configuration.postgres_uri() == "postgresql://lake%20user:p%40ss%3A%2Fword"
        "@catalog.example:5432/lake%2Fdb"
    )


def test_invalid_port_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="must be an integer"):
        MetadataConfiguration.from_environment(
            postgres_environment(METADATA_DATABASE_PORT="postgres")
        )


def test_maintain_lakes_is_empty_by_default() -> None:
    configuration = MetadataConfiguration.from_environment(postgres_environment())

    assert configuration.maintain_lakes == ()


def test_maintain_lakes_is_trimmed_and_deduplicated() -> None:
    configuration = MetadataConfiguration.from_environment(
        postgres_environment(MAINTAIN_LAKES=" lake_b, lake_a, lake_b, ,")
    )

    assert configuration.maintain_lakes == ("lake_b", "lake_a")


def test_env_file_does_not_override_process_values(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("EXISTING=file\nNEW_VALUE='loaded'\n", encoding="utf-8")
    environment = {"EXISTING": "process"}

    load_env_file(path, environment)

    assert environment == {"EXISTING": "process", "NEW_VALUE": "loaded"}
