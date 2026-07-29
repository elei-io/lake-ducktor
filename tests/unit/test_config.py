from pathlib import Path

import pytest

from lakeducktor.config import (
    ConfigurationError,
    MetadataConfiguration,
    StorageConfiguration,
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


def test_s3_compatible_storage_configuration_parses_endpoint() -> None:
    configuration = StorageConfiguration.from_environment(
        {
            "CATALOG_STORAGE": "s3-compatible",
            "CATALOG_STORAGE_ENDPOINT": "https://objects.example:9443",
            "CATALOG_STORAGE_REGION": "ap-northeast-1",
            "CATALOG_STORAGE_ACCESS_KEY_ID": "key",
            "CATALOG_STORAGE_SECRET_ACCESS_KEY": "secret",
            "CATALOG_STORAGE_BUCKET": "lake",
        }
    )

    assert configuration.endpoint == "objects.example:9443"
    assert configuration.use_ssl is True
    assert configuration.bucket == "lake"


def test_storage_endpoint_path_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="must not contain"):
        StorageConfiguration.from_environment(
            {
                "CATALOG_STORAGE": "s3-compatible",
                "CATALOG_STORAGE_ENDPOINT": "http://objects.example/path",
                "CATALOG_STORAGE_REGION": "us-east-1",
                "CATALOG_STORAGE_ACCESS_KEY_ID": "key",
                "CATALOG_STORAGE_SECRET_ACCESS_KEY": "secret",
                "CATALOG_STORAGE_BUCKET": "lake",
            }
        )
