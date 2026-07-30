from pathlib import Path

import pytest

from lakeducktor.config import (
    ConfigurationError,
    MetadataConfiguration,
    RunConfiguration,
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


def test_run_configuration_defaults_are_boring() -> None:
    configuration = RunConfiguration.from_environment({})

    assert configuration.poll_interval_seconds == 60
    assert configuration.treatment_stuck_after_seconds == 3_600
    assert configuration.metrics_host == "0.0.0.0"
    assert configuration.metrics_port == 8_000
    assert configuration.conflict_backoff_base_seconds == 5
    assert configuration.conflict_backoff_max_seconds == 60


def test_run_configuration_accepts_operational_overrides() -> None:
    configuration = RunConfiguration.from_environment(
        {
            "POLL_INTERVAL_SECONDS": "2.5",
            "TREATMENT_STUCK_AFTER_SECONDS": "900",
            "METRICS_HOST": "127.0.0.1",
            "METRICS_PORT": "9090",
            "CONFLICT_BACKOFF_BASE_SECONDS": "3",
            "CONFLICT_BACKOFF_MAX_SECONDS": "30",
        }
    )

    assert configuration.poll_interval_seconds == 2.5
    assert configuration.treatment_stuck_after_seconds == 900
    assert configuration.metrics_host == "127.0.0.1"
    assert configuration.metrics_port == 9_090
    assert configuration.conflict_backoff_base_seconds == 3
    assert configuration.conflict_backoff_max_seconds == 30


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("POLL_INTERVAL_SECONDS", "0"),
        ("POLL_INTERVAL_SECONDS", "nan"),
        ("TREATMENT_STUCK_AFTER_SECONDS", "-1"),
        ("METRICS_HOST", " "),
        ("METRICS_PORT", "65536"),
        ("CONFLICT_BACKOFF_BASE_SECONDS", "0"),
        ("CONFLICT_BACKOFF_MAX_SECONDS", "nan"),
    ],
)
def test_invalid_run_configuration_is_rejected(name: str, value: str) -> None:
    with pytest.raises(ConfigurationError):
        RunConfiguration.from_environment({name: value})
