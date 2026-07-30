"""Configuration loading without a runtime settings framework."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from urllib.parse import quote, urlsplit

_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigurationError(ValueError):
    """Configuration is missing or internally inconsistent."""


def load_env_file(path: Path, environment: dict[str, str] | None = None) -> None:
    """Load a small, conventional KEY=VALUE file without overriding the process."""

    target = os.environ if environment is None else environment
    if not path.exists():
        return

    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        if "=" not in line:
            raise ConfigurationError(
                f"{path}:{line_number}: expected an environment assignment"
            )
        name, value = line.split("=", 1)
        name = name.strip()
        if not _ENVIRONMENT_NAME.fullmatch(name):
            raise ConfigurationError(
                f"{path}:{line_number}: invalid environment variable name"
            )
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        target.setdefault(name, value)


def _required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"{name} is required")
    return value


def _comma_separated(environment: Mapping[str, str], name: str) -> tuple[str, ...]:
    values = (item.strip() for item in environment.get(name, "").split(","))
    return tuple(dict.fromkeys(item for item in values if item))


def _positive_float(
    environment: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    raw_value = environment.get(name, str(default)).strip()
    try:
        value = float(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a positive number") from error
    if not isfinite(value) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive number")
    return value


@dataclass(frozen=True, slots=True)
class MetadataConfiguration:
    """Inputs required to locate an existing DuckLake metadata catalogue."""

    backend_hint: str
    host: str
    port: int
    username: str
    password: str
    database: str
    schema: str | None = None
    maintain_lakes: tuple[str, ...] = ()

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> MetadataConfiguration:
        values = os.environ if environment is None else environment
        backend = _required(values, "METADATA_DATABASE").lower()
        if backend == "postgresql":
            backend = "postgres"
        if backend != "postgres":
            raise ConfigurationError(
                "the current attachment scaffold supports METADATA_DATABASE=postgres"
            )

        raw_port = _required(values, "METADATA_DATABASE_PORT")
        try:
            port = int(raw_port)
        except ValueError as error:
            raise ConfigurationError(
                "METADATA_DATABASE_PORT must be an integer"
            ) from error
        if not 1 <= port <= 65535:
            raise ConfigurationError(
                "METADATA_DATABASE_PORT must be between 1 and 65535"
            )

        schema = values.get("METADATA_DATABASE_SCHEMA", "").strip() or None
        return cls(
            backend_hint=backend,
            host=_required(values, "METADATA_DATABASE_HOST"),
            port=port,
            username=_required(values, "METADATA_DATABASE_USERNAME"),
            password=_required(values, "METADATA_DATABASE_PASSWORD"),
            database=_required(values, "METADATA_DATABASE_NAME"),
            schema=schema,
            maintain_lakes=_comma_separated(values, "MAINTAIN_LAKES"),
        )

    def postgres_uri(self) -> str:
        """Return a percent-encoded URI suitable for DuckDB's Postgres extension."""

        username = quote(self.username, safe="")
        password = quote(self.password, safe="")
        host = quote(self.host, safe="[]:.")
        database = quote(self.database, safe="")
        return f"postgresql://{username}:{password}@{host}:{self.port}/{database}"


@dataclass(frozen=True, slots=True)
class StorageConfiguration:
    """Storage access required by a writable DuckLake attachment."""

    provider: str
    data_path: str | None = None
    endpoint: str = ""
    region: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""
    bucket: str = ""
    use_ssl: bool = False

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> StorageConfiguration:
        values = os.environ if environment is None else environment
        provider = _required(values, "CATALOG_STORAGE").lower()
        if provider == "filesystem":
            data_path = Path(_required(values, "CATALOG_DATA_PATH")).expanduser()
            if not data_path.is_absolute():
                raise ConfigurationError(
                    "CATALOG_DATA_PATH must be absolute for filesystem storage"
                )
            return cls(
                provider=provider,
                data_path=f"{str(data_path).rstrip('/')}/",
            )
        if provider != "s3-compatible":
            raise ConfigurationError(
                "CATALOG_STORAGE must be filesystem or s3-compatible"
            )
        raw_endpoint = _required(values, "CATALOG_STORAGE_ENDPOINT")
        parsed = urlsplit(raw_endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ConfigurationError(
                "CATALOG_STORAGE_ENDPOINT must be an http or https URL"
            )
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ConfigurationError(
                "CATALOG_STORAGE_ENDPOINT must not contain a path, query, or fragment"
            )
        return cls(
            provider=provider,
            endpoint=parsed.netloc,
            region=_required(values, "CATALOG_STORAGE_REGION"),
            access_key_id=_required(values, "CATALOG_STORAGE_ACCESS_KEY_ID"),
            secret_access_key=_required(
                values,
                "CATALOG_STORAGE_SECRET_ACCESS_KEY",
            ),
            bucket=_required(values, "CATALOG_STORAGE_BUCKET"),
            use_ssl=parsed.scheme == "https",
        )


@dataclass(frozen=True, slots=True)
class RunConfiguration:
    """Operational settings for one long-lived worker."""

    poll_interval_seconds: float
    treatment_stuck_after_seconds: float
    metrics_host: str
    metrics_port: int
    conflict_backoff_base_seconds: float = 5.0
    conflict_backoff_max_seconds: float = 60.0
    orphan_scan_interval_seconds: float = 3_600.0

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> RunConfiguration:
        values = os.environ if environment is None else environment
        raw_port = values.get("METRICS_PORT", "8000").strip()
        try:
            metrics_port = int(raw_port)
        except ValueError as error:
            raise ConfigurationError("METRICS_PORT must be an integer") from error
        if not 1 <= metrics_port <= 65535:
            raise ConfigurationError("METRICS_PORT must be between 1 and 65535")
        metrics_host = values.get("METRICS_HOST", "0.0.0.0").strip()
        if not metrics_host:
            raise ConfigurationError("METRICS_HOST must not be empty")
        conflict_backoff_base_seconds = _positive_float(
            values,
            "CONFLICT_BACKOFF_BASE_SECONDS",
            5,
        )
        conflict_backoff_max_seconds = _positive_float(
            values,
            "CONFLICT_BACKOFF_MAX_SECONDS",
            60,
        )
        if conflict_backoff_max_seconds < conflict_backoff_base_seconds:
            raise ConfigurationError(
                "CONFLICT_BACKOFF_MAX_SECONDS must be greater than or equal to "
                "CONFLICT_BACKOFF_BASE_SECONDS"
            )
        return cls(
            poll_interval_seconds=_positive_float(
                values,
                "POLL_INTERVAL_SECONDS",
                60,
            ),
            treatment_stuck_after_seconds=_positive_float(
                values,
                "TREATMENT_STUCK_AFTER_SECONDS",
                3_600,
            ),
            metrics_host=metrics_host,
            metrics_port=metrics_port,
            conflict_backoff_base_seconds=conflict_backoff_base_seconds,
            conflict_backoff_max_seconds=conflict_backoff_max_seconds,
            orphan_scan_interval_seconds=_positive_float(
                values,
                "ORPHAN_SCAN_INTERVAL_SECONDS",
                3_600,
            ),
        )
