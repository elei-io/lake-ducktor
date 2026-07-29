"""Configuration loading without a runtime settings framework."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

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
        )

    def postgres_uri(self) -> str:
        """Return a percent-encoded URI suitable for DuckDB's Postgres extension."""

        username = quote(self.username, safe="")
        password = quote(self.password, safe="")
        host = quote(self.host, safe="[]:.")
        database = quote(self.database, safe="")
        return f"postgresql://{username}:{password}@{host}:{self.port}/{database}"
