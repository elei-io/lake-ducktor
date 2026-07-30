"""Process-scoped DuckDB connection settings."""

from __future__ import annotations

import os
from collections.abc import Mapping

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


def connection_config(
    defaults: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return settings that must be applied independently to every connection."""

    config = dict(defaults or {})
    if os.environ.get("DUCKDB_ALLOW_UNSIGNED_EXTENSIONS", "").strip().lower() in (
        _TRUE_VALUES
    ):
        config["allow_unsigned_extensions"] = "true"
    return config
