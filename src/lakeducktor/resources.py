"""Parse the fixed resource envelope assigned to one worker."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from lakeducktor.config import ConfigurationError
from lakeducktor.model import ResourceEnvelope

_MEMORY = re.compile(
    r"^(?P<amount>(?:\d+(?:\.\d*)?|\.\d+))\s*"
    r"(?P<unit>B|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$",
    re.IGNORECASE,
)
_MEMORY_MULTIPLIERS = {
    "B": 1,
    "KB": 1_000,
    "MB": 1_000_000,
    "GB": 1_000_000_000,
    "TB": 1_000_000_000_000,
    "KIB": 1 << 10,
    "MIB": 1 << 20,
    "GIB": 1 << 30,
    "TIB": 1 << 40,
}


def parse_memory_bytes(value: str) -> int:
    """Parse the decimal and binary memory units accepted by DuckDB."""

    match = _MEMORY.fullmatch(value.strip())
    if match is None:
        raise ConfigurationError("DUCKDB_MEMORY must be a size such as 4GB or 4096MiB")
    try:
        amount = Decimal(match.group("amount"))
    except InvalidOperation as error:
        raise ConfigurationError(
            "DUCKDB_MEMORY must be a size such as 4GB or 4096MiB"
        ) from error
    memory_bytes = int(amount * _MEMORY_MULTIPLIERS[match.group("unit").upper()])
    if memory_bytes <= 0:
        raise ConfigurationError("DUCKDB_MEMORY must be greater than zero")
    return memory_bytes


def resource_envelope_from_environment(
    environment: Mapping[str, str] | None = None,
) -> ResourceEnvelope:
    """Read the immutable execution envelope for this worker."""

    values = os.environ if environment is None else environment
    raw_threads = values.get("DUCKDB_THREADS", "").strip()
    if not raw_threads:
        raise ConfigurationError("DUCKDB_THREADS is required for select")
    if not raw_threads.isdecimal():
        raise ConfigurationError("DUCKDB_THREADS must be a positive integer")
    threads = int(raw_threads)
    if threads <= 0:
        raise ConfigurationError("DUCKDB_THREADS must be a positive integer")

    memory = values.get("DUCKDB_MEMORY", "").strip()
    if not memory:
        raise ConfigurationError("DUCKDB_MEMORY is required for select")
    return ResourceEnvelope(
        duckdb_threads=threads,
        duckdb_memory=memory,
        duckdb_memory_bytes=parse_memory_bytes(memory),
    )
