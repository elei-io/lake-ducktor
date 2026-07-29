"""Capability adapters for supported DuckLake metadata backends."""

from lakeducktor.adapters.duckdb import DuckDBCapabilitiesAdapter
from lakeducktor.adapters.postgres import PostgresCapabilitiesAdapter
from lakeducktor.adapters.sqlite import SQLiteCapabilitiesAdapter

__all__ = [
    "DuckDBCapabilitiesAdapter",
    "PostgresCapabilitiesAdapter",
    "SQLiteCapabilitiesAdapter",
]
