"""DuckLake attachment and catalogue inspection."""

from __future__ import annotations

from collections.abc import Iterable

import duckdb

from lakeducktor.config import MetadataConfiguration
from lakeducktor.model import BackendDetection, DuckDBExtension, MetadataBackend

_PROBE_ALIAS = "lakeducktor_metadata_probe"
_LAKE_ALIAS = "lakeducktor_lake"
_DUCKLAKE_SIGNATURE = frozenset(
    {
        "ducklake_metadata",
        "ducklake_snapshot",
        "ducklake_schema",
        "ducklake_table",
    }
)


class BackendDetectionError(RuntimeError):
    """An existing DuckLake could not be identified safely."""


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _loaded_duckdb_extensions(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[DuckDBExtension, ...]:
    rows = connection.execute(
        """
        SELECT extension_name, extension_version, install_mode, installed_from
        FROM duckdb_extensions()
        WHERE loaded
        ORDER BY extension_name
        """
    ).fetchall()
    return tuple(
        DuckDBExtension(
            name=str(name),
            version=str(version or "unknown"),
            install_mode=str(install_mode or "unknown").lower(),
            source=str(installed_from or "built-in"),
        )
        for name, version, install_mode, installed_from in rows
    )


def _select_metadata_schemas(
    candidates: Iterable[str],
    configured: str | None,
    maintained: Iterable[str] = (),
) -> tuple[str, ...]:
    available = tuple(sorted(set(candidates)))
    if configured is not None:
        if configured not in available:
            raise BackendDetectionError(
                "METADATA_DATABASE_SCHEMA does not contain a DuckLake catalogue"
            )
        available = (configured,)
    if not available:
        raise BackendDetectionError(
            "no DuckLake metadata schema was found in the configured database"
        )
    requested = tuple(dict.fromkeys(maintained))
    if requested:
        missing = sorted(set(requested).difference(available))
        if missing:
            names = ", ".join(missing)
            raise BackendDetectionError(
                f"MAINTAIN_LAKES contains unknown or excluded lakes: {names}"
            )
        requested_set = set(requested)
        available = tuple(
            metadata_schema
            for metadata_schema in available
            if metadata_schema in requested_set
        )
    return available


def _discover_postgres_schemas(
    connection: duckdb.DuckDBPyConnection,
    postgres_uri: str,
    configured: str | None,
    maintained: Iterable[str] = (),
) -> tuple[str, ...]:
    uri = _sql_string(postgres_uri)
    connection.execute(f"ATTACH '{uri}' AS {_PROBE_ALIAS} (TYPE postgres, READ_ONLY)")
    try:
        placeholders = ", ".join("?" for _ in _DUCKLAKE_SIGNATURE)
        rows = connection.execute(
            f"""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_catalog = ?
              AND table_name IN ({placeholders})
            GROUP BY table_schema
            HAVING count(DISTINCT table_name) = ?
            ORDER BY table_schema
            """,
            [
                _PROBE_ALIAS,
                *_DUCKLAKE_SIGNATURE,
                len(_DUCKLAKE_SIGNATURE),
            ],
        ).fetchall()
        return _select_metadata_schemas(
            (str(row[0]) for row in rows),
            configured,
            maintained,
        )
    finally:
        connection.execute(f"DETACH {_PROBE_ALIAS}")


def detect_metadata_backend(
    configuration: MetadataConfiguration,
) -> BackendDetection:
    """Attach to an existing lake read-only and ask DuckLake for its backend."""

    if configuration.backend_hint != "postgres":
        raise BackendDetectionError(
            f"unsupported metadata attachment hint: {configuration.backend_hint}"
        )

    connection = duckdb.connect(database=":memory:", config={"threads": "1"})
    try:
        connection.execute("PRAGMA disable_checkpoint_on_shutdown")
        connection.execute("INSTALL postgres")
        connection.execute("LOAD postgres")
        connection.execute("INSTALL ducklake")
        connection.execute("LOAD ducklake")

        postgres_uri = configuration.postgres_uri()
        metadata_schemas = _discover_postgres_schemas(
            connection,
            postgres_uri,
            configuration.schema,
            configuration.maintain_lakes,
        )
        uri = _sql_string(postgres_uri)
        detected: set[tuple[MetadataBackend, str]] = set()
        for index, metadata_schema in enumerate(metadata_schemas):
            lake_alias = f"{_LAKE_ALIAS}_{index}"
            schema = _sql_string(metadata_schema)
            connection.execute(
                f"""
                ATTACH 'ducklake:postgres:{uri}' AS {lake_alias} (
                    METADATA_SCHEMA '{schema}',
                    READ_ONLY,
                    CREATE_IF_NOT_EXISTS false
                )
                """
            )
            row = connection.execute(
                """
                SELECT catalog_type, extension_version
                FROM ducklake_settings(?)
                """,
                [lake_alias],
            ).fetchone()
            if row is None:
                raise BackendDetectionError(
                    "DuckLake returned no metadata backend information"
                )
            try:
                backend = MetadataBackend(str(row[0]).lower())
            except ValueError as error:
                raise BackendDetectionError(
                    f"DuckLake reported an unsupported metadata backend: {row[0]}"
                ) from error
            detected.add((backend, str(row[1])))

        if len(detected) != 1:
            raise BackendDetectionError(
                "the discovered DuckLake schemas reported inconsistent backends"
            )
        backend, extension_version = detected.pop()
        return BackendDetection(
            backend=backend,
            metadata_schemas=metadata_schemas,
            extension_version=extension_version,
            duckdb_extensions=_loaded_duckdb_extensions(connection),
        )
    except BackendDetectionError:
        raise
    except duckdb.Error as error:
        raise BackendDetectionError(
            "could not inspect the configured DuckLake catalogue"
        ) from error
    finally:
        connection.close()
