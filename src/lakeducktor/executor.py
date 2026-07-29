"""Execute one native DuckLake maintenance operation."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from typing import Protocol

import duckdb

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.model import (
    ResourceEnvelope,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)

_TREATMENT_ALIAS = "lakeducktor_treatment"
_STORAGE_SECRET = "lakeducktor_storage"


class ExecutionError(RuntimeError):
    """A selected native maintenance operation could not be completed."""


class TreatmentConnection(Protocol):
    def execute(
        self,
        query: str,
        parameters: tuple[object, ...] | list[object],
    ) -> TreatmentConnection: ...

    def fetchone(self) -> tuple[object, ...] | None: ...


type DuckDBConnectionFactory = Callable[[], duckdb.DuckDBPyConnection]


def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def execute_native_treatment(
    connection: TreatmentConnection,
    selection: TreatmentSelection,
) -> TreatmentResult:
    """Invoke DuckLake for exactly one selected table."""

    if selection.kind is TreatmentKind.MERGE:
        if selection.max_compacted_files is None or selection.max_compacted_files <= 0:
            raise ExecutionError("merge treatment is missing max_compacted_files")
        row = connection.execute(
            """
            SELECT
                coalesce(sum(files_processed), 0)::BIGINT,
                coalesce(sum(files_created), 0)::BIGINT
            FROM ducklake_merge_adjacent_files(
                ?,
                ?,
                schema => ?,
                max_compacted_files => ?
            )
            """,
            [
                _TREATMENT_ALIAS,
                selection.table_name,
                selection.schema_name,
                selection.max_compacted_files,
            ],
        ).fetchone()
    elif selection.kind is TreatmentKind.DELETE_REWRITE:
        row = connection.execute(
            """
            SELECT
                coalesce(sum(files_processed), 0)::BIGINT,
                coalesce(sum(files_created), 0)::BIGINT
            FROM ducklake_rewrite_data_files(
                ?,
                ?,
                schema => ?
            )
            """,
            [
                _TREATMENT_ALIAS,
                selection.table_name,
                selection.schema_name,
            ],
        ).fetchone()
    else:
        raise ExecutionError(f"unsupported treatment kind: {selection.kind}")
    if row is None:
        raise ExecutionError("DuckLake returned no treatment result")
    return TreatmentResult(
        files_processed=int(row[0]),
        files_created=int(row[1]),
    )


def execute_treatment(
    metadata: MetadataConfiguration,
    storage: StorageConfiguration,
    envelope: ResourceEnvelope,
    selection: TreatmentSelection,
    *,
    connect: DuckDBConnectionFactory = _connect_duckdb,
) -> TreatmentResult:
    """Attach one DuckLake writable and execute its selected treatment."""

    connection = connect()
    attached = False
    try:
        connection.execute("SET threads = ?", [envelope.duckdb_threads])
        connection.execute("SET memory_limit = ?", [envelope.duckdb_memory])
        for extension in ("httpfs", "postgres", "ducklake"):
            connection.execute(f"INSTALL {extension}")
            connection.execute(f"LOAD {extension}")
        connection.execute(
            f"""
            CREATE SECRET {_STORAGE_SECRET} (
                TYPE s3,
                KEY_ID ?,
                SECRET ?,
                REGION ?,
                ENDPOINT ?,
                URL_STYLE 'path',
                USE_SSL ?,
                SCOPE ?
            )
            """,
            [
                storage.access_key_id,
                storage.secret_access_key,
                storage.region,
                storage.endpoint,
                storage.use_ssl,
                f"s3://{storage.bucket}",
            ],
        )
        uri = _sql_string(metadata.postgres_uri())
        metadata_schema = _sql_string(selection.metadata_schema)
        connection.execute(
            f"""
            ATTACH 'ducklake:postgres:{uri}' AS {_TREATMENT_ALIAS} (
                METADATA_SCHEMA '{metadata_schema}',
                CREATE_IF_NOT_EXISTS false
            )
            """
        )
        attached = True
        return execute_native_treatment(connection, selection)
    except ExecutionError:
        raise
    except duckdb.Error as error:
        raise ExecutionError("DuckLake treatment failed") from error
    finally:
        if attached:
            with suppress(duckdb.Error):
                connection.execute(f"DETACH {_TREATMENT_ALIAS}")
        connection.close()
