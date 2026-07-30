"""Execute one native DuckLake maintenance operation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing import get_context
from multiprocessing.connection import Connection
from threading import Event
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


class ExecutionFailureReason(StrEnum):
    CONCURRENT_COMPACTION = "concurrent_compaction"
    INTERRUPTED = "interrupted"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    SNAPSHOT_RETRY_EXHAUSTED = "snapshot_retry_exhausted"
    STORAGE_ERROR = "storage_error"
    UNKNOWN = "unknown"


_TRANSIENT_REASONS = {
    ExecutionFailureReason.CONCURRENT_COMPACTION,
    ExecutionFailureReason.SNAPSHOT_RETRY_EXHAUSTED,
}


class ExecutionError(RuntimeError):
    """A selected native maintenance operation could not be completed."""

    def __init__(
        self,
        message: str,
        *,
        reason: ExecutionFailureReason = ExecutionFailureReason.UNKNOWN,
    ) -> None:
        super().__init__(message)
        self.reason = reason

    @property
    def transient(self) -> bool:
        return self.reason in _TRANSIENT_REASONS


class TreatmentConnection(Protocol):
    def execute(
        self,
        query: str,
        parameters: tuple[object, ...] | list[object],
    ) -> TreatmentConnection: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


type DuckDBConnectionFactory = Callable[[], duckdb.DuckDBPyConnection]


def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def classify_duckdb_error(error: duckdb.Error) -> ExecutionFailureReason:
    message = str(error).lower()
    if "another transaction has compacted" in message:
        return ExecutionFailureReason.CONCURRENT_COMPACTION
    if (
        "exceeded the maximum retry count" in message
        or "ducklake_snapshot_pkey" in message
    ):
        return ExecutionFailureReason.SNAPSHOT_RETRY_EXHAUSTED
    if "out of memory" in message or "failed to allocate" in message:
        return ExecutionFailureReason.RESOURCE_EXHAUSTED
    if any(
        marker in message
        for marker in (
            "http error",
            "io error",
            "s3",
            "connection reset",
            "connection refused",
        )
    ):
        return ExecutionFailureReason.STORAGE_ERROR
    return ExecutionFailureReason.UNKNOWN


def concise_duckdb_error(error: duckdb.Error, limit: int = 500) -> str:
    """Return one bounded log-safe line retaining the native failure cause."""

    message = " ".join(str(error).split())
    if len(message) > limit:
        message = message[: limit - 3] + "..."
    return f"{type(error).__name__}: {message}"


def execute_native_treatment(
    connection: TreatmentConnection,
    selection: TreatmentSelection,
) -> TreatmentResult:
    """Invoke DuckLake for exactly one selected table."""

    if selection.kind is TreatmentKind.MERGE:
        if selection.max_compacted_files is None or selection.max_compacted_files <= 0:
            raise ExecutionError("merge treatment is missing max_compacted_files")
        if (
            selection.execution_target_file_size_bytes is None
            or selection.execution_target_file_size_bytes <= 0
        ):
            raise ExecutionError("merge treatment is missing execution target size")
        rows = connection.execute(
            """
            SELECT
                coalesce(sum(files_processed), 0)::BIGINT,
                coalesce(sum(files_created), 0)::BIGINT
            FROM ducklake_merge_adjacent_files(
                ?,
                ?,
                schema => ?,
                max_compacted_files => ?,
                max_file_size => ?
            )
            """,
            [
                _TREATMENT_ALIAS,
                selection.table_name,
                selection.schema_name,
                selection.max_compacted_files,
                selection.execution_target_file_size_bytes,
            ],
        ).fetchall()
    elif selection.kind is TreatmentKind.DELETE_REWRITE:
        rows = connection.execute(
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
        ).fetchall()
    else:
        raise ExecutionError(f"unsupported treatment kind: {selection.kind}")
    if len(rows) != 1 or len(rows[0]) != 2:
        raise ExecutionError("DuckLake returned an invalid treatment result")
    row = rows[0]
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
    try:
        connection.execute("PRAGMA disable_checkpoint_on_shutdown")
        connection.execute("SET threads = ?", [envelope.duckdb_threads])
        connection.execute("SET memory_limit = ?", [envelope.duckdb_memory])
        for extension in ("httpfs", "postgres", "ducklake"):
            connection.execute(f"INSTALL {extension}")
            connection.execute(f"LOAD {extension}")
        if selection.kind is TreatmentKind.MERGE:
            if (
                selection.execution_target_file_size_bytes is None
                or selection.execution_target_file_size_bytes <= 0
            ):
                raise ExecutionError("merge treatment is missing execution target size")
            connection.execute(
                "SET ducklake_target_file_size = ?",
                [f"{selection.execution_target_file_size_bytes}B"],
            )
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
        return execute_native_treatment(connection, selection)
    except ExecutionError:
        raise
    except duckdb.Error as error:
        reason = classify_duckdb_error(error)
        raise ExecutionError(
            "DuckLake treatment failed "
            f"reason={reason.value} cause={concise_duckdb_error(error)}",
            reason=reason,
        ) from error
    finally:
        connection.close()


def _isolated_child(
    result_pipe: Connection,
    metadata: MetadataConfiguration,
    storage: StorageConfiguration,
    envelope: ResourceEnvelope,
    selection: TreatmentSelection,
) -> None:
    try:
        result = execute_treatment(metadata, storage, envelope, selection)
        result_pipe.send(("success", result.files_processed, result.files_created))
    except ExecutionError as error:
        result_pipe.send(("error", error.reason.value, str(error)))
    except BaseException as error:
        result_pipe.send(("error", ExecutionFailureReason.UNKNOWN.value, repr(error)))
    finally:
        result_pipe.close()


@dataclass(slots=True)
class IsolatedTreatmentExecutor:
    """Execute each treatment in a spawned process with one private connection."""

    stop_event: Event
    poll_interval_seconds: float = 0.1

    def __call__(
        self,
        metadata: MetadataConfiguration,
        storage: StorageConfiguration,
        envelope: ResourceEnvelope,
        selection: TreatmentSelection,
    ) -> TreatmentResult:
        context = get_context("spawn")
        receive_pipe, send_pipe = context.Pipe(duplex=False)
        process = context.Process(
            target=_isolated_child,
            args=(send_pipe, metadata, storage, envelope, selection),
            name="lakeducktor-treatment",
        )
        process.start()
        send_pipe.close()
        try:
            while True:
                if receive_pipe.poll(self.poll_interval_seconds):
                    message = receive_pipe.recv()
                    break
                if self.stop_event.is_set():
                    process.terminate()
                    process.join(timeout=5)
                    if process.is_alive():
                        process.kill()
                        process.join()
                    raise ExecutionError(
                        "DuckLake treatment interrupted by shutdown",
                        reason=ExecutionFailureReason.INTERRUPTED,
                    )
                if not process.is_alive():
                    raise ExecutionError(
                        f"DuckLake treatment process exited code={process.exitcode}",
                    )
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join()
                raise ExecutionError("DuckLake treatment process did not exit")
        finally:
            receive_pipe.close()
            if process.is_alive():
                process.kill()
                process.join()

        if message[0] == "success":
            return TreatmentResult(
                files_processed=int(message[1]),
                files_created=int(message[2]),
            )
        reason = ExecutionFailureReason(str(message[1]))
        raise ExecutionError(str(message[2]), reason=reason)
