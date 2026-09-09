"""Execute one native DuckLake maintenance operation."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing import get_context
from multiprocessing.connection import Connection
from threading import Event
from typing import Protocol
from urllib.parse import quote

import duckdb

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.duckdb_config import connection_config
from lakeducktor.model import (
    ResourceEnvelope,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)

_TREATMENT_ALIAS = "lakeducktor_treatment"
_STORAGE_SECRET = "lakeducktor_storage"
_DUCKLAKE_MAX_RETRY_COUNT = 20
_DUCKLAKE_RETRY_WAIT_MS = 100
_DUCKLAKE_RETRY_BACKOFF = 1.2


class ExecutionFailureReason(StrEnum):
    CONCURRENT_COMPACTION = "concurrent_compaction"
    TRANSACTION_CONFLICT = "transaction_conflict"
    INTERRUPTED = "interrupted"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    SNAPSHOT_RETRY_EXHAUSTED = "snapshot_retry_exhausted"
    STORAGE_ERROR = "storage_error"
    UNKNOWN = "unknown"


_TRANSIENT_REASONS = {
    ExecutionFailureReason.CONCURRENT_COMPACTION,
    ExecutionFailureReason.TRANSACTION_CONFLICT,
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
    return duckdb.connect(database=":memory:", config=connection_config())


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
    if "transaction conflict" in message and any(
        marker in message
        for marker in (
            "another transaction has inserted",
            "another transaction has deleted",
        )
    ):
        return ExecutionFailureReason.TRANSACTION_CONFLICT
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

    message = str(error)
    # Redact before truncation so a long message cannot expose a partial secret.
    for name in (
        "METADATA_DATABASE_PASSWORD",
        "CATALOG_STORAGE_ACCESS_KEY_ID",
        "CATALOG_STORAGE_SECRET_ACCESS_KEY",
    ):
        value = os.environ.get(name, "")
        if value:
            for representation in sorted(
                {value, quote(value, safe=""), value.replace("'", "''")},
                key=len,
                reverse=True,
            ):
                message = message.replace(representation, "[REDACTED]")
    message = re.sub(
        r"([a-z][a-z0-9+.-]*://)[^/\s@]+@",
        r"\1[REDACTED]@",
        message,
        flags=re.IGNORECASE,
    )
    message = " ".join(message.split())
    if len(message) > limit:
        message = message[: limit - 3] + "..."
    return f"{type(error).__name__}: {message}"


def execute_native_treatment(
    connection: TreatmentConnection,
    selection: TreatmentSelection,
) -> TreatmentResult:
    """Invoke DuckLake for exactly one selected table."""

    rows_processed = 0
    if selection.kind is TreatmentKind.SNAPSHOT_EXPIRATION:
        rows = connection.execute(
            """
            SELECT count(*)::BIGINT
            FROM ducklake_expire_snapshots(?)
            """,
            [_TREATMENT_ALIAS],
        ).fetchall()
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ExecutionError("DuckLake returned an invalid expiration result")
        return TreatmentResult(
            files_processed=0,
            files_created=0,
            snapshots_processed=int(rows[0][0]),
        )
    if selection.kind is TreatmentKind.SCHEDULED_FILE_CLEANUP:
        rows = connection.execute(
            """
            SELECT count(*)::BIGINT
            FROM ducklake_cleanup_old_files(?)
            """,
            [_TREATMENT_ALIAS],
        ).fetchall()
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ExecutionError("DuckLake returned an invalid cleanup result")
        return TreatmentResult(
            files_processed=int(rows[0][0]),
            files_created=0,
        )
    if selection.kind is TreatmentKind.ORPHAN_FILE_CLEANUP:
        rows = connection.execute(
            """
            SELECT count(*)::BIGINT
            FROM ducklake_delete_orphaned_files(?)
            """,
            [_TREATMENT_ALIAS],
        ).fetchall()
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ExecutionError("DuckLake returned an invalid orphan cleanup result")
        return TreatmentResult(
            files_processed=int(rows[0][0]),
            files_created=0,
        )
    if (
        selection.table_id is None
        or selection.schema_name is None
        or selection.table_name is None
    ):
        raise ExecutionError(
            f"table-scoped treatment is missing its table: {selection.kind}"
        )
    if selection.kind is TreatmentKind.INLINE_FLUSH:
        files_before = _active_table_file_count(connection, selection)
        rows = connection.execute(
            """
            SELECT coalesce(sum(rows_flushed), 0)::BIGINT
            FROM ducklake_flush_inlined_data(
                ?,
                table_name => ?,
                schema_name => ?
            )
            """,
            [
                _TREATMENT_ALIAS,
                selection.table_name,
                selection.schema_name,
            ],
        ).fetchall()
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ExecutionError("DuckLake returned an invalid treatment result")
        rows_processed = int(rows[0][0])
        files_after = _active_table_file_count(connection, selection)
        rows = [(0, max(0, files_after - files_before))]
    elif selection.kind is TreatmentKind.MERGE:
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
        rows_processed=rows_processed,
    )


def _active_table_file_count(
    connection: TreatmentConnection,
    selection: TreatmentSelection,
) -> int:
    rows = connection.execute(
        """
        SELECT
            count(DISTINCT data_file)::BIGINT
            + count(DISTINCT delete_file)::BIGINT
        FROM ducklake_list_files(
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
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ExecutionError("DuckLake returned an invalid active-file count")
    return int(rows[0][0])


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
        extensions = (
            ("httpfs", "postgres", "ducklake")
            if storage.provider == "s3-compatible"
            else ("postgres", "ducklake")
        )
        for extension in extensions:
            connection.execute(f"INSTALL {extension}")
            connection.execute(f"LOAD {extension}")
        connection.execute(
            "SET ducklake_max_retry_count = ?",
            [_DUCKLAKE_MAX_RETRY_COUNT],
        )
        connection.execute(
            "SET ducklake_retry_wait_ms = ?",
            [_DUCKLAKE_RETRY_WAIT_MS],
        )
        connection.execute(
            "SET ducklake_retry_backoff = ?",
            [_DUCKLAKE_RETRY_BACKOFF],
        )
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
        if storage.provider == "s3-compatible":
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
        storage_options = ""
        if storage.provider == "filesystem":
            if storage.data_path is None:
                raise ExecutionError("filesystem storage is missing its data path")
            data_path = _sql_string(storage.data_path)
            storage_options = f", DATA_PATH '{data_path}', OVERRIDE_DATA_PATH true"
        connection.execute(
            f"""
            ATTACH 'ducklake:postgres:{uri}' AS {_TREATMENT_ALIAS} (
                METADATA_SCHEMA '{metadata_schema}',
                CREATE_IF_NOT_EXISTS false
                {storage_options}
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
        result_pipe.send(
            (
                "success",
                result.files_processed,
                result.files_created,
                result.rows_processed,
                result.snapshots_processed,
            )
        )
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
                rows_processed=int(message[3]),
                snapshots_processed=int(message[4]),
            )
        reason = ExecutionFailureReason(str(message[1]))
        raise ExecutionError(str(message[2]), reason=reason)
