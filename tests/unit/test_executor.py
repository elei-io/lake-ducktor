from dataclasses import replace

import duckdb
import pytest

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.executor import (
    ExecutionError,
    ExecutionFailureReason,
    classify_duckdb_error,
    concise_duckdb_error,
    execute_native_treatment,
    execute_treatment,
)
from lakeducktor.model import ResourceEnvelope, TreatmentKind, TreatmentSelection


class FakeConnection:
    def __init__(
        self,
        rows: list[tuple[object, ...]] | None = None,
        *,
        row_batches: list[list[tuple[object, ...]]] | None = None,
    ) -> None:
        self.rows = [(4, 2)] if rows is None else rows
        self.row_batches = list(row_batches) if row_batches is not None else None
        self.query = ""
        self.parameters: list[object] = []
        self.queries: list[tuple[str, list[object]]] = []
        self.closed = False
        self.result_exhausted = False

    def execute(
        self,
        query: str,
        parameters: tuple[object, ...] | list[object] = (),
    ) -> FakeConnection:
        self.query = query
        self.parameters = list(parameters)
        self.queries.append((query, self.parameters))
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        self.result_exhausted = True
        if self.row_batches is not None:
            return self.row_batches.pop(0)
        return self.rows

    def close(self) -> None:
        self.closed = True


def selection(
    kind: TreatmentKind,
    *,
    max_compacted_files: int | None,
) -> TreatmentSelection:
    return TreatmentSelection(
        kind=kind,
        priority_rank=1,
        metadata_schema="lake",
        table_id=7,
        schema_name="analytics",
        table_name="events",
        input_bytes=100,
        admitted_bytes=200,
        sorting_enabled=False,
        memory_headroom_bytes=1_000,
        usable_memory_bytes=3_000,
        max_compacted_files=max_compacted_files,
        execution_target_file_size_bytes=100 if max_compacted_files else None,
    )


def test_merge_calls_native_function_with_schema_and_bound() -> None:
    connection = FakeConnection()

    result = execute_native_treatment(
        connection,
        selection(TreatmentKind.MERGE, max_compacted_files=3),
    )

    assert "ducklake_merge_adjacent_files" in connection.query
    assert connection.parameters == [
        "lakeducktor_treatment",
        "events",
        "analytics",
        3,
        100,
    ]
    assert result.files_processed == 4
    assert result.files_created == 2
    assert connection.result_exhausted is True


def test_rewrite_calls_native_function_without_overriding_threshold() -> None:
    connection = FakeConnection()

    execute_native_treatment(
        connection,
        selection(TreatmentKind.DELETE_REWRITE, max_compacted_files=None),
    )

    assert "ducklake_rewrite_data_files" in connection.query
    assert "delete_threshold" not in connection.query
    assert connection.parameters == [
        "lakeducktor_treatment",
        "events",
        "analytics",
    ]


def test_inline_flush_calls_native_table_scoped_function() -> None:
    connection = FakeConnection(
        row_batches=[
            [(3,)],
            [(50,)],
            [(5,)],
        ]
    )

    result = execute_native_treatment(
        connection,
        selection(TreatmentKind.INLINE_FLUSH, max_compacted_files=None),
    )

    flush = next(
        (query, parameters)
        for query, parameters in connection.queries
        if "ducklake_flush_inlined_data" in query
    )
    assert flush[1] == [
        "lakeducktor_treatment",
        "events",
        "analytics",
    ]
    assert (
        sum("ducklake_list_files" in query for query, _parameters in connection.queries)
        == 2
    )
    assert connection.row_batches == []
    assert result.rows_processed == 50
    assert result.files_processed == 0
    assert result.files_created == 2


def test_scheduled_cleanup_uses_native_policy_without_overrides() -> None:
    connection = FakeConnection(rows=[(7,)])
    chosen = replace(
        selection(TreatmentKind.SCHEDULED_FILE_CLEANUP, max_compacted_files=None),
        table_id=None,
        schema_name=None,
        table_name=None,
    )

    result = execute_native_treatment(connection, chosen)

    assert "ducklake_cleanup_old_files" in connection.query
    assert "older_than" not in connection.query
    assert "cleanup_all" not in connection.query
    assert connection.parameters == ["lakeducktor_treatment"]
    assert result.files_processed == 7
    assert result.files_created == 0


def test_snapshot_expiration_uses_native_policy_without_overrides() -> None:
    connection = FakeConnection(rows=[(4,)])
    chosen = replace(
        selection(TreatmentKind.SNAPSHOT_EXPIRATION, max_compacted_files=None),
        table_id=None,
        schema_name=None,
        table_name=None,
    )

    result = execute_native_treatment(connection, chosen)

    assert "ducklake_expire_snapshots" in connection.query
    assert "older_than" not in connection.query
    assert "versions" not in connection.query
    assert connection.parameters == ["lakeducktor_treatment"]
    assert result.snapshots_processed == 4
    assert result.files_processed == 0


def test_orphan_cleanup_uses_native_policy_without_overrides() -> None:
    connection = FakeConnection(rows=[(9,)])
    chosen = replace(
        selection(TreatmentKind.ORPHAN_FILE_CLEANUP, max_compacted_files=None),
        table_id=None,
        schema_name=None,
        table_name=None,
    )

    result = execute_native_treatment(connection, chosen)

    assert "ducklake_delete_orphaned_files" in connection.query
    assert "older_than" not in connection.query
    assert connection.parameters == ["lakeducktor_treatment"]
    assert result.files_processed == 9
    assert result.files_created == 0


def test_merge_requires_native_batch_bound() -> None:
    with pytest.raises(ExecutionError, match="max_compacted_files"):
        execute_native_treatment(
            FakeConnection(),
            selection(TreatmentKind.MERGE, max_compacted_files=None),
        )


@pytest.mark.parametrize("rows", [[], [(4,)], [(4, 2), (3, 1)]])
def test_native_result_must_be_fully_drained_to_exactly_one_row(
    rows: list[tuple[object, ...]],
) -> None:
    connection = FakeConnection(rows)

    with pytest.raises(ExecutionError, match="invalid treatment result"):
        execute_native_treatment(
            connection,
            selection(TreatmentKind.MERGE, max_compacted_files=3),
        )

    assert connection.result_exhausted is True


def test_executor_configures_writable_connection_without_exposing_secrets() -> None:
    connection = FakeConnection()
    metadata = MetadataConfiguration(
        backend_hint="postgres",
        host="catalog.example",
        port=5432,
        username="user",
        password="password",
        database="lake",
    )
    storage = StorageConfiguration(
        provider="s3-compatible",
        endpoint="objects.example",
        region="us-east-1",
        access_key_id="access",
        secret_access_key="secret",
        bucket="lake",
        use_ssl=True,
    )

    result = execute_treatment(
        metadata,
        storage,
        ResourceEnvelope(4, "4GB", 4_000_000_000),
        selection(TreatmentKind.MERGE, max_compacted_files=3),
        connect=lambda: connection,
    )

    assert result.files_processed == 4
    assert connection.result_exhausted is True
    assert connection.closed is True
    queries = [query for query, _parameters in connection.queries]
    assert "PRAGMA disable_checkpoint_on_shutdown" in queries
    assert "SET threads = ?" in queries
    assert "SET memory_limit = ?" in queries
    assert "SET ducklake_max_retry_count = ?" in queries
    assert "SET ducklake_retry_wait_ms = ?" in queries
    assert "SET ducklake_retry_backoff = ?" in queries
    assert "SET ducklake_target_file_size = ?" in queries
    query_parameters = dict(connection.queries)
    assert query_parameters["SET ducklake_max_retry_count = ?"] == [20]
    assert query_parameters["SET ducklake_retry_wait_ms = ?"] == [100]
    assert query_parameters["SET ducklake_retry_backoff = ?"] == [1.2]
    assert any("CREATE SECRET" in query for query in queries)
    attach = next(query for query in queries if "ATTACH" in query)
    assert "CREATE_IF_NOT_EXISTS false" in attach
    assert "READ_ONLY" not in attach
    assert not any("DETACH" in query for query in queries)
    assert all("secret" not in query for query in queries)


def test_filesystem_executor_overrides_data_path_without_s3_setup() -> None:
    connection = FakeConnection()
    metadata = MetadataConfiguration(
        backend_hint="postgres",
        host="catalog.example",
        port=5432,
        username="user",
        password="password",
        database="lake",
    )
    storage = StorageConfiguration(
        provider="filesystem",
        data_path="/srv/lakes/atlas/",
    )

    execute_treatment(
        metadata,
        storage,
        ResourceEnvelope(4, "4GB", 4_000_000_000),
        selection(TreatmentKind.MERGE, max_compacted_files=3),
        connect=lambda: connection,
    )

    queries = [query for query, _parameters in connection.queries]
    assert not any("httpfs" in query for query in queries)
    assert not any("CREATE SECRET" in query for query in queries)
    attach = next(query for query in queries if "ATTACH" in query)
    assert "DATA_PATH '/srv/lakes/atlas/'" in attach
    assert "OVERRIDE_DATA_PATH true" in attach


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        (
            "another transaction has compacted it",
            ExecutionFailureReason.CONCURRENT_COMPACTION,
        ),
        (
            "Exceeded the maximum retry count of 10",
            ExecutionFailureReason.SNAPSHOT_RETRY_EXHAUSTED,
        ),
        (
            "duplicate key violates ducklake_snapshot_pkey",
            ExecutionFailureReason.SNAPSHOT_RETRY_EXHAUSTED,
        ),
        ("Out of Memory Error", ExecutionFailureReason.RESOURCE_EXHAUSTED),
        ("HTTP Error reading S3", ExecutionFailureReason.STORAGE_ERROR),
        (
            'Transaction conflict - attempting to delete from table with index "18" '
            "- but another transaction has inserted into it",
            ExecutionFailureReason.TRANSACTION_CONFLICT,
        ),
        (
            'Transaction conflict - attempting to compact table with index "18" '
            "- but another transaction has deleted from it",
            ExecutionFailureReason.TRANSACTION_CONFLICT,
        ),
        ("Transaction conflict with unknown cause", ExecutionFailureReason.UNKNOWN),
        ("binder failed", ExecutionFailureReason.UNKNOWN),
    ],
)
def test_native_errors_are_classified(
    message: str,
    reason: ExecutionFailureReason,
) -> None:
    assert classify_duckdb_error(duckdb.Error(message)) is reason


def test_native_error_message_is_single_line_and_bounded() -> None:
    error = duckdb.IOException("first line\n" + ("detail " * 200))

    message = concise_duckdb_error(error, limit=80)

    assert message.startswith("IOException: first line detail")
    assert "\n" not in message
    assert len(message) <= len("IOException: ") + 80
    assert message.endswith("...")


def test_native_error_redacts_secrets_before_truncation(monkeypatch):
    from urllib.parse import quote

    import duckdb

    from lakeducktor.executor import concise_duckdb_error

    secret = "demo/password+private"
    monkeypatch.setenv("METADATA_DATABASE_PASSWORD", secret)
    message = (
        f"Connection failed postgresql://demo:{quote(secret, safe='')}@db/db {secret}"
    )
    result = concise_duckdb_error(duckdb.Error(message))
    assert secret not in result
    assert quote(secret, safe="") not in result
    assert "[REDACTED]" in result
    assert "Connection failed" in result
    assert secret[:6] not in concise_duckdb_error(duckdb.Error(secret), limit=8)


def test_native_error_redacts_sql_escaped_and_multiline_secrets(monkeypatch):
    import duckdb

    from lakeducktor.executor import concise_duckdb_error

    secret = "secret'with\nnewline"
    monkeypatch.setenv("CATALOG_STORAGE_SECRET_ACCESS_KEY", secret)
    for value in [secret, secret.replace("'", "''")]:
        result = concise_duckdb_error(duckdb.Error("failure " + value))
        assert "[REDACTED]" in result
        assert "newline" not in result
