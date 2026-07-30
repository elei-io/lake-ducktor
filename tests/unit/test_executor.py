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
    ) -> None:
        self.rows = [(4, 2)] if rows is None else rows
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
    assert "SET ducklake_target_file_size = ?" in queries
    assert any("CREATE SECRET" in query for query in queries)
    attach = next(query for query in queries if "ATTACH" in query)
    assert "CREATE_IF_NOT_EXISTS false" in attach
    assert "READ_ONLY" not in attach
    assert not any("DETACH" in query for query in queries)
    assert all("secret" not in query for query in queries)


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
