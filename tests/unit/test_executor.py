import pytest

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.executor import (
    ExecutionError,
    execute_native_treatment,
    execute_treatment,
)
from lakeducktor.model import ResourceEnvelope, TreatmentKind, TreatmentSelection


class FakeConnection:
    def __init__(self, row: tuple[object, ...] = (4, 2)) -> None:
        self.row = row
        self.query = ""
        self.parameters: list[object] = []
        self.queries: list[tuple[str, list[object]]] = []
        self.closed = False

    def execute(
        self,
        query: str,
        parameters: tuple[object, ...] | list[object] = (),
    ) -> FakeConnection:
        self.query = query
        self.parameters = list(parameters)
        self.queries.append((query, self.parameters))
        return self

    def fetchone(self) -> tuple[object, ...]:
        return self.row

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
    ]
    assert result.files_processed == 4
    assert result.files_created == 2


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
    assert connection.closed is True
    queries = [query for query, _parameters in connection.queries]
    assert "SET threads = ?" in queries
    assert "SET memory_limit = ?" in queries
    assert any("CREATE SECRET" in query for query in queries)
    attach = next(query for query in queries if "ATTACH" in query)
    assert "CREATE_IF_NOT_EXISTS false" in attach
    assert "READ_ONLY" not in attach
    assert all("secret" not in query for query in queries)
