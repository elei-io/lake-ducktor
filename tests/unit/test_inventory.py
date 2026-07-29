from collections.abc import Iterable
from unittest.mock import Mock, patch

from lakeducktor.config import MetadataConfiguration
from lakeducktor.inventory import (
    CompatibleFileGroupRow,
    DuckDBInventorySource,
    LakeSummaryRow,
    TableInventoryRow,
    collect_inventory,
    inventory_catalog,
)
from lakeducktor.model import (
    BackendDetection,
    CatalogInventory,
    MetadataBackend,
)


class FakeInventorySource:
    def __init__(self) -> None:
        self.summaries: dict[str, LakeSummaryRow] = {
            "lake_a": (11, 1_700_000_000_000, 2, 1_699_999_000_000),
            "lake_b": (None, None, 0, None),
        }
        self.table_rows: dict[str, tuple[TableInventoryRow, ...]] = {
            "lake_a": (
                (
                    7,
                    "main",
                    "events",
                    True,
                    256,
                    0.5,
                    True,
                    3,
                    600,
                    60,
                    100,
                    200,
                    280,
                    300,
                    2,
                    20,
                    4,
                    1,
                    1,
                    300,
                    2,
                    20,
                    4,
                    6,
                    5,
                ),
            ),
            "lake_b": (),
        }
        self.group_rows: dict[str, tuple[CompatibleFileGroupRow, ...]] = {
            "lake_a": ((7, 1, None, 3, 600, 3, 600),),
            "lake_b": (),
        }

    def lake_summary(self, metadata_schema: str) -> LakeSummaryRow:
        return self.summaries[metadata_schema]

    def tables(self, metadata_schema: str) -> Iterable[TableInventoryRow]:
        return self.table_rows[metadata_schema]

    def compatible_file_groups(
        self,
        metadata_schema: str,
    ) -> Iterable[CompatibleFileGroupRow]:
        return self.group_rows[metadata_schema]


def test_inventory_builds_immutable_physical_facts_without_a_connection() -> None:
    inventory = collect_inventory(
        FakeInventorySource(),
        ("lake_b", "lake_a", "lake_a"),
    )

    assert [lake.metadata_schema for lake in inventory.lakes] == ["lake_a", "lake_b"]
    lake = inventory.lakes[0]
    assert lake.latest_snapshot_id == 11
    assert lake.latest_snapshot_at is not None
    assert lake.latest_snapshot_at.timestamp() == 1_700_000_000
    assert lake.scheduled_files == 2
    assert lake.oldest_scheduled_at is not None

    table = lake.tables[0]
    assert (table.metadata_schema, table.table_id) == ("lake_a", 7)
    assert (table.schema_name, table.table_name) == ("main", "events")
    assert table.auto_compact is True
    assert table.target_file_size_bytes == 256
    assert table.rewrite_delete_threshold == 0.5
    assert table.sorting_enabled is True
    assert table.active_data_files == 3
    assert table.active_data_bytes == 600
    assert table.active_data_rows == 60
    assert table.recent_data_files_60s == 5
    assert table.data_file_sizes.minimum_bytes == 100
    assert table.data_file_sizes.median_bytes == 200
    assert table.data_file_sizes.p90_bytes == 280
    assert table.data_file_sizes.maximum_bytes == 300
    assert table.active_delete_files == 2
    assert table.active_delete_bytes == 20
    assert table.deleted_rows == 4
    assert table.dangling_delete_files == 1
    assert table.rewrite_data_files == 1
    assert table.rewrite_input_bytes == 300
    assert table.rewrite_delete_files == 2
    assert table.rewrite_delete_bytes == 20
    assert table.rewrite_deleted_rows == 4
    assert table.rewrite_original_rows == 6
    assert len(table.compatible_file_groups) == 1
    group = table.compatible_file_groups[0]
    assert group.schema_version == 1
    assert group.partition_id is None
    assert group.merge_candidate_files == 3
    assert group.merge_candidate_bytes == 600

    assert inventory.table_count == 1
    assert lake.table_count == 1
    assert lake.active_data_files == 3
    assert lake.active_data_bytes == 600
    assert lake.active_delete_files == 2
    assert lake.active_delete_bytes == 20
    assert lake.dangling_delete_files == 1
    assert inventory.active_data_files == 3
    assert inventory.active_data_bytes == 600
    assert inventory.active_delete_files == 2
    assert inventory.active_delete_bytes == 20
    assert inventory.scheduled_files == 2


def test_inventory_preserves_empty_lakes_without_synthetic_values() -> None:
    inventory = collect_inventory(FakeInventorySource(), ("lake_b",))

    lake = inventory.lakes[0]
    assert lake.latest_snapshot_id is None
    assert lake.latest_snapshot_at is None
    assert lake.oldest_scheduled_at is None
    assert lake.tables == ()


def test_merge_groups_exclude_files_native_compaction_will_skip() -> None:
    connection = Mock()
    inline_tables = Mock()
    inline_tables.fetchall.return_value = [("ducklake_inlined_delete_7",)]
    groups = Mock()
    groups.fetchall.return_value = [(7, 1, None, 2, 100, 2, 100)]
    connection.execute.side_effect = [inline_tables, groups]

    rows = tuple(
        DuckDBInventorySource(connection, "catalog").compatible_file_groups("lake")
    )

    assert rows == ((7, 1, None, 2, 100, 2, 100),)
    query = connection.execute.call_args_list[1].args[0]
    assert '"lake"."ducklake_delete_file"' in query
    assert "NOT EXISTS" in query
    assert '"lake"."ducklake_inlined_delete_7"' in query


def test_inventory_disables_checkpoint_on_shutdown_before_attaching() -> None:
    connection = Mock()
    connection.execute.return_value = connection
    expected = CatalogInventory(lakes=())
    configuration = MetadataConfiguration(
        backend_hint="postgres",
        host="catalog.example",
        port=5432,
        username="user",
        password="password",
        database="lake",
    )
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake",),
        extension_version="ducklake-version",
        duckdb_extensions=(),
    )

    with (
        patch("lakeducktor.inventory.duckdb.connect", return_value=connection),
        patch("lakeducktor.inventory.collect_inventory", return_value=expected),
    ):
        result = inventory_catalog(configuration, detection)

    assert result is expected
    queries = [call.args[0] for call in connection.execute.call_args_list]
    assert queries[0] == "PRAGMA disable_checkpoint_on_shutdown"
    assert connection.close.call_count == 1
