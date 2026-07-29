from collections.abc import Iterable

from lakeducktor.inventory import (
    CompatibleFileGroupRow,
    LakeSummaryRow,
    TableInventoryRow,
    collect_inventory,
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
