from collections.abc import Iterable

from lakeducktor.inventory import (
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
                ),
            ),
            "lake_b": (),
        }

    def lake_summary(self, metadata_schema: str) -> LakeSummaryRow:
        return self.summaries[metadata_schema]

    def tables(self, metadata_schema: str) -> Iterable[TableInventoryRow]:
        return self.table_rows[metadata_schema]


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
