from collections.abc import Iterable
from unittest.mock import Mock, patch

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.inventory import (
    CompatibleFileGroupRow,
    DuckDBInventorySource,
    InlinedDataRow,
    InventoryError,
    LakeSummaryRow,
    MaintenanceInventory,
    TableInventoryRow,
    _cleanup_eligible_files,
    _expiring_snapshots,
    _orphan_files,
    collect_inventory,
    inventory_catalog,
)
from lakeducktor.model import (
    BackendDetection,
    CatalogInventory,
    LakeInventory,
    MetadataBackend,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)


class FakeInventorySource:
    def __init__(self) -> None:
        self.summaries: dict[str, LakeSummaryRow] = {
            "lake_a": (
                11,
                1_700_000_000_000,
                2,
                1_699_999_000_000,
                "1 week",
                "1 month",
            ),
            "lake_b": (None, None, 0, None, None, None),
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
                    10,
                ),
            ),
            "lake_b": (),
        }
        self.group_rows: dict[str, tuple[CompatibleFileGroupRow, ...]] = {
            "lake_a": ((7, 1, None, 3, 600, 3, 600),),
            "lake_b": (),
        }
        self.inlined_rows: dict[str, tuple[InlinedDataRow, ...]] = {
            "lake_a": ((7, 50, 1_024),),
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

    def inlined_data(self, metadata_schema: str) -> Iterable[InlinedDataRow]:
        return self.inlined_rows[metadata_schema]


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
    assert lake.delete_older_than == "1 week"
    assert lake.expire_older_than == "1 month"

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
    assert table.data_inlining_row_limit == 10
    assert table.inlined_data_rows == 50
    assert table.inlined_data_bytes == 1_024
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


def test_inlined_data_counts_live_rows_and_serialized_bytes_by_table() -> None:
    connection = Mock()
    mapping = Mock()
    mapping.fetchall.return_value = [
        (7, "ducklake_inlined_data_7_1"),
        (7, "ducklake_inlined_data_7_2"),
    ]
    summary = Mock()
    summary.fetchall.return_value = [(7, 50, 1_024)]
    connection.execute.side_effect = [mapping, summary]

    rows = tuple(DuckDBInventorySource(connection, "catalog").inlined_data("lake"))

    assert rows == ((7, 50, 1_024),)
    query = connection.execute.call_args_list[1].args[0]
    assert '"lake"."ducklake_inlined_data_7_1"' in query
    assert '"lake"."ducklake_inlined_data_7_2"' in query
    assert "end_snapshot IS NULL" in query
    assert "to_json(inlined_row)" in query


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


def test_cleanup_probe_delegates_policy_to_native_dry_run() -> None:
    connection = Mock()
    result = Mock()
    result.fetchone.return_value = (7,)
    connection.execute.side_effect = [
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        result,
    ]
    configuration = MetadataConfiguration(
        backend_hint="postgres",
        host="catalog.example",
        port=5432,
        username="user",
        password="password",
        database="lake",
    )

    with patch("lakeducktor.inventory.duckdb.connect", return_value=connection):
        eligible = _cleanup_eligible_files(configuration, "lake")

    assert eligible == 7
    queries = [call.args[0] for call in connection.execute.call_args_list]
    attach = next(query for query in queries if "ATTACH" in query)
    cleanup = next(query for query in queries if "ducklake_cleanup_old_files" in query)
    assert "READ_ONLY" in attach
    assert "dry_run => true" in cleanup
    assert "older_than" not in cleanup
    assert "cleanup_all" not in cleanup
    assert not any("DETACH" in query for query in queries)


def test_expiration_probe_delegates_policy_to_native_dry_run() -> None:
    connection = Mock()
    result = Mock()
    result.fetchone.return_value = (4,)
    connection.execute.side_effect = [
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        result,
    ]
    configuration = MetadataConfiguration(
        backend_hint="postgres",
        host="catalog.example",
        port=5432,
        username="user",
        password="password",
        database="lake",
    )

    with patch("lakeducktor.inventory.duckdb.connect", return_value=connection):
        eligible = _expiring_snapshots(configuration, "lake")

    assert eligible == 4
    queries = [call.args[0] for call in connection.execute.call_args_list]
    expiration = next(
        query for query in queries if "ducklake_expire_snapshots" in query
    )
    assert "dry_run => true" in expiration
    assert "older_than" not in expiration
    assert "versions" not in expiration
    assert not any("DETACH" in query for query in queries)


def test_orphan_probe_uses_storage_and_never_overrides_native_retention() -> None:
    connection = Mock()
    result = Mock()
    result.fetchone.return_value = (9,)
    connection.execute.side_effect = [
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        connection,
        result,
    ]
    configuration = MetadataConfiguration(
        backend_hint="postgres",
        host="catalog.example",
        port=5432,
        username="user",
        password="password",
        database="lake",
    )
    storage = StorageConfiguration(
        provider="filesystem",
        data_path="/var/lib/lakes/",
    )

    with patch("lakeducktor.inventory.duckdb.connect", return_value=connection):
        eligible = _orphan_files(configuration, storage, "lake")

    assert eligible == 9
    queries = [call.args[0] for call in connection.execute.call_args_list]
    attach = next(query for query in queries if "ATTACH" in query)
    orphan = next(
        query for query in queries if "ducklake_delete_orphaned_files" in query
    )
    assert "DATA_PATH '/var/lib/lakes/'" in attach
    assert "OVERRIDE_DATA_PATH true" in attach
    assert "READ_ONLY" not in attach
    assert "dry_run => true" in orphan
    assert "older_than" not in orphan
    assert not any("DETACH" in query for query in queries)


def test_maintenance_inventory_isolates_and_throttles_orphan_probe_failure() -> None:
    storage = StorageConfiguration(provider="filesystem", data_path="/lakes/")
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake",),
        extension_version="v1",
        duckdb_extensions=(),
    )
    now = [100.0]
    probe_outcomes: list[bool] = []

    def collect(
        _configuration,
        _detection,
        _storage,
        *,
        orphan_probe_schemas,
    ) -> CatalogInventory:
        assert orphan_probe_schemas == frozenset()
        return CatalogInventory(
            lakes=(
                LakeInventory(
                    metadata_schema="lake",
                    latest_snapshot_id=1,
                    latest_snapshot_at=None,
                    scheduled_files=0,
                    oldest_scheduled_at=None,
                    tables=(),
                    orphan_files=0,
                ),
            )
        )

    collector = MaintenanceInventory(
        storage,
        orphan_scan_interval_seconds=3_600,
        orphan_cleanup_enabled=True,
        clock=lambda: now[0],
        orphan_probe_observer=probe_outcomes.append,
    )
    configuration = Mock()
    with (
        patch("lakeducktor.inventory.inventory_catalog", side_effect=collect),
        patch(
            "lakeducktor.inventory._orphan_files",
            side_effect=(InventoryError("staging file disappeared"), 5),
        ) as orphan_probe,
    ):
        assert collector(configuration, detection).lakes[0].orphan_files == 0
        assert collector(configuration, detection).lakes[0].orphan_files == 0
        assert orphan_probe.call_count == 1

        now[0] += 3_600
        assert collector(configuration, detection).lakes[0].orphan_files == 5
        assert orphan_probe.call_count == 2

        collector.treatment_completed(
            TreatmentSelection(
                kind=TreatmentKind.ORPHAN_FILE_CLEANUP,
                priority_rank=1,
                metadata_schema="lake",
                table_id=None,
                schema_name=None,
                table_name=None,
                input_bytes=0,
                admitted_bytes=0,
                sorting_enabled=False,
                memory_headroom_bytes=0,
                usable_memory_bytes=1,
                max_compacted_files=None,
            ),
            TreatmentResult(5, 0),
        )
        assert collector(configuration, detection).lakes[0].orphan_files == 0

    assert probe_outcomes == [False, True]


def test_maintenance_inventory_can_disable_orphan_cleanup() -> None:
    storage = StorageConfiguration(provider="filesystem", data_path="/lakes/")
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake",),
        extension_version="v1",
        duckdb_extensions=(),
    )
    inventory = CatalogInventory(
        lakes=(
            LakeInventory(
                metadata_schema="lake",
                latest_snapshot_id=1,
                latest_snapshot_at=None,
                scheduled_files=0,
                oldest_scheduled_at=None,
                tables=(),
            ),
        )
    )
    collector = MaintenanceInventory(
        storage,
        orphan_cleanup_enabled=False,
    )

    with (
        patch("lakeducktor.inventory.inventory_catalog", return_value=inventory),
        patch("lakeducktor.inventory._orphan_files") as orphan_probe,
    ):
        result = collector(Mock(), detection)

    assert result.lakes[0].orphan_files == 0
    orphan_probe.assert_not_called()


def test_one_shot_inventory_honors_orphan_cleanup_environment(monkeypatch):
    from lakeducktor.config import StorageConfiguration
    from lakeducktor.inventory import MaintenanceInventory

    monkeypatch.delenv("ORPHAN_CLEANUP_ENABLED", raising=False)
    inventory = MaintenanceInventory(StorageConfiguration(provider="filesystem"))
    assert inventory.orphan_cleanup_enabled is False
    monkeypatch.setenv("ORPHAN_CLEANUP_ENABLED", "true")
    assert MaintenanceInventory(inventory.storage).orphan_cleanup_enabled is True
