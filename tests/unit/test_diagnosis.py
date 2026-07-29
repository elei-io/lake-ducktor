from dataclasses import replace

import pytest

from lakeducktor.diagnosis import DiagnosisError, diagnose_inventory, diagnose_table
from lakeducktor.model import (
    CatalogInventory,
    CompatibleFileGroup,
    DiagnosisState,
    FileSizeDistribution,
    LakeInventory,
    TableInventory,
)


def table_inventory(**overrides) -> TableInventory:
    table = TableInventory(
        metadata_schema="lake",
        table_id=7,
        schema_name="main",
        table_name="events",
        auto_compact=True,
        target_file_size_bytes=100,
        rewrite_delete_threshold=0.95,
        sorting_enabled=False,
        active_data_files=1,
        active_data_bytes=100,
        active_data_rows=10,
        data_file_sizes=FileSizeDistribution(100, 100, 100, 100),
        compatible_file_groups=(),
        active_delete_files=0,
        active_delete_bytes=0,
        deleted_rows=0,
        dangling_delete_files=0,
        rewrite_data_files=0,
        rewrite_input_bytes=0,
        rewrite_delete_files=0,
        rewrite_delete_bytes=0,
        rewrite_deleted_rows=0,
        rewrite_original_rows=0,
    )
    return replace(table, **overrides)


def test_healthy_table_has_no_actionable_debt() -> None:
    diagnosis = diagnose_table(table_inventory())

    assert diagnosis.state is DiagnosisState.HEALTHY
    assert diagnosis.reasons == ("healthy",)


def test_merge_estimate_respects_compatible_group_boundaries() -> None:
    groups = (
        CompatibleFileGroup(1, 1, 1, 40, 1, 40),
        CompatibleFileGroup(1, 1, 1, 40, 1, 40),
    )

    diagnosis = diagnose_table(
        table_inventory(
            active_data_files=2,
            active_data_bytes=80,
            compatible_file_groups=groups,
        )
    )

    assert diagnosis.state is DiagnosisState.HEALTHY
    assert diagnosis.expected_files_eliminated == 0


def test_merge_pressure_requires_expected_file_elimination() -> None:
    group = CompatibleFileGroup(
        schema_version=1,
        partition_id=None,
        active_files=4,
        active_bytes=160,
        merge_candidate_files=4,
        merge_candidate_bytes=160,
    )

    diagnosis = diagnose_table(
        table_inventory(
            active_data_files=4,
            active_data_bytes=160,
            compatible_file_groups=(group,),
        )
    )

    assert diagnosis.state is DiagnosisState.ACTIONABLE
    assert diagnosis.merge_groups == 1
    assert diagnosis.merge_input_files == 4
    assert diagnosis.expected_files_eliminated == 2
    assert diagnosis.reasons == ("merge_pressure",)


def test_delete_rewrite_pressure_uses_inventorys_effective_threshold_result() -> None:
    diagnosis = diagnose_table(
        table_inventory(
            active_delete_files=2,
            rewrite_data_files=1,
            rewrite_input_bytes=90,
            rewrite_delete_files=2,
            rewrite_deleted_rows=96,
            rewrite_original_rows=100,
        )
    )

    assert diagnosis.state is DiagnosisState.ACTIONABLE
    assert diagnosis.reasons == ("delete_rewrite_pressure",)
    assert diagnosis.rewrite_data_files == 1


def test_auto_compact_exclusion_is_authoritative() -> None:
    diagnosis = diagnose_table(
        table_inventory(
            auto_compact=False,
            rewrite_data_files=1,
            rewrite_input_bytes=90,
        )
    )

    assert diagnosis.state is DiagnosisState.EXCLUDED
    assert diagnosis.reasons == (
        "delete_rewrite_pressure",
        "auto_compact_disabled",
    )


def test_dangling_delete_files_require_attention_not_rewrite() -> None:
    diagnosis = diagnose_table(table_inventory(dangling_delete_files=2))

    assert diagnosis.state is DiagnosisState.ATTENTION
    assert diagnosis.reasons == ("dangling_delete_files",)


def test_actionable_pressure_does_not_hide_dangling_delete_files() -> None:
    group = CompatibleFileGroup(1, None, 3, 160, 3, 160)

    diagnosis = diagnose_table(
        table_inventory(
            compatible_file_groups=(group,),
            dangling_delete_files=2,
        )
    )

    assert diagnosis.state is DiagnosisState.ACTIONABLE
    assert diagnosis.reasons == ("merge_pressure", "dangling_delete_files")


def test_scheduled_cleanup_is_observed_at_lake_scope() -> None:
    lake = LakeInventory(
        metadata_schema="lake",
        latest_snapshot_id=1,
        latest_snapshot_at=None,
        scheduled_files=3,
        oldest_scheduled_at=None,
        tables=(table_inventory(),),
    )

    diagnosis = diagnose_inventory(CatalogInventory(lakes=(lake,)))

    assert diagnosis.lakes[0].state is DiagnosisState.ATTENTION
    assert diagnosis.lakes[0].scheduled_files == 3


def test_invalid_native_setting_is_rejected() -> None:
    with pytest.raises(DiagnosisError, match="target_file_size"):
        diagnose_table(table_inventory(target_file_size_bytes=0))
