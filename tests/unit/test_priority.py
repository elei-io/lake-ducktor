from dataclasses import replace
from datetime import UTC, datetime

import pytest

from lakeducktor.model import (
    CatalogDiagnosis,
    CompatibleFileGroup,
    DiagnosisState,
    LakeDiagnosis,
    PriorityState,
    TableDiagnosis,
    TreatmentKind,
)
from lakeducktor.priority import PriorityError, prioritize


def table_diagnosis(table_id: int, **overrides) -> TableDiagnosis:
    table = TableDiagnosis(
        metadata_schema="lake",
        table_id=table_id,
        schema_name="main",
        table_name=f"table_{table_id}",
        state=DiagnosisState.HEALTHY,
        reasons=("healthy",),
        target_file_size_bytes=100,
        sorting_enabled=False,
        active_data_bytes=100,
        recent_data_files_60s=0,
        merge_groups=0,
        merge_input_files=0,
        merge_input_bytes=0,
        expected_files_eliminated=0,
        rewrite_data_files=0,
        rewrite_input_bytes=0,
        rewrite_delete_files=0,
        rewrite_deleted_rows=0,
        rewrite_original_rows=0,
        dangling_delete_files=0,
    )
    return replace(table, **overrides)


def catalog(*tables: TableDiagnosis) -> CatalogDiagnosis:
    return CatalogDiagnosis(
        lakes=(
            LakeDiagnosis(
                metadata_schema="lake",
                state=DiagnosisState.ACTIONABLE,
                scheduled_files=0,
                tables=tables,
            ),
        )
    )


def test_rewrite_blocks_only_the_same_tables_merge() -> None:
    rewrite_and_merge = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure", "delete_rewrite_pressure"),
        merge_groups=1,
        merge_input_files=5,
        merge_input_bytes=100,
        expected_files_eliminated=4,
        rewrite_data_files=1,
        rewrite_input_bytes=200,
        rewrite_delete_files=2,
        rewrite_deleted_rows=96,
        rewrite_original_rows=100,
    )
    independent_merge = table_diagnosis(
        2,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        merge_groups=1,
        merge_input_files=3,
        merge_input_bytes=60,
        expected_files_eliminated=2,
    )

    plan = prioritize(catalog(rewrite_and_merge, independent_merge))

    assert [candidate.table_id for candidate in plan.delete_rewrites] == [1]
    assert [candidate.table_id for candidate in plan.merges] == [2, 1]
    assert plan.merges[0].state is PriorityState.RUNNABLE
    assert plan.merges[1].state is PriorityState.BLOCKED
    assert plan.merges[1].blocked_by is TreatmentKind.DELETE_REWRITE
    assert plan.runnable == 2
    assert plan.blocked == 1


def test_inline_flush_is_ranked_and_blocks_same_table_merge() -> None:
    flush_and_merge = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("inline_flush_pressure", "merge_pressure"),
        inlined_data_rows=100,
        inlined_data_bytes=10_000,
        inline_flush_threshold_rows=50,
        merge_groups=1,
        merge_input_files=3,
        merge_input_bytes=60,
        expected_files_eliminated=2,
    )

    plan = prioritize(catalog(flush_and_merge))

    assert [candidate.table_id for candidate in plan.inline_flushes] == [1]
    assert plan.inline_flushes[0].data_inlining_row_limit == 10
    assert plan.merges[0].state is PriorityState.BLOCKED
    assert plan.merges[0].blocked_by is TreatmentKind.INLINE_FLUSH
    assert plan.runnable == 1


def test_inline_flush_is_ranked_when_only_byte_ceiling_is_reached() -> None:
    byte_heavy = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("inline_flush_pressure",),
        inlined_data_rows=1,
        inlined_data_bytes=8 * 1024 * 1024,
        inline_flush_threshold_rows=50,
        inline_flush_max_bytes=8 * 1024 * 1024,
    )

    plan = prioritize(catalog(byte_heavy))

    assert [candidate.table_id for candidate in plan.inline_flushes] == [1]


def test_priority_preserves_compatible_merge_groups_for_admission() -> None:
    group = CompatibleFileGroup(1, 7, 4, 160, 4, 160)
    candidate = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        merge_groups=1,
        merge_input_files=4,
        merge_input_bytes=160,
        expected_files_eliminated=2,
        merge_candidate_groups=(group,),
    )

    plan = prioritize(catalog(candidate))

    assert plan.merges[0].input_groups == (group,)


def test_active_writer_waits_for_a_useful_merge_batch() -> None:
    group = CompatibleFileGroup(1, 7, 31, 31, 31, 31)
    candidate = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        recent_data_files_60s=31,
        merge_groups=1,
        merge_input_files=31,
        merge_input_bytes=31,
        expected_files_eliminated=30,
        merge_candidate_groups=(group,),
    )

    plan = prioritize(catalog(candidate))

    assert plan.merges[0].state is PriorityState.WAITING
    assert plan.merges[0].waiting_reason == "active_writer_batching"
    assert plan.merges[0].input_groups == ()
    assert plan.merges[0].ready_groups == 0
    assert plan.runnable == 0
    assert plan.waiting == 1


@pytest.mark.parametrize(
    ("files", "input_bytes"),
    (
        (32, 32),
        (2, 100),
    ),
)
def test_active_writer_runs_when_group_reaches_a_useful_batch(
    files: int,
    input_bytes: int,
) -> None:
    group = CompatibleFileGroup(1, 7, files, input_bytes, files, input_bytes)
    candidate = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        recent_data_files_60s=files,
        merge_groups=1,
        merge_input_files=files,
        merge_input_bytes=input_bytes,
        expected_files_eliminated=files - 1,
        merge_candidate_groups=(group,),
    )

    merge = prioritize(catalog(candidate)).merges[0]

    assert merge.state is PriorityState.RUNNABLE
    assert merge.waiting_reason is None
    assert merge.input_groups == (group,)
    assert merge.ready_groups == 1


def test_quiet_writer_drains_a_small_merge_tail() -> None:
    group = CompatibleFileGroup(1, 7, 2, 2, 2, 2)
    candidate = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        recent_data_files_60s=0,
        merge_groups=1,
        merge_input_files=2,
        merge_input_bytes=2,
        expected_files_eliminated=1,
        merge_candidate_groups=(group,),
    )

    merge = prioritize(catalog(candidate)).merges[0]

    assert merge.state is PriorityState.RUNNABLE
    assert merge.input_groups == (group,)
    assert merge.ready_groups == 1


def test_active_writer_trigger_admits_all_productive_compatible_groups() -> None:
    waiting = CompatibleFileGroup(1, 7, 3, 3, 3, 3)
    ready = CompatibleFileGroup(1, 8, 32, 32, 32, 32)
    candidate = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        recent_data_files_60s=35,
        merge_groups=2,
        merge_input_files=35,
        merge_input_bytes=35,
        expected_files_eliminated=33,
        merge_candidate_groups=(waiting, ready),
    )

    merge = prioritize(catalog(candidate)).merges[0]

    assert merge.state is PriorityState.RUNNABLE
    assert merge.ready_groups == 1
    assert merge.input_groups == (waiting, ready)


def test_scheduled_cleanup_is_ranked_at_lake_scope() -> None:
    diagnosis = CatalogDiagnosis(
        lakes=(
            LakeDiagnosis(
                metadata_schema="lake",
                state=DiagnosisState.ACTIONABLE,
                scheduled_files=10,
                tables=(),
                cleanup_eligible_files=7,
                oldest_scheduled_at=datetime(2026, 7, 1, tzinfo=UTC),
                delete_older_than="1 week",
            ),
        )
    )

    plan = prioritize(diagnosis)

    assert len(plan.scheduled_file_cleanups) == 1
    cleanup = plan.scheduled_file_cleanups[0]
    assert cleanup.metadata_schema == "lake"
    assert cleanup.eligible_files == 7
    assert cleanup.delete_older_than == "1 week"
    assert plan.runnable == 1


def test_snapshot_and_orphan_housekeeping_are_ranked_at_lake_scope() -> None:
    diagnosis = CatalogDiagnosis(
        lakes=(
            LakeDiagnosis(
                metadata_schema="lake",
                state=DiagnosisState.ACTIONABLE,
                scheduled_files=0,
                tables=(),
                expiring_snapshots=4,
                orphan_files=9,
                delete_older_than="2 days",
                expire_older_than="1 week",
            ),
        )
    )

    plan = prioritize(diagnosis)

    assert plan.snapshot_expirations[0].snapshots == 4
    assert plan.snapshot_expirations[0].expire_older_than == "1 week"
    assert plan.orphan_file_cleanups[0].orphan_files == 9
    assert plan.orphan_file_cleanups[0].delete_older_than == "2 days"
    assert plan.runnable == 2


def test_rewrites_rank_fraction_then_deleted_rows_then_cost() -> None:
    candidates = (
        table_diagnosis(
            1,
            state=DiagnosisState.ACTIONABLE,
            reasons=("delete_rewrite_pressure",),
            rewrite_data_files=1,
            rewrite_input_bytes=500,
            rewrite_delete_files=1,
            rewrite_deleted_rows=90,
            rewrite_original_rows=100,
        ),
        table_diagnosis(
            2,
            state=DiagnosisState.ACTIONABLE,
            reasons=("delete_rewrite_pressure",),
            rewrite_data_files=1,
            rewrite_input_bytes=300,
            rewrite_delete_files=1,
            rewrite_deleted_rows=95,
            rewrite_original_rows=100,
        ),
    )

    plan = prioritize(catalog(*candidates))

    assert [candidate.table_id for candidate in plan.delete_rewrites] == [2, 1]
    assert plan.delete_rewrites[0].deleted_fraction == 0.95


def test_merge_ranking_is_deterministic_and_benefit_first() -> None:
    candidates = (
        table_diagnosis(
            9,
            state=DiagnosisState.ACTIONABLE,
            reasons=("merge_pressure",),
            merge_groups=1,
            merge_input_files=4,
            merge_input_bytes=80,
            expected_files_eliminated=2,
        ),
        table_diagnosis(
            3,
            state=DiagnosisState.ACTIONABLE,
            reasons=("merge_pressure",),
            merge_groups=1,
            merge_input_files=4,
            merge_input_bytes=40,
            expected_files_eliminated=2,
        ),
        table_diagnosis(
            5,
            state=DiagnosisState.ACTIONABLE,
            reasons=("merge_pressure",),
            merge_groups=1,
            merge_input_files=5,
            merge_input_bytes=200,
            expected_files_eliminated=4,
        ),
    )

    plan = prioritize(catalog(*candidates))

    assert [candidate.table_id for candidate in plan.merges] == [5, 3, 9]
    assert plan.merges[1].average_input_file_bytes == 10


def test_recent_writes_apply_a_small_bounded_merge_penalty() -> None:
    active = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        merge_groups=1,
        merge_input_files=10,
        merge_input_bytes=100,
        expected_files_eliminated=10,
        recent_data_files_60s=32,
    )
    quiet = table_diagnosis(
        2,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        merge_groups=1,
        merge_input_files=4,
        merge_input_bytes=40,
        expected_files_eliminated=3,
    )

    plan = prioritize(catalog(active, quiet))

    assert [candidate.table_id for candidate in plan.merges] == [2, 1]
    assert plan.merges[1].activity_penalty == 8
    assert plan.merges[1].adjusted_expected_files_eliminated == 2


def test_recent_write_penalty_is_capped_at_32_files() -> None:
    candidate = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        reasons=("merge_pressure",),
        merge_groups=1,
        merge_input_files=10,
        merge_input_bytes=100,
        expected_files_eliminated=9,
        recent_data_files_60s=100,
    )

    merge = prioritize(catalog(candidate)).merges[0]

    assert merge.recent_data_files_60s == 100
    assert merge.activity_penalty == 8
    assert merge.adjusted_expected_files_eliminated == 1


def test_excluded_and_attention_tables_remain_observable_but_unqueued() -> None:
    plan = prioritize(
        catalog(
            table_diagnosis(1, state=DiagnosisState.EXCLUDED),
            table_diagnosis(2, state=DiagnosisState.ATTENTION),
        )
    )

    assert plan.delete_rewrites == ()
    assert plan.merges == ()
    assert plan.excluded_tables == 1
    assert plan.attention_tables == 1


def test_rewrite_without_original_rows_is_rejected() -> None:
    invalid = table_diagnosis(
        1,
        state=DiagnosisState.ACTIONABLE,
        rewrite_data_files=1,
        rewrite_deleted_rows=1,
        rewrite_original_rows=0,
    )

    with pytest.raises(PriorityError, match="original rows"):
        prioritize(catalog(invalid))
