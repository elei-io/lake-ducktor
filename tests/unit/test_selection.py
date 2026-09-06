from dataclasses import replace

import pytest

from lakeducktor.model import (
    CompatibleFileGroup,
    DeleteRewritePriority,
    InlineFlushPriority,
    MergePriority,
    OrphanFileCleanupPriority,
    PriorityPlan,
    PriorityState,
    ResourceEnvelope,
    ScheduledFileCleanupPriority,
    SelectionReason,
    SnapshotExpirationPriority,
    TreatmentKind,
)
from lakeducktor.selection import (
    SelectionError,
    readmit_treatment,
    select_treatment,
    treatment_memory_budget,
)

_ENVELOPE = ResourceEnvelope(
    duckdb_threads=1,
    duckdb_memory="125000250B",
    duckdb_memory_bytes=125_000_250,
)


def rewrite(rank: int, table_id: int, footprint: int) -> DeleteRewritePriority:
    return DeleteRewritePriority(
        rank=rank,
        metadata_schema="lake",
        table_id=table_id,
        schema_name="main",
        table_name=f"table_{table_id}",
        data_files=1,
        delete_files=1,
        deleted_rows=96,
        original_rows=100,
        deleted_fraction=0.96,
        input_bytes=100,
        table_footprint_bytes=footprint,
        sorting_enabled=False,
    )


def merge(
    rank: int,
    table_id: int,
    *,
    state: PriorityState = PriorityState.RUNNABLE,
    target: int = 100,
    minimum: int = 0,
    input_groups: tuple[CompatibleFileGroup, ...] = (),
) -> MergePriority:
    return MergePriority(
        rank=rank,
        metadata_schema="lake",
        table_id=table_id,
        schema_name="main",
        table_name=f"table_{table_id}",
        state=state,
        blocked_by=TreatmentKind.DELETE_REWRITE
        if state is PriorityState.BLOCKED
        else None,
        groups=1,
        input_files=10,
        input_bytes=500,
        average_input_file_bytes=50,
        target_file_size_bytes=target,
        expected_files_eliminated=5,
        recent_data_files_60s=0,
        activity_penalty=0,
        adjusted_expected_files_eliminated=5,
        sorting_enabled=False,
        minimum_input_file_bytes=minimum,
        input_groups=input_groups,
    )


def flush(rank: int, table_id: int, input_bytes: int) -> InlineFlushPriority:
    return InlineFlushPriority(
        rank=rank,
        metadata_schema="lake",
        table_id=table_id,
        schema_name="main",
        table_name=f"table_{table_id}",
        inlined_rows=50,
        input_bytes=input_bytes,
        threshold_rows=50,
        data_inlining_row_limit=10,
        sorting_enabled=False,
    )


def plan(
    *,
    expirations: tuple[SnapshotExpirationPriority, ...] = (),
    cleanups: tuple[ScheduledFileCleanupPriority, ...] = (),
    orphans: tuple[OrphanFileCleanupPriority, ...] = (),
    flushes: tuple[InlineFlushPriority, ...] = (),
    rewrites: tuple[DeleteRewritePriority, ...] = (),
    merges: tuple[MergePriority, ...] = (),
) -> PriorityPlan:
    return PriorityPlan(
        delete_rewrites=rewrites,
        merges=merges,
        excluded_tables=0,
        attention_tables=0,
        inline_flushes=flushes,
        scheduled_file_cleanups=cleanups,
        snapshot_expirations=expirations,
        orphan_file_cleanups=orphans,
    )


def test_native_policy_cleanup_is_selected_without_memory_admission() -> None:
    cleanup = ScheduledFileCleanupPriority(
        rank=1,
        metadata_schema="lake",
        eligible_files=7,
        scheduled_files=10,
        oldest_scheduled_at=None,
        delete_older_than=None,
    )

    decision = select_treatment(
        plan(cleanups=(cleanup,), rewrites=(rewrite(1, 1, 500),)),
        _ENVELOPE,
    )

    assert decision.selected is not None
    assert decision.selected.kind is TreatmentKind.SCHEDULED_FILE_CLEANUP
    assert decision.selected.table_id is None
    assert decision.selected.input_files == 7
    assert decision.selected.retention_policy == "native_default"
    assert decision.memory_deferred == 1


def test_lake_housekeeping_uses_native_policy_and_fixed_order() -> None:
    expiration = SnapshotExpirationPriority(
        rank=1,
        metadata_schema="lake",
        snapshots=4,
        expire_older_than="1 week",
    )
    orphan = OrphanFileCleanupPriority(
        rank=1,
        metadata_schema="lake",
        orphan_files=9,
        delete_older_than=None,
    )

    first = select_treatment(
        plan(expirations=(expiration,), orphans=(orphan,)),
        _ENVELOPE,
    ).selected
    assert first is not None
    assert first.kind is TreatmentKind.SNAPSHOT_EXPIRATION
    assert first.input_snapshots == 4
    assert first.retention_policy == "1 week"

    second = select_treatment(plan(orphans=(orphan,)), _ENVELOPE).selected
    assert second is not None
    assert second.kind is TreatmentKind.ORPHAN_FILE_CLEANUP
    assert second.input_files == 9
    assert second.retention_policy == "native_default"


def test_first_fitting_rewrite_is_selected_before_merge_lane() -> None:
    decision = select_treatment(
        plan(
            rewrites=(rewrite(1, 1, 500), rewrite(2, 2, 200)),
            merges=(merge(1, 3),),
        ),
        _ENVELOPE,
    )

    assert decision.reason is SelectionReason.SELECTED
    assert decision.selected is not None
    assert decision.selected.kind is TreatmentKind.DELETE_REWRITE
    assert decision.selected.table_id == 2
    assert decision.selected.admitted_bytes == 200
    assert decision.memory_deferred == 1


def test_oversized_rewrite_does_not_block_independent_merge() -> None:
    decision = select_treatment(
        plan(rewrites=(rewrite(1, 1, 500),), merges=(merge(1, 2),)),
        _ENVELOPE,
    )

    assert decision.selected is not None
    assert decision.selected.kind is TreatmentKind.MERGE
    assert decision.selected.table_id == 2
    assert decision.selected.max_compacted_files == 2
    assert decision.selected.admitted_bytes == 200
    assert decision.memory_deferred == 1


def test_inline_flush_is_selected_before_merge_and_admitted_by_bytes() -> None:
    decision = select_treatment(
        plan(flushes=(flush(1, 1, 200),), merges=(merge(1, 2),)),
        _ENVELOPE,
    )

    assert decision.selected is not None
    assert decision.selected.kind is TreatmentKind.INLINE_FLUSH
    assert decision.selected.input_rows == 50
    assert decision.selected.admitted_bytes == 200


def test_oversized_inline_flush_does_not_block_independent_merge() -> None:
    decision = select_treatment(
        plan(flushes=(flush(1, 1, 500),), merges=(merge(1, 2),)),
        _ENVELOPE,
    )

    assert decision.selected is not None
    assert decision.selected.kind is TreatmentKind.MERGE
    assert decision.memory_deferred == 1


def test_merge_batch_uses_multiple_groups_within_input_and_memory_bounds() -> None:
    decision = select_treatment(plan(merges=(merge(1, 1),)), _ENVELOPE)

    assert decision.selected is not None
    assert decision.selected.input_bytes == 500
    assert decision.selected.max_compacted_files == 2
    assert decision.selected.admitted_bytes == 200
    assert decision.selected.execution_target_file_size_bytes == 100


def test_merge_admits_whole_compatible_groups_by_actual_inputs() -> None:
    groups = (
        CompatibleFileGroup(1, 1, 20, 80, 20, 80),
        CompatibleFileGroup(1, 2, 30, 90, 30, 90),
        CompatibleFileGroup(1, 3, 470, 95, 470, 95),
    )
    candidate = replace(
        merge(1, 1, input_groups=groups),
        groups=3,
        input_files=520,
        input_bytes=265,
        expected_files_eliminated=517,
    )

    selected = select_treatment(plan(merges=(candidate,)), _ENVELOPE).selected

    assert selected is not None
    assert selected.max_compacted_files == 2
    assert selected.admitted_input_files == 50
    assert selected.admitted_bytes == 170
    assert selected.input_files == 520
    assert selected.execution_target_file_size_bytes == 100


def test_sorted_merge_admits_one_small_compatible_group() -> None:
    groups = (
        CompatibleFileGroup(1, 1, 16, 15_000_000, 16, 15_000_000),
        CompatibleFileGroup(1, 2, 8, 9_000_000, 8, 9_000_000),
    )
    candidate = replace(
        merge(1, 1, target=128_000_000, input_groups=groups),
        sorting_enabled=True,
        groups=2,
        input_files=24,
        input_bytes=24_000_000,
        expected_files_eliminated=22,
    )

    selected = select_treatment(
        plan(merges=(candidate,)),
        ResourceEnvelope(4, "4GB", 4_000_000_000),
    ).selected

    assert selected is not None
    assert selected.max_compacted_files == 1
    assert selected.admitted_input_files == 16
    assert selected.admitted_bytes == 15_000_000


def test_sorted_merge_is_deferred_when_one_group_exceeds_memory_allowance() -> None:
    groups = (CompatibleFileGroup(1, 1, 2, 251_000_000, 2, 251_000_000),)
    candidate = replace(
        merge(1, 1, target=512_000_000, input_groups=groups),
        sorting_enabled=True,
        input_files=2,
        input_bytes=251_000_000,
        expected_files_eliminated=1,
    )

    decision = select_treatment(
        plan(merges=(candidate,)),
        ResourceEnvelope(4, "4GB", 4_000_000_000),
    )

    assert decision.selected is None
    assert decision.reason is SelectionReason.NO_TREATMENT_FITS_MEMORY
    assert decision.memory_deferred == 1


def test_merge_group_admission_stops_at_usable_memory() -> None:
    groups = (
        CompatibleFileGroup(1, 1, 2, 100, 2, 100),
        CompatibleFileGroup(1, 2, 2, 200, 2, 200),
    )
    candidate = replace(
        merge(1, 1, input_groups=groups),
        groups=2,
        input_files=4,
        input_bytes=300,
        expected_files_eliminated=2,
    )

    selected = select_treatment(plan(merges=(candidate,)), _ENVELOPE).selected

    assert selected is not None
    assert selected.max_compacted_files == 1
    assert selected.admitted_input_files == 2
    assert selected.admitted_bytes == 100


def test_merge_execution_target_is_reduced_to_fit_memory() -> None:
    decision = select_treatment(
        plan(merges=(merge(1, 1, target=300),)),
        _ENVELOPE,
    )

    assert decision.selected is not None
    assert decision.selected.execution_target_file_size_bytes == 250
    assert decision.memory_deferred == 0


def test_tiny_file_merge_target_caps_estimated_native_inputs() -> None:
    candidate = replace(
        merge(1, 1, target=5_000_000, minimum=889),
        input_files=10_000,
        input_bytes=8_890_000,
        average_input_file_bytes=889,
        expected_files_eliminated=9_998,
    )

    selected = select_treatment(
        plan(merges=(candidate,)),
        ResourceEnvelope(1, "1GB", 1_000_000_000),
    ).selected

    assert selected is not None
    assert selected.max_compacted_files == 1
    assert selected.execution_target_file_size_bytes == 455_168
    assert selected.admitted_bytes == 455_168


def test_large_files_can_fill_many_output_groups_with_at_most_512_inputs() -> None:
    candidate = replace(
        merge(1, 1, target=5_000_000, minimum=2_900_000),
        input_files=500,
        input_bytes=1_450_000_000,
        average_input_file_bytes=2_900_000,
        expected_files_eliminated=250,
    )

    selected = select_treatment(
        plan(merges=(candidate,)),
        ResourceEnvelope(4, "4GB", 4_000_000_000),
    ).selected

    assert selected is not None
    assert selected.max_compacted_files == 250
    assert selected.admitted_bytes == 1_250_000_000


def test_blocked_merge_is_not_runnable_or_memory_deferred() -> None:
    decision = select_treatment(
        plan(merges=(merge(1, 1, state=PriorityState.BLOCKED),)),
        _ENVELOPE,
    )

    assert decision.selected is None
    assert decision.reason is SelectionReason.NO_RUNNABLE_TREATMENTS
    assert decision.memory_deferred == 0


def test_waiting_merge_is_not_runnable_or_memory_deferred() -> None:
    decision = select_treatment(
        plan(merges=(merge(1, 1, state=PriorityState.WAITING),)),
        _ENVELOPE,
    )

    assert decision.selected is None
    assert decision.reason is SelectionReason.NO_RUNNABLE_TREATMENTS
    assert decision.memory_deferred == 0


def test_invalid_merge_output_estimate_is_rejected() -> None:
    candidate = replace(merge(1, 1), expected_files_eliminated=10)

    with pytest.raises(SelectionError, match="output estimate"):
        select_treatment(plan(merges=(candidate,)), _ENVELOPE)


def test_unavailable_table_is_skipped_without_changing_priority() -> None:
    decision = select_treatment(
        plan(
            merges=(
                merge(1, 1),
                merge(2, 2),
            )
        ),
        _ENVELOPE,
        frozenset({("lake", 1)}),
    )

    assert decision.selected is not None
    assert decision.selected.table_id == 2
    assert decision.selected.priority_rank == 2


def test_revalidation_keeps_identity_but_refreshes_name_and_bound() -> None:
    previous = select_treatment(
        plan(merges=(merge(1, 1),)),
        _ENVELOPE,
    ).selected
    assert previous is not None
    current = replace(
        merge(4, 1, target=80),
        table_name="renamed",
        input_files=4,
        expected_files_eliminated=2,
    )

    refreshed = readmit_treatment(plan(merges=(current,)), _ENVELOPE, previous)

    assert refreshed is not None
    assert refreshed.table_name == "renamed"
    assert refreshed.priority_rank == 4
    assert refreshed.max_compacted_files == 2


def test_sorted_treatment_reserves_more_memory_headroom() -> None:
    envelope = ResourceEnvelope(4, "4GB", 4_000_000_000)

    assert treatment_memory_budget(envelope, False) == (
        1_000_000_000,
        3_000_000_000,
    )
    assert treatment_memory_budget(envelope, True) == (
        2_000_000_000,
        2_000_000_000,
    )


def test_thread_minimum_can_consume_the_available_treatment_budget() -> None:
    envelope = ResourceEnvelope(8, "1GB", 1_000_000_000)

    assert treatment_memory_budget(envelope, False) == (1_000_000_000, 0)


def test_sorted_merge_without_compatible_groups_is_not_admitted() -> None:
    envelope = ResourceEnvelope(4, "4GB", 4_000_000_000)
    unsorted = replace(
        merge(1, 1, target=2_500_000_000, minimum=10_000_000),
        average_input_file_bytes=10_000_000,
        sorting_enabled=False,
    )
    sorted_table = replace(unsorted, sorting_enabled=True)

    unsorted_selection = select_treatment(
        plan(merges=(unsorted,)),
        envelope,
    ).selected
    sorted_selection = select_treatment(
        plan(merges=(sorted_table,)),
        envelope,
    ).selected

    assert unsorted_selection is not None
    assert sorted_selection is None
    assert unsorted_selection.max_compacted_files == 1
    assert unsorted_selection.admitted_bytes == 2_500_000_000
