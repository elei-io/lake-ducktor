from dataclasses import replace

import pytest

from lakeducktor.model import (
    DeleteRewritePriority,
    MergePriority,
    PriorityPlan,
    PriorityState,
    ResourceEnvelope,
    SelectionReason,
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
        sorting_enabled=False,
    )


def plan(
    *,
    rewrites: tuple[DeleteRewritePriority, ...] = (),
    merges: tuple[MergePriority, ...] = (),
) -> PriorityPlan:
    return PriorityPlan(
        delete_rewrites=rewrites,
        merges=merges,
        excluded_tables=0,
        attention_tables=0,
    )


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


def test_merge_batch_is_bounded_by_output_working_set() -> None:
    decision = select_treatment(plan(merges=(merge(1, 1),)), _ENVELOPE)

    assert decision.selected is not None
    assert decision.selected.input_bytes == 500
    assert decision.selected.max_compacted_files == 2
    assert decision.selected.admitted_bytes == 200


def test_merge_larger_than_memory_is_deferred() -> None:
    decision = select_treatment(
        plan(merges=(merge(1, 1, target=300),)),
        _ENVELOPE,
    )

    assert decision.selected is None
    assert decision.reason is SelectionReason.NO_TREATMENT_FITS_MEMORY
    assert decision.memory_deferred == 1


def test_blocked_merge_is_not_runnable_or_memory_deferred() -> None:
    decision = select_treatment(
        plan(merges=(merge(1, 1, state=PriorityState.BLOCKED),)),
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


def test_current_sorting_state_reduces_native_merge_batch() -> None:
    envelope = ResourceEnvelope(4, "4GB", 4_000_000_000)
    unsorted = replace(merge(1, 1, target=500_000_000), sorting_enabled=False)
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
    assert sorted_selection is not None
    assert unsorted_selection.max_compacted_files == 5
    assert sorted_selection.max_compacted_files == 4
