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
from lakeducktor.selection import SelectionError, select_treatment

_ENVELOPE = ResourceEnvelope(
    duckdb_threads=4,
    duckdb_memory="250B",
    duckdb_memory_bytes=250,
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
