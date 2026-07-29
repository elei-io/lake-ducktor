"""Select one bounded treatment for a worker without executing it."""

from __future__ import annotations

from lakeducktor.model import (
    DeleteRewritePriority,
    MergePriority,
    PriorityPlan,
    PriorityState,
    ResourceEnvelope,
    SelectionDecision,
    SelectionReason,
    TreatmentKind,
    TreatmentSelection,
)


class SelectionError(RuntimeError):
    """A priority candidate cannot be admitted safely."""


_MINIMUM_BYTES_PER_THREAD = 125_000_000
_UNSORTED_HEADROOM_DIVISOR = 4
_SORTED_HEADROOM_DIVISOR = 2


def treatment_memory_budget(
    envelope: ResourceEnvelope,
    sorting_enabled: bool,
) -> tuple[int, int]:
    """Return reserved headroom and usable output memory for one treatment."""

    ratio_denominator = (
        _SORTED_HEADROOM_DIVISOR if sorting_enabled else _UNSORTED_HEADROOM_DIVISOR
    )
    ratio_headroom = (
        envelope.duckdb_memory_bytes + ratio_denominator - 1
    ) // ratio_denominator
    thread_headroom = envelope.duckdb_threads * _MINIMUM_BYTES_PER_THREAD
    headroom = min(
        envelope.duckdb_memory_bytes,
        max(ratio_headroom, thread_headroom),
    )
    return headroom, envelope.duckdb_memory_bytes - headroom


def _rewrite_selection(
    candidate: DeleteRewritePriority,
    envelope: ResourceEnvelope,
) -> TreatmentSelection | None:
    if candidate.table_footprint_bytes <= 0:
        raise SelectionError(
            "delete rewrite has an invalid table footprint for "
            f"table_id={candidate.table_id}"
        )
    headroom, usable_memory = treatment_memory_budget(
        envelope,
        candidate.sorting_enabled,
    )
    if candidate.table_footprint_bytes > usable_memory:
        return None
    return TreatmentSelection(
        kind=TreatmentKind.DELETE_REWRITE,
        priority_rank=candidate.rank,
        metadata_schema=candidate.metadata_schema,
        table_id=candidate.table_id,
        schema_name=candidate.schema_name,
        table_name=candidate.table_name,
        input_bytes=candidate.input_bytes,
        admitted_bytes=candidate.table_footprint_bytes,
        sorting_enabled=candidate.sorting_enabled,
        memory_headroom_bytes=headroom,
        usable_memory_bytes=usable_memory,
        max_compacted_files=None,
    )


def _merge_selection(
    candidate: MergePriority,
    envelope: ResourceEnvelope,
) -> TreatmentSelection | None:
    if candidate.target_file_size_bytes <= 0:
        raise SelectionError(
            f"merge has an invalid target size for table_id={candidate.table_id}"
        )
    expected_output_files = candidate.input_files - candidate.expected_files_eliminated
    if expected_output_files <= 0:
        raise SelectionError(
            f"merge has an invalid output estimate for table_id={candidate.table_id}"
        )
    headroom, usable_memory = treatment_memory_budget(
        envelope,
        candidate.sorting_enabled,
    )
    output_capacity = usable_memory // candidate.target_file_size_bytes
    if output_capacity == 0:
        return None
    max_compacted_files = min(expected_output_files, output_capacity)
    return TreatmentSelection(
        kind=TreatmentKind.MERGE,
        priority_rank=candidate.rank,
        metadata_schema=candidate.metadata_schema,
        table_id=candidate.table_id,
        schema_name=candidate.schema_name,
        table_name=candidate.table_name,
        input_bytes=candidate.input_bytes,
        admitted_bytes=max_compacted_files * candidate.target_file_size_bytes,
        sorting_enabled=candidate.sorting_enabled,
        memory_headroom_bytes=headroom,
        usable_memory_bytes=usable_memory,
        max_compacted_files=max_compacted_files,
    )


def select_treatment(
    plan: PriorityPlan,
    envelope: ResourceEnvelope,
    unavailable_tables: frozenset[tuple[str, int]] = frozenset(),
) -> SelectionDecision:
    """Choose one treatment using fixed lane order and memory admission."""

    if envelope.duckdb_threads <= 0 or envelope.duckdb_memory_bytes <= 0:
        raise SelectionError("resource envelope must be greater than zero")

    fitting_rewrites: list[TreatmentSelection] = []
    fitting_merges: list[TreatmentSelection] = []
    memory_deferred = 0
    available_runnable = 0

    for candidate in plan.delete_rewrites:
        key = (candidate.metadata_schema, candidate.table_id)
        if key in unavailable_tables:
            continue
        available_runnable += 1
        selection = _rewrite_selection(candidate, envelope)
        if selection is None:
            memory_deferred += 1
        else:
            fitting_rewrites.append(selection)

    for candidate in plan.merges:
        if candidate.state is PriorityState.BLOCKED:
            continue
        key = (candidate.metadata_schema, candidate.table_id)
        if key in unavailable_tables:
            continue
        available_runnable += 1
        selection = _merge_selection(candidate, envelope)
        if selection is None:
            memory_deferred += 1
        else:
            fitting_merges.append(selection)

    selected = next(iter(fitting_rewrites or fitting_merges), None)
    if selected is not None:
        reason = SelectionReason.SELECTED
    elif available_runnable:
        reason = SelectionReason.NO_TREATMENT_FITS_MEMORY
    else:
        reason = SelectionReason.NO_RUNNABLE_TREATMENTS
    return SelectionDecision(
        reason=reason,
        envelope=envelope,
        selected=selected,
        memory_deferred=memory_deferred,
    )


def readmit_treatment(
    plan: PriorityPlan,
    envelope: ResourceEnvelope,
    previous: TreatmentSelection,
) -> TreatmentSelection | None:
    """Re-admit the same treatment from freshly diagnosed state."""

    key = (previous.metadata_schema, previous.table_id)
    if previous.kind is TreatmentKind.DELETE_REWRITE:
        for candidate in plan.delete_rewrites:
            if (candidate.metadata_schema, candidate.table_id) == key:
                return _rewrite_selection(candidate, envelope)
        return None
    for candidate in plan.merges:
        if (
            candidate.metadata_schema,
            candidate.table_id,
        ) == key and candidate.state is PriorityState.RUNNABLE:
            return _merge_selection(candidate, envelope)
    return None
