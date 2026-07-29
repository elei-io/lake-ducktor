"""Select one bounded treatment for a worker without executing it."""

from __future__ import annotations

from lakeducktor.model import (
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
    output_capacity = envelope.duckdb_memory_bytes // candidate.target_file_size_bytes
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
        max_compacted_files=max_compacted_files,
    )


def select_treatment(
    plan: PriorityPlan,
    envelope: ResourceEnvelope,
) -> SelectionDecision:
    """Choose one treatment using fixed lane order and memory admission."""

    if envelope.duckdb_threads <= 0 or envelope.duckdb_memory_bytes <= 0:
        raise SelectionError("resource envelope must be greater than zero")

    fitting_rewrites: list[TreatmentSelection] = []
    fitting_merges: list[TreatmentSelection] = []
    memory_deferred = 0

    for candidate in plan.delete_rewrites:
        if candidate.table_footprint_bytes <= 0:
            raise SelectionError(
                "delete rewrite has an invalid table footprint for "
                f"table_id={candidate.table_id}"
            )
        if candidate.table_footprint_bytes > envelope.duckdb_memory_bytes:
            memory_deferred += 1
            continue
        fitting_rewrites.append(
            TreatmentSelection(
                kind=TreatmentKind.DELETE_REWRITE,
                priority_rank=candidate.rank,
                metadata_schema=candidate.metadata_schema,
                table_id=candidate.table_id,
                schema_name=candidate.schema_name,
                table_name=candidate.table_name,
                input_bytes=candidate.input_bytes,
                admitted_bytes=candidate.table_footprint_bytes,
                max_compacted_files=None,
            )
        )

    for candidate in plan.merges:
        if candidate.state is PriorityState.BLOCKED:
            continue
        selection = _merge_selection(candidate, envelope)
        if selection is None:
            memory_deferred += 1
        else:
            fitting_merges.append(selection)

    selected = next(iter(fitting_rewrites or fitting_merges), None)
    if selected is not None:
        reason = SelectionReason.SELECTED
    elif plan.runnable:
        reason = SelectionReason.NO_TREATMENT_FITS_MEMORY
    else:
        reason = SelectionReason.NO_RUNNABLE_TREATMENTS
    return SelectionDecision(
        reason=reason,
        envelope=envelope,
        selected=selected,
        memory_deferred=memory_deferred,
    )
