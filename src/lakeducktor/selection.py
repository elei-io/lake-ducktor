"""Select one bounded treatment for a worker without executing it."""

from __future__ import annotations

from lakeducktor.model import (
    DeleteRewritePriority,
    InlineFlushPriority,
    MergePriority,
    OrphanFileCleanupPriority,
    PriorityPlan,
    PriorityState,
    ResourceEnvelope,
    ScheduledFileCleanupPriority,
    SelectionDecision,
    SelectionReason,
    SnapshotExpirationPriority,
    TreatmentKind,
    TreatmentSelection,
)


class SelectionError(RuntimeError):
    """A priority candidate cannot be admitted safely."""


_MINIMUM_BYTES_PER_THREAD = 125_000_000
_MAXIMUM_MERGE_INPUT_FILES = 512
_UNSORTED_HEADROOM_DIVISOR = 4
_SORTED_HEADROOM_DIVISOR = 2


def _snapshot_expiration_selection(
    candidate: SnapshotExpirationPriority,
    envelope: ResourceEnvelope,
) -> TreatmentSelection:
    if candidate.snapshots <= 0:
        raise SelectionError(
            "snapshot expiration has no eligible snapshots for "
            f"lake={candidate.metadata_schema}"
        )
    return TreatmentSelection(
        kind=TreatmentKind.SNAPSHOT_EXPIRATION,
        priority_rank=candidate.rank,
        metadata_schema=candidate.metadata_schema,
        table_id=None,
        schema_name=None,
        table_name=None,
        input_bytes=0,
        admitted_bytes=0,
        sorting_enabled=False,
        memory_headroom_bytes=0,
        usable_memory_bytes=envelope.duckdb_memory_bytes,
        max_compacted_files=None,
        input_snapshots=candidate.snapshots,
        retention_policy=candidate.expire_older_than,
    )


def _scheduled_file_cleanup_selection(
    candidate: ScheduledFileCleanupPriority,
    envelope: ResourceEnvelope,
) -> TreatmentSelection:
    if candidate.eligible_files <= 0:
        raise SelectionError(
            "scheduled-file cleanup has no eligible files for "
            f"lake={candidate.metadata_schema}"
        )
    return TreatmentSelection(
        kind=TreatmentKind.SCHEDULED_FILE_CLEANUP,
        priority_rank=candidate.rank,
        metadata_schema=candidate.metadata_schema,
        table_id=None,
        schema_name=None,
        table_name=None,
        input_bytes=0,
        admitted_bytes=0,
        sorting_enabled=False,
        memory_headroom_bytes=0,
        usable_memory_bytes=envelope.duckdb_memory_bytes,
        max_compacted_files=None,
        input_files=candidate.eligible_files,
        retention_policy=candidate.delete_older_than or "native_default",
    )


def _orphan_file_cleanup_selection(
    candidate: OrphanFileCleanupPriority,
    envelope: ResourceEnvelope,
) -> TreatmentSelection:
    if candidate.orphan_files <= 0:
        raise SelectionError(
            "orphan-file cleanup has no eligible files for "
            f"lake={candidate.metadata_schema}"
        )
    return TreatmentSelection(
        kind=TreatmentKind.ORPHAN_FILE_CLEANUP,
        priority_rank=candidate.rank,
        metadata_schema=candidate.metadata_schema,
        table_id=None,
        schema_name=None,
        table_name=None,
        input_bytes=0,
        admitted_bytes=0,
        sorting_enabled=False,
        memory_headroom_bytes=0,
        usable_memory_bytes=envelope.duckdb_memory_bytes,
        max_compacted_files=None,
        input_files=candidate.orphan_files,
        retention_policy=candidate.delete_older_than or "native_default",
    )


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
        input_files=candidate.data_files,
    )


def _inline_flush_selection(
    candidate: InlineFlushPriority,
    envelope: ResourceEnvelope,
) -> TreatmentSelection | None:
    if candidate.inlined_rows <= 0 or candidate.input_bytes <= 0:
        raise SelectionError(
            f"inline flush has invalid input for table_id={candidate.table_id}"
        )
    headroom, usable_memory = treatment_memory_budget(
        envelope,
        candidate.sorting_enabled,
    )
    if candidate.input_bytes > usable_memory:
        return None
    return TreatmentSelection(
        kind=TreatmentKind.INLINE_FLUSH,
        priority_rank=candidate.rank,
        metadata_schema=candidate.metadata_schema,
        table_id=candidate.table_id,
        schema_name=candidate.schema_name,
        table_name=candidate.table_name,
        input_bytes=candidate.input_bytes,
        admitted_bytes=candidate.input_bytes,
        sorting_enabled=candidate.sorting_enabled,
        memory_headroom_bytes=headroom,
        usable_memory_bytes=usable_memory,
        max_compacted_files=None,
        input_rows=candidate.inlined_rows,
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
    minimum_input_file_bytes = (
        candidate.minimum_input_file_bytes
        if candidate.minimum_input_file_bytes > 0
        else candidate.average_input_file_bytes
    )
    if minimum_input_file_bytes <= 0:
        raise SelectionError(
            f"merge has an invalid minimum file size for table_id={candidate.table_id}"
        )
    execution_target_file_size_bytes = min(
        candidate.target_file_size_bytes,
        usable_memory,
        minimum_input_file_bytes * _MAXIMUM_MERGE_INPUT_FILES,
    )
    if execution_target_file_size_bytes == 0:
        return None
    estimated_inputs_per_group = (
        execution_target_file_size_bytes + minimum_input_file_bytes - 1
    ) // minimum_input_file_bytes
    groups_by_input_count = max(
        1,
        _MAXIMUM_MERGE_INPUT_FILES // estimated_inputs_per_group,
    )
    groups_by_memory = max(
        1,
        usable_memory // execution_target_file_size_bytes,
    )
    maximum_output_groups = min(
        expected_output_files,
        groups_by_input_count,
        groups_by_memory,
    )
    return TreatmentSelection(
        kind=TreatmentKind.MERGE,
        priority_rank=candidate.rank,
        metadata_schema=candidate.metadata_schema,
        table_id=candidate.table_id,
        schema_name=candidate.schema_name,
        table_name=candidate.table_name,
        input_bytes=candidate.input_bytes,
        admitted_bytes=execution_target_file_size_bytes * maximum_output_groups,
        sorting_enabled=candidate.sorting_enabled,
        memory_headroom_bytes=headroom,
        usable_memory_bytes=usable_memory,
        max_compacted_files=maximum_output_groups,
        input_files=candidate.input_files,
        lake_target_file_size_bytes=candidate.target_file_size_bytes,
        execution_target_file_size_bytes=execution_target_file_size_bytes,
    )


def select_treatment(
    plan: PriorityPlan,
    envelope: ResourceEnvelope,
    unavailable_tables: frozenset[tuple[str, int | None]] = frozenset(),
) -> SelectionDecision:
    """Choose one treatment using fixed lane order and memory admission."""

    if envelope.duckdb_threads <= 0 or envelope.duckdb_memory_bytes <= 0:
        raise SelectionError("resource envelope must be greater than zero")

    fitting_expirations: list[TreatmentSelection] = []
    fitting_scheduled_cleanups: list[TreatmentSelection] = []
    fitting_orphan_cleanups: list[TreatmentSelection] = []
    fitting_rewrites: list[TreatmentSelection] = []
    fitting_flushes: list[TreatmentSelection] = []
    fitting_merges: list[TreatmentSelection] = []
    memory_deferred = 0
    available_runnable = 0

    for candidate in plan.snapshot_expirations:
        key = (candidate.metadata_schema, None)
        if key in unavailable_tables:
            continue
        available_runnable += 1
        fitting_expirations.append(
            _snapshot_expiration_selection(candidate, envelope)
        )

    for candidate in plan.scheduled_file_cleanups:
        key = (candidate.metadata_schema, None)
        if key in unavailable_tables:
            continue
        available_runnable += 1
        fitting_scheduled_cleanups.append(
            _scheduled_file_cleanup_selection(candidate, envelope)
        )

    for candidate in plan.orphan_file_cleanups:
        key = (candidate.metadata_schema, None)
        if key in unavailable_tables:
            continue
        available_runnable += 1
        fitting_orphan_cleanups.append(
            _orphan_file_cleanup_selection(candidate, envelope)
        )

    for candidate in plan.inline_flushes:
        key = (candidate.metadata_schema, candidate.table_id)
        if key in unavailable_tables:
            continue
        available_runnable += 1
        selection = _inline_flush_selection(candidate, envelope)
        if selection is None:
            memory_deferred += 1
        else:
            fitting_flushes.append(selection)

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

    selected = next(
        iter(
            fitting_expirations
            or fitting_scheduled_cleanups
            or fitting_orphan_cleanups
            or fitting_rewrites
            or fitting_flushes
            or fitting_merges
        ),
        None,
    )
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
    if previous.kind is TreatmentKind.SNAPSHOT_EXPIRATION:
        for candidate in plan.snapshot_expirations:
            if candidate.metadata_schema == previous.metadata_schema:
                return _snapshot_expiration_selection(candidate, envelope)
        return None
    if previous.kind is TreatmentKind.SCHEDULED_FILE_CLEANUP:
        for candidate in plan.scheduled_file_cleanups:
            if candidate.metadata_schema == previous.metadata_schema:
                return _scheduled_file_cleanup_selection(candidate, envelope)
        return None
    if previous.kind is TreatmentKind.ORPHAN_FILE_CLEANUP:
        for candidate in plan.orphan_file_cleanups:
            if candidate.metadata_schema == previous.metadata_schema:
                return _orphan_file_cleanup_selection(candidate, envelope)
        return None
    if previous.kind is TreatmentKind.INLINE_FLUSH:
        for candidate in plan.inline_flushes:
            if (candidate.metadata_schema, candidate.table_id) == key:
                return _inline_flush_selection(candidate, envelope)
        return None
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
