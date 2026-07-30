"""Explain physical maintenance needs without choosing execution order."""

from __future__ import annotations

from lakeducktor.model import (
    CatalogDiagnosis,
    CatalogInventory,
    DiagnosisState,
    LakeDiagnosis,
    TableDiagnosis,
    TableInventory,
)


class DiagnosisError(RuntimeError):
    """Inventory contains values that cannot be diagnosed safely."""


_INLINE_FLUSH_LIMIT_MULTIPLIER = 5


def inline_flush_threshold(data_inlining_row_limit: int) -> int:
    """Derive accumulated-row pressure from DuckLake's effective write limit."""

    if data_inlining_row_limit < 0:
        raise DiagnosisError("data_inlining_row_limit cannot be negative")
    return max(1, data_inlining_row_limit * _INLINE_FLUSH_LIMIT_MULTIPLIER)


def diagnose_table(table: TableInventory) -> TableDiagnosis:
    """Diagnose one table from immutable inventory facts."""

    if table.target_file_size_bytes <= 0:
        raise DiagnosisError(f"invalid target_file_size for table_id={table.table_id}")
    if not 0 <= table.rewrite_delete_threshold <= 1:
        raise DiagnosisError(
            f"invalid rewrite_delete_threshold for table_id={table.table_id}"
        )
    flush_threshold_rows = inline_flush_threshold(table.data_inlining_row_limit)
    if table.inlined_data_rows < 0 or table.inlined_data_bytes < 0:
        raise DiagnosisError(f"invalid inlined data for table_id={table.table_id}")

    merge_groups = 0
    merge_input_files = 0
    merge_input_bytes = 0
    expected_files_eliminated = 0
    for group in table.compatible_file_groups:
        if group.merge_candidate_files < 2:
            continue
        expected_outputs = max(
            1,
            (group.merge_candidate_bytes + table.target_file_size_bytes - 1)
            // table.target_file_size_bytes,
        )
        eliminated = max(0, group.merge_candidate_files - expected_outputs)
        if eliminated == 0:
            continue
        merge_groups += 1
        merge_input_files += group.merge_candidate_files
        merge_input_bytes += group.merge_candidate_bytes
        expected_files_eliminated += eliminated

    reasons: list[str] = []
    if table.inlined_data_rows >= flush_threshold_rows:
        reasons.append("inline_flush_pressure")
    if merge_groups:
        reasons.append("merge_pressure")
    if table.rewrite_data_files:
        reasons.append("delete_rewrite_pressure")
    has_maintenance_pressure = bool(reasons)
    if table.dangling_delete_files:
        reasons.append("dangling_delete_files")

    if has_maintenance_pressure:
        if table.auto_compact:
            state = DiagnosisState.ACTIONABLE
        else:
            state = DiagnosisState.EXCLUDED
            reasons.append("auto_compact_disabled")
    elif table.dangling_delete_files:
        state = DiagnosisState.ATTENTION
    else:
        state = DiagnosisState.HEALTHY
        reasons.append("healthy")

    return TableDiagnosis(
        metadata_schema=table.metadata_schema,
        table_id=table.table_id,
        schema_name=table.schema_name,
        table_name=table.table_name,
        state=state,
        reasons=tuple(reasons),
        target_file_size_bytes=table.target_file_size_bytes,
        sorting_enabled=table.sorting_enabled,
        active_data_bytes=table.active_data_bytes,
        recent_data_files_60s=table.recent_data_files_60s,
        merge_groups=merge_groups,
        merge_input_files=merge_input_files,
        merge_input_bytes=merge_input_bytes,
        expected_files_eliminated=expected_files_eliminated,
        rewrite_data_files=table.rewrite_data_files,
        rewrite_input_bytes=table.rewrite_input_bytes,
        rewrite_delete_files=table.rewrite_delete_files,
        rewrite_deleted_rows=table.rewrite_deleted_rows,
        rewrite_original_rows=table.rewrite_original_rows,
        dangling_delete_files=table.dangling_delete_files,
        minimum_merge_candidate_file_bytes=table.data_file_sizes.minimum_bytes,
        data_inlining_row_limit=table.data_inlining_row_limit,
        inline_flush_threshold_rows=flush_threshold_rows,
        inlined_data_rows=table.inlined_data_rows,
        inlined_data_bytes=table.inlined_data_bytes,
    )


def diagnose_inventory(inventory: CatalogInventory) -> CatalogDiagnosis:
    """Diagnose every selected lake and table without side effects."""

    lakes: list[LakeDiagnosis] = []
    for lake in inventory.lakes:
        tables = tuple(diagnose_table(table) for table in lake.tables)
        states = {table.state for table in tables}
        if (
            lake.expiring_snapshots
            or lake.cleanup_eligible_files
            or lake.orphan_files
            or DiagnosisState.ACTIONABLE in states
        ):
            state = DiagnosisState.ACTIONABLE
        elif lake.scheduled_files or DiagnosisState.ATTENTION in states:
            state = DiagnosisState.ATTENTION
        elif DiagnosisState.EXCLUDED in states:
            state = DiagnosisState.EXCLUDED
        else:
            state = DiagnosisState.HEALTHY
        lakes.append(
            LakeDiagnosis(
                metadata_schema=lake.metadata_schema,
                state=state,
                scheduled_files=lake.scheduled_files,
                tables=tables,
                expiring_snapshots=lake.expiring_snapshots,
                cleanup_eligible_files=lake.cleanup_eligible_files,
                orphan_files=lake.orphan_files,
                oldest_scheduled_at=lake.oldest_scheduled_at,
                delete_older_than=lake.delete_older_than,
                expire_older_than=lake.expire_older_than,
            )
        )
    return CatalogDiagnosis(lakes=tuple(lakes))
