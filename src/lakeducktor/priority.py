"""Build deterministic treatment lanes from current diagnoses."""

from __future__ import annotations

from fractions import Fraction

from lakeducktor.model import (
    CatalogDiagnosis,
    DeleteRewritePriority,
    DiagnosisState,
    MergePriority,
    PriorityPlan,
    PriorityState,
    TableDiagnosis,
    TreatmentKind,
)


class PriorityError(RuntimeError):
    """A diagnosis cannot be ranked without inventing missing information."""


_RECENT_FILE_PENALTY_CAP = 32
_RECENT_FILES_PER_ELIMINATION = 4


def merge_activity_penalty(recent_data_files_60s: int) -> Fraction:
    """Return the bounded merge penalty for files inserted in the last minute."""

    if recent_data_files_60s < 0:
        raise PriorityError("recent data file count cannot be negative")
    return Fraction(
        min(recent_data_files_60s, _RECENT_FILE_PENALTY_CAP),
        _RECENT_FILES_PER_ELIMINATION,
    )


def _tables(diagnosis: CatalogDiagnosis) -> tuple[TableDiagnosis, ...]:
    return tuple(table for lake in diagnosis.lakes for table in lake.tables)


def prioritize(diagnosis: CatalogDiagnosis) -> PriorityPlan:
    """Create separate, explainable rewrite and merge rankings."""

    tables = _tables(diagnosis)
    rewrite_tables = [
        table
        for table in tables
        if table.state is DiagnosisState.ACTIONABLE and table.rewrite_data_files > 0
    ]
    for table in rewrite_tables:
        if table.rewrite_original_rows <= 0:
            raise PriorityError(
                f"delete rewrite is missing original rows for table_id={table.table_id}"
            )
        if table.rewrite_deleted_rows < 0:
            raise PriorityError(
                "delete rewrite has negative deleted rows for "
                f"table_id={table.table_id}"
            )

    rewrite_tables.sort(
        key=lambda table: (
            -Fraction(
                table.rewrite_deleted_rows,
                table.rewrite_original_rows,
            ),
            -table.rewrite_deleted_rows,
            table.rewrite_input_bytes,
            table.metadata_schema,
            table.table_id,
        )
    )
    delete_rewrites = tuple(
        DeleteRewritePriority(
            rank=rank,
            metadata_schema=table.metadata_schema,
            table_id=table.table_id,
            schema_name=table.schema_name,
            table_name=table.table_name,
            data_files=table.rewrite_data_files,
            delete_files=table.rewrite_delete_files,
            deleted_rows=table.rewrite_deleted_rows,
            original_rows=table.rewrite_original_rows,
            deleted_fraction=(table.rewrite_deleted_rows / table.rewrite_original_rows),
            input_bytes=table.rewrite_input_bytes,
            table_footprint_bytes=table.active_data_bytes,
            sorting_enabled=table.sorting_enabled,
        )
        for rank, table in enumerate(rewrite_tables, start=1)
    )

    rewrite_keys = {
        (candidate.metadata_schema, candidate.table_id) for candidate in delete_rewrites
    }
    merge_tables = [
        table
        for table in tables
        if table.state is DiagnosisState.ACTIONABLE
        and table.expected_files_eliminated > 0
    ]
    for table in merge_tables:
        if table.merge_input_files <= 0 or table.target_file_size_bytes <= 0:
            raise PriorityError(
                f"merge is missing sizing facts for table_id={table.table_id}"
            )

    def merge_key(table: TableDiagnosis) -> tuple:
        blocked = (table.metadata_schema, table.table_id) in rewrite_keys
        adjusted_eliminations = Fraction(
            table.expected_files_eliminated
        ) - merge_activity_penalty(table.recent_data_files_60s)
        relative_average_size = Fraction(
            table.merge_input_bytes,
            table.merge_input_files * table.target_file_size_bytes,
        )
        return (
            blocked,
            -adjusted_eliminations,
            -table.expected_files_eliminated,
            relative_average_size,
            table.merge_input_bytes,
            table.metadata_schema,
            table.table_id,
        )

    merge_tables.sort(key=merge_key)
    merge_priorities = []
    for rank, table in enumerate(merge_tables, start=1):
        activity_penalty = merge_activity_penalty(table.recent_data_files_60s)
        adjusted_eliminations = (
            Fraction(table.expected_files_eliminated) - activity_penalty
        )
        merge_priorities.append(
            MergePriority(
                rank=rank,
                metadata_schema=table.metadata_schema,
                table_id=table.table_id,
                schema_name=table.schema_name,
                table_name=table.table_name,
                state=(
                    PriorityState.BLOCKED
                    if (table.metadata_schema, table.table_id) in rewrite_keys
                    else PriorityState.RUNNABLE
                ),
                blocked_by=(
                    TreatmentKind.DELETE_REWRITE
                    if (table.metadata_schema, table.table_id) in rewrite_keys
                    else None
                ),
                groups=table.merge_groups,
                input_files=table.merge_input_files,
                input_bytes=table.merge_input_bytes,
                average_input_file_bytes=(
                    table.merge_input_bytes // table.merge_input_files
                ),
                target_file_size_bytes=table.target_file_size_bytes,
                expected_files_eliminated=table.expected_files_eliminated,
                recent_data_files_60s=table.recent_data_files_60s,
                activity_penalty=float(activity_penalty),
                adjusted_expected_files_eliminated=float(adjusted_eliminations),
                sorting_enabled=table.sorting_enabled,
                minimum_input_file_bytes=(table.minimum_merge_candidate_file_bytes),
            )
        )
    merges = tuple(merge_priorities)

    return PriorityPlan(
        delete_rewrites=delete_rewrites,
        merges=merges,
        excluded_tables=sum(table.state is DiagnosisState.EXCLUDED for table in tables),
        attention_tables=sum(
            table.state is DiagnosisState.ATTENTION for table in tables
        ),
    )
