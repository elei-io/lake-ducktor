from dataclasses import replace

import pytest

from lakeducktor.model import (
    CatalogDiagnosis,
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
