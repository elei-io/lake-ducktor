"""Coordinate selection, revalidation, and one native treatment."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import replace
from time import monotonic

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.coordination import (
    CoordinationError,
    TreatmentClaim,
    TreatmentCoordinator,
)
from lakeducktor.diagnosis import DiagnosisError, diagnose_inventory
from lakeducktor.executor import ExecutionError, execute_treatment
from lakeducktor.inventory import InventoryError, inventory_catalog
from lakeducktor.model import (
    BackendDetection,
    CatalogInventory,
    DiagnosisState,
    MaintenanceOutcome,
    MaintenanceState,
    PriorityPlan,
    ResourceEnvelope,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)
from lakeducktor.priority import PriorityError, prioritize
from lakeducktor.selection import (
    SelectionError,
    readmit_treatment,
    select_treatment,
)

_LOGGER = logging.getLogger("lakeducktor")

type InventoryFunction = Callable[
    [MetadataConfiguration, BackendDetection],
    CatalogInventory,
]
type ExecutionFunction = Callable[
    [
        MetadataConfiguration,
        StorageConfiguration,
        ResourceEnvelope,
        TreatmentSelection,
    ],
    TreatmentResult,
]


class MaintenanceError(RuntimeError):
    """A one-shot maintenance attempt failed."""


def _fresh_state(
    configuration: MetadataConfiguration,
    detection: BackendDetection,
    metadata_schema: str,
    inventory: InventoryFunction,
):
    lake_detection = replace(detection, metadata_schemas=(metadata_schema,))
    fresh_inventory = inventory(configuration, lake_detection)
    diagnosis = diagnose_inventory(fresh_inventory)
    return diagnosis, prioritize(diagnosis)


def _still_actionable(
    diagnosis,
    selection: TreatmentSelection,
) -> tuple[bool, bool]:
    table = next(
        (
            table
            for lake in diagnosis.lakes
            for table in lake.tables
            if table.metadata_schema == selection.metadata_schema
            and table.table_id == selection.table_id
        ),
        None,
    )
    if table is None:
        return False, False
    if table.state is not DiagnosisState.ACTIONABLE:
        return True, False
    if selection.kind is TreatmentKind.DELETE_REWRITE:
        return True, table.rewrite_data_files > 0
    return True, table.expected_files_eliminated > 0


def _release_claim(
    claim: TreatmentClaim,
    selection: TreatmentSelection,
) -> None:
    claim.release()
    _LOGGER.info(
        "claim_released lake=%s table_id=%s",
        selection.metadata_schema,
        selection.table_id,
    )


def maintain_once(
    configuration: MetadataConfiguration,
    storage: StorageConfiguration,
    detection: BackendDetection,
    envelope: ResourceEnvelope,
    initial_plan: PriorityPlan,
    coordinator: TreatmentCoordinator,
    *,
    inventory: InventoryFunction = inventory_catalog,
    execute: ExecutionFunction = execute_treatment,
) -> MaintenanceOutcome:
    """Claim, revalidate, and execute at most one treatment."""

    unavailable: set[tuple[str, int]] = set()
    claim_contention = 0
    try:
        while True:
            decision = select_treatment(
                initial_plan,
                envelope,
                frozenset(unavailable),
            )
            selection = decision.selected
            if selection is None:
                return MaintenanceOutcome(
                    state=MaintenanceState.NO_TREATMENT,
                    selection=None,
                    result=None,
                    selection_reason=decision.reason,
                    claim_contention=claim_contention,
                    duration_seconds=None,
                    table_present=None,
                    still_actionable=None,
                )
            claim = coordinator.try_claim(
                selection.metadata_schema,
                selection.table_id,
            )
            if claim is not None:
                break
            claim_contention += 1
            unavailable.add((selection.metadata_schema, selection.table_id))
            _LOGGER.info(
                "claim_busy kind=%s lake=%s table_id=%s",
                selection.kind.value,
                selection.metadata_schema,
                selection.table_id,
            )

        _LOGGER.info(
            "claim_acquired kind=%s lake=%s table_id=%s",
            selection.kind.value,
            selection.metadata_schema,
            selection.table_id,
        )
        try:
            _, fresh_plan = _fresh_state(
                configuration,
                detection,
                selection.metadata_schema,
                inventory,
            )
            revalidated = readmit_treatment(fresh_plan, envelope, selection)
            if revalidated is None:
                outcome = MaintenanceOutcome(
                    state=MaintenanceState.STALE,
                    selection=selection,
                    result=None,
                    selection_reason=None,
                    claim_contention=claim_contention,
                    duration_seconds=None,
                    table_present=None,
                    still_actionable=None,
                )
            else:
                selection = revalidated
                _LOGGER.info(
                    "treatment_started kind=%s lake=%s table_id=%s "
                    "schema=%r table=%r input_bytes=%s admitted_bytes=%s "
                    "sorting_enabled=%s memory_headroom_bytes=%s "
                    "usable_memory_bytes=%s max_compacted_files=%s",
                    selection.kind.value,
                    selection.metadata_schema,
                    selection.table_id,
                    selection.schema_name,
                    selection.table_name,
                    selection.input_bytes,
                    selection.admitted_bytes,
                    str(selection.sorting_enabled).lower(),
                    selection.memory_headroom_bytes,
                    selection.usable_memory_bytes,
                    selection.max_compacted_files
                    if selection.max_compacted_files is not None
                    else "none",
                )
                started = monotonic()
                try:
                    result = execute(
                        configuration,
                        storage,
                        envelope,
                        selection,
                    )
                except Exception as error:
                    duration_seconds = monotonic() - started
                    _LOGGER.error(
                        "treatment_failed kind=%s lake=%s table_id=%s "
                        "duration_seconds=%.3f error=%s",
                        selection.kind.value,
                        selection.metadata_schema,
                        selection.table_id,
                        duration_seconds,
                        error,
                    )
                    raise
                duration_seconds = monotonic() - started
                _LOGGER.info(
                    "treatment_native_completed kind=%s lake=%s table_id=%s "
                    "files_processed=%s files_created=%s duration_seconds=%.3f",
                    selection.kind.value,
                    selection.metadata_schema,
                    selection.table_id,
                    result.files_processed,
                    result.files_created,
                    duration_seconds,
                )
                verification, _ = _fresh_state(
                    configuration,
                    detection,
                    selection.metadata_schema,
                    inventory,
                )
                table_present, still_actionable = _still_actionable(
                    verification,
                    selection,
                )
                outcome = MaintenanceOutcome(
                    state=MaintenanceState.COMPLETED,
                    selection=selection,
                    result=result,
                    selection_reason=None,
                    claim_contention=claim_contention,
                    duration_seconds=duration_seconds,
                    table_present=table_present,
                    still_actionable=still_actionable,
                )
        except Exception:
            try:
                _release_claim(claim, selection)
            except CoordinationError:
                _LOGGER.exception(
                    "claim_release_failed lake=%s table_id=%s",
                    selection.metadata_schema,
                    selection.table_id,
                )
            raise
        _release_claim(claim, selection)
        return outcome
    except (
        CoordinationError,
        DiagnosisError,
        ExecutionError,
        InventoryError,
        PriorityError,
        SelectionError,
    ) as error:
        raise MaintenanceError(str(error)) from error
