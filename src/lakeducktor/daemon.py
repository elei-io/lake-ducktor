"""Coordinate selection, revalidation, and one native treatment."""

from __future__ import annotations

import logging
import signal
from collections.abc import Callable
from dataclasses import replace
from threading import Event
from time import monotonic
from types import FrameType
from typing import Protocol

from lakeducktor.capabilities import CapabilitiesError, adapter_for
from lakeducktor.config import (
    MetadataConfiguration,
    RunConfiguration,
    StorageConfiguration,
)
from lakeducktor.coordination import (
    CoordinationError,
    PostgresTreatmentCoordinator,
    TreatmentClaim,
    TreatmentCoordinator,
)
from lakeducktor.diagnosis import DiagnosisError, diagnose_inventory
from lakeducktor.executor import ExecutionError, execute_treatment
from lakeducktor.inventory import InventoryError, inventory_catalog
from lakeducktor.lake import BackendDetectionError, detect_metadata_backend
from lakeducktor.model import (
    BackendDetection,
    CatalogDiagnosis,
    CatalogInventory,
    DiagnosisState,
    MaintenanceOutcome,
    MaintenanceState,
    MetadataBackend,
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
from lakeducktor.telemetry import (
    TelemetryError,
    TelemetryServer,
    WorkerTelemetry,
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
type DetectionFunction = Callable[[MetadataConfiguration], BackendDetection]
type CoordinatorFactory = Callable[
    [MetadataConfiguration],
    TreatmentCoordinator,
]
type CycleFunction = Callable[[], MaintenanceOutcome]


class MaintenanceError(RuntimeError):
    """A one-shot maintenance attempt failed."""


class RunError(RuntimeError):
    """The long-lived worker could not be started."""


class MaintenanceObserver(Protocol):
    def observe_plan(
        self,
        inventory: CatalogInventory,
        diagnosis: CatalogDiagnosis,
        plan: PriorityPlan,
        memory_deferred: int,
    ) -> None: ...

    def treatment_started(self, selection: TreatmentSelection) -> None: ...

    def treatment_finished(
        self,
        selection: TreatmentSelection,
        result: TreatmentResult | None,
        error: Exception | None,
        duration_seconds: float,
    ) -> None: ...


class RunLoopObserver(MaintenanceObserver, Protocol):
    def cycle_started(self) -> None: ...

    def cycle_completed(self, outcome: MaintenanceOutcome) -> None: ...

    def cycle_failed(self) -> None: ...

    def idle(self, duration_seconds: float) -> None: ...

    def request_stop(self) -> None: ...

    def stopped(self) -> None: ...


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
    observer: MaintenanceObserver | None = None,
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
                if observer is not None:
                    observer.treatment_started(selection)
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
                    if observer is not None:
                        observer.treatment_finished(
                            selection,
                            None,
                            error,
                            duration_seconds,
                        )
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
                if observer is not None:
                    observer.treatment_finished(
                        selection,
                        result,
                        None,
                        duration_seconds,
                    )
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


def maintenance_cycle(
    configuration: MetadataConfiguration,
    storage: StorageConfiguration,
    envelope: ResourceEnvelope,
    *,
    observer: MaintenanceObserver | None = None,
    detect: DetectionFunction = detect_metadata_backend,
    inventory: InventoryFunction = inventory_catalog,
    coordinator_factory: CoordinatorFactory = PostgresTreatmentCoordinator,
    execute: ExecutionFunction = execute_treatment,
) -> MaintenanceOutcome:
    """Discover all state afresh and perform at most one treatment."""

    try:
        detection = detect(configuration)
        adapter_for(detection)
        if detection.backend is not MetadataBackend.POSTGRES:
            raise MaintenanceError(
                f"unsupported coordination backend: {detection.backend.value}"
            )
        current_inventory = inventory(configuration, detection)
        diagnosis = diagnose_inventory(current_inventory)
        plan = prioritize(diagnosis)
        decision = select_treatment(plan, envelope)
        if observer is not None:
            observer.observe_plan(
                current_inventory,
                diagnosis,
                plan,
                decision.memory_deferred,
            )
        _LOGGER.info(
            "cycle metadata_backend=%s lakes=%s tables=%s "
            "actionable_tables=%s runnable=%s blocked=%s memory_deferred=%s",
            detection.backend.value,
            len(detection.metadata_schemas),
            current_inventory.table_count,
            sum(lake.actionable_tables for lake in diagnosis.lakes),
            plan.runnable,
            plan.blocked,
            decision.memory_deferred,
        )
        return maintain_once(
            configuration,
            storage,
            detection,
            envelope,
            plan,
            coordinator_factory(configuration),
            inventory=inventory,
            execute=execute,
            observer=observer,
        )
    except MaintenanceError:
        raise
    except (
        BackendDetectionError,
        CapabilitiesError,
        CoordinationError,
        DiagnosisError,
        ExecutionError,
        InventoryError,
        PriorityError,
        SelectionError,
    ) as error:
        raise MaintenanceError(str(error)) from error


def run_loop(
    cycle: CycleFunction,
    configuration: RunConfiguration,
    observer: RunLoopObserver,
    stop_event: Event,
) -> None:
    """Drain useful work immediately and wait interruptibly when idle."""

    try:
        while not stop_event.is_set():
            observer.cycle_started()
            should_wait = False
            try:
                outcome = cycle()
            except Exception as error:
                observer.cycle_failed()
                should_wait = True
                _LOGGER.exception(
                    "cycle_failed error=%s retry_seconds=%.3f",
                    error,
                    configuration.poll_interval_seconds,
                )
            else:
                observer.cycle_completed(outcome)
                if outcome.state is MaintenanceState.NO_TREATMENT:
                    should_wait = True
                    _LOGGER.info(
                        "worker_idle reason=%s claim_contention=%s poll_seconds=%.3f",
                        outcome.selection_reason.value
                        if outcome.selection_reason is not None
                        else "unknown",
                        outcome.claim_contention,
                        configuration.poll_interval_seconds,
                    )
                elif (
                    outcome.state is MaintenanceState.COMPLETED
                    and outcome.result is not None
                    and outcome.result.files_processed == 0
                    and outcome.result.files_created == 0
                ):
                    should_wait = True
                    _LOGGER.warning(
                        "worker_idle reason=no_progress poll_seconds=%.3f",
                        configuration.poll_interval_seconds,
                    )
            if stop_event.is_set():
                break
            if should_wait:
                observer.idle(configuration.poll_interval_seconds)
                stop_event.wait(configuration.poll_interval_seconds)
    finally:
        observer.stopped()


def _install_signal_handlers(
    stop_event: Event,
    observer: RunLoopObserver,
) -> Callable[[], None]:
    previous = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }

    def request_stop(signum: int, _frame: FrameType | None) -> None:
        if not stop_event.is_set():
            _LOGGER.info("shutdown_requested signal=%s", signal.Signals(signum).name)
        observer.request_stop()
        stop_event.set()

    for signum in previous:
        signal.signal(signum, request_stop)

    def restore() -> None:
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    return restore


def run_service(
    metadata: MetadataConfiguration,
    storage: StorageConfiguration,
    envelope: ResourceEnvelope,
    configuration: RunConfiguration,
) -> None:
    """Run one isolated DuckDB worker with independent health supervision."""

    telemetry = WorkerTelemetry(
        poll_interval_seconds=configuration.poll_interval_seconds,
        treatment_stuck_after_seconds=configuration.treatment_stuck_after_seconds,
    )
    try:
        server = TelemetryServer(
            telemetry,
            configuration.metrics_host,
            configuration.metrics_port,
        )
    except TelemetryError as error:
        raise RunError(str(error)) from error
    stop_event = Event()
    restore_signals = _install_signal_handlers(stop_event, telemetry)
    try:
        server.start()
        host, port = server.address
        _LOGGER.info(
            "worker_started poll_interval_seconds=%.3f "
            "treatment_stuck_after_seconds=%.3f metrics=%s:%s",
            configuration.poll_interval_seconds,
            configuration.treatment_stuck_after_seconds,
            host,
            port,
        )
        run_loop(
            lambda: maintenance_cycle(
                metadata,
                storage,
                envelope,
                observer=telemetry,
            ),
            configuration,
            telemetry,
            stop_event,
        )
    finally:
        restore_signals()
        server.close()
        _LOGGER.info("worker_stopped")
