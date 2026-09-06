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
from lakeducktor.executor import (
    ExecutionError,
    ExecutionFailureReason,
    IsolatedTreatmentExecutor,
    execute_treatment,
)
from lakeducktor.inventory import (
    InventoryError,
    MaintenanceInventory,
    inventory_catalog,
)
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

    def __init__(
        self,
        message: str,
        *,
        reason: ExecutionFailureReason | None = None,
        selection: TreatmentSelection | None = None,
        committed_progress: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.selection = selection
        self.committed_progress = committed_progress

    @property
    def transient(self) -> bool:
        return self.reason in {
            ExecutionFailureReason.CONCURRENT_COMPACTION,
            ExecutionFailureReason.SNAPSHOT_RETRY_EXHAUSTED,
        }


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

    def treatment_progress_observed(
        self,
        selection: TreatmentSelection,
        files_before: int,
        files_after: int,
        snapshot_before: int | None,
        snapshot_after: int | None,
    ) -> None: ...


class RunLoopObserver(MaintenanceObserver, Protocol):
    def cycle_started(self) -> None: ...

    def cycle_completed(self, outcome: MaintenanceOutcome) -> None: ...

    def cycle_failed(self) -> None: ...

    def idle(self, duration_seconds: float) -> None: ...

    def request_stop(self) -> None: ...

    def stopped(self) -> None: ...

    def retry_scheduled(
        self,
        reason: ExecutionFailureReason,
        duration_seconds: float,
    ) -> None: ...

    def treatment_blocked(
        self,
        selection: TreatmentSelection,
        reason: ExecutionFailureReason,
    ) -> None: ...


def _fresh_state(
    configuration: MetadataConfiguration,
    detection: BackendDetection,
    metadata_schema: str,
    inventory: InventoryFunction,
):
    lake_detection = replace(detection, metadata_schemas=(metadata_schema,))
    fresh_inventory = inventory(configuration, lake_detection)
    diagnosis = diagnose_inventory(fresh_inventory)
    return fresh_inventory, diagnosis, prioritize(diagnosis)


def _selected_table_input_files(
    inventory: CatalogInventory,
    selection: TreatmentSelection,
) -> int | None:
    for lake in inventory.lakes:
        if lake.metadata_schema == selection.metadata_schema:
            if selection.kind is TreatmentKind.SNAPSHOT_EXPIRATION:
                return lake.expiring_snapshots
            if selection.kind is TreatmentKind.SCHEDULED_FILE_CLEANUP:
                return lake.cleanup_eligible_files
            if selection.kind is TreatmentKind.ORPHAN_FILE_CLEANUP:
                return lake.orphan_files
        for table in lake.tables:
            if (
                table.metadata_schema == selection.metadata_schema
                and table.table_id == selection.table_id
            ):
                if selection.kind is TreatmentKind.DELETE_REWRITE:
                    return table.rewrite_data_files
                if selection.kind is TreatmentKind.INLINE_FLUSH:
                    return table.inlined_data_rows
                return sum(
                    group.merge_candidate_files
                    for group in table.compatible_file_groups
                )
    return None


def _lake_snapshot(
    inventory: CatalogInventory,
    metadata_schema: str,
) -> int | None:
    for lake in inventory.lakes:
        if lake.metadata_schema == metadata_schema:
            return lake.latest_snapshot_id
    return None


def _still_actionable(
    diagnosis,
    selection: TreatmentSelection,
) -> tuple[bool, bool]:
    if selection.kind in {
        TreatmentKind.SNAPSHOT_EXPIRATION,
        TreatmentKind.SCHEDULED_FILE_CLEANUP,
        TreatmentKind.ORPHAN_FILE_CLEANUP,
    }:
        lake = next(
            (
                lake
                for lake in diagnosis.lakes
                if lake.metadata_schema == selection.metadata_schema
            ),
            None,
        )
        if lake is None:
            return False, False
        if selection.kind is TreatmentKind.SNAPSHOT_EXPIRATION:
            return True, lake.expiring_snapshots > 0
        if selection.kind is TreatmentKind.SCHEDULED_FILE_CLEANUP:
            return True, lake.cleanup_eligible_files > 0
        return True, lake.orphan_files > 0
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
    if selection.kind is TreatmentKind.INLINE_FLUSH:
        return True, (
            table.inlined_data_rows >= table.inline_flush_threshold_rows
            or table.inlined_data_bytes >= table.inline_flush_max_bytes
        )
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
    unavailable_tables: frozenset[tuple[str, int | None]] = frozenset(),
) -> MaintenanceOutcome:
    """Claim, revalidate, and execute at most one treatment."""

    if inventory is inventory_catalog:
        inventory = MaintenanceInventory(storage)
    unavailable = set(unavailable_tables)
    claim_contention = 0
    failure_committed_progress: bool | None = None
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
            )
            if claim is not None:
                break
            claim_contention += 1
            unavailable.update(
                (
                    candidate.metadata_schema,
                    getattr(candidate, "table_id", None),
                )
                for candidate in (
                    *initial_plan.snapshot_expirations,
                    *initial_plan.scheduled_file_cleanups,
                    *initial_plan.orphan_file_cleanups,
                    *initial_plan.inline_flushes,
                    *initial_plan.delete_rewrites,
                    *initial_plan.merges,
                )
                if candidate.metadata_schema == selection.metadata_schema
            )
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
            fresh_inventory, _, fresh_plan = _fresh_state(
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
                    "schema=%r table=%r input_files=%s input_rows=%s "
                    "input_snapshots=%s "
                    "admitted_input_files=%s "
                    "input_bytes=%s "
                    "lake_target_bytes=%s execution_target_bytes=%s "
                    "admitted_bytes=%s "
                    "sorting_enabled=%s memory_headroom_bytes=%s "
                    "usable_memory_bytes=%s max_compacted_files=%s "
                    "retention_policy=%s",
                    selection.kind.value,
                    selection.metadata_schema,
                    selection.table_id,
                    selection.schema_name,
                    selection.table_name,
                    selection.input_files,
                    selection.input_rows,
                    selection.input_snapshots,
                    selection.admitted_input_files,
                    selection.input_bytes,
                    selection.lake_target_file_size_bytes
                    if selection.lake_target_file_size_bytes is not None
                    else "none",
                    selection.execution_target_file_size_bytes
                    if selection.execution_target_file_size_bytes is not None
                    else "none",
                    selection.admitted_bytes,
                    str(selection.sorting_enabled).lower(),
                    selection.memory_headroom_bytes,
                    selection.usable_memory_bytes,
                    selection.max_compacted_files
                    if selection.max_compacted_files is not None
                    else "none",
                    selection.retention_policy
                    if selection.retention_policy is not None
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
                    try:
                        lake_detection = replace(
                            detection,
                            metadata_schemas=(selection.metadata_schema,),
                        )
                        after_inventory = inventory(configuration, lake_detection)
                        files_after = _selected_table_input_files(
                            after_inventory,
                            selection,
                        )
                        snapshot_before = _lake_snapshot(
                            fresh_inventory,
                            selection.metadata_schema,
                        )
                        snapshot_after = _lake_snapshot(
                            after_inventory,
                            selection.metadata_schema,
                        )
                        if files_after is not None:
                            failure_committed_progress = (
                                files_after < selection.input_files
                                or (
                                    snapshot_before is not None
                                    and snapshot_after is not None
                                    and snapshot_after > snapshot_before
                                )
                            )
                            if observer is not None:
                                observer.treatment_progress_observed(
                                    selection,
                                    selection.input_files,
                                    files_after,
                                    snapshot_before,
                                    snapshot_after,
                                )
                            _LOGGER.info(
                                "treatment_failed_progress kind=%s lake=%s "
                                "table_id=%s input_files_before=%s "
                                "input_files_after=%s files_eliminated=%s "
                                "snapshot_before=%s snapshot_after=%s",
                                selection.kind.value,
                                selection.metadata_schema,
                                selection.table_id,
                                selection.input_files,
                                files_after,
                                max(0, selection.input_files - files_after),
                                snapshot_before,
                                snapshot_after,
                            )
                    except Exception as progress_error:
                        _LOGGER.warning(
                            "treatment_failed_progress_unavailable kind=%s "
                            "lake=%s table_id=%s error=%s",
                            selection.kind.value,
                            selection.metadata_schema,
                            selection.table_id,
                            progress_error,
                        )
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
                treatment_completed = getattr(
                    inventory,
                    "treatment_completed",
                    None,
                )
                if treatment_completed is not None:
                    treatment_completed(selection, result)
                if observer is not None:
                    observer.treatment_finished(
                        selection,
                        result,
                        None,
                        duration_seconds,
                    )
                _LOGGER.info(
                    "treatment_native_completed kind=%s lake=%s table_id=%s "
                    "files_processed=%s files_created=%s rows_processed=%s "
                    "snapshots_processed=%s "
                    "duration_seconds=%.3f",
                    selection.kind.value,
                    selection.metadata_schema,
                    selection.table_id,
                    result.files_processed,
                    result.files_created,
                    result.rows_processed,
                    result.snapshots_processed,
                    duration_seconds,
                )
                if (
                    selection.admitted_input_files > 0
                    and result.files_processed > selection.admitted_input_files
                ):
                    _LOGGER.warning(
                        "treatment_input_bound_exceeded kind=%s lake=%s "
                        "table_id=%s admitted_input_files=%s "
                        "actual_input_files=%s",
                        selection.kind.value,
                        selection.metadata_schema,
                        selection.table_id,
                        selection.admitted_input_files,
                        result.files_processed,
                    )
                _, verification, _ = _fresh_state(
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
    except ExecutionError as error:
        raise MaintenanceError(
            str(error),
            reason=error.reason,
            selection=selection,
            committed_progress=failure_committed_progress,
        ) from error
    except (
        CoordinationError,
        DiagnosisError,
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
    unavailable_tables: frozenset[tuple[str, int | None]] = frozenset(),
) -> MaintenanceOutcome:
    """Discover all state afresh and perform at most one treatment."""

    try:
        if inventory is inventory_catalog:
            inventory = MaintenanceInventory(storage)
        detection = detect(configuration)
        adapter_for(detection)
        if detection.backend is not MetadataBackend.POSTGRES:
            raise MaintenanceError(
                f"unsupported coordination backend: {detection.backend.value}"
            )
        current_inventory = inventory(configuration, detection)
        diagnosis = diagnose_inventory(current_inventory)
        plan = prioritize(diagnosis)
        decision = select_treatment(plan, envelope, unavailable_tables)
        if observer is not None:
            observer.observe_plan(
                current_inventory,
                diagnosis,
                plan,
                decision.memory_deferred,
            )
        _LOGGER.info(
            "cycle metadata_backend=%s lakes=%s tables=%s "
            "actionable_tables=%s expiring_snapshots=%s "
            "cleanup_eligible_files=%s orphan_files=%s "
            "runnable=%s blocked=%s waiting=%s memory_deferred=%s",
            detection.backend.value,
            len(detection.metadata_schemas),
            current_inventory.table_count,
            sum(lake.actionable_tables for lake in diagnosis.lakes),
            sum(lake.expiring_snapshots for lake in diagnosis.lakes),
            sum(lake.cleanup_eligible_files for lake in diagnosis.lakes),
            sum(lake.orphan_files for lake in diagnosis.lakes),
            plan.runnable,
            plan.blocked,
            plan.waiting,
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
            unavailable_tables=unavailable_tables,
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
    blocked_tables: set[tuple[str, int | None]] | None = None,
) -> None:
    """Drain useful work immediately and wait interruptibly when idle."""

    consecutive_conflicts = 0
    failure_blocked_tables = blocked_tables if blocked_tables is not None else set()
    try:
        while not stop_event.is_set():
            observer.cycle_started()
            should_wait = False
            wait_seconds = configuration.poll_interval_seconds
            try:
                outcome = cycle()
            except MaintenanceError as error:
                observer.cycle_failed()
                should_wait = True
                selection = error.selection
                if (
                    error.reason is not None
                    and not error.transient
                    and error.reason is not ExecutionFailureReason.INTERRUPTED
                    and error.committed_progress is False
                    and selection is not None
                    and selection.table_id is not None
                ):
                    key = (selection.metadata_schema, selection.table_id)
                    if key not in failure_blocked_tables:
                        failure_blocked_tables.add(key)
                        observer.treatment_blocked(selection, error.reason)
                        _LOGGER.error(
                            "treatment_blocked kind=%s lake=%s table_id=%s "
                            "reason=%s committed_progress=false",
                            selection.kind.value,
                            selection.metadata_schema,
                            selection.table_id,
                            error.reason.value,
                        )
                if error.transient and error.reason is not None:
                    consecutive_conflicts += 1
                    wait_seconds = min(
                        configuration.conflict_backoff_max_seconds,
                        configuration.conflict_backoff_base_seconds
                        * (2 ** (consecutive_conflicts - 1)),
                    )
                    observer.retry_scheduled(error.reason, wait_seconds)
                else:
                    consecutive_conflicts = 0
                _LOGGER.exception(
                    "cycle_failed error=%s reason=%s retry_seconds=%.3f",
                    error,
                    error.reason.value if error.reason is not None else "unknown",
                    wait_seconds,
                )
            except Exception as error:
                consecutive_conflicts = 0
                observer.cycle_failed()
                should_wait = True
                _LOGGER.exception(
                    "cycle_failed error=%s reason=unknown retry_seconds=%.3f",
                    error,
                    wait_seconds,
                )
            else:
                consecutive_conflicts = 0
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
                    and outcome.result.rows_processed == 0
                    and outcome.result.snapshots_processed == 0
                ):
                    should_wait = True
                    _LOGGER.warning(
                        "worker_idle reason=no_progress poll_seconds=%.3f",
                        configuration.poll_interval_seconds,
                    )
            if stop_event.is_set():
                break
            if should_wait:
                observer.idle(wait_seconds)
                stop_event.wait(wait_seconds)
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
    execute_isolated = IsolatedTreatmentExecutor(stop_event)
    blocked_tables: set[tuple[str, int | None]] = set()
    maintenance_inventory = MaintenanceInventory(
        storage,
        orphan_scan_interval_seconds=configuration.orphan_scan_interval_seconds,
        orphan_cleanup_enabled=configuration.orphan_cleanup_enabled,
        orphan_probe_observer=telemetry.orphan_probe_completed,
    )
    restore_signals = _install_signal_handlers(stop_event, telemetry)
    try:
        server.start()
        host, port = server.address
        _LOGGER.info(
            "worker_started poll_interval_seconds=%.3f "
            "treatment_stuck_after_seconds=%.3f "
            "orphan_scan_interval_seconds=%.3f "
            "orphan_cleanup_enabled=%s metrics=%s:%s",
            configuration.poll_interval_seconds,
            configuration.treatment_stuck_after_seconds,
            configuration.orphan_scan_interval_seconds,
            configuration.orphan_cleanup_enabled,
            host,
            port,
        )
        run_loop(
            lambda: maintenance_cycle(
                metadata,
                storage,
                envelope,
                observer=telemetry,
                inventory=maintenance_inventory,
                execute=execute_isolated,
                unavailable_tables=frozenset(blocked_tables),
            ),
            configuration,
            telemetry,
            stop_event,
            blocked_tables,
        )
    finally:
        restore_signals()
        server.close()
        _LOGGER.info("worker_stopped")
