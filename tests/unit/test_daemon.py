import pytest

from lakeducktor.config import (
    MetadataConfiguration,
    RunConfiguration,
    StorageConfiguration,
)
from lakeducktor.daemon import MaintenanceError, maintain_once, run_loop
from lakeducktor.diagnosis import diagnose_inventory
from lakeducktor.executor import ExecutionError
from lakeducktor.model import (
    BackendDetection,
    CatalogInventory,
    CompatibleFileGroup,
    FileSizeDistribution,
    LakeInventory,
    MaintenanceOutcome,
    MaintenanceState,
    MetadataBackend,
    ResourceEnvelope,
    SelectionReason,
    TableInventory,
    TreatmentResult,
)
from lakeducktor.priority import prioritize

_METADATA = MetadataConfiguration(
    backend_hint="postgres",
    host="catalog.example",
    port=5432,
    username="user",
    password="password",
    database="lake",
)
_STORAGE = StorageConfiguration(
    provider="s3-compatible",
    endpoint="objects.example",
    region="us-east-1",
    access_key_id="key",
    secret_access_key="secret",
    bucket="lake",
    use_ssl=True,
)
_DETECTION = BackendDetection(
    backend=MetadataBackend.POSTGRES,
    metadata_schemas=("lake", "other"),
    extension_version="v1",
    duckdb_extensions=(),
)
_ENVELOPE = ResourceEnvelope(
    duckdb_threads=4,
    duckdb_memory="500001000B",
    duckdb_memory_bytes=500_001_000,
)


def table(table_id: int, *, files: int = 0, file_bytes: int = 0) -> TableInventory:
    groups = (
        (
            CompatibleFileGroup(
                schema_version=1,
                partition_id=None,
                active_files=files,
                active_bytes=file_bytes,
                merge_candidate_files=files,
                merge_candidate_bytes=file_bytes,
            ),
        )
        if files
        else ()
    )
    return TableInventory(
        metadata_schema="lake",
        table_id=table_id,
        schema_name="main",
        table_name=f"table_{table_id}",
        auto_compact=True,
        target_file_size_bytes=100,
        rewrite_delete_threshold=0.95,
        sorting_enabled=False,
        active_data_files=files,
        active_data_bytes=file_bytes,
        active_data_rows=files,
        data_file_sizes=FileSizeDistribution(0, 0, 0, 0),
        compatible_file_groups=groups,
        active_delete_files=0,
        active_delete_bytes=0,
        deleted_rows=0,
        dangling_delete_files=0,
        rewrite_data_files=0,
        rewrite_input_bytes=0,
        rewrite_delete_files=0,
        rewrite_delete_bytes=0,
        rewrite_deleted_rows=0,
        rewrite_original_rows=0,
    )


def catalog(*tables: TableInventory) -> CatalogInventory:
    return CatalogInventory(
        lakes=(
            LakeInventory(
                metadata_schema="lake",
                latest_snapshot_id=1,
                latest_snapshot_at=None,
                scheduled_files=0,
                oldest_scheduled_at=None,
                tables=tables,
            ),
        )
    )


class FakeClaim:
    def __init__(self) -> None:
        self.released = False

    def release(self) -> None:
        self.released = True


class FakeCoordinator:
    def __init__(self, busy: set[int] | None = None) -> None:
        self.busy = busy or set()
        self.attempts: list[int] = []
        self.claims: list[FakeClaim] = []

    def try_claim(self, _metadata_schema: str, table_id: int) -> FakeClaim | None:
        self.attempts.append(table_id)
        if table_id in self.busy:
            return None
        claim = FakeClaim()
        self.claims.append(claim)
        return claim


class InventorySequence:
    def __init__(self, *inventories: CatalogInventory) -> None:
        self.inventories = list(inventories)
        self.schemas: list[tuple[str, ...]] = []

    def __call__(
        self,
        _configuration: MetadataConfiguration,
        detection: BackendDetection,
    ) -> CatalogInventory:
        self.schemas.append(detection.metadata_schemas)
        return self.inventories.pop(0)


class RecordingObserver:
    def __init__(self) -> None:
        self.events: list[object] = []

    def cycle_started(self) -> None:
        self.events.append("cycle_started")

    def cycle_completed(self, outcome: MaintenanceOutcome) -> None:
        self.events.append(("cycle_completed", outcome.state))

    def cycle_failed(self) -> None:
        self.events.append("cycle_failed")

    def idle(self, duration_seconds: float) -> None:
        self.events.append(("idle", duration_seconds))

    def request_stop(self) -> None:
        self.events.append("request_stop")

    def stopped(self) -> None:
        self.events.append("stopped")

    def observe_plan(self, *_arguments) -> None:
        return

    def treatment_started(self, selection) -> None:
        self.events.append(("treatment_started", selection.table_id))

    def treatment_finished(
        self,
        selection,
        result,
        error,
        duration_seconds: float,
    ) -> None:
        self.events.append(
            (
                "treatment_finished",
                selection.table_id,
                result,
                error,
                duration_seconds,
            )
        )


class RecordingStopEvent:
    def __init__(self) -> None:
        self.set_value = False
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return self.set_value

    def set(self) -> None:
        self.set_value = True

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        self.set()
        return True


def no_treatment_outcome() -> MaintenanceOutcome:
    return MaintenanceOutcome(
        state=MaintenanceState.NO_TREATMENT,
        selection=None,
        result=None,
        selection_reason=SelectionReason.NO_RUNNABLE_TREATMENTS,
        claim_contention=0,
        duration_seconds=None,
        table_present=None,
        still_actionable=None,
    )


def stale_outcome() -> MaintenanceOutcome:
    return MaintenanceOutcome(
        state=MaintenanceState.STALE,
        selection=None,
        result=None,
        selection_reason=None,
        claim_contention=0,
        duration_seconds=None,
        table_present=None,
        still_actionable=None,
    )


def test_busy_top_candidate_does_not_idle_the_worker() -> None:
    initial = catalog(
        table(1, files=6, file_bytes=120), table(2, files=4, file_bytes=80)
    )
    initial_plan = prioritize(diagnose_inventory(initial))
    inventory = InventorySequence(
        catalog(table(2, files=4, file_bytes=80)),
        catalog(table(2)),
    )
    coordinator = FakeCoordinator(busy={1})
    executed = []

    outcome = maintain_once(
        _METADATA,
        _STORAGE,
        _DETECTION,
        _ENVELOPE,
        initial_plan,
        coordinator,
        inventory=inventory,
        execute=lambda _metadata, _storage, _envelope, selection: (
            executed.append(selection) or TreatmentResult(4, 1)
        ),
    )

    assert coordinator.attempts == [1, 2]
    assert outcome.state is MaintenanceState.COMPLETED
    assert outcome.selection is not None
    assert outcome.selection.table_id == 2
    assert outcome.claim_contention == 1
    assert outcome.table_present is True
    assert outcome.still_actionable is False
    assert executed[0].table_id == 2
    assert coordinator.claims[0].released is True
    assert inventory.schemas == [("lake",), ("lake",)]


def test_stale_treatment_is_not_executed() -> None:
    initial = catalog(table(1, files=4, file_bytes=80))
    coordinator = FakeCoordinator()
    executed = []

    outcome = maintain_once(
        _METADATA,
        _STORAGE,
        _DETECTION,
        _ENVELOPE,
        prioritize(diagnose_inventory(initial)),
        coordinator,
        inventory=InventorySequence(catalog(table(1))),
        execute=lambda *_arguments: executed.append(True),
    )

    assert outcome.state is MaintenanceState.STALE
    assert executed == []
    assert coordinator.claims[0].released is True


def test_failed_treatment_releases_claim() -> None:
    initial = catalog(table(1, files=4, file_bytes=80))
    coordinator = FakeCoordinator()

    def fail(*_arguments) -> TreatmentResult:
        raise ExecutionError("native failure")

    with pytest.raises(MaintenanceError, match="native failure"):
        maintain_once(
            _METADATA,
            _STORAGE,
            _DETECTION,
            _ENVELOPE,
            prioritize(diagnose_inventory(initial)),
            coordinator,
            inventory=InventorySequence(initial),
            execute=fail,
        )

    assert coordinator.claims[0].released is True


def test_treatment_observer_brackets_only_native_execution() -> None:
    initial = catalog(table(1, files=4, file_bytes=80))
    observer = RecordingObserver()

    outcome = maintain_once(
        _METADATA,
        _STORAGE,
        _DETECTION,
        _ENVELOPE,
        prioritize(diagnose_inventory(initial)),
        FakeCoordinator(),
        inventory=InventorySequence(initial, catalog(table(1))),
        execute=lambda *_arguments: TreatmentResult(4, 1),
        observer=observer,
    )

    assert outcome.state is MaintenanceState.COMPLETED
    assert observer.events[0] == ("treatment_started", 1)
    finished = observer.events[1]
    assert isinstance(finished, tuple)
    assert finished[:4] == (
        "treatment_finished",
        1,
        TreatmentResult(4, 1),
        None,
    )


def test_run_loop_sleeps_interruptibly_when_there_is_no_work() -> None:
    stop_event = RecordingStopEvent()
    observer = RecordingObserver()
    configuration = RunConfiguration(12.5, 60, "127.0.0.1", 8_000)

    run_loop(
        no_treatment_outcome,
        configuration,
        observer,
        stop_event,  # type: ignore[arg-type]
    )

    assert stop_event.waits == [12.5]
    assert observer.events == [
        "cycle_started",
        ("cycle_completed", MaintenanceState.NO_TREATMENT),
        ("idle", 12.5),
        "stopped",
    ]


def test_run_loop_retries_failures_after_the_same_interruptible_wait() -> None:
    stop_event = RecordingStopEvent()
    observer = RecordingObserver()
    configuration = RunConfiguration(7, 60, "127.0.0.1", 8_000)

    def fail() -> MaintenanceOutcome:
        raise MaintenanceError("catalog unavailable")

    run_loop(
        fail,
        configuration,
        observer,
        stop_event,  # type: ignore[arg-type]
    )

    assert stop_event.waits == [7]
    assert observer.events == [
        "cycle_started",
        "cycle_failed",
        ("idle", 7),
        "stopped",
    ]


def test_run_loop_replans_stale_work_immediately() -> None:
    stop_event = RecordingStopEvent()
    observer = RecordingObserver()
    configuration = RunConfiguration(7, 60, "127.0.0.1", 8_000)
    cycles = 0

    def cycle() -> MaintenanceOutcome:
        nonlocal cycles
        cycles += 1
        if cycles == 2:
            stop_event.set()
        return stale_outcome()

    run_loop(
        cycle,
        configuration,
        observer,
        stop_event,  # type: ignore[arg-type]
    )

    assert cycles == 2
    assert stop_event.waits == []
    assert observer.events == [
        "cycle_started",
        ("cycle_completed", MaintenanceState.STALE),
        "cycle_started",
        ("cycle_completed", MaintenanceState.STALE),
        "stopped",
    ]
