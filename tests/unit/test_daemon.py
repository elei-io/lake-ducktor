import pytest

from lakeducktor.config import (
    MetadataConfiguration,
    RunConfiguration,
    StorageConfiguration,
)
from lakeducktor.daemon import MaintenanceError, maintain_once, run_loop
from lakeducktor.diagnosis import diagnose_inventory
from lakeducktor.executor import ExecutionError, ExecutionFailureReason
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
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
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
        recent_data_files_60s=0,
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


def catalog(
    *tables: TableInventory,
    scheduled_files: int = 0,
    cleanup_eligible_files: int = 0,
) -> CatalogInventory:
    return CatalogInventory(
        lakes=(
            LakeInventory(
                metadata_schema="lake",
                latest_snapshot_id=1,
                latest_snapshot_at=None,
                scheduled_files=scheduled_files,
                oldest_scheduled_at=None,
                tables=tables,
                cleanup_eligible_files=cleanup_eligible_files,
            ),
        )
    )


class FakeClaim:
    def __init__(self) -> None:
        self.released = False

    def release(self) -> None:
        self.released = True


class FakeCoordinator:
    def __init__(self, busy: set[str] | None = None) -> None:
        self.busy = busy or set()
        self.attempts: list[str] = []
        self.claims: list[FakeClaim] = []

    def try_claim(self, metadata_schema: str) -> FakeClaim | None:
        self.attempts.append(metadata_schema)
        if metadata_schema in self.busy:
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

    def treatment_progress_observed(
        self,
        selection,
        files_before,
        files_after,
        snapshot_before,
        snapshot_after,
    ) -> None:
        self.events.append(
            (
                "treatment_progress_observed",
                selection.table_id,
                files_before,
                files_after,
                snapshot_before,
                snapshot_after,
            )
        )

    def retry_scheduled(
        self,
        reason: ExecutionFailureReason,
        duration_seconds: float,
    ) -> None:
        self.events.append(("retry_scheduled", reason, duration_seconds))

    def treatment_blocked(
        self,
        selection,
        reason: ExecutionFailureReason,
    ) -> None:
        self.events.append(("treatment_blocked", selection.table_id, reason))


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


class CountingStopEvent(RecordingStopEvent):
    def __init__(self, waits_before_stop: int) -> None:
        super().__init__()
        self.waits_before_stop = waits_before_stop

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        if len(self.waits) >= self.waits_before_stop:
            self.set()
        return self.is_set()


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


def test_busy_lake_skips_other_tables_in_the_same_lake() -> None:
    initial = catalog(
        table(1, files=6, file_bytes=120), table(2, files=4, file_bytes=80)
    )
    initial_plan = prioritize(diagnose_inventory(initial))
    inventory = InventorySequence()
    coordinator = FakeCoordinator(busy={"lake"})
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

    assert coordinator.attempts == ["lake"]
    assert outcome.state is MaintenanceState.NO_TREATMENT
    assert outcome.selection is None
    assert outcome.claim_contention == 1
    assert executed == []
    assert coordinator.claims == []
    assert inventory.schemas == []


def test_scheduled_cleanup_is_revalidated_and_executed_at_lake_scope() -> None:
    initial = catalog(scheduled_files=7, cleanup_eligible_files=3)
    after = catalog(scheduled_files=4, cleanup_eligible_files=0)
    executed = []

    outcome = maintain_once(
        _METADATA,
        _STORAGE,
        _DETECTION,
        _ENVELOPE,
        prioritize(diagnose_inventory(initial)),
        FakeCoordinator(),
        inventory=InventorySequence(initial, after),
        execute=lambda _metadata, _storage, _envelope, selection: (
            executed.append(selection) or TreatmentResult(3, 0)
        ),
    )

    assert outcome.state is MaintenanceState.COMPLETED
    assert len(executed) == 1
    assert executed[0].kind.value == "scheduled_file_cleanup"
    assert executed[0].table_id is None
    assert outcome.result == TreatmentResult(3, 0)
    assert outcome.still_actionable is False


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


def test_failed_treatment_reports_post_failure_progress() -> None:
    initial = catalog(table(1, files=4, file_bytes=80))
    observer = RecordingObserver()

    def fail(*_arguments) -> TreatmentResult:
        raise ExecutionError(
            "native conflict",
            reason=ExecutionFailureReason.CONCURRENT_COMPACTION,
        )

    with pytest.raises(MaintenanceError) as error:
        maintain_once(
            _METADATA,
            _STORAGE,
            _DETECTION,
            _ENVELOPE,
            prioritize(diagnose_inventory(initial)),
            FakeCoordinator(),
            inventory=InventorySequence(
                initial,
                catalog(table(1, files=2, file_bytes=40)),
            ),
            execute=fail,
            observer=observer,
        )

    assert error.value.reason is ExecutionFailureReason.CONCURRENT_COMPACTION
    assert error.value.committed_progress is True
    assert (
        "treatment_progress_observed",
        1,
        4,
        2,
        1,
        1,
    ) in observer.events


def test_failed_treatment_reports_verified_zero_progress() -> None:
    initial = catalog(table(1, files=4, file_bytes=80))

    def fail(*_arguments) -> TreatmentResult:
        raise ExecutionError(
            "native OOM",
            reason=ExecutionFailureReason.RESOURCE_EXHAUSTED,
        )

    with pytest.raises(MaintenanceError) as error:
        maintain_once(
            _METADATA,
            _STORAGE,
            _DETECTION,
            _ENVELOPE,
            prioritize(diagnose_inventory(initial)),
            FakeCoordinator(),
            inventory=InventorySequence(initial, initial),
            execute=fail,
        )

    assert error.value.reason is ExecutionFailureReason.RESOURCE_EXHAUSTED
    assert error.value.committed_progress is False


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


@pytest.mark.parametrize(
    "reason",
    [
        ExecutionFailureReason.CONCURRENT_COMPACTION,
        ExecutionFailureReason.TRANSACTION_CONFLICT,
    ],
)
def test_transient_conflicts_use_bounded_exponential_backoff(
    reason: ExecutionFailureReason,
) -> None:
    stop_event = CountingStopEvent(3)
    observer = RecordingObserver()
    configuration = RunConfiguration(
        7,
        60,
        "127.0.0.1",
        8_000,
        conflict_backoff_base_seconds=5,
        conflict_backoff_max_seconds=12,
    )

    def fail() -> MaintenanceOutcome:
        raise MaintenanceError(
            "compaction conflict",
            reason=reason,
        )

    run_loop(
        fail,
        configuration,
        observer,
        stop_event,  # type: ignore[arg-type]
    )

    assert stop_event.waits == [5, 10, 12]
    assert [
        event
        for event in observer.events
        if isinstance(event, tuple) and event[0] == "retry_scheduled"
    ] == [
        ("retry_scheduled", reason, 5),
        ("retry_scheduled", reason, 10),
        ("retry_scheduled", reason, 12),
    ]


@pytest.mark.parametrize(
    "reason",
    (
        ExecutionFailureReason.RESOURCE_EXHAUSTED,
        ExecutionFailureReason.STORAGE_ERROR,
    ),
)
def test_non_transient_zero_progress_failure_blocks_the_table_without_retrying(
    reason: ExecutionFailureReason,
) -> None:
    stop_event = CountingStopEvent(2)
    observer = RecordingObserver()
    configuration = RunConfiguration(7, 60, "127.0.0.1", 8_000)
    selected = TreatmentSelection(
        kind=TreatmentKind.MERGE,
        priority_rank=1,
        metadata_schema="lake",
        table_id=1,
        schema_name="main",
        table_name="table_1",
        input_bytes=80,
        admitted_bytes=80,
        sorting_enabled=True,
        memory_headroom_bytes=250,
        usable_memory_bytes=250,
        max_compacted_files=1,
        input_files=4,
        admitted_input_files=4,
    )
    blocked: set[tuple[str, int | None]] = set()
    cycles = 0

    def cycle() -> MaintenanceOutcome:
        nonlocal cycles
        cycles += 1
        if cycles == 1:
            raise MaintenanceError(
                "native failure",
                reason=reason,
                selection=selected,
                committed_progress=False,
            )
        assert blocked == {("lake", 1)}
        return no_treatment_outcome()

    run_loop(
        cycle,
        configuration,
        observer,
        stop_event,  # type: ignore[arg-type]
        blocked,
    )

    assert cycles == 2
    assert blocked == {("lake", 1)}
    assert (
        "treatment_blocked",
        1,
        reason,
    ) in observer.events


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


def test_run_loop_treats_flushed_rows_as_progress() -> None:
    stop_event = RecordingStopEvent()
    observer = RecordingObserver()
    configuration = RunConfiguration(7, 60, "127.0.0.1", 8_000)
    cycles = 0

    def cycle() -> MaintenanceOutcome:
        nonlocal cycles
        cycles += 1
        if cycles == 1:
            return MaintenanceOutcome(
                state=MaintenanceState.COMPLETED,
                selection=None,
                result=TreatmentResult(
                    files_processed=0,
                    files_created=0,
                    rows_processed=50,
                ),
                selection_reason=None,
                claim_contention=0,
                duration_seconds=1,
                table_present=True,
                still_actionable=False,
            )
        return no_treatment_outcome()

    run_loop(
        cycle,
        configuration,
        observer,
        stop_event,  # type: ignore[arg-type]
    )

    assert cycles == 2
    assert stop_event.waits == [7]
