from concurrent.futures import ThreadPoolExecutor
from time import sleep
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest
from prometheus_client import generate_latest

from lakeducktor.executor import ExecutionError, ExecutionFailureReason
from lakeducktor.model import (
    MaintenanceOutcome,
    MaintenanceState,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)
from lakeducktor.telemetry import TelemetryServer, WorkerPhase, WorkerTelemetry


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def selection() -> TreatmentSelection:
    return TreatmentSelection(
        kind=TreatmentKind.MERGE,
        priority_rank=1,
        metadata_schema="lake",
        table_id=7,
        schema_name="main",
        table_name="events",
        input_bytes=1_000,
        admitted_bytes=1_000,
        sorting_enabled=False,
        memory_headroom_bytes=100,
        usable_memory_bytes=900,
        max_compacted_files=10,
    )


def telemetry(clock: FakeClock) -> WorkerTelemetry:
    return WorkerTelemetry(
        poll_interval_seconds=10,
        treatment_stuck_after_seconds=30,
        clock=clock,
        wall_clock=lambda: 1_000,
    )


def test_starting_worker_is_live_but_not_ready() -> None:
    health = telemetry(FakeClock()).health_snapshot()

    assert health.live is True
    assert health.ready is False
    assert health.phase is WorkerPhase.STARTING
    assert health.reason == "starting"


def test_overdue_cycle_and_treatment_are_not_ready_but_remain_live() -> None:
    clock = FakeClock()
    worker = telemetry(clock)
    worker.cycle_started()
    clock.advance(31)

    cycle_health = worker.watchdog_tick()

    assert cycle_health.live is True
    assert cycle_health.ready is False
    assert cycle_health.reason == "cycle_stuck"

    worker.treatment_started(selection())
    clock.advance(31)

    treatment_health = worker.watchdog_tick()

    assert treatment_health.live is True
    assert treatment_health.ready is False
    assert treatment_health.reason == "treatment_stuck"


def test_idle_worker_becomes_unready_if_the_loop_does_not_wake() -> None:
    clock = FakeClock()
    worker = telemetry(clock)
    worker.idle(10)
    clock.advance(21)

    health = worker.watchdog_tick()

    assert health.live is True
    assert health.ready is False
    assert health.reason == "loop_stuck"


def test_stop_request_removes_readiness_without_failing_liveness() -> None:
    clock = FakeClock()
    worker = telemetry(clock)
    worker.cycle_started()
    worker.request_stop()

    health = worker.health_snapshot()

    assert health.live is True
    assert health.ready is False
    assert health.reason == "stopping"


def test_cycle_failure_remains_unready_during_backoff() -> None:
    clock = FakeClock()
    worker = telemetry(clock)
    worker.cycle_failed()
    worker.idle(10)

    health = worker.health_snapshot()

    assert health.live is True
    assert health.ready is False
    assert health.phase is WorkerPhase.ERROR
    assert health.reason == "cycle_failed"


def test_treatment_progress_and_claim_contention_are_counted() -> None:
    clock = FakeClock()
    worker = telemetry(clock)
    chosen = selection()
    result = TreatmentResult(files_processed=124, files_created=62)
    worker.treatment_started(chosen)
    worker.treatment_finished(chosen, result, None, 42)
    worker.cycle_completed(
        MaintenanceOutcome(
            state=MaintenanceState.COMPLETED,
            selection=chosen,
            result=result,
            selection_reason=None,
            claim_contention=3,
            duration_seconds=42,
            table_present=True,
            still_actionable=True,
        )
    )

    metrics = generate_latest(worker.registry).decode()

    assert 'lakeducktor_treatment_files_processed_total{kind="merge"} 124.0' in metrics
    assert 'lakeducktor_treatment_files_created_total{kind="merge"} 62.0' in metrics
    assert 'lakeducktor_treatment_files_eliminated_total{kind="merge"} 62.0' in metrics
    assert "lakeducktor_claim_contention_total 3.0" in metrics


def test_conflict_retry_and_partial_progress_are_counted() -> None:
    worker = telemetry(FakeClock())
    chosen = selection()
    error = ExecutionError(
        "conflict",
        reason=ExecutionFailureReason.CONCURRENT_COMPACTION,
    )
    worker.treatment_started(chosen)
    worker.treatment_progress_observed(chosen, 512, 128, 10, 11)
    worker.treatment_finished(chosen, None, error, 4)
    worker.retry_scheduled(ExecutionFailureReason.CONCURRENT_COMPACTION, 5)

    metrics = generate_latest(worker.registry).decode()

    assert (
        'lakeducktor_treatment_conflicts_total{reason="concurrent_compaction"} 1.0'
        in metrics
    )
    assert (
        'lakeducktor_treatment_retries_total{reason="concurrent_compaction"} 1.0'
        in metrics
    )
    assert (
        'lakeducktor_treatment_partial_progress_files_total{kind="merge"} 384.0'
        in metrics
    )


def test_recent_insertion_files_are_exposed_as_an_aggregate_metric() -> None:
    worker = telemetry(FakeClock())
    tables = (
        SimpleNamespace(
            state=SimpleNamespace(value="actionable"),
            recent_data_files_60s=3,
            rewrite_data_files=0,
            inlined_data_rows=0,
            inlined_data_bytes=0,
        ),
        SimpleNamespace(
            state=SimpleNamespace(value="healthy"),
            recent_data_files_60s=4,
            rewrite_data_files=0,
            inlined_data_rows=0,
            inlined_data_bytes=0,
        ),
    )
    worker.observe_plan(
        SimpleNamespace(
            scheduled_files=0,
            lakes=(
                SimpleNamespace(
                    dangling_delete_files=0,
                    cleanup_eligible_files=3,
                ),
            ),
        ),
        SimpleNamespace(lakes=(SimpleNamespace(tables=tables),)),
        SimpleNamespace(
            excluded_tables=0,
            attention_tables=0,
            runnable=1,
            blocked=0,
            merges=(),
        ),
        memory_deferred=0,
    )

    metrics = generate_latest(worker.registry).decode()

    assert 'lakeducktor_recent_data_files{window="60s"} 7.0' in metrics
    assert "lakeducktor_cleanup_eligible_files 3.0" in metrics


def test_http_health_endpoints_and_metrics_share_no_worker_connection() -> None:
    clock = FakeClock()
    worker = telemetry(clock)
    worker.cycle_started()
    server = TelemetryServer(worker, "127.0.0.1", 0)
    server.start()
    host, port = server.address
    base_url = f"http://{host}:{port}"
    try:
        with urlopen(f"{base_url}/livez", timeout=2) as response:
            assert response.status == 200
        with urlopen(f"{base_url}/readyz", timeout=2) as response:
            assert response.status == 200
        with urlopen(f"{base_url}/metrics", timeout=2) as response:
            metrics = response.read().decode()
        assert "lakeducktor_worker_ready 1.0" in metrics

        clock.advance(31)
        worker.watchdog_tick()
        with pytest.raises(HTTPError) as error:
            urlopen(f"{base_url}/readyz", timeout=2)
        assert error.value.code == 503
        with urlopen(f"{base_url}/livez", timeout=2) as response:
            assert response.status == 200
    finally:
        server.close()


def test_http_server_queues_concurrent_probe_and_scrape_bursts() -> None:
    worker = telemetry(FakeClock())
    worker.cycle_started()
    server = TelemetryServer(worker, "127.0.0.1", 0)
    server.start()
    host, port = server.address
    urls = [
        f"http://{host}:{port}{path}"
        for _ in range(20)
        for path in ("/livez", "/readyz", "/metrics")
    ]

    def fetch(url: str) -> int:
        with urlopen(url, timeout=5) as response:
            response.read()
            return response.status

    try:
        with ThreadPoolExecutor(max_workers=30) as pool:
            statuses = tuple(pool.map(fetch, urls))
    finally:
        server.close()

    assert statuses == (200,) * len(urls)


def test_http_responses_remain_complete_while_treatment_is_stuck() -> None:
    worker = WorkerTelemetry(
        poll_interval_seconds=0.05,
        treatment_stuck_after_seconds=0.1,
    )
    worker.cycle_started()
    worker.treatment_started(selection())
    server = TelemetryServer(worker, "127.0.0.1", 0)
    server.start()
    host, port = server.address
    sleep(0.2)

    def fetch(path: str) -> int:
        try:
            with urlopen(f"http://{host}:{port}{path}", timeout=2) as response:
                response.read()
                return response.status
        except HTTPError as error:
            error.read()
            return error.code

    try:
        paths = tuple(
            path for _ in range(30) for path in ("/livez", "/readyz", "/metrics")
        )
        with ThreadPoolExecutor(max_workers=20) as pool:
            statuses = tuple(pool.map(fetch, paths))
    finally:
        server.close()

    assert statuses[0::3] == (200,) * 30
    assert statuses[1::3] == (503,) * 30
    assert statuses[2::3] == (200,) * 30
