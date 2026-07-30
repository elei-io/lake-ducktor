"""Expose structured logs, health, and bounded-cardinality metrics."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Lock, Thread
from time import monotonic, time
from typing import Protocol

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from lakeducktor.executor import ExecutionFailureReason
from lakeducktor.model import (
    CatalogDiagnosis,
    CatalogInventory,
    MaintenanceOutcome,
    PriorityPlan,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)

_LOGGER = logging.getLogger("lakeducktor")


class TelemetryError(RuntimeError):
    """The worker health and metrics service could not be operated."""


class WorkerPhase(StrEnum):
    STARTING = "starting"
    CYCLING = "cycling"
    TREATING = "treating"
    IDLE = "idle"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    live: bool
    ready: bool
    phase: WorkerPhase
    reason: str
    phase_age_seconds: float


class Clock(Protocol):
    def __call__(self) -> float: ...


class WorkerTelemetry:
    """Thread-safe scalar state; it never owns or accesses DuckDB connections."""

    def __init__(
        self,
        *,
        poll_interval_seconds: float,
        treatment_stuck_after_seconds: float,
        registry: CollectorRegistry | None = None,
        clock: Clock = monotonic,
        wall_clock: Clock = time,
    ) -> None:
        self.registry = registry or CollectorRegistry()
        self._clock = clock
        self._wall_clock = wall_clock
        self._poll_interval_seconds = poll_interval_seconds
        self._stuck_after_seconds = treatment_stuck_after_seconds
        self._watchdog_interval_seconds = min(
            5.0,
            max(0.1, treatment_stuck_after_seconds / 10),
        )
        self._watchdog_timeout_seconds = max(
            5.0,
            self._watchdog_interval_seconds * 3,
        )
        now = self._clock()
        self._lock = Lock()
        self._phase = WorkerPhase.STARTING
        self._phase_started = now
        self._idle_deadline = now
        self._watchdog_last_seen = now
        self._stop_requested = False
        self._selection: TreatmentSelection | None = None
        self._reported_unready_reason: str | None = None

        self._ready = Gauge(
            "lakeducktor_worker_ready",
            "Whether the worker is ready to perform maintenance.",
            registry=self.registry,
        )
        self._live = Gauge(
            "lakeducktor_worker_live",
            "Whether the worker watchdog is alive.",
            registry=self.registry,
        )
        self._stuck = Gauge(
            "lakeducktor_worker_stuck",
            "Whether the active worker phase exceeded its deadline.",
            registry=self.registry,
        )
        self._running = Gauge(
            "lakeducktor_treatment_running",
            "Whether a treatment of this kind is currently running.",
            ("kind",),
            registry=self.registry,
        )
        self._treatment_started_at = Gauge(
            "lakeducktor_treatment_started_timestamp_seconds",
            "Unix timestamp when the running treatment started.",
            ("kind",),
            registry=self.registry,
        )
        self._treatment_stuck = Gauge(
            "lakeducktor_treatment_stuck",
            "Whether the running treatment exceeded its configured duration.",
            ("kind",),
            registry=self.registry,
        )
        self._treatments = Counter(
            "lakeducktor_treatments_total",
            "Native treatment outcomes.",
            ("kind", "outcome"),
            registry=self.registry,
        )
        self._treatment_duration = Histogram(
            "lakeducktor_treatment_duration_seconds",
            "Native treatment duration.",
            ("kind", "outcome"),
            registry=self.registry,
        )
        self._treatment_conflicts = Counter(
            "lakeducktor_treatment_conflicts_total",
            "Native treatment conflicts by classified reason.",
            ("reason",),
            registry=self.registry,
        )
        self._treatment_retries = Counter(
            "lakeducktor_treatment_retries_total",
            "Retries scheduled after transient treatment failures.",
            ("reason",),
            registry=self.registry,
        )
        self._treatment_backoff = Histogram(
            "lakeducktor_treatment_retry_backoff_seconds",
            "Backoff scheduled before retrying a transient treatment failure.",
            ("reason",),
            registry=self.registry,
        )
        self._partial_progress = Counter(
            "lakeducktor_treatment_partial_progress_files_total",
            "Input files eliminated despite a failed native treatment.",
            ("kind",),
            registry=self.registry,
        )
        self._files_processed = Counter(
            "lakeducktor_treatment_files_processed_total",
            "Input files processed by successful native treatments.",
            ("kind",),
            registry=self.registry,
        )
        self._files_created = Counter(
            "lakeducktor_treatment_files_created_total",
            "Output files created by successful native treatments.",
            ("kind",),
            registry=self.registry,
        )
        self._files_eliminated = Counter(
            "lakeducktor_treatment_files_eliminated_total",
            "Net files eliminated by successful native treatments.",
            ("kind",),
            registry=self.registry,
        )
        self._claim_contention = Counter(
            "lakeducktor_claim_contention_total",
            "Treatment claims found to be owned by another worker.",
            registry=self.registry,
        )
        self._cycles = Counter(
            "lakeducktor_cycles_total",
            "Completed worker cycles.",
            ("outcome",),
            registry=self.registry,
        )
        self._last_cycle = Gauge(
            "lakeducktor_cycle_last_completed_timestamp_seconds",
            "Unix timestamp of the latest completed worker cycle.",
            registry=self.registry,
        )
        self._actionable_tables = Gauge(
            "lakeducktor_actionable_tables",
            "Tables currently diagnosed as actionable.",
            registry=self.registry,
        )
        self._excluded_tables = Gauge(
            "lakeducktor_excluded_tables",
            "Tables excluded by current lake policy.",
            registry=self.registry,
        )
        self._attention_tables = Gauge(
            "lakeducktor_attention_tables",
            "Tables requiring non-treatment attention.",
            registry=self.registry,
        )
        self._runnable_treatments = Gauge(
            "lakeducktor_runnable_treatments",
            "Treatments currently runnable before live claims.",
            registry=self.registry,
        )
        self._blocked_treatments = Gauge(
            "lakeducktor_blocked_treatments",
            "Treatments blocked by treatment ordering.",
            registry=self.registry,
        )
        self._memory_deferred = Gauge(
            "lakeducktor_memory_deferred_treatments",
            "Runnable treatments that do not fit this worker.",
            registry=self.registry,
        )
        self._recent_data_files = Gauge(
            "lakeducktor_recent_data_files",
            "Active files created by insertion snapshots in the recent window.",
            ("window",),
            registry=self.registry,
        )
        self._merge_file_debt = Gauge(
            "lakeducktor_merge_expected_files_eliminated",
            "Estimated files remaining to eliminate through merge treatments.",
            registry=self.registry,
        )
        self._rewrite_file_debt = Gauge(
            "lakeducktor_rewrite_data_files",
            "Active data files currently eligible for delete rewrite.",
            registry=self.registry,
        )
        self._scheduled_files = Gauge(
            "lakeducktor_scheduled_files",
            "Files scheduled for deletion across maintained lakes.",
            registry=self.registry,
        )
        self._dangling_delete_files = Gauge(
            "lakeducktor_dangling_delete_files",
            "Active delete files whose data file is no longer active.",
            registry=self.registry,
        )
        for kind in TreatmentKind:
            self._running.labels(kind=kind.value).set(0)
            self._treatment_started_at.labels(kind=kind.value).set(0)
            self._treatment_stuck.labels(kind=kind.value).set(0)
            self._files_processed.labels(kind=kind.value)
            self._files_created.labels(kind=kind.value)
            self._files_eliminated.labels(kind=kind.value)
        self._set_health_metrics(self.health_snapshot())

    @property
    def watchdog_interval_seconds(self) -> float:
        return self._watchdog_interval_seconds

    def cycle_started(self) -> None:
        with self._lock:
            self._phase = WorkerPhase.CYCLING
            self._phase_started = self._clock()
            self._selection = None

    def observe_plan(
        self,
        inventory: CatalogInventory,
        diagnosis: CatalogDiagnosis,
        plan: PriorityPlan,
        memory_deferred: int,
    ) -> None:
        tables = tuple(table for lake in diagnosis.lakes for table in lake.tables)
        self._actionable_tables.set(
            sum(table.state.value == "actionable" for table in tables)
        )
        self._excluded_tables.set(plan.excluded_tables)
        self._attention_tables.set(plan.attention_tables)
        self._runnable_treatments.set(plan.runnable)
        self._blocked_treatments.set(plan.blocked)
        self._memory_deferred.set(memory_deferred)
        self._recent_data_files.labels(window="60s").set(
            sum(table.recent_data_files_60s for table in tables)
        )
        self._merge_file_debt.set(
            sum(candidate.expected_files_eliminated for candidate in plan.merges)
        )
        self._rewrite_file_debt.set(sum(table.rewrite_data_files for table in tables))
        self._scheduled_files.set(inventory.scheduled_files)
        self._dangling_delete_files.set(
            sum(lake.dangling_delete_files for lake in inventory.lakes)
        )

    def treatment_started(self, selection: TreatmentSelection) -> None:
        now = self._clock()
        with self._lock:
            self._phase = WorkerPhase.TREATING
            self._phase_started = now
            self._selection = selection
        self._running.labels(kind=selection.kind.value).set(1)
        self._treatment_started_at.labels(kind=selection.kind.value).set(
            self._wall_clock()
        )

    def treatment_finished(
        self,
        selection: TreatmentSelection,
        result: TreatmentResult | None,
        error: Exception | None,
        duration_seconds: float,
    ) -> None:
        outcome = "success" if error is None else "failure"
        self._running.labels(kind=selection.kind.value).set(0)
        self._treatment_stuck.labels(kind=selection.kind.value).set(0)
        self._treatments.labels(kind=selection.kind.value, outcome=outcome).inc()
        self._treatment_duration.labels(
            kind=selection.kind.value,
            outcome=outcome,
        ).observe(duration_seconds)
        if error is not None:
            reason = getattr(
                error,
                "reason",
                ExecutionFailureReason.UNKNOWN,
            )
            if reason in {
                ExecutionFailureReason.CONCURRENT_COMPACTION,
                ExecutionFailureReason.SNAPSHOT_RETRY_EXHAUSTED,
            }:
                self._treatment_conflicts.labels(reason=reason.value).inc()
        if result is not None:
            self._files_processed.labels(kind=selection.kind.value).inc(
                result.files_processed
            )
            self._files_created.labels(kind=selection.kind.value).inc(
                result.files_created
            )
            self._files_eliminated.labels(kind=selection.kind.value).inc(
                max(0, result.files_processed - result.files_created)
            )
        with self._lock:
            self._phase = WorkerPhase.CYCLING
            self._phase_started = self._clock()
            self._selection = None

    def treatment_progress_observed(
        self,
        selection: TreatmentSelection,
        files_before: int,
        files_after: int,
        snapshot_before: int | None,
        snapshot_after: int | None,
    ) -> None:
        del snapshot_before, snapshot_after
        self._partial_progress.labels(kind=selection.kind.value).inc(
            max(0, files_before - files_after)
        )

    def retry_scheduled(
        self,
        reason: ExecutionFailureReason,
        duration_seconds: float,
    ) -> None:
        self._treatment_retries.labels(reason=reason.value).inc()
        self._treatment_backoff.labels(reason=reason.value).observe(duration_seconds)

    def cycle_completed(self, outcome: MaintenanceOutcome) -> None:
        self._cycles.labels(outcome=outcome.state.value).inc()
        self._claim_contention.inc(outcome.claim_contention)
        self._last_cycle.set(self._wall_clock())
        with self._lock:
            now = self._clock()
            self._phase = WorkerPhase.IDLE
            self._phase_started = now
            self._idle_deadline = now
            self._selection = None

    def cycle_failed(self) -> None:
        self._cycles.labels(outcome="failure").inc()
        self._last_cycle.set(self._wall_clock())
        with self._lock:
            now = self._clock()
            self._phase = WorkerPhase.ERROR
            self._phase_started = now
            self._idle_deadline = now
            self._selection = None

    def idle(self, duration_seconds: float) -> None:
        with self._lock:
            now = self._clock()
            if self._phase is not WorkerPhase.ERROR:
                self._phase = WorkerPhase.IDLE
            self._phase_started = now
            self._idle_deadline = now + duration_seconds

    def request_stop(self) -> None:
        with self._lock:
            self._stop_requested = True

    def stopped(self) -> None:
        with self._lock:
            self._phase = WorkerPhase.STOPPED
            self._phase_started = self._clock()
            self._selection = None

    def watchdog_tick(self) -> HealthSnapshot:
        with self._lock:
            self._watchdog_last_seen = self._clock()
        snapshot = self.health_snapshot()
        self._set_health_metrics(snapshot)
        unready_reason = None if snapshot.ready else snapshot.reason
        if unready_reason != self._reported_unready_reason:
            if unready_reason in {"treatment_stuck", "cycle_stuck", "loop_stuck"}:
                selection = self.current_selection()
                _LOGGER.error(
                    "worker_not_ready reason=%s phase=%s "
                    "runtime_seconds=%.3f kind=%s lake=%s table_id=%s",
                    unready_reason,
                    snapshot.phase.value,
                    snapshot.phase_age_seconds,
                    selection.kind.value if selection is not None else "none",
                    selection.metadata_schema if selection is not None else "none",
                    selection.table_id if selection is not None else "none",
                )
            elif self._reported_unready_reason in {
                "treatment_stuck",
                "cycle_stuck",
                "loop_stuck",
            }:
                _LOGGER.info(
                    "worker_ready previous_reason=%s",
                    self._reported_unready_reason,
                )
            self._reported_unready_reason = unready_reason
        return snapshot

    def current_selection(self) -> TreatmentSelection | None:
        with self._lock:
            return self._selection

    def health_snapshot(self) -> HealthSnapshot:
        with self._lock:
            now = self._clock()
            phase = self._phase
            phase_age = max(0.0, now - self._phase_started)
            watchdog_alive = (
                now - self._watchdog_last_seen <= self._watchdog_timeout_seconds
            )
            if not watchdog_alive:
                return HealthSnapshot(
                    False,
                    False,
                    phase,
                    "watchdog_stale",
                    phase_age,
                )
            if phase is WorkerPhase.STOPPED:
                return HealthSnapshot(False, False, phase, "stopped", phase_age)
            if self._stop_requested:
                return HealthSnapshot(True, False, phase, "stopping", phase_age)
            if phase is WorkerPhase.STARTING:
                return HealthSnapshot(True, False, phase, "starting", phase_age)
            if phase is WorkerPhase.ERROR:
                return HealthSnapshot(True, False, phase, "cycle_failed", phase_age)
            if phase is WorkerPhase.TREATING and phase_age > self._stuck_after_seconds:
                return HealthSnapshot(
                    True,
                    False,
                    phase,
                    "treatment_stuck",
                    phase_age,
                )
            if phase is WorkerPhase.CYCLING and phase_age > self._stuck_after_seconds:
                return HealthSnapshot(
                    True,
                    False,
                    phase,
                    "cycle_stuck",
                    phase_age,
                )
            if (
                phase is WorkerPhase.IDLE
                and now > self._idle_deadline + self._poll_interval_seconds
            ):
                return HealthSnapshot(
                    True,
                    False,
                    phase,
                    "loop_stuck",
                    phase_age,
                )
            return HealthSnapshot(True, True, phase, "ready", phase_age)

    def _set_health_metrics(self, snapshot: HealthSnapshot) -> None:
        self._live.set(int(snapshot.live))
        self._ready.set(int(snapshot.ready))
        stuck = snapshot.reason in {
            "treatment_stuck",
            "cycle_stuck",
            "loop_stuck",
        }
        self._stuck.set(int(stuck))
        selection = self.current_selection()
        for kind in TreatmentKind:
            self._treatment_stuck.labels(kind=kind.value).set(
                int(
                    snapshot.reason == "treatment_stuck"
                    and selection is not None
                    and selection.kind is kind
                )
            )


def _handler(
    telemetry: WorkerTelemetry,
) -> type[BaseHTTPRequestHandler]:
    class TelemetryHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/metrics":
                payload = generate_latest(telemetry.registry)
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            elif self.path in {"/livez", "/readyz"}:
                snapshot = telemetry.health_snapshot()
                healthy = snapshot.live if self.path == "/livez" else snapshot.ready
                payload = f"{snapshot.reason}\n".encode()
                self.send_response(200 if healthy else 503)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
            else:
                payload = b"not found\n"
                self.send_response(404)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_arguments: object) -> None:
            return

    return TelemetryHandler


class _TelemetryHTTPServer(ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True


class TelemetryServer:
    """Run watchdog and single-threaded HTTP serving outside the worker."""

    def __init__(
        self,
        telemetry: WorkerTelemetry,
        host: str,
        port: int,
    ) -> None:
        self._telemetry = telemetry
        try:
            self._server = _TelemetryHTTPServer(
                (host, port),
                _handler(telemetry),
            )
        except OSError as error:
            raise TelemetryError(
                f"could not bind health server to {host}:{port}"
            ) from error
        self._monitor_stop = Event()
        self._http_thread = Thread(
            target=self._server.serve_forever,
            name="lakeducktor-http",
            daemon=True,
        )
        self._watchdog_thread = Thread(
            target=self._watchdog,
            name="lakeducktor-watchdog",
            daemon=True,
        )

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    def start(self) -> None:
        self._telemetry.watchdog_tick()
        self._http_thread.start()
        self._watchdog_thread.start()

    def close(self) -> None:
        self._monitor_stop.set()
        self._server.shutdown()
        self._server.server_close()
        self._http_thread.join(timeout=5)
        self._watchdog_thread.join(timeout=5)

    def _watchdog(self) -> None:
        while not self._monitor_stop.wait(self._telemetry.watchdog_interval_seconds):
            self._telemetry.watchdog_tick()
