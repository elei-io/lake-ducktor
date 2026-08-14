"""LakeDucktor command-line entry point."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from lakeducktor import __version__
from lakeducktor.capabilities import CapabilitiesError, adapter_for
from lakeducktor.config import (
    ConfigurationError,
    MetadataConfiguration,
    RunConfiguration,
    StorageConfiguration,
    load_env_file,
)
from lakeducktor.coordination import PostgresTreatmentCoordinator
from lakeducktor.daemon import MaintenanceError, RunError, maintain_once, run_service
from lakeducktor.diagnosis import DiagnosisError, diagnose_inventory
from lakeducktor.inventory import InventoryError, MaintenanceInventory
from lakeducktor.lake import BackendDetectionError, detect_metadata_backend
from lakeducktor.model import MaintenanceState, MetadataBackend
from lakeducktor.priority import PriorityError, prioritize
from lakeducktor.resources import resource_envelope_from_environment
from lakeducktor.selection import SelectionError, select_treatment

_LOGGER = logging.getLogger("lakeducktor")


def configure_logging() -> None:
    """Configure useful process-level logs without another dependency."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="lakeducktor")
    command.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="environment file to load without overriding existing variables",
    )
    actions = command.add_subparsers(dest="command", required=True)
    actions.add_parser(
        "detect-backend",
        help="attach read-only and report the DuckLake metadata backend",
    )
    actions.add_parser(
        "inventory",
        help="collect a read-only inventory of current physical lake state",
    )
    actions.add_parser(
        "diagnose",
        help="explain current physical maintenance needs without mutating lakes",
    )
    actions.add_parser(
        "prioritize",
        help="rank current maintenance candidates without executing them",
    )
    actions.add_parser(
        "select",
        help="select one treatment that fits this worker without executing it",
    )
    actions.add_parser(
        "maintain",
        help="claim, revalidate, and execute at most one treatment",
    )
    actions.add_parser(
        "run",
        help="continuously discover, select, and execute maintenance",
    )
    return command


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    _LOGGER.info("starting version=%s", __version__)
    arguments = parser().parse_args(argv)
    try:
        load_env_file(arguments.env_file)
        configuration = MetadataConfiguration.from_environment()
        envelope = (
            resource_envelope_from_environment()
            if arguments.command in {"select", "maintain", "run"}
            else None
        )
        storage = (
            StorageConfiguration.from_environment()
            if arguments.command
            in {"inventory", "diagnose", "prioritize", "select", "maintain", "run"}
            else None
        )
        if arguments.command == "run":
            run_configuration = RunConfiguration.from_environment()
            assert envelope is not None
            assert storage is not None
            try:
                run_service(
                    configuration,
                    storage,
                    envelope,
                    run_configuration,
                )
            except RunError as error:
                _LOGGER.error("run_failed error=%s", error)
                return 1
            return 0
        detection = detect_metadata_backend(configuration)
        adapter = adapter_for(detection)
    except (CapabilitiesError, ConfigurationError, BackendDetectionError) as error:
        _LOGGER.error("startup_failed error=%s", error)
        return 1

    _LOGGER.info("detected metadata_backend=%s", detection.backend.value)
    _LOGGER.info("detected lakes=%s", len(detection.metadata_schemas))
    _LOGGER.info(
        "selected adapter=%s",
        type(adapter).__name__,
    )
    if arguments.command in {
        "inventory",
        "diagnose",
        "prioritize",
        "select",
        "maintain",
    }:
        assert storage is not None
        maintenance_inventory = MaintenanceInventory(storage)
        try:
            inventory = maintenance_inventory(configuration, detection)
        except InventoryError as error:
            _LOGGER.error("inventory_failed error=%s", error)
            return 1
        for lake in inventory.lakes:
            _LOGGER.info(
                "detected lake=%s snapshot=%s tables=%s "
                "active_data_files=%s active_data_bytes=%s "
                "active_delete_files=%s active_delete_bytes=%s "
                "inlined_data_rows=%s inlined_data_bytes=%s "
                "dangling_delete_files=%s scheduled_files=%s "
                "expiring_snapshots=%s cleanup_eligible_files=%s "
                "orphan_files=%s delete_older_than=%s "
                "expire_older_than=%s",
                lake.metadata_schema,
                lake.latest_snapshot_id
                if lake.latest_snapshot_id is not None
                else "none",
                lake.table_count,
                lake.active_data_files,
                lake.active_data_bytes,
                lake.active_delete_files,
                lake.active_delete_bytes,
                sum(table.inlined_data_rows for table in lake.tables),
                sum(table.inlined_data_bytes for table in lake.tables),
                lake.dangling_delete_files,
                lake.scheduled_files,
                getattr(lake, "expiring_snapshots", 0),
                getattr(lake, "cleanup_eligible_files", 0),
                getattr(lake, "orphan_files", 0),
                getattr(lake, "delete_older_than", None) or "native_default",
                getattr(lake, "expire_older_than", None) or "unset",
            )
        if arguments.command in {"diagnose", "prioritize", "select", "maintain"}:
            try:
                diagnosis = diagnose_inventory(inventory)
            except DiagnosisError as error:
                _LOGGER.error("diagnosis_failed error=%s", error)
                return 1
            for lake in diagnosis.lakes:
                _LOGGER.info(
                    "diagnosis lake=%s state=%s actionable_tables=%s "
                    "excluded_tables=%s attention_tables=%s "
                    "scheduled_files=%s cleanup_eligible_files=%s "
                    "expiring_snapshots=%s orphan_files=%s "
                    "delete_older_than=%s expire_older_than=%s",
                    lake.metadata_schema,
                    lake.state.value,
                    lake.actionable_tables,
                    lake.excluded_tables,
                    lake.attention_tables,
                    lake.scheduled_files,
                    getattr(lake, "cleanup_eligible_files", 0),
                    getattr(lake, "expiring_snapshots", 0),
                    getattr(lake, "orphan_files", 0),
                    getattr(lake, "delete_older_than", None) or "native_default",
                    getattr(lake, "expire_older_than", None) or "unset",
                )
                for table in lake.tables:
                    _LOGGER.info(
                        "diagnosis lake=%s table_id=%s schema=%r table=%r "
                        "state=%s reasons=%s sorting_enabled=%s merge_groups=%s "
                        "data_inlining_row_limit=%s "
                        "inline_flush_groups=%s inline_flush_max_bytes=%s "
                        "inline_flush_threshold_rows=%s "
                        "inlined_data_rows=%s inlined_data_bytes=%s "
                        "merge_input_files=%s merge_input_bytes=%s "
                        "expected_files_eliminated=%s "
                        "recent_data_files_60s=%s rewrite_data_files=%s "
                        "rewrite_input_bytes=%s rewrite_delete_files=%s "
                        "rewrite_deleted_rows=%s dangling_delete_files=%s",
                        table.metadata_schema,
                        table.table_id,
                        table.schema_name,
                        table.table_name,
                        table.state.value,
                        ",".join(table.reasons),
                        str(table.sorting_enabled).lower(),
                        table.merge_groups,
                        table.data_inlining_row_limit,
                        table.inline_flush_groups,
                        table.inline_flush_max_bytes,
                        table.inline_flush_threshold_rows,
                        table.inlined_data_rows,
                        table.inlined_data_bytes,
                        table.merge_input_files,
                        table.merge_input_bytes,
                        table.expected_files_eliminated,
                        table.recent_data_files_60s,
                        table.rewrite_data_files,
                        table.rewrite_input_bytes,
                        table.rewrite_delete_files,
                        table.rewrite_deleted_rows,
                        table.dangling_delete_files,
                    )
            if arguments.command in {"prioritize", "select", "maintain"}:
                try:
                    plan = prioritize(diagnosis)
                except PriorityError as error:
                    _LOGGER.error("priority_failed error=%s", error)
                    return 1
                if arguments.command == "prioritize":
                    for candidate in getattr(plan, "snapshot_expirations", ()):
                        _LOGGER.info(
                            "priority kind=snapshot_expiration rank=%s "
                            "lake=%s state=runnable snapshots=%s "
                            "retention_policy=%s",
                            candidate.rank,
                            candidate.metadata_schema,
                            candidate.snapshots,
                            candidate.expire_older_than,
                        )
                    for candidate in plan.scheduled_file_cleanups:
                        _LOGGER.info(
                            "priority kind=scheduled_file_cleanup rank=%s "
                            "lake=%s state=runnable eligible_files=%s "
                            "scheduled_files=%s oldest_scheduled_at=%s "
                            "retention_policy=%s",
                            candidate.rank,
                            candidate.metadata_schema,
                            candidate.eligible_files,
                            candidate.scheduled_files,
                            candidate.oldest_scheduled_at.isoformat()
                            if candidate.oldest_scheduled_at is not None
                            else "none",
                            candidate.delete_older_than or "native_default",
                        )
                    for candidate in getattr(plan, "orphan_file_cleanups", ()):
                        _LOGGER.info(
                            "priority kind=orphan_file_cleanup rank=%s "
                            "lake=%s state=runnable orphan_files=%s "
                            "retention_policy=%s",
                            candidate.rank,
                            candidate.metadata_schema,
                            candidate.orphan_files,
                            candidate.delete_older_than or "native_default",
                        )
                    for candidate in plan.inline_flushes:
                        _LOGGER.info(
                            "priority kind=inline_flush rank=%s lake=%s "
                            "table_id=%s schema=%r table=%r state=runnable "
                            "sorting_enabled=%s inlined_rows=%s "
                            "input_bytes=%s threshold_rows=%s "
                            "data_inlining_row_limit=%s output_groups=%s "
                            "max_input_bytes=%s",
                            candidate.rank,
                            candidate.metadata_schema,
                            candidate.table_id,
                            candidate.schema_name,
                            candidate.table_name,
                            str(candidate.sorting_enabled).lower(),
                            candidate.inlined_rows,
                            candidate.input_bytes,
                            candidate.threshold_rows,
                            candidate.data_inlining_row_limit,
                            candidate.output_groups,
                            candidate.max_input_bytes,
                        )
                    for candidate in plan.delete_rewrites:
                        _LOGGER.info(
                            "priority kind=delete_rewrite rank=%s lake=%s "
                            "table_id=%s schema=%r table=%r state=runnable "
                            "sorting_enabled=%s "
                            "data_files=%s delete_files=%s deleted_rows=%s "
                            "original_rows=%s deleted_fraction=%.6f "
                            "input_bytes=%s",
                            candidate.rank,
                            candidate.metadata_schema,
                            candidate.table_id,
                            candidate.schema_name,
                            candidate.table_name,
                            str(candidate.sorting_enabled).lower(),
                            candidate.data_files,
                            candidate.delete_files,
                            candidate.deleted_rows,
                            candidate.original_rows,
                            candidate.deleted_fraction,
                            candidate.input_bytes,
                        )
                    for candidate in plan.merges:
                        _LOGGER.info(
                            "priority kind=merge rank=%s lake=%s table_id=%s "
                            "schema=%r table=%r state=%s blocked_by=%s "
                            "waiting_reason=%s "
                            "sorting_enabled=%s "
                            "groups=%s ready_groups=%s admission_groups=%s "
                            "input_files=%s input_bytes=%s "
                            "average_input_file_bytes=%s "
                            "expected_files_eliminated=%s "
                            "recent_data_files_60s=%s activity_penalty=%.2f "
                            "adjusted_expected_files_eliminated=%.2f",
                            candidate.rank,
                            candidate.metadata_schema,
                            candidate.table_id,
                            candidate.schema_name,
                            candidate.table_name,
                            candidate.state.value,
                            candidate.blocked_by.value
                            if candidate.blocked_by is not None
                            else "none",
                            candidate.waiting_reason or "none",
                            str(candidate.sorting_enabled).lower(),
                            candidate.groups,
                            candidate.ready_groups,
                            len(candidate.input_groups),
                            candidate.input_files,
                            candidate.input_bytes,
                            candidate.average_input_file_bytes,
                            candidate.expected_files_eliminated,
                            candidate.recent_data_files_60s,
                            candidate.activity_penalty,
                            candidate.adjusted_expected_files_eliminated,
                        )
                    _LOGGER.info(
                        "priority_summary snapshot_expirations=%s "
                        "scheduled_file_cleanups=%s orphan_file_cleanups=%s "
                        "inline_flushes=%s "
                        "delete_rewrites=%s merges=%s "
                        "runnable=%s blocked=%s waiting=%s "
                        "excluded=%s attention=%s",
                        len(getattr(plan, "snapshot_expirations", ())),
                        len(plan.scheduled_file_cleanups),
                        len(getattr(plan, "orphan_file_cleanups", ())),
                        len(plan.inline_flushes),
                        len(plan.delete_rewrites),
                        len(plan.merges),
                        plan.runnable,
                        plan.blocked,
                        plan.waiting,
                        plan.excluded_tables,
                        plan.attention_tables,
                    )
                else:
                    assert envelope is not None
                    _LOGGER.info(
                        "resources duckdb_threads=%s duckdb_memory=%s "
                        "duckdb_memory_bytes=%s",
                        envelope.duckdb_threads,
                        envelope.duckdb_memory,
                        envelope.duckdb_memory_bytes,
                    )
                    if arguments.command == "select":
                        try:
                            decision = select_treatment(plan, envelope)
                        except SelectionError as error:
                            _LOGGER.error("selection_failed error=%s", error)
                            return 1
                        selected = decision.selected
                        if selected is None:
                            _LOGGER.info(
                                "selection=none reason=%s memory_deferred=%s",
                                decision.reason.value,
                                decision.memory_deferred,
                            )
                        else:
                            _LOGGER.info(
                                "selected treatment=%s priority_rank=%s lake=%s "
                                "table_id=%s schema=%r table=%r input_bytes=%s "
                                "input_rows=%s input_snapshots=%s "
                                "admitted_input_files=%s "
                                "admitted_bytes=%s sorting_enabled=%s "
                                "memory_headroom_bytes=%s "
                                "usable_memory_bytes=%s max_compacted_files=%s "
                                "retention_policy=%s "
                                "memory_deferred=%s",
                                selected.kind.value,
                                selected.priority_rank,
                                selected.metadata_schema,
                                selected.table_id,
                                selected.schema_name,
                                selected.table_name,
                                selected.input_bytes,
                                selected.input_rows,
                                selected.input_snapshots,
                                selected.admitted_input_files,
                                selected.admitted_bytes,
                                str(selected.sorting_enabled).lower(),
                                selected.memory_headroom_bytes,
                                selected.usable_memory_bytes,
                                selected.max_compacted_files
                                if selected.max_compacted_files is not None
                                else "none",
                                selected.retention_policy
                                if selected.retention_policy is not None
                                else "none",
                                decision.memory_deferred,
                            )
                    else:
                        assert storage is not None
                        if detection.backend is not MetadataBackend.POSTGRES:
                            _LOGGER.error(
                                "maintenance_failed error=unsupported "
                                "coordination backend: %s",
                                detection.backend.value,
                            )
                            return 1
                        try:
                            outcome = maintain_once(
                                configuration,
                                storage,
                                detection,
                                envelope,
                                plan,
                                PostgresTreatmentCoordinator(configuration),
                                inventory=maintenance_inventory,
                            )
                        except MaintenanceError as error:
                            _LOGGER.error("maintenance_failed error=%s", error)
                            return 1
                        if outcome.state is MaintenanceState.NO_TREATMENT:
                            assert outcome.selection_reason is not None
                            _LOGGER.info(
                                "maintenance state=no_treatment reason=%s "
                                "claim_contention=%s",
                                outcome.selection_reason.value,
                                outcome.claim_contention,
                            )
                        elif outcome.state is MaintenanceState.STALE:
                            assert outcome.selection is not None
                            _LOGGER.info(
                                "maintenance state=stale reason=revalidation "
                                "kind=%s lake=%s table_id=%s "
                                "claim_contention=%s",
                                outcome.selection.kind.value,
                                outcome.selection.metadata_schema,
                                outcome.selection.table_id,
                                outcome.claim_contention,
                            )
                        else:
                            assert outcome.selection is not None
                            assert outcome.result is not None
                            assert outcome.duration_seconds is not None
                            _LOGGER.info(
                                "treatment_completed kind=%s lake=%s table_id=%s "
                                "files_processed=%s files_created=%s "
                                "rows_processed=%s snapshots_processed=%s "
                                "duration_seconds=%.3f sorting_enabled=%s "
                                "table_present=%s "
                                "still_actionable=%s claim_contention=%s",
                                outcome.selection.kind.value,
                                outcome.selection.metadata_schema,
                                outcome.selection.table_id,
                                outcome.result.files_processed,
                                outcome.result.files_created,
                                outcome.result.rows_processed,
                                outcome.result.snapshots_processed,
                                outcome.duration_seconds,
                                str(outcome.selection.sorting_enabled).lower(),
                                str(outcome.table_present).lower(),
                                str(outcome.still_actionable).lower(),
                                outcome.claim_contention,
                            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
