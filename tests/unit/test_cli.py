import logging
from types import SimpleNamespace

import pytest

from lakeducktor import __version__
from lakeducktor.cli import main
from lakeducktor.model import (
    BackendDetection,
    DuckDBExtension,
    MaintenanceOutcome,
    MaintenanceState,
    MetadataBackend,
    ResourceEnvelope,
    SelectionDecision,
    SelectionReason,
    TreatmentKind,
    TreatmentResult,
    TreatmentSelection,
)

_EXTENSIONS = (
    DuckDBExtension(
        name="core_functions",
        version="v1",
        install_mode="statically_linked",
        source="built-in",
    ),
    DuckDBExtension(
        name="ducklake",
        version="abc123",
        install_mode="repository",
        source="core",
    ),
)


@pytest.fixture(autouse=True)
def storage_configuration(monkeypatch) -> None:
    monkeypatch.setattr(
        "lakeducktor.cli.StorageConfiguration.from_environment",
        lambda: object(),
    )


def test_entry_point_reports_detected_backend(
    monkeypatch,
    caplog,
    capsys,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: BackendDetection(
            backend=MetadataBackend.POSTGRES,
            metadata_schemas=("ducklake",),
            extension_version="v1",
            duckdb_extensions=_EXTENSIONS,
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "detect-backend"])

    assert result == 0
    assert capsys.readouterr().out == ""
    messages = [record.getMessage() for record in caplog.records]
    assert f"starting version={__version__}" in messages
    assert "detected metadata_backend=postgres" in messages
    assert "detected lakes=1" in messages
    assert "selected adapter=PostgresCapabilitiesAdapter" in messages
    assert not any(message.startswith("detected extension=") for message in messages)


def test_entry_point_summarizes_multiple_lakes(
    monkeypatch,
    caplog,
    capsys,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: BackendDetection(
            backend=MetadataBackend.POSTGRES,
            metadata_schemas=("lake_a", "lake_b"),
            extension_version="v1",
            duckdb_extensions=_EXTENSIONS,
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "detect-backend"])

    assert result == 0
    assert capsys.readouterr().out == ""
    messages = [record.getMessage() for record in caplog.records]
    assert "detected metadata_backend=postgres" in messages
    assert "detected lakes=2" in messages


def test_inventory_command_logs_aggregate_physical_facts(
    monkeypatch,
    caplog,
    capsys,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake_a", "lake_b"),
        extension_version="v1",
        duckdb_extensions=_EXTENSIONS,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: detection,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.MaintenanceInventory",
        lambda _storage: (
            lambda _configuration, _detection: SimpleNamespace(
                lakes=(
                    SimpleNamespace(
                        metadata_schema="lake_a",
                        latest_snapshot_id=11,
                        table_count=4,
                        active_data_files=12,
                        active_data_bytes=1_024,
                        active_delete_files=3,
                        active_delete_bytes=128,
                        tables=(),
                        dangling_delete_files=1,
                        scheduled_files=2,
                    ),
                    SimpleNamespace(
                        metadata_schema="lake_b",
                        latest_snapshot_id=None,
                        table_count=0,
                        active_data_files=0,
                        active_data_bytes=0,
                        active_delete_files=0,
                        active_delete_bytes=0,
                        tables=(),
                        dangling_delete_files=0,
                        scheduled_files=0,
                    ),
                ),
            )
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "inventory"])

    assert result == 0
    assert capsys.readouterr().out == ""
    assert (
        "detected lake=lake_a snapshot=11 tables=4 active_data_files=12 "
        "active_data_bytes=1024 active_delete_files=3 active_delete_bytes=128 "
        "inlined_data_rows=0 inlined_data_bytes=0 "
        "dangling_delete_files=1 scheduled_files=2 expiring_snapshots=0 "
        "cleanup_eligible_files=0 orphan_files=0 "
        "delete_older_than=native_default expire_older_than=unset"
    ) in [record.getMessage() for record in caplog.records]


def test_diagnose_command_logs_lake_and_table_explanations(
    monkeypatch,
    caplog,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake_a",),
        extension_version="v1",
        duckdb_extensions=_EXTENSIONS,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: detection,
    )
    inventory_lake = SimpleNamespace(
        metadata_schema="lake_a",
        latest_snapshot_id=11,
        table_count=1,
        active_data_files=4,
        active_data_bytes=160,
        active_delete_files=0,
        active_delete_bytes=0,
        tables=(),
        dangling_delete_files=0,
        scheduled_files=0,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.MaintenanceInventory",
        lambda _storage: lambda _configuration, _detection: SimpleNamespace(
            lakes=(inventory_lake,)
        ),
    )
    table_diagnosis = SimpleNamespace(
        metadata_schema="lake_a",
        table_id=7,
        schema_name="main",
        table_name="events",
        state=SimpleNamespace(value="actionable"),
        reasons=("merge_pressure",),
        sorting_enabled=False,
        merge_groups=1,
        data_inlining_row_limit=10,
        inline_flush_groups=1,
        inline_flush_max_bytes=8 * 1024 * 1024,
        inline_flush_threshold_rows=50,
        inlined_data_rows=0,
        inlined_data_bytes=0,
        merge_input_files=4,
        merge_input_bytes=160,
        expected_files_eliminated=2,
        recent_data_files_60s=0,
        rewrite_data_files=0,
        rewrite_input_bytes=0,
        rewrite_delete_files=0,
        rewrite_deleted_rows=0,
        dangling_delete_files=0,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.diagnose_inventory",
        lambda _inventory: SimpleNamespace(
            lakes=(
                SimpleNamespace(
                    metadata_schema="lake_a",
                    state=SimpleNamespace(value="actionable"),
                    actionable_tables=1,
                    excluded_tables=0,
                    attention_tables=0,
                    scheduled_files=0,
                    tables=(table_diagnosis,),
                ),
            )
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "diagnose"])

    assert result == 0
    messages = [record.getMessage() for record in caplog.records]
    assert (
        "diagnosis lake=lake_a state=actionable actionable_tables=1 "
        "excluded_tables=0 attention_tables=0 scheduled_files=0 "
        "cleanup_eligible_files=0 expiring_snapshots=0 orphan_files=0 "
        "delete_older_than=native_default "
        "expire_older_than=unset"
    ) in messages
    assert any(
        message.startswith(
            "diagnosis lake=lake_a table_id=7 schema='main' table='events' "
            "state=actionable reasons=merge_pressure"
        )
        for message in messages
    )


def test_prioritize_command_logs_separate_treatment_lanes(
    monkeypatch,
    caplog,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake_a",),
        extension_version="v1",
        duckdb_extensions=_EXTENSIONS,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: detection,
    )
    inventory_lake = SimpleNamespace(
        metadata_schema="lake_a",
        latest_snapshot_id=11,
        table_count=1,
        active_data_files=4,
        active_data_bytes=160,
        active_delete_files=2,
        active_delete_bytes=10,
        tables=(),
        dangling_delete_files=0,
        scheduled_files=0,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.MaintenanceInventory",
        lambda _storage: lambda _configuration, _detection: SimpleNamespace(
            lakes=(inventory_lake,)
        ),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.diagnose_inventory",
        lambda _inventory: SimpleNamespace(lakes=()),
    )
    rewrite = SimpleNamespace(
        rank=1,
        metadata_schema="lake_a",
        table_id=7,
        schema_name="main",
        table_name="events",
        data_files=1,
        delete_files=2,
        deleted_rows=96,
        original_rows=100,
        deleted_fraction=0.96,
        input_bytes=200,
        sorting_enabled=False,
    )
    merge = SimpleNamespace(
        rank=1,
        metadata_schema="lake_a",
        table_id=7,
        schema_name="main",
        table_name="events",
        state=SimpleNamespace(value="blocked"),
        blocked_by=SimpleNamespace(value="delete_rewrite"),
        groups=1,
        input_files=4,
        input_bytes=160,
        average_input_file_bytes=40,
        expected_files_eliminated=2,
        recent_data_files_60s=8,
        activity_penalty=2,
        adjusted_expected_files_eliminated=0,
        sorting_enabled=False,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.prioritize",
        lambda _diagnosis: SimpleNamespace(
            scheduled_file_cleanups=(),
            inline_flushes=(),
            delete_rewrites=(rewrite,),
            merges=(merge,),
            runnable=1,
            blocked=1,
            excluded_tables=0,
            attention_tables=0,
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "prioritize"])

    assert result == 0
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        message.startswith("priority kind=delete_rewrite rank=1 lake=lake_a table_id=7")
        for message in messages
    )
    assert any(
        message.startswith(
            "priority kind=merge rank=1 lake=lake_a table_id=7 "
            "schema='main' table='events' state=blocked "
            "blocked_by=delete_rewrite"
        )
        and "recent_data_files_60s=8 activity_penalty=2.00 "
        "adjusted_expected_files_eliminated=0.00"
        in message
        for message in messages
    )
    assert (
        "priority_summary snapshot_expirations=0 scheduled_file_cleanups=0 "
        "orphan_file_cleanups=0 inline_flushes=0 "
        "delete_rewrites=1 merges=1 "
        "runnable=1 blocked=1 "
        "excluded=0 attention=0"
    ) in messages


def test_select_command_logs_resource_envelope_and_one_treatment(
    monkeypatch,
    caplog,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: object(),
    )
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake_a",),
        extension_version="v1",
        duckdb_extensions=_EXTENSIONS,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: detection,
    )
    inventory_lake = SimpleNamespace(
        metadata_schema="lake_a",
        latest_snapshot_id=11,
        table_count=0,
        active_data_files=0,
        active_data_bytes=0,
        active_delete_files=0,
        active_delete_bytes=0,
        tables=(),
        dangling_delete_files=0,
        scheduled_files=0,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.MaintenanceInventory",
        lambda _storage: lambda _configuration, _detection: SimpleNamespace(
            lakes=(inventory_lake,)
        ),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.diagnose_inventory",
        lambda _inventory: SimpleNamespace(lakes=()),
    )
    plan = SimpleNamespace()
    monkeypatch.setattr("lakeducktor.cli.prioritize", lambda _diagnosis: plan)
    envelope = ResourceEnvelope(4, "4GB", 4_000_000_000)
    monkeypatch.setattr(
        "lakeducktor.cli.resource_envelope_from_environment",
        lambda: envelope,
    )
    selected = TreatmentSelection(
        kind=TreatmentKind.MERGE,
        priority_rank=1,
        metadata_schema="lake_a",
        table_id=7,
        schema_name="main",
        table_name="events",
        input_bytes=1_000,
        admitted_bytes=512,
        sorting_enabled=False,
        memory_headroom_bytes=1_000_000_000,
        usable_memory_bytes=3_000_000_000,
        max_compacted_files=1,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.select_treatment",
        lambda _plan, _envelope: SelectionDecision(
            reason=SelectionReason.SELECTED,
            envelope=envelope,
            selected=selected,
            memory_deferred=2,
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "select"])

    assert result == 0
    messages = [record.getMessage() for record in caplog.records]
    assert (
        "resources duckdb_threads=4 duckdb_memory=4GB duckdb_memory_bytes=4000000000"
    ) in messages
    assert (
        "selected treatment=merge priority_rank=1 lake=lake_a table_id=7 "
        "schema='main' table='events' input_bytes=1000 input_rows=0 "
        "input_snapshots=0 "
        "admitted_input_files=0 "
        "admitted_bytes=512 "
        "sorting_enabled=false memory_headroom_bytes=1000000000 "
        "usable_memory_bytes=3000000000 "
        "max_compacted_files=1 retention_policy=none memory_deferred=2"
    ) in messages


def test_maintain_command_logs_verified_treatment_outcome(
    monkeypatch,
    caplog,
    tmp_path,
) -> None:
    caplog.set_level(logging.INFO, logger="lakeducktor")
    configuration = object()
    storage = object()
    envelope = ResourceEnvelope(4, "4GB", 4_000_000_000)
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: configuration,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.StorageConfiguration.from_environment",
        lambda: storage,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.resource_envelope_from_environment",
        lambda: envelope,
    )
    detection = BackendDetection(
        backend=MetadataBackend.POSTGRES,
        metadata_schemas=("lake_a",),
        extension_version="v1",
        duckdb_extensions=_EXTENSIONS,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: detection,
    )
    inventory_lake = SimpleNamespace(
        metadata_schema="lake_a",
        latest_snapshot_id=11,
        table_count=0,
        active_data_files=0,
        active_data_bytes=0,
        active_delete_files=0,
        active_delete_bytes=0,
        tables=(),
        dangling_delete_files=0,
        scheduled_files=0,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.MaintenanceInventory",
        lambda _storage: lambda _configuration, _detection: SimpleNamespace(
            lakes=(inventory_lake,)
        ),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.diagnose_inventory",
        lambda _inventory: SimpleNamespace(lakes=()),
    )
    plan = SimpleNamespace()
    monkeypatch.setattr("lakeducktor.cli.prioritize", lambda _diagnosis: plan)
    coordinator = object()
    monkeypatch.setattr(
        "lakeducktor.cli.PostgresTreatmentCoordinator",
        lambda _configuration: coordinator,
    )
    selected = TreatmentSelection(
        kind=TreatmentKind.MERGE,
        priority_rank=1,
        metadata_schema="lake_a",
        table_id=7,
        schema_name="main",
        table_name="events",
        input_bytes=1_000,
        admitted_bytes=512,
        sorting_enabled=False,
        memory_headroom_bytes=1_000_000_000,
        usable_memory_bytes=3_000_000_000,
        max_compacted_files=1,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.maintain_once",
        lambda *_arguments, **_keywords: MaintenanceOutcome(
            state=MaintenanceState.COMPLETED,
            selection=selected,
            result=TreatmentResult(files_processed=4, files_created=1),
            selection_reason=None,
            claim_contention=1,
            duration_seconds=12.5,
            table_present=True,
            still_actionable=False,
        ),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "maintain"])

    assert result == 0
    messages = [record.getMessage() for record in caplog.records]
    assert (
        "treatment_completed kind=merge lake=lake_a table_id=7 "
        "files_processed=4 files_created=1 rows_processed=0 "
        "snapshots_processed=0 "
        "duration_seconds=12.500 "
        "sorting_enabled=false "
        "table_present=true still_actionable=false claim_contention=1"
    ) in messages


def test_run_command_delegates_without_a_one_shot_detection(
    monkeypatch,
    tmp_path,
) -> None:
    metadata = object()
    storage = object()
    envelope = ResourceEnvelope(4, "4GB", 4_000_000_000)
    run_configuration = object()
    calls = []
    monkeypatch.setattr(
        "lakeducktor.cli.MetadataConfiguration.from_environment",
        lambda: metadata,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.StorageConfiguration.from_environment",
        lambda: storage,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.resource_envelope_from_environment",
        lambda: envelope,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.RunConfiguration.from_environment",
        lambda: run_configuration,
    )
    monkeypatch.setattr(
        "lakeducktor.cli.detect_metadata_backend",
        lambda _configuration: (_ for _ in ()).throw(
            AssertionError("run must detect inside each cycle")
        ),
    )
    monkeypatch.setattr(
        "lakeducktor.cli.run_service",
        lambda *arguments: calls.append(arguments),
    )

    result = main(["--env-file", str(tmp_path / "missing"), "run"])

    assert result == 0
    assert calls == [(metadata, storage, envelope, run_configuration)]
