"""Create and run a concurrent DuckLake writer/maintenance soak.

This is an explicit operator tool, not part of the unit test suite.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import random
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import psycopg
from psycopg import sql

from lakeducktor.config import (
    MetadataConfiguration,
    StorageConfiguration,
    load_env_file,
)
from lakeducktor.coordination import advisory_lock_key

LAKE_SCHEMA = "ducklake_b545b95f250a49f28b753109d95b13ed"
TARGET_FILE_SIZE = "5MB"
TARGET_FILE_SIZE_BYTES = 5_000_000
TABLE_KINDS = ("tiny", "medium", "large")
TABLE_CODES = {"tiny": 1, "medium": 2, "large": 3}
SEED_ROWS_PER_FILE = {"tiny": 1, "medium": 10_000, "large": 40_000}
SEED_FILE_COUNTS = {"tiny": 10_000, "medium": 100, "large": 500}
TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class Runtime:
    metadata: MetadataConfiguration
    storage: StorageConfiguration


def runtime() -> Runtime:
    load_env_file(Path(".env"))
    return Runtime(
        metadata=MetadataConfiguration.from_environment(),
        storage=StorageConfiguration.from_environment(),
    )


def table_names(run_id: str) -> dict[str, str]:
    normalized = re.sub(r"[^a-z0-9]+", "_", run_id.lower()).strip("_")
    if not normalized:
        raise ValueError("run id must contain a letter or number")
    names = {kind: f"lakeducktor_soak_{normalized}_{kind}" for kind in TABLE_KINDS}
    if any(len(name) > 63 or not TABLE_NAME.fullmatch(name) for name in names.values()):
        raise ValueError("run id produces an invalid or overlong table name")
    return names


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def open_lake(alias: str = "soak") -> duckdb.DuckDBPyConnection:
    values = runtime()
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("PRAGMA disable_checkpoint_on_shutdown")
        for extension in ("httpfs", "postgres", "ducklake"):
            connection.execute(f"INSTALL {extension}")
            connection.execute(f"LOAD {extension}")
        connection.execute(
            """
            CREATE SECRET soak_storage (
                TYPE s3,
                KEY_ID ?,
                SECRET ?,
                REGION ?,
                ENDPOINT ?,
                URL_STYLE 'path',
                USE_SSL ?,
                SCOPE ?
            )
            """,
            [
                values.storage.access_key_id,
                values.storage.secret_access_key,
                values.storage.region,
                values.storage.endpoint,
                values.storage.use_ssl,
                f"s3://{values.storage.bucket}",
            ],
        )
        uri = _sql_string(values.metadata.postgres_uri())
        connection.execute(
            f"""
            ATTACH 'ducklake:postgres:{uri}' AS {alias} (
                METADATA_SCHEMA '{LAKE_SCHEMA}',
                CREATE_IF_NOT_EXISTS false
            )
            """
        )
        connection.execute("SET ducklake_max_retry_count = 100")
        return connection
    except BaseException:
        connection.close()
        raise


def set_table_option(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    key: str,
    value: str | bool,
) -> None:
    connection.execute(
        """
        CALL soak.set_option(
            ?,
            ?,
            schema => 'main',
            table_name => ?
        )
        """,
        [key, value, table],
    ).fetchall()


def setup(run_id: str) -> None:
    names = table_names(run_id)
    connection = open_lake()
    try:
        for table in names.values():
            connection.execute(
                f"""
                CREATE TABLE soak.main.{table} (
                    id BIGINT,
                    writer_id VARCHAR,
                    sequence BIGINT,
                    payload VARCHAR
                )
                """
            )
        connection.execute(
            f"ALTER TABLE soak.main.{names['large']} SET SORTED BY (id ASC)"
        )
        for table in names.values():
            set_table_option(connection, table, "data_inlining_row_limit", "0")
            set_table_option(connection, table, "target_file_size", TARGET_FILE_SIZE)
            set_table_option(connection, table, "auto_compact", False)
    finally:
        connection.close()
    print(json.dumps({"event": "setup_complete", "tables": names}), flush=True)


def reset_table(run_id: str, kind: str) -> None:
    table = table_names(run_id)[kind]
    connection = open_lake()
    try:
        connection.execute(f"DROP TABLE soak.main.{table}")
        connection.execute(
            f"""
            CREATE TABLE soak.main.{table} (
                id BIGINT,
                writer_id VARCHAR,
                sequence BIGINT,
                payload VARCHAR
            )
            """
        )
        set_table_option(connection, table, "auto_compact", False)
        set_table_option(connection, table, "data_inlining_row_limit", "0")
        set_table_option(connection, table, "target_file_size", TARGET_FILE_SIZE)
        if kind == "large":
            connection.execute(f"ALTER TABLE soak.main.{table} SET SORTED BY (id ASC)")
    finally:
        connection.close()
    print(
        json.dumps({"event": "table_reset", "kind": kind, "table": table}),
        flush=True,
    )


def enable_maintenance(run_id: str) -> None:
    connection = open_lake()
    try:
        for table in table_names(run_id).values():
            set_table_option(connection, table, "auto_compact", True)
    finally:
        connection.close()
    print(json.dumps({"event": "maintenance_enabled"}), flush=True)


def seed(
    run_id: str,
    kind: str,
    start_file: int = 0,
    transaction_files: int = 1,
    end_file: int | None = None,
) -> None:
    if transaction_files <= 0:
        raise ValueError("transaction_files must be positive")
    table = table_names(run_id)[kind]
    file_count = min(SEED_FILE_COUNTS[kind], end_file or SEED_FILE_COUNTS[kind])
    rows_per_file = SEED_ROWS_PER_FILE[kind]
    table_code = TABLE_CODES[kind]
    id_base = table_code * 1_000_000_000_000
    started = time.monotonic()
    connection = open_lake()
    try:
        file_index = start_file
        while file_index < file_count:
            transaction_end = min(file_count, file_index + transaction_files)
            if transaction_files > 1:
                connection.execute("BEGIN TRANSACTION")
            try:
                for transaction_file in range(file_index, transaction_end):
                    sequence_base = transaction_file * 100_000
                    connection.execute(
                        f"""
                        INSERT INTO soak.main.{table}
                        SELECT
                            ? + ? + i,
                            'seed',
                            ? + i,
                            sha256(
                                concat(
                                    random()::VARCHAR,
                                    ':',
                                    i::VARCHAR,
                                    ':',
                                    ?::VARCHAR
                                )
                            )
                        FROM range(?) AS rows(i)
                        """,
                        [
                            id_base,
                            sequence_base,
                            sequence_base,
                            transaction_file,
                            rows_per_file,
                        ],
                    )
                if transaction_files > 1:
                    connection.execute("COMMIT")
            except BaseException:
                if transaction_files > 1:
                    with contextlib.suppress(duckdb.TransactionException):
                        connection.execute("ROLLBACK")
                raise
            file_index = transaction_end
            completed = file_index
            interval = 100 if kind == "tiny" else 10
            if completed % interval == 0 or completed == file_count:
                print(
                    json.dumps(
                        {
                            "event": "seed_progress",
                            "kind": kind,
                            "files": completed,
                            "elapsed_seconds": round(
                                time.monotonic() - started,
                                3,
                            ),
                        }
                    ),
                    flush=True,
                )
    finally:
        connection.close()


def seed_gaps(run_id: str, kind: str) -> list[tuple[int, int]]:
    table = table_names(run_id)[kind]
    id_base = TABLE_CODES[kind] * 1_000_000_000_000
    values = runtime()
    with psycopg.connect(values.metadata.postgres_uri()) as connection:
        rows = connection.execute(
            sql.SQL(
                """
                SELECT DISTINCT columns.min_value
                FROM {}.ducklake_file_column_stats AS columns
                JOIN {}.ducklake_data_file AS files
                  USING (data_file_id, table_id)
                JOIN {}.ducklake_table AS tables
                  USING (table_id)
                WHERE tables.table_name = %s
                  AND tables.end_snapshot IS NULL
                  AND files.end_snapshot IS NULL
                  AND columns.column_id = 1
                """
            ).format(
                sql.Identifier(LAKE_SCHEMA),
                sql.Identifier(LAKE_SCHEMA),
                sql.Identifier(LAKE_SCHEMA),
            ),
            [table],
        ).fetchall()
    present = {(int(row[0]) - id_base) // 100_000 for row in rows}
    missing = [index for index in range(SEED_FILE_COUNTS[kind]) if index not in present]
    ranges: list[tuple[int, int]] = []
    for index in missing:
        if ranges and ranges[-1][1] == index:
            ranges[-1] = (ranges[-1][0], index + 1)
        else:
            ranges.append((index, index + 1))
    return ranges


def _append_json(path: Path, entry: dict[str, object]) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(entry, sort_keys=True) + "\n")


def writer(run_id: str, kind: str, duration_seconds: float, log_path: Path) -> None:
    table = table_names(run_id)[kind]
    table_code = TABLE_CODES[kind]
    id_base = table_code * 1_000_000_000_000 + 500_000_000_000
    writer_id = f"writer_{kind}"
    randomizer = random.Random(f"{run_id}:{kind}")
    deadline = time.monotonic() + duration_seconds
    sequence = 0
    connection: duckdb.DuckDBPyConnection | None = None
    commits = 0
    errors = 0
    rows_committed = 0
    _append_json(
        log_path,
        {
            "event": "writer_started",
            "kind": kind,
            "duration_seconds": duration_seconds,
            "time": time.time(),
        },
    )
    try:
        while time.monotonic() < deadline:
            if connection is None:
                try:
                    connection = open_lake()
                except Exception as error:
                    errors += 1
                    _append_json(
                        log_path,
                        {
                            "event": "connect_error",
                            "error": repr(error),
                            "kind": kind,
                            "time": time.time(),
                        },
                    )
                    time.sleep(1)
                    continue
            batch_size = randomizer.randint(1, 20)
            start_sequence = sequence
            sequence += batch_size
            start_id = id_base + start_sequence
            end_id = start_id + batch_size - 1
            transaction_started = time.monotonic()
            try:
                connection.execute(
                    f"""
                    INSERT INTO soak.main.{table}
                    SELECT
                        ? + i,
                        ?,
                        ? + i,
                        sha256(
                            concat(
                                random()::VARCHAR,
                                ':',
                                i::VARCHAR,
                                ':',
                                ?::VARCHAR
                            )
                        )
                    FROM range(?) AS rows(i)
                    """,
                    [
                        start_id,
                        writer_id,
                        start_sequence,
                        start_sequence,
                        batch_size,
                    ],
                )
                commits += 1
                rows_committed += batch_size
                _append_json(
                    log_path,
                    {
                        "batch_size": batch_size,
                        "duration_seconds": round(
                            time.monotonic() - transaction_started,
                            6,
                        ),
                        "end_id": end_id,
                        "event": "commit",
                        "kind": kind,
                        "start_id": start_id,
                        "time": time.time(),
                    },
                )
            except Exception as error:
                errors += 1
                _append_json(
                    log_path,
                    {
                        "batch_size": batch_size,
                        "duration_seconds": round(
                            time.monotonic() - transaction_started,
                            6,
                        ),
                        "end_id": end_id,
                        "error": repr(error),
                        "event": "write_error",
                        "kind": kind,
                        "start_id": start_id,
                        "time": time.time(),
                    },
                )
                connection.close()
                connection = None
                time.sleep(1)
            time.sleep(randomizer.uniform(0.25, 1.0))
    finally:
        if connection is not None:
            connection.close()
        _append_json(
            log_path,
            {
                "commits": commits,
                "errors": errors,
                "event": "writer_stopped",
                "kind": kind,
                "rows_committed": rows_committed,
                "time": time.time(),
            },
        )
        print(
            json.dumps(
                {
                    "event": "writer_complete",
                    "kind": kind,
                    "commits": commits,
                    "errors": errors,
                    "rows_committed": rows_committed,
                }
            ),
            flush=True,
        )


def _read_url(url: str) -> tuple[int | None, str | None, str | None]:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.status, response.read().decode(), None
    except (OSError, urllib.error.URLError) as error:
        return None, None, repr(error)


def _metric(body: str | None, name: str) -> float | None:
    if body is None:
        return None
    for line in body.splitlines():
        if line.startswith(name + " "):
            return float(line.rsplit(" ", 1)[1])
    return None


def catalog_stats(run_id: str) -> dict[str, dict[str, int | bool]]:
    values = runtime()
    names = table_names(run_id)
    result: dict[str, dict[str, int | bool]] = {}
    with (
        psycopg.connect(values.metadata.postgres_uri()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SET statement_timeout = '30s'")
        cursor.execute("SET lock_timeout = '10s'")
        for kind, table in names.items():
            cursor.execute(
                sql.SQL(
                    """
                        SELECT
                            tables.table_id,
                            count(files.data_file_id),
                            coalesce(sum(files.record_count), 0),
                            coalesce(sum(files.file_size_bytes), 0),
                            coalesce(min(files.file_size_bytes), 0),
                            coalesce(
                                percentile_cont(0.5) WITHIN GROUP (
                                    ORDER BY files.file_size_bytes
                                ),
                                0
                            )::BIGINT,
                            coalesce(max(files.file_size_bytes), 0),
                            coalesce(
                                (
                                    SELECT value::BIGINT
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'table'
                                      AND scope_id = tables.table_id
                                      AND key = 'target_file_size'
                                ),
                                (
                                    SELECT value::BIGINT
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'schema'
                                      AND scope_id = tables.schema_id
                                      AND key = 'target_file_size'
                                ),
                                (
                                    SELECT value::BIGINT
                                    FROM {}.ducklake_metadata
                                    WHERE scope IS NULL
                                      AND key = 'target_file_size'
                                ),
                                512000000
                            ),
                            coalesce(
                                (
                                    SELECT value::BOOLEAN
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'table'
                                      AND scope_id = tables.table_id
                                      AND key = 'auto_compact'
                                ),
                                (
                                    SELECT value::BOOLEAN
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'schema'
                                      AND scope_id = tables.schema_id
                                      AND key = 'auto_compact'
                                ),
                                (
                                    SELECT value::BOOLEAN
                                    FROM {}.ducklake_metadata
                                    WHERE scope IS NULL
                                      AND key = 'auto_compact'
                                ),
                                true
                            ),
                            coalesce(
                                (
                                    SELECT value::BIGINT
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'table'
                                      AND scope_id = tables.table_id
                                      AND key = 'data_inlining_row_limit'
                                ),
                                (
                                    SELECT value::BIGINT
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'schema'
                                      AND scope_id = tables.schema_id
                                      AND key = 'data_inlining_row_limit'
                                ),
                                (
                                    SELECT value::BIGINT
                                    FROM {}.ducklake_metadata
                                    WHERE scope IS NULL
                                      AND key = 'data_inlining_row_limit'
                                ),
                                10
                            ),
                            coalesce(
                                (
                                    SELECT value::BOOLEAN
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'table'
                                      AND scope_id = tables.table_id
                                      AND key = 'sort_on_insert'
                                ),
                                (
                                    SELECT value::BOOLEAN
                                    FROM {}.ducklake_metadata
                                    WHERE scope = 'schema'
                                      AND scope_id = tables.schema_id
                                      AND key = 'sort_on_insert'
                                ),
                                (
                                    SELECT value::BOOLEAN
                                    FROM {}.ducklake_metadata
                                    WHERE scope IS NULL
                                      AND key = 'sort_on_insert'
                                ),
                                true
                            ),
                            EXISTS (
                                SELECT 1
                                FROM {}.ducklake_sort_info
                                WHERE table_id = tables.table_id
                                  AND end_snapshot IS NULL
                            )
                        FROM {}.ducklake_table AS tables
                        LEFT JOIN {}.ducklake_data_file AS files
                          ON files.table_id = tables.table_id
                         AND files.end_snapshot IS NULL
                        WHERE tables.table_name = %s
                          AND tables.end_snapshot IS NULL
                        GROUP BY tables.table_id, tables.schema_id
                        """
                ).format(
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                    sql.Identifier(LAKE_SCHEMA),
                ),
                [table],
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"table not found: {table}")
            result[kind] = {
                "table_id": int(row[0]),
                "active_files": int(row[1]),
                "rows": int(row[2]),
                "bytes": int(row[3]),
                "minimum_file_bytes": int(row[4]),
                "median_file_bytes": int(row[5]),
                "maximum_file_bytes": int(row[6]),
                "target_file_size_bytes": int(row[7]),
                "auto_compact": bool(row[8]),
                "data_inlining_row_limit": int(row[9]),
                "sort_on_insert": bool(row[10]),
                "sorting_enabled": bool(row[11]),
            }
    return result


_OPTION_KEYS = (
    "target_file_size_bytes",
    "auto_compact",
    "data_inlining_row_limit",
    "sort_on_insert",
    "sorting_enabled",
)


def option_snapshot(
    stats: dict[str, dict[str, int | bool]],
) -> dict[str, dict[str, int | bool]]:
    return {
        kind: {key: facts[key] for key in _OPTION_KEYS} for kind, facts in stats.items()
    }


def assert_options_unchanged(
    expected: dict[str, dict[str, int | bool]],
    stats: dict[str, dict[str, int | bool]],
    events_path: Path,
) -> None:
    actual = option_snapshot(stats)
    if actual == expected:
        return
    _append_json(
        events_path,
        {
            "actual": actual,
            "event": "option_mutation",
            "expected": expected,
            "time": time.time(),
        },
    )
    raise RuntimeError("effective table options changed during the soak")


def _log_entries(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        return ()
    return tuple(
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )


def validate(
    run_id: str,
    artifact_dir: Path,
    kinds: tuple[str, ...] = TABLE_KINDS,
) -> dict[str, object]:
    names = table_names(run_id)
    validation: dict[str, object] = {"tables": {}}
    connection = open_lake()
    try:
        for kind in kinds:
            table = names[kind]
            writer_log = artifact_dir / f"writer-{kind}.jsonl"
            entries = _log_entries(writer_log)
            commits = tuple(entry for entry in entries if entry["event"] == "commit")
            errors = tuple(
                entry
                for entry in entries
                if entry["event"] in {"connect_error", "write_error"}
            )
            acknowledged_rows = sum(int(entry["batch_size"]) for entry in commits)
            connection.execute(
                """
                CREATE OR REPLACE TEMP TABLE acknowledged_ids (id BIGINT)
                """
            )
            if commits:
                connection.executemany(
                    "INSERT INTO acknowledged_ids VALUES (?)",
                    [
                        (acknowledged_id,)
                        for entry in commits
                        for acknowledged_id in range(
                            int(entry["start_id"]),
                            int(entry["end_id"]) + 1,
                        )
                    ],
                )
            row = connection.execute(
                f"""
                SELECT
                    count(*),
                    count(DISTINCT id),
                    count(*) FILTER (WHERE writer_id = 'seed'),
                    count(*) FILTER (WHERE writer_id = ?)
                FROM soak.main.{table}
                """,
                [f"writer_{kind}"],
            ).fetchone()
            if row is None:
                raise RuntimeError(f"no validation result for {table}")
            missing = connection.execute(
                f"""
                SELECT count(*)
                FROM acknowledged_ids
                ANTI JOIN soak.main.{table} AS actual USING (id)
                """
            ).fetchone()
            if missing is None:
                raise RuntimeError(f"no acknowledgement result for {table}")
            expected_seed_rows = SEED_FILE_COUNTS[kind] * SEED_ROWS_PER_FILE[kind]
            validation["tables"][kind] = {
                "acknowledged_rows": acknowledged_rows,
                "duplicate_ids": int(row[0]) - int(row[1]),
                "expected_seed_rows": expected_seed_rows,
                "missing_acknowledged_rows": int(missing[0]),
                "seed_rows": int(row[2]),
                "total_rows": int(row[0]),
                "writer_error_count": len(errors),
                "writer_rows": int(row[3]),
            }
    finally:
        connection.close()
    validation["catalog"] = {
        kind: facts for kind, facts in catalog_stats(run_id).items() if kind in kinds
    }
    return validation


def _start_process(
    command: list[str],
    log_path: Path,
    environment: dict[str, str] | None = None,
    *,
    start_new_session: bool = False,
) -> tuple[subprocess.Popen[str], Any]:
    log = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=Path.cwd(),
        env=environment,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=start_new_session,
    )
    return process, log


def _stop_processes(processes: Iterable[subprocess.Popen[str]]) -> None:
    running = tuple(process for process in processes if process.poll() is None)
    for process in running:
        process.send_signal(signal.SIGINT)
    deadline = time.monotonic() + 30
    for process in running:
        remaining = max(0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.terminate()
    for process in running:
        if process.poll() is None:
            process.kill()
            process.wait()


def treatment_failure_summary(artifact_dir: Path) -> dict[str, object]:
    failures: list[dict[str, object]] = []
    pattern = re.compile(
        r"ERROR lakeducktor treatment_failed "
        r"kind=(?P<kind>\w+) .* table_id=(?P<table_id>\d+) "
        r"duration_seconds=(?P<duration>[\d.]+) .* "
        r"reason=(?P<reason>\w+)"
    )
    for path in sorted(artifact_dir.glob("maintainer-*.log")):
        for line in path.read_text(encoding="utf-8").splitlines():
            match = pattern.search(line)
            if match is None:
                continue
            failures.append(
                {
                    "duration_seconds": float(match["duration"]),
                    "kind": match["kind"],
                    "reason": match["reason"],
                    "table_id": int(match["table_id"]),
                    "worker_log": path.name,
                }
            )
    reasons: dict[str, int] = {}
    for failure in failures:
        reason = str(failure["reason"])
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "count": len(failures),
        "failures": failures,
        "reasons": reasons,
    }


def _wait_until_ready(
    ports: tuple[int, ...],
    processes: tuple[subprocess.Popen[str], ...],
    timeout_seconds: float = 60,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("a maintainer exited before becoming ready")
        if all(
            _read_url(f"http://127.0.0.1:{port}/readyz")[0] == 200 for port in ports
        ):
            return
        time.sleep(0.25)
    raise RuntimeError("maintainers did not become ready")


def _wait_for_log(
    path: Path,
    marker: str,
    process: subprocess.Popen[str],
    timeout_seconds: float,
) -> float:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.exists() and marker in path.read_text(encoding="utf-8"):
            return time.monotonic()
        if process.poll() is not None:
            raise RuntimeError(
                f"process exited before log marker {marker!r}: {process.returncode}"
            )
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for log marker: {marker}")


def _verify_advisory_lock_released(
    metadata_schema: str,
    timeout_seconds: float = 10,
) -> float:
    values = runtime()
    lock_key = advisory_lock_key(metadata_schema)
    started = time.monotonic()
    deadline = started + timeout_seconds
    with psycopg.connect(
        values.metadata.postgres_uri(),
        autocommit=True,
    ) as connection:
        while time.monotonic() < deadline:
            row = connection.execute(
                "SELECT pg_try_advisory_lock(%s)",
                [lock_key],
            ).fetchone()
            if row is not None and bool(row[0]):
                connection.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
                return time.monotonic() - started
            time.sleep(0.05)
    raise RuntimeError("PostgreSQL advisory lock was not released after process death")


def orchestrate(
    run_id: str,
    artifact_dir: Path,
    writer_seconds: float,
    drain_seconds: float,
    maintainer_count: int = 3,
    writer_kinds: tuple[str, ...] = TABLE_KINDS,
    base_port: int = 8_000,
) -> None:
    if maintainer_count <= 0:
        raise ValueError("maintainer count must be positive")
    if not writer_kinds:
        raise ValueError("at least one writer kind is required")
    artifact_dir.mkdir(parents=True, exist_ok=False)
    events_path = artifact_dir / "observations.jsonl"
    ports = tuple(range(base_port, base_port + maintainer_count))
    baseline_stats = catalog_stats(run_id)
    expected_options = option_snapshot(baseline_stats)
    (artifact_dir / "expected-options.json").write_text(
        json.dumps(expected_options, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    base_environment = dict(os.environ)
    base_environment["MAINTAIN_LAKES"] = LAKE_SCHEMA
    base_environment["POLL_INTERVAL_SECONDS"] = "5"
    base_environment["TREATMENT_STUCK_AFTER_SECONDS"] = "900"
    executable = str(Path(sys.executable).with_name("lakeducktor"))
    maintainers: list[subprocess.Popen[str]] = []
    writers: list[subprocess.Popen[str]] = []
    log_handles: list[Any] = []
    health_errors = 0
    premature_exits: list[dict[str, object]] = []
    started = time.monotonic()
    try:
        for index, port in enumerate(ports, start=1):
            environment = dict(base_environment)
            environment["METRICS_PORT"] = str(port)
            process, log = _start_process(
                [executable, "run"],
                artifact_dir / f"maintainer-{index}.log",
                environment,
            )
            maintainers.append(process)
            log_handles.append(log)

        _wait_until_ready(ports, tuple(maintainers))

        for kind in writer_kinds:
            process, log = _start_process(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "writer",
                    "--run-id",
                    run_id,
                    "--kind",
                    kind,
                    "--duration-seconds",
                    str(writer_seconds),
                    "--log-path",
                    str(artifact_dir / f"writer-{kind}.jsonl"),
                ],
                artifact_dir / f"writer-{kind}.log",
                base_environment,
            )
            writers.append(process)
            log_handles.append(log)

        next_report = time.monotonic()
        while any(process.poll() is None for process in writers):
            current_stats = catalog_stats(run_id)
            assert_options_unchanged(expected_options, current_stats, events_path)
            observation: dict[str, object] = {
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "event": "active_sample",
                "maintainers": [],
                "tables": current_stats,
                "time": time.time(),
                "writers": [process.poll() for process in writers],
            }
            for index, (process, port) in enumerate(
                zip(maintainers, ports, strict=True),
                start=1,
            ):
                ready, ready_body, ready_error = _read_url(
                    f"http://127.0.0.1:{port}/readyz"
                )
                metrics_status, metrics_body, metrics_error = _read_url(
                    f"http://127.0.0.1:{port}/metrics"
                )
                if ready_error or metrics_error:
                    health_errors += 1
                observation["maintainers"].append(
                    {
                        "exit_code": process.poll(),
                        "index": index,
                        "merge_debt": _metric(
                            metrics_body,
                            "lakeducktor_merge_expected_files_eliminated",
                        ),
                        "metrics_error": metrics_error,
                        "metrics_status": metrics_status,
                        "ready_body": ready_body,
                        "ready_error": ready_error,
                        "ready_status": ready,
                    }
                )
                if process.poll() is not None:
                    premature_exits.append(
                        {
                            "exit_code": process.poll(),
                            "kind": "maintainer",
                            "process": index,
                        }
                    )
            _append_json(events_path, observation)
            if time.monotonic() >= next_report:
                files = {
                    kind: facts["active_files"]
                    for kind, facts in observation["tables"].items()
                }
                print(
                    json.dumps(
                        {
                            "event": "active_progress",
                            "elapsed_seconds": observation["elapsed_seconds"],
                            "active_files": files,
                            "health_errors": health_errors,
                        }
                    ),
                    flush=True,
                )
                next_report = time.monotonic() + 30
            time.sleep(5)

        for process in writers:
            process.wait()
        writer_exits = [process.returncode for process in writers]

        drain_started = time.monotonic()
        zero_debt_samples = 0
        next_report = drain_started
        while time.monotonic() - drain_started < drain_seconds:
            debts: list[float | None] = []
            maintainer_sample: list[dict[str, object]] = []
            for process, port in zip(
                maintainers,
                ports,
                strict=True,
            ):
                status, body, error = _read_url(f"http://127.0.0.1:{port}/metrics")
                debt = _metric(
                    body,
                    "lakeducktor_merge_expected_files_eliminated",
                )
                debts.append(debt)
                maintainer_sample.append(
                    {
                        "exit_code": process.poll(),
                        "merge_debt": debt,
                        "metrics_error": error,
                        "metrics_status": status,
                    }
                )
                if error:
                    health_errors += 1
            current_stats = catalog_stats(run_id)
            assert_options_unchanged(expected_options, current_stats, events_path)
            _append_json(
                events_path,
                {
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "event": "drain_sample",
                    "maintainers": maintainer_sample,
                    "tables": current_stats,
                    "time": time.time(),
                },
            )
            if debts and all(debt == 0 for debt in debts):
                zero_debt_samples += 1
                if zero_debt_samples >= 3:
                    break
            else:
                zero_debt_samples = 0
            if time.monotonic() >= next_report:
                print(
                    json.dumps(
                        {
                            "event": "drain_progress",
                            "elapsed_seconds": round(
                                time.monotonic() - drain_started,
                                3,
                            ),
                            "active_files": {
                                kind: facts["active_files"]
                                for kind, facts in current_stats.items()
                            },
                            "merge_debt": debts,
                            "health_errors": health_errors,
                        }
                    ),
                    flush=True,
                )
                next_report = time.monotonic() + 30
            time.sleep(5)

        _stop_processes(maintainers)
        validation = validate(run_id, artifact_dir, writer_kinds)
        treatment_failures = treatment_failure_summary(artifact_dir)
        failures: list[str] = []
        if health_errors:
            failures.append(f"{health_errors} health requests failed")
        if premature_exits:
            failures.append("a maintainer exited before shutdown")
        if any(code != 0 for code in writer_exits):
            failures.append("a writer process failed")
        if zero_debt_samples < 3:
            failures.append("maintenance debt did not drain")
        for kind, facts in validation["tables"].items():
            if facts["writer_error_count"]:
                failures.append(f"{kind} writer recorded errors")
            if facts["missing_acknowledged_rows"]:
                failures.append(f"{kind} lost acknowledged rows")
            if facts["duplicate_ids"]:
                failures.append(f"{kind} contains duplicate ids")
            if facts["seed_rows"] != facts["expected_seed_rows"]:
                failures.append(f"{kind} seed row count changed")
        summary = {
            "drain_seconds": round(time.monotonic() - drain_started, 3),
            "failures": failures,
            "health_request_errors": health_errors,
            "maintainer_exit_codes": [process.returncode for process in maintainers],
            "maintainer_count": maintainer_count,
            "premature_exits": premature_exits,
            "run_id": run_id,
            "status": "failed" if failures else "passed",
            "treatment_failures": treatment_failures,
            "validation": validation,
            "writer_exit_codes": writer_exits,
            "writer_kinds": writer_kinds,
            "writer_seconds": writer_seconds,
        }
        (artifact_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps({"event": "soak_complete", **summary}), flush=True)
        if failures:
            raise RuntimeError("; ".join(failures))
    except BaseException as error:
        failure_path = artifact_dir / "run-failure.json"
        if not failure_path.exists():
            failure_path.write_text(
                json.dumps(
                    {
                        "error": repr(error),
                        "event": "run_failed",
                        "time": time.time(),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        raise
    finally:
        _stop_processes((*writers, *maintainers))
        for log in log_handles:
            log.close()


def crash_recovery(
    run_id: str,
    artifact_dir: Path,
    writer_seconds: float,
    drain_seconds: float,
    kind: str = "medium",
    base_port: int = 8_100,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=False)
    events_path = artifact_dir / "observations.jsonl"
    baseline_stats = catalog_stats(run_id)
    expected_options = option_snapshot(baseline_stats)
    (artifact_dir / "expected-options.json").write_text(
        json.dumps(expected_options, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["MAINTAIN_LAKES"] = LAKE_SCHEMA
    environment["POLL_INTERVAL_SECONDS"] = "1"
    environment["TREATMENT_STUCK_AFTER_SECONDS"] = "30"
    executable = str(Path(sys.executable).with_name("lakeducktor"))
    names = table_names(run_id)
    processes: list[subprocess.Popen[str]] = []
    logs: list[Any] = []
    first_log = artifact_dir / "maintainer-killed.log"
    survivor_log = artifact_dir / "maintainer-survivor.log"
    writer_log = artifact_dir / f"writer-{kind}.jsonl"
    try:
        writer, writer_output = _start_process(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "writer",
                "--run-id",
                run_id,
                "--kind",
                kind,
                "--duration-seconds",
                str(writer_seconds),
                "--log-path",
                str(writer_log),
            ],
            artifact_dir / f"writer-{kind}.log",
            environment,
        )
        processes.append(writer)
        logs.append(writer_output)

        first_environment = dict(environment)
        first_environment["METRICS_PORT"] = str(base_port)
        first, first_output = _start_process(
            [executable, "run"],
            first_log,
            first_environment,
            start_new_session=True,
        )
        processes.append(first)
        logs.append(first_output)
        _wait_until_ready((base_port,), (first,))
        _wait_for_log(first_log, "treatment_started", first, 120)
        killed_at = time.monotonic()
        os.killpg(first.pid, signal.SIGKILL)
        first.wait(timeout=10)
        lock_release_seconds = _verify_advisory_lock_released(
            LAKE_SCHEMA,
        )
        _append_json(
            events_path,
            {
                "event": "maintainer_killed",
                "exit_code": first.returncode,
                "lock_release_seconds": lock_release_seconds,
                "table": names[kind],
                "time": time.time(),
            },
        )

        survivor_environment = dict(environment)
        survivor_environment["METRICS_PORT"] = str(base_port + 1)
        survivor, survivor_output = _start_process(
            [executable, "run"],
            survivor_log,
            survivor_environment,
        )
        processes.append(survivor)
        logs.append(survivor_output)
        _wait_until_ready((base_port + 1,), (survivor,))
        reacquired_at = _wait_for_log(
            survivor_log,
            "claim_acquired",
            survivor,
            120,
        )

        health_errors = 0
        while writer.poll() is None:
            stats = catalog_stats(run_id)
            assert_options_unchanged(expected_options, stats, events_path)
            ready, _body, ready_error = _read_url(
                f"http://127.0.0.1:{base_port + 1}/readyz"
            )
            metrics, metrics_body, metrics_error = _read_url(
                f"http://127.0.0.1:{base_port + 1}/metrics"
            )
            if ready_error or metrics_error:
                health_errors += 1
            _append_json(
                events_path,
                {
                    "event": "recovery_sample",
                    "merge_debt": _metric(
                        metrics_body,
                        "lakeducktor_merge_expected_files_eliminated",
                    ),
                    "metrics_error": metrics_error,
                    "metrics_status": metrics,
                    "ready_error": ready_error,
                    "ready_status": ready,
                    "tables": stats,
                    "time": time.time(),
                },
            )
            time.sleep(1)
        writer.wait()

        drain_started = time.monotonic()
        zero_debt_samples = 0
        while time.monotonic() - drain_started < drain_seconds:
            stats = catalog_stats(run_id)
            assert_options_unchanged(expected_options, stats, events_path)
            status, body, error = _read_url(f"http://127.0.0.1:{base_port + 1}/metrics")
            if error:
                health_errors += 1
            debt = _metric(
                body,
                "lakeducktor_merge_expected_files_eliminated",
            )
            _append_json(
                events_path,
                {
                    "debt": debt,
                    "event": "recovery_drain_sample",
                    "metrics_error": error,
                    "metrics_status": status,
                    "tables": stats,
                    "time": time.time(),
                },
            )
            if debt == 0:
                zero_debt_samples += 1
                if zero_debt_samples >= 3:
                    break
            else:
                zero_debt_samples = 0
            time.sleep(1)

        _stop_processes((survivor,))
        validation = validate(run_id, artifact_dir, (kind,))
        facts = validation["tables"][kind]
        failures = []
        if first.returncode != -signal.SIGKILL:
            failures.append("maintainer was not killed as expected")
        if survivor.returncode != 0:
            failures.append("surviving maintainer did not stop cleanly")
        if writer.returncode != 0 or facts["writer_error_count"]:
            failures.append("writer failed during crash recovery")
        if facts["missing_acknowledged_rows"] or facts["duplicate_ids"]:
            failures.append("row validation failed after crash recovery")
        if zero_debt_samples < 3:
            failures.append("surviving maintainer did not drain debt")
        if health_errors:
            failures.append(f"{health_errors} health requests failed")
        summary = {
            "failures": failures,
            "killed_exit_code": first.returncode,
            "lock_release_seconds": lock_release_seconds,
            "reacquire_seconds": reacquired_at - killed_at,
            "run_id": run_id,
            "status": "failed" if failures else "passed",
            "survivor_exit_code": survivor.returncode,
            "validation": validation,
            "writer_exit_code": writer.returncode,
        }
        (artifact_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps({"event": "crash_recovery_complete", **summary}), flush=True)
        if failures:
            raise RuntimeError("; ".join(failures))
    except BaseException as error:
        failure_path = artifact_dir / "run-failure.json"
        if not failure_path.exists():
            failure_path.write_text(
                json.dumps(
                    {
                        "error": repr(error),
                        "event": "crash_recovery_failed",
                        "time": time.time(),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        raise
    finally:
        _stop_processes(processes)
        for log in logs:
            log.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("setup", "enable"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--run-id", required=True)

    reset_parser = subparsers.add_parser("reset")
    reset_parser.add_argument("--run-id", required=True)
    reset_parser.add_argument("--kind", choices=TABLE_KINDS, required=True)

    seed_parser = subparsers.add_parser("seed")
    seed_parser.add_argument("--run-id", required=True)
    seed_parser.add_argument("--kind", choices=TABLE_KINDS, required=True)
    seed_parser.add_argument("--start-file", type=int, default=0)
    seed_parser.add_argument("--end-file", type=int)
    seed_parser.add_argument("--transaction-files", type=int, default=1)

    writer_parser = subparsers.add_parser("writer")
    writer_parser.add_argument("--run-id", required=True)
    writer_parser.add_argument("--kind", choices=TABLE_KINDS, required=True)
    writer_parser.add_argument("--duration-seconds", type=float, required=True)
    writer_parser.add_argument("--log-path", type=Path, required=True)

    stats_parser = subparsers.add_parser("stats")
    stats_parser.add_argument("--run-id", required=True)

    gaps_parser = subparsers.add_parser("gaps")
    gaps_parser.add_argument("--run-id", required=True)
    gaps_parser.add_argument("--kind", choices=TABLE_KINDS, required=True)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--run-id", required=True)
    run_parser.add_argument("--artifact-dir", type=Path, required=True)
    run_parser.add_argument("--writer-seconds", type=float, default=600)
    run_parser.add_argument("--drain-seconds", type=float, default=1_800)
    run_parser.add_argument("--maintainers", type=int, default=3)
    run_parser.add_argument("--writer-kind", action="append", choices=TABLE_KINDS)
    run_parser.add_argument("--base-port", type=int, default=8_000)

    crash_parser = subparsers.add_parser("crash")
    crash_parser.add_argument("--run-id", required=True)
    crash_parser.add_argument("--artifact-dir", type=Path, required=True)
    crash_parser.add_argument("--writer-seconds", type=float, default=120)
    crash_parser.add_argument("--drain-seconds", type=float, default=600)
    crash_parser.add_argument("--kind", choices=TABLE_KINDS, default="medium")
    crash_parser.add_argument("--base-port", type=int, default=8_100)
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    if arguments.command == "setup":
        setup(arguments.run_id)
    elif arguments.command == "reset":
        reset_table(arguments.run_id, arguments.kind)
    elif arguments.command == "enable":
        enable_maintenance(arguments.run_id)
    elif arguments.command == "seed":
        seed(
            arguments.run_id,
            arguments.kind,
            arguments.start_file,
            arguments.transaction_files,
            arguments.end_file,
        )
    elif arguments.command == "writer":
        writer(
            arguments.run_id,
            arguments.kind,
            arguments.duration_seconds,
            arguments.log_path,
        )
    elif arguments.command == "stats":
        print(json.dumps(catalog_stats(arguments.run_id), indent=2), flush=True)
    elif arguments.command == "gaps":
        print(
            json.dumps(
                {
                    "kind": arguments.kind,
                    "missing_ranges": seed_gaps(arguments.run_id, arguments.kind),
                }
            ),
            flush=True,
        )
    elif arguments.command == "run":
        orchestrate(
            arguments.run_id,
            arguments.artifact_dir,
            arguments.writer_seconds,
            arguments.drain_seconds,
            arguments.maintainers,
            tuple(arguments.writer_kind or TABLE_KINDS),
            arguments.base_port,
        )
    elif arguments.command == "crash":
        crash_recovery(
            arguments.run_id,
            arguments.artifact_dir,
            arguments.writer_seconds,
            arguments.drain_seconds,
            arguments.kind,
            arguments.base_port,
        )
    else:
        raise AssertionError(arguments.command)


if __name__ == "__main__":
    main()
