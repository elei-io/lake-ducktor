"""Disposable native regression; run with the image's pinned DuckLake extension."""

import tempfile

import duckdb

from lakeducktor.model import (
    CompatibleFileGroup,
    MergePriority,
    PriorityPlan,
    PriorityState,
    ResourceEnvelope,
)
from lakeducktor.selection import select_treatment


def check_productive_batches():
    """A bounded call must clear the large backlog before newly arriving pairs."""
    with tempfile.TemporaryDirectory(prefix="ducktor-priority-") as directory:
        c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        c.execute("LOAD ducklake")
        c.execute("SET memory_limit = '128MB'")
        c.execute("SET threads = 1")
        c.execute(f"ATTACH 'ducklake:{directory}/catalog.duckdb' AS lake")
        c.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        c.execute("CREATE TABLE lake.t (p INTEGER, id INTEGER)")
        c.execute("ALTER TABLE lake.t SET PARTITIONED BY (p)")
        c.execute("ALTER TABLE lake.t SET SORTED BY (id)")
        for i in range(40):
            c.execute("INSERT INTO lake.t VALUES (0, ?)", [i])
        for partition in range(1, 16):
            for i in range(2):
                c.execute("INSERT INTO lake.t VALUES (?, ?)", [partition, i])
        before = c.execute("SELECT * FROM lake.t ORDER BY p, id").fetchall()
        snapshot = c.execute(
            "SELECT id FROM ducklake_current_snapshot('lake')"
        ).fetchone()[0]
        policy = c.execute("SELECT * FROM lake.options()").fetchall()
        result = c.execute(
            "SELECT files_processed, files_created "
            "FROM ducklake_merge_adjacent_files('lake', 't', max_compacted_files => 1)"
        ).fetchall()
        assert result == [(40, 1)], f"small groups won over the backlog: {result}"
        # Continued small writes must neither lose rows nor change the output bound.
        c.execute("INSERT INTO lake.t VALUES (16, 0)")
        c.execute("INSERT INTO lake.t VALUES (16, 1)")
        result = c.execute(
            "SELECT files_processed, files_created "
            "FROM ducklake_merge_adjacent_files('lake', 't', max_compacted_files => 3)"
        ).fetchall()
        assert result == [(2, 1)] * 3, result
        c.execute("CALL ducklake_merge_adjacent_files('lake', 't')")
        assert (
            c.execute(
                "SELECT * FROM ducklake_merge_adjacent_files("
                "'lake', 't', max_compacted_files => 1)"
            ).fetchall()
            == []
        )
        assert c.execute("SELECT * FROM lake.t ORDER BY p, id").fetchall() == (
            before + [(16, 0), (16, 1)]
        )
        assert (
            c.execute(
                f"SELECT * FROM lake.t AT (VERSION => {snapshot}) ORDER BY p, id"
            ).fetchall()
            == before
        )
        assert c.execute("SELECT * FROM lake.options()").fetchall() == policy
        c.close()
        print("Productive batch regression passed: 40 files merged before pairs.")


def check_small_group_with_tiny_file():
    """Known small groups must not inherit a hypothetical 512-file batch bound."""
    with tempfile.TemporaryDirectory(prefix="ducktor-tiny-file-") as directory:
        c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        c.execute("LOAD ducklake")
        c.execute("SET memory_limit = '512MB'")
        c.execute("SET threads = 1")
        c.execute(f"ATTACH 'ducklake:{directory}/catalog.duckdb' AS lake")
        c.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        c.execute("CREATE TABLE lake.t (id BIGINT, payload VARCHAR)")
        c.execute("ALTER TABLE lake.t SET SORTED BY (id)")
        c.execute("INSERT INTO lake.t VALUES (-1, 'tiny')")
        for start in (0, 50000):
            c.execute(
                "INSERT INTO lake.t SELECT i, md5(i::VARCHAR) FROM range(?, ?) t(i)",
                [start, start + 50000],
            )
        sizes = [
            row[0]
            for row in c.execute(
                "SELECT data_file_size_bytes FROM ducklake_list_files('lake', 't')"
            ).fetchall()
        ]
        assert len(sizes) == 3
        assert sorted(sizes)[1] > min(sizes) * 512, sizes
        target = max(sizes) + 1
        c.execute("CALL lake.set_option('target_file_size', ?)", [f"{target}B"])
        before = c.execute("SELECT * FROM lake.t ORDER BY id").fetchall()
        snapshot = c.execute(
            "SELECT id FROM ducklake_current_snapshot('lake')"
        ).fetchone()[0]
        policy = c.execute("SELECT * FROM lake.options()").fetchall()
        size = sum(sizes)
        candidate = MergePriority(
            rank=1,
            metadata_schema="lake",
            table_id=1,
            schema_name="main",
            table_name="t",
            state=PriorityState.RUNNABLE,
            blocked_by=None,
            groups=1,
            input_files=3,
            input_bytes=size,
            average_input_file_bytes=size // 3,
            target_file_size_bytes=target,
            expected_files_eliminated=1,
            recent_data_files_60s=0,
            activity_penalty=0,
            adjusted_expected_files_eliminated=1,
            sorting_enabled=True,
            minimum_input_file_bytes=min(sizes),
            input_groups=(
                CompatibleFileGroup(1, 1, 3, size, 3, size, sorted(sizes)[1]),
            ),
        )
        selected = select_treatment(
            PriorityPlan((), (candidate,), 0, 0),
            ResourceEnvelope(1, "512MB", 512_000_000),
        ).selected
        assert selected is not None, "tiny file blocked a three-file group"
        assert selected.execution_target_file_size_bytes == target
        assert selected.admitted_input_files == 3
        c.execute("SET ducklake_target_file_size = ?", [f"{target}B"])
        result = c.execute(
            "SELECT files_processed, files_created FROM ducklake_merge_adjacent_files("
            "'lake', 't', max_compacted_files => 1, max_file_size => ?)",
            [target],
        ).fetchall()
        assert result and all(2 <= n <= 3 and outputs == 1 for n, outputs in result), (
            result
        )
        assert len(result) == 1
        assert c.execute("SELECT * FROM lake.t ORDER BY id").fetchall() == before
        assert (
            c.execute(
                f"SELECT * FROM lake.t AT (VERSION => {snapshot}) ORDER BY id"
            ).fetchall()
            == before
        )
        assert c.execute("SELECT * FROM lake.options()").fetchall() == policy
        c.close()
        print("Tiny-file regression passed: bounded merge, rows and history preserved.")


def main():
    check_small_group_with_tiny_file()
    check_productive_batches()
    with tempfile.TemporaryDirectory(prefix="ducktor-batches-") as directory:
        c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        c.execute("LOAD ducklake")
        c.execute("SET memory_limit = '512MB'")
        c.execute("SET threads = 1")
        c.execute("PRAGMA disable_checkpoint_on_shutdown")
        c.execute(f"ATTACH 'ducklake:{directory}/catalog.duckdb' AS lake")
        c.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        c.execute("CALL lake.set_option('target_file_size', '512MB')")
        c.execute("CREATE TABLE lake.t (id BIGINT, payload VARCHAR)")
        c.execute("ALTER TABLE lake.t SET SORTED BY (id)")
        for i in range(600):
            c.execute("INSERT INTO lake.t VALUES (?, ?)", [i, "row-" + str(i)])
        before = c.execute("SELECT * FROM lake.t ORDER BY id").fetchall()
        snapshot = c.execute(
            "SELECT id FROM ducklake_current_snapshot('lake')"
        ).fetchall()[0][0]
        policy = c.execute("SELECT * FROM lake.options()").fetchall()
        passes = 0
        while passes < 12:
            sizes = [
                r[0]
                for r in c.execute(
                    "SELECT data_file_size_bytes FROM ducklake_list_files('lake', 't')"
                ).fetchall()
            ]
            if len(sizes) == 1:
                break
            n, size = len(sizes), sum(sizes)
            candidate = MergePriority(
                rank=1,
                metadata_schema="lake",
                table_id=1,
                schema_name="main",
                table_name="t",
                state=PriorityState.RUNNABLE,
                blocked_by=None,
                groups=1,
                input_files=n,
                input_bytes=size,
                average_input_file_bytes=size // n,
                target_file_size_bytes=512_000_000,
                expected_files_eliminated=n - 1,
                recent_data_files_60s=0,
                activity_penalty=0,
                adjusted_expected_files_eliminated=n - 1,
                sorting_enabled=True,
                minimum_input_file_bytes=min(sizes),
                input_groups=(
                    CompatibleFileGroup(1, 1, n, size, n, size, sorted(sizes)[1]),
                ),
            )
            selected = select_treatment(
                PriorityPlan((), (candidate,), 0, 0),
                ResourceEnvelope(1, "512MB", 512_000_000),
            ).selected
            assert selected is not None, "oversized group remains deferred"
            target = selected.execution_target_file_size_bytes
            c.execute("SET ducklake_target_file_size = ?", [f"{target}B"])
            result = c.execute(
                "SELECT sum(files_processed) "
                "FROM ducklake_merge_adjacent_files('lake', 't', "
                "max_compacted_files => 1, max_file_size => ?)",
                [target],
            ).fetchall()[0][0]
            assert (
                result is not None
                and 2 <= result <= selected.admitted_input_files <= 512
            )
            after_count = c.execute(
                "SELECT count(*) FROM ducklake_list_files('lake', 't')"
            ).fetchall()[0][0]
            assert after_count < n, "batch made no progress"
            assert c.execute("SELECT * FROM lake.t ORDER BY id").fetchall() == before
            assert (
                c.execute(
                    f"SELECT * FROM lake.t AT (VERSION => {snapshot}) ORDER BY id"
                ).fetchall()
                == before[:600]
            )
            passes += 1
            print(f"pass={passes} inputs={result} remaining={after_count}", flush=True)
            if passes == 1:
                c.execute("INSERT INTO lake.t VALUES (600, 'row-600')")
                before.append((600, "row-600"))
        assert after_count == 1, "did not drain the backlog"
        assert c.execute("SELECT * FROM lake.options()").fetchall() == policy
        c.close()
        print("Sorted batch regression passed: rows and history preserved.")


if __name__ == "__main__":
    main()
