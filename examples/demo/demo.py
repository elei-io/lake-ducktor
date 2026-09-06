"""Seed disposable data, invoke the real CLI, and verify row preservation."""

from __future__ import annotations

import json
import subprocess

import duckdb

from lakeducktor.config import MetadataConfiguration, StorageConfiguration
from lakeducktor.duckdb_config import connection_config


def connect():
    metadata = MetadataConfiguration.from_environment()
    storage = StorageConfiguration.from_environment()
    connection = duckdb.connect(config=connection_config())
    for extension in ("postgres", "ducklake"):
        connection.execute(f"LOAD {extension}")
    uri = metadata.postgres_uri().replace("'", "''")
    path = storage.data_path.replace("'", "''")
    connection.execute(
        f"ATTACH 'ducklake:postgres:{uri}' AS lake "
        f"(METADATA_SCHEMA 'ducklake', DATA_PATH '{path}')"
    )
    return connection


def snapshot():
    with connect() as connection:
        rows = connection.execute(
            "SELECT id, payload FROM lake.main.demo ORDER BY id"
        ).fetchall()
        # Read native file metadata, not directory contents (which include old files).
        files = connection.execute(
            "SELECT count(*) FROM ducklake_list_files('lake', 'demo')"
        ).fetchone()[0]
        return rows, files


def main():
    with connect() as connection:
        connection.execute("CREATE TABLE lake.main.demo (id BIGINT, payload VARCHAR)")
        connection.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        connection.execute("CALL lake.set_option('target_file_size', '16MB')")
        for batch in range(40):
            connection.execute(
                "INSERT INTO lake.main.demo SELECT i, 'row-' || i::VARCHAR "
                "FROM range(?, ?) AS t(i)",
                [batch * 100, (batch + 1) * 100],
            )
    before_rows, before_files = snapshot()
    assert len(before_rows) == 4000 and before_files >= 32
    subprocess.run(["lakeducktor", "inventory"], check=True)
    subprocess.run(["lakeducktor", "select"], check=True)
    subprocess.run(["lakeducktor", "maintain"], check=True)
    after_rows, after_files = snapshot()
    assert after_rows == before_rows, "Maintenance changed the data"
    assert after_files < before_files, "Maintenance did not reduce active files"
    print(
        json.dumps(
            {
                "rows_before": len(before_rows),
                "rows_after": len(after_rows),
                "files_before": before_files,
                "files_after": after_files,
                "exact_row_match": True,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
