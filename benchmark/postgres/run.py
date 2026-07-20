#!/usr/bin/env python3

import argparse
import json
import os
import re
import resource
import subprocess
import time
from pathlib import Path


SCALES = {
    "small": 100,
    "medium": 10_000,
    "large": 100_000,
    "xlarge": 1_000_000,
}

SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def run(command, *, capture=True):
    return subprocess.run(command, check=True, text=True, capture_output=capture)


def pg_connection_string():
    required = ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGSSLMODE")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"missing PostgreSQL environment variables: {', '.join(missing)}")
    return " ".join(
        [
            f"host={os.environ['PGHOST']}",
            f"port={os.environ['PGPORT']}",
            f"dbname={os.environ['PGDATABASE']}",
            f"user={os.environ['PGUSER']}",
            f"sslmode={os.environ['PGSSLMODE']}",
            "application_name=ducklake-metadata-benchmark",
        ]
    )


class Benchmark:
    def __init__(self, root: Path, output: Path, adapter: str):
        self.root = root
        self.output = output
        self.duckdb = root / "build/release/duckdb"
        self.postgres_extension = (
            root / "build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension"
        )
        if not self.duckdb.exists() or not self.postgres_extension.exists():
            raise RuntimeError("DuckDB or postgres_scanner has not been built")
        self.connection = pg_connection_string()
        self.adapter = adapter

    def psql(self, sql: str):
        run(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-q", "-c", sql])

    def duckdb_sql(self, sql: str):
        adapter_option = ", METADATA_ADAPTER 'postgres_fast'" if self.adapter == "postgres_fast" else ""
        statement = (
            f"LOAD '{self.postgres_extension}'; "
            f"ATTACH 'ducklake:postgres:{self.connection}' AS dl "
            f"(DATA_PATH '/work/data/ducklake-bench', METADATA_SCHEMA '{self.schema}'{adapter_option}); "
            f"{sql}"
        )
        return run(
            [str(self.duckdb), "-unsigned", "-csv", "-noheader", "-c", statement]
        ).stdout.strip()

    def reset_fixture(self, schema: str, snapshots: int):
        if not SCHEMA_RE.fullmatch(schema):
            raise ValueError(f"unsafe benchmark schema name: {schema}")
        self.schema = schema
        self.psql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE; CREATE SCHEMA "{schema}";')
        self.duckdb_sql("DETACH dl;")
        if snapshots <= 1:
            return
        self.psql(
            f"""
            INSERT INTO "{schema}".ducklake_snapshot
                (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
            SELECT snapshot_id,
                   TIMESTAMPTZ '2020-01-01 00:00:00+00' + snapshot_id * INTERVAL '1 second',
                   0,
                   1,
                   0
            FROM generate_series(1, {snapshots - 1}) AS snapshot_id;

            INSERT INTO "{schema}".ducklake_snapshot_changes
                (snapshot_id, changes_made, author, commit_message, commit_extra_info)
            SELECT snapshot_id, '', NULL, NULL, NULL
            FROM generate_series(1, {snapshots - 1}) AS snapshot_id;

            ANALYZE "{schema}".ducklake_snapshot;
            ANALYZE "{schema}".ducklake_snapshot_changes;
            """
        )

    def reset_table_info_fixture(self, schema: str, files: int):
        self.reset_fixture(schema, 1)
        table_count = min(1_000, max(1, files // 100))
        delete_files = files // 10
        self.psql(
            f"""
            INSERT INTO "{schema}".ducklake_table
                (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id,
                 table_name, path, path_is_relative)
            SELECT table_id, md5(table_id::text)::uuid, 0, NULL, 0,
                   'table_' || table_id, 'table_' || table_id || '/', true
            FROM generate_series(1, {table_count}) AS table_id;

            INSERT INTO "{schema}".ducklake_data_file
                (data_file_id, table_id, begin_snapshot, end_snapshot, file_order,
                 path, path_is_relative, file_format, record_count, file_size_bytes)
            SELECT file_id, 1 + ((file_id - 1) % {table_count}), 0, NULL, file_id,
                   'data_' || file_id || '.parquet', true, 'parquet', 100,
                   1000 + (file_id % 100)
            FROM generate_series(1, {files}) AS file_id;

            INSERT INTO "{schema}".ducklake_delete_file
                (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id,
                 path, path_is_relative, format, delete_count, file_size_bytes)
            SELECT {files} + file_id, 1 + ((file_id - 1) % {table_count}), 0, NULL, file_id,
                   'delete_' || file_id || '.parquet', true, 'parquet', 1,
                   100 + (file_id % 10)
            FROM generate_series(1, {delete_files}) AS file_id;

            ANALYZE "{schema}".ducklake_table;
            ANALYZE "{schema}".ducklake_data_file;
            ANALYZE "{schema}".ducklake_delete_file;
            """
        )

    def reset_cleanup_fixture(self, schema: str, files: int):
        self.reset_fixture(schema, 1)
        self.psql(
            f"""
            INSERT INTO "{schema}".ducklake_files_scheduled_for_deletion
                (data_file_id, path, path_is_relative, schedule_start)
            SELECT file_id, 'old_' || file_id || '.parquet', true,
                   TIMESTAMPTZ '2020-01-01 00:00:00+00'
            FROM generate_series(1, {files}) AS file_id;
            ANALYZE "{schema}".ducklake_files_scheduled_for_deletion;
            """
        )

    def reset_file_discovery_fixture(self, schema: str, files: int):
        self.reset_fixture(schema, 1)
        self.duckdb_sql("CREATE TABLE dl.main.file_bench(i BIGINT);")
        delete_files = files // 10
        self.psql(
            f"""
            INSERT INTO "{schema}".ducklake_data_file
                (data_file_id, table_id, begin_snapshot, end_snapshot, file_order,
                 path, path_is_relative, file_format, record_count, file_size_bytes)
            SELECT file_id, tbl.table_id, 1, NULL, file_id,
                   'data_' || file_id || '.parquet', true, 'parquet', 100,
                   1000 + (file_id % 100)
            FROM generate_series(1, {files}) AS file_id
            CROSS JOIN (SELECT table_id FROM "{schema}".ducklake_table
                        WHERE table_name = 'file_bench' AND end_snapshot IS NULL) tbl;

            INSERT INTO "{schema}".ducklake_delete_file
                (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id,
                 path, path_is_relative, format, delete_count, file_size_bytes)
            SELECT {files} + file_id, tbl.table_id, 1, NULL, file_id,
                   'delete_' || file_id || '.parquet', true, 'parquet', 1,
                   100 + (file_id % 10)
            FROM generate_series(1, {delete_files}) AS file_id
            CROSS JOIN (SELECT table_id FROM "{schema}".ducklake_table
                        WHERE table_name = 'file_bench' AND end_snapshot IS NULL) tbl;

            ANALYZE "{schema}".ducklake_data_file;
            ANALYZE "{schema}".ducklake_delete_file;
            """
        )

    def reset_commit_stats_fixture(self, schema: str, stat_rows: int):
        self.reset_fixture(schema, 1)
        table_count = max(1, stat_rows // 100)
        self.psql(
            f"""
            INSERT INTO "{schema}".ducklake_table_stats
                (table_id, record_count, next_row_id, file_size_bytes)
            SELECT table_id, 10, 10, 1000
            FROM generate_series(1, {table_count}) AS table_id;

            INSERT INTO "{schema}".ducklake_table_column_stats
                (table_id, column_id, contains_null, contains_nan,
                 min_value, max_value, extra_stats)
            SELECT table_id, column_id, false, false, '0', '9', NULL
            FROM generate_series(1, {table_count}) AS table_id
            CROSS JOIN generate_series(1, 100) AS column_id;

            ANALYZE "{schema}".ducklake_table_stats;
            ANALYZE "{schema}".ducklake_table_column_stats;
            """
        )

    def query_for(self, operation: str, snapshots: int, selectivity: float):
        if operation == "list_snapshots":
            return "SELECT count(*) FROM ducklake_snapshots('dl');"
        if operation == "current_snapshot":
            return "SELECT * FROM ducklake_current_snapshot('dl');"
        if operation == "table_info":
            return (
                "SELECT count(*), sum(file_count), sum(file_size_bytes), "
                "sum(delete_file_count), sum(delete_file_size_bytes) "
                "FROM ducklake_table_info('dl');"
            )
        if operation == "cleanup_candidates":
            return (
                "SELECT count(*) FROM ducklake_cleanup_old_files("
                "'dl', cleanup_all => true, dry_run => true);"
            )
        if operation == "file_discovery":
            return (
                "SELECT count(*), count(delete_file), sum(data_file_size_bytes), "
                "sum(delete_file_size_bytes) FROM ducklake_list_files("
                "'dl', 'file_bench', schema => 'main');"
            )
        if operation == "commit_stats_refresh":
            table_filter = "AND stats.table_id = 1" if self.adapter == "postgres_fast" else ""
            return f"""
                SELECT count(*), bit_xor(hash(stats.table_id, columns.column_id,
                    stats.record_count, stats.next_row_id, stats.file_size_bytes,
                    columns.contains_null, columns.contains_nan,
                    columns.min_value, columns.max_value, columns.extra_stats))
                FROM \"__ducklake_metadata_dl\".\"{self.schema}\".ducklake_table_stats stats
                LEFT JOIN \"__ducklake_metadata_dl\".\"{self.schema}\".ducklake_table_column_stats columns
                    USING (table_id)
                WHERE stats.record_count IS NOT NULL
                  AND stats.file_size_bytes IS NOT NULL
                  {table_filter};
            """
        expire_count = min(snapshots - 1, max(0, round(snapshots * selectivity)))
        cutoff = f"2020-01-01 00:00:00+00" if expire_count == 0 else None
        if expire_count:
            cutoff_seconds = expire_count + 1
            cutoff = f"2020-01-01 00:00:00+00" if cutoff_seconds == 0 else None
            cutoff = time.strftime(
                "%Y-%m-%d %H:%M:%S+00",
                time.gmtime(1_577_836_800 + cutoff_seconds),
            )
        dry_run = "true" if operation == "expire_snapshots_dry_run" else "false"
        return (
            "SELECT count(*) FROM ducklake_expire_snapshots("
            f"'dl', older_than => TIMESTAMPTZ '{cutoff}', dry_run => {dry_run});"
        )

    def measure(self, operation: str, snapshots: int, selectivity: float):
        query = self.query_for(operation, snapshots, selectivity)
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        started = time.perf_counter()
        result = self.duckdb_sql(query)
        elapsed = time.perf_counter() - started
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        return {
            "operation": operation,
            "snapshots": snapshots,
            "selectivity": selectivity,
            "elapsed_seconds": elapsed,
            "child_user_cpu_seconds": after.ru_utime - before.ru_utime,
            "child_system_cpu_seconds": after.ru_stime - before.ru_stime,
            "max_rss_kib": after.ru_maxrss,
            "result": result,
        }

    def write_result(self, result):
        self.output.parent.mkdir(parents=True, exist_ok=True)
        with self.output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(result, sort_keys=True) + "\n")
        print(json.dumps(result, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/work/ducklake"))
    parser.add_argument("--output", type=Path, default=Path("/work/results/baseline.jsonl"))
    parser.add_argument("--scale", choices=SCALES, required=True)
    parser.add_argument("--adapter", choices=("postgres", "postgres_fast"), default="postgres")
    parser.add_argument(
        "--operation",
        choices=(
            "list_snapshots",
            "current_snapshot",
            "table_info",
            "cleanup_candidates",
            "file_discovery",
            "commit_stats_refresh",
            "expire_snapshots_dry_run",
            "expire_snapshots",
        ),
        required=True,
    )
    parser.add_argument("--selectivity", type=float, default=0.5)
    parser.add_argument("--trials", type=int, default=3)
    args = parser.parse_args()

    if not 0 <= args.selectivity <= 1:
        parser.error("--selectivity must be between 0 and 1")
    snapshots = SCALES[args.scale]
    benchmark = Benchmark(args.root, args.output, args.adapter)
    for trial in range(args.trials):
        adapter_name = "fast" if args.adapter == "postgres_fast" else "ref"
        schema = f"dlbench_{adapter_name}_{args.scale}_{args.operation[:12]}_{trial}"
        if args.operation == "table_info":
            benchmark.reset_table_info_fixture(schema, snapshots)
        elif args.operation == "cleanup_candidates":
            benchmark.reset_cleanup_fixture(schema, snapshots)
        elif args.operation == "file_discovery":
            benchmark.reset_file_discovery_fixture(schema, snapshots)
        elif args.operation == "commit_stats_refresh":
            benchmark.reset_commit_stats_fixture(schema, snapshots)
        else:
            benchmark.reset_fixture(schema, snapshots)
        result = benchmark.measure(args.operation, snapshots, args.selectivity)
        result.update({"adapter": args.adapter, "scale": args.scale, "trial": trial})
        benchmark.write_result(result)


if __name__ == "__main__":
    main()
