"""Renders the regression benchmarks for one DuckLake metadata catalog by expanding their catalog operations"""

import argparse
import glob
import os
import re
from pathlib import Path, PurePosixPath

POSTGRES_FUNCTIONS = Path(__file__).with_name("postgres_catalog.sql")
BENCHMARK_DATA_DIRECTORY = "duckdb_benchmark_data"
OPERATION = re.compile(r"@(\w+)\s+(.*?)\s*;?")
STORE = re.compile(r"[\w${}]+")
TRANSFER = re.compile(r"(\S+)\s+TO\s+(\S+)", re.IGNORECASE)
ATTACH = re.compile(r"(IF NOT EXISTS\s+)?(\S+)\s+AS\s+(\w+)(?:\s*\((.*)\))?", re.IGNORECASE)
REFERENCE = re.compile(r"^\s*(?:include|template)\s+(\S+)", re.IGNORECASE | re.MULTILINE)
VERBATIM = re.compile(r"^\s*(?:load|run|init|cleanup|reload)\s+(\S+)\s*$", re.IGNORECASE | re.MULTILINE)
PLACEHOLDER = re.compile(r"\$\{\w+\}")
DATABASE = re.compile(r"[a-z0-9_-]+")


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


class DuckDBCatalog:
    """Keeps every metadata store in a DuckDB file in the benchmark directory"""

    def header(self):
        return []

    def create(self, store):
        # A zero-byte file is not a valid database, so copy an empty one over any partial previous attempt
        path = "${BENCHMARK_DIR}/" + store
        return [
            f"COPY (SELECT 1 AS part, 1 AS v) TO '{path}_blank'",
            "(FORMAT CSV, PARTITION_BY (part), OVERWRITE);",
            f"ATTACH '{path}_blank/blank.db' AS blank;",
            "DETACH blank;",
            f"COPY (SELECT content FROM read_blob('{path}_blank/blank.db'))",
            f"TO '{path}.db' (FORMAT blob);",
            f"COPY (SELECT ''::BLOB AS content) TO '{path}.db.wal' (FORMAT blob);",
        ]

    def attach(self, if_not_exists, store, alias, options):
        suffix = f" ({options})" if options else ""
        return [f"ATTACH {if_not_exists}'ducklake:${{BENCHMARK_DIR}}/{store}.db' AS {alias}{suffix};"]

    def seal(self, store):
        return []

    def copy(self, source, target):
        # The empty WAL discards commits that a killed process left behind
        return [
            f"COPY (SELECT content FROM read_blob('${{BENCHMARK_DIR}}/{source}.db'))",
            f"TO '${{BENCHMARK_DIR}}/{target}.db' (FORMAT blob);",
            f"COPY (SELECT ''::BLOB AS content) TO '${{BENCHMARK_DIR}}/{target}.db.wal' (FORMAT blob);",
        ]

    def rollback(self, source, target):
        return ["ROLLBACK;"]


class PostgresCatalog:
    """Keeps every metadata store in a schema of the database that the pg_database argument names"""

    def __init__(self, database):
        # The runner lowercases argument defaults, so a name with upper case would select another database
        if not DATABASE.fullmatch(database):
            raise ValueError(f"invalid PostgreSQL database {database}: use only lower case letters, digits, _ and -")
        lines = POSTGRES_FUNCTIONS.read_text(encoding="utf-8").splitlines()
        self.functions = " ".join(" ".join(line for line in lines if not line.lstrip().startswith("--")).split())
        self.database = database

    def header(self):
        return ["require postgres_scanner", "", f"argument pg_database {self.database}", ""]

    def server(self, sql):
        # Detach the administrative connection right away, so it never shows up in a timed catalog listing
        return [
            "ATTACH 'dbname=${pg_database}' AS pg_admin (TYPE postgres);",
            f"CALL postgres_execute('pg_admin', {sql_string(sql)}, prepare = false);",
            "DETACH pg_admin;",
        ]

    def create(self, store):
        return self.server(f'DROP SCHEMA IF EXISTS "{store}" CASCADE')

    def attach(self, if_not_exists, store, alias, options):
        suffix = f", {options}" if options else ""
        return [
            f"ATTACH {if_not_exists}'ducklake:postgres:dbname=${{pg_database}}' AS {alias} "
            f"(METADATA_SCHEMA '{store}'{suffix});"
        ]

    def seal(self, store):
        return self.server(f"{self.functions} SELECT pg_temp.ducklake_regression_seal('{store}')")

    def copy(self, source, target):
        return self.server(f"{self.functions} SELECT pg_temp.ducklake_regression_copy('{source}', '{target}')")

    def rollback(self, source, target):
        # The aborted mutation leaves dead tuples behind; the lake stays attached, so its catalog stays warm
        return ["ROLLBACK;"] + self.copy(source, target)


def store(value):
    if not STORE.fullmatch(value):
        raise ValueError(f"invalid metadata store name {value}")
    return value


def expand(catalog, operation, arguments):
    """Returns the SQL lines that perform one catalog operation"""
    if operation in ("create", "seal"):
        return getattr(catalog, operation)(store(arguments))
    if operation in ("copy", "rollback"):
        transfer = TRANSFER.fullmatch(arguments)
        if not transfer:
            raise ValueError(f"@{operation} expects SOURCE TO TARGET")
        return getattr(catalog, operation)(store(transfer.group(1)), store(transfer.group(2)))
    if operation == "attach":
        attach = ATTACH.fullmatch(arguments)
        if not attach:
            raise ValueError("@attach expects [IF NOT EXISTS] STORE AS ALIAS [(OPTIONS)]")
        if_not_exists, name, alias, options = attach.groups()
        return catalog.attach("IF NOT EXISTS " if if_not_exists else "", store(name), alias, options)
    raise ValueError(f"unknown catalog operation @{operation}")


def render(path, text, catalog, header):
    lines = catalog.header() if header else []
    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        try:
            if "'ducklake:" in stripped:
                raise ValueError("attach DuckLake catalogs with @attach, so every metadata catalog can render them")
            if not stripped.startswith("@"):
                lines.append(line)
                continue
            operation = OPERATION.fullmatch(stripped)
            if not operation:
                raise ValueError("expected @OPERATION ARGUMENTS")
            lines.extend(expand(catalog, *operation.groups()))
        except ValueError as error:
            raise ValueError(f"{path}:{number}: {error}") from None
    return "\n".join(lines) + "\n"


def benchmark_path(path):
    """Returns a path to render after checking that writing it cannot leave the benchmark directory of the root"""
    parts = PurePosixPath(path).parts
    if PurePosixPath(path).is_absolute() or ".." in parts or parts[:1] != ("benchmark",):
        raise ValueError(f"{path}: benchmarks and their includes and templates must be relative paths in benchmark/")
    return path


def check_verbatim_files(repository, path, text):
    """Rejects the SQL files that blocks read verbatim when they attach a DuckLake, since nothing renders them"""
    for reference in VERBATIM.findall(text):
        # A template parameter can stand for any part of the name, so check every file the reference could name
        pattern = os.path.join(repository, PLACEHOLDER.sub("*", reference).lower())
        for file in glob.glob(pattern):
            if os.path.isfile(file) and b"'ducklake:" in Path(file).read_bytes():
                name = os.path.relpath(file, repository)
                raise ValueError(f"{path}: {name} attaches a DuckLake directly; attach it with @attach instead")


def render_root(repository, root, benchmarks, catalog):
    """Creates a runner root holding the rendered benchmarks, the files they read and links to everything else"""
    root.mkdir(parents=True)
    for entry in repository.iterdir():
        if entry.name not in ("benchmark", BENCHMARK_DATA_DIRECTORY):
            (root / entry.name).symlink_to(entry, target_is_directory=entry.is_dir())
    for directory, _, files in os.walk(repository / "benchmark"):
        target = root / Path(directory).relative_to(repository)
        target.mkdir(exist_ok=True)
        for file in files:
            if not file.endswith((".benchmark", ".benchmark.in")):
                (target / file).symlink_to(Path(directory) / file)
    pending = list(benchmarks)
    rendered = set()
    while pending:
        path = benchmark_path(pending.pop())
        if path in rendered:
            continue
        rendered.add(path)
        text = (repository / path).read_text(encoding="utf-8")
        pending.extend(reference.lower() for reference in REFERENCE.findall(text))
        check_verbatim_files(repository, path, text)
        target = root / path
        # Other files are linked into the root; replace a link instead of writing through it into the repository
        if target.is_symlink():
            target.unlink()
        target.write_text(render(path, text, catalog, path in benchmarks), encoding="utf-8")


def create_catalog(name, database):
    return PostgresCatalog(database) if name == "postgres" else DuckDBCatalog()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", choices=("duckdb", "postgres"), default="duckdb")
    parser.add_argument("--postgres-database", default="ducklake_regression", help="default pg_database argument")
    parser.add_argument("root", type=Path, help="new directory to pass to the benchmark runner as --root-dir")
    parser.add_argument("benchmarks", nargs="+", help="benchmark files to render")
    arguments = parser.parse_args()
    catalog = create_catalog(arguments.catalog, arguments.postgres_database)
    render_root(Path(__file__).resolve().parents[2], arguments.root, arguments.benchmarks, catalog)


if __name__ == "__main__":
    main()
