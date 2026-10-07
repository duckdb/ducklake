"""Runs DuckDB's regression runner and fails when any benchmark regresses in two runs in a row"""

import argparse
import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
from contextlib import ExitStack, suppress
from pathlib import Path
from uuid import uuid4

from catalog import create_catalog, render_root

REPOSITORY = Path(__file__).resolve().parents[2]
RUNNER = REPOSITORY / "duckdb" / "scripts" / "regression" / "test_runner.py"
REGRESSED = re.compile(r"^(?:confirm|samples): (\S+): .*\| regression$", re.MULTILINE)
COLOR = re.compile(r"\x1b\[[0-9;]*m")
# Rendered benchmarks name a database that never exists, so a runner without its wrapper fails instead
UNSET_DATABASE = "pg_database_not_set"

sys.path.append(str(RUNNER.parent))
from benchmark import EXTENSION_DIRECTORY_ENV, find_extension_directory  # noqa: E402


def run(root, passthrough, benchmarks):
    command = [sys.executable, str(RUNNER), *passthrough, "--verbose", "--nofail", "--benchmarks", str(benchmarks)]
    output = []
    # In a session of its own, the comparison can be stopped as a whole before its databases are dropped
    with subprocess.Popen(
        command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True
    ) as process:
        try:
            for line in process.stdout:
                print(line, end="", flush=True)
                output.append(line)
        except BaseException:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    return process.returncode, set(REGRESSED.findall(COLOR.sub("", "".join(output))))


def interrupt(signum, frame):
    raise SystemExit(128 + signum)


def create_database(name):
    try:
        process = subprocess.run(
            ["createdb", "--no-password", "--", name], capture_output=True, text=True, errors="replace", check=False
        )
    except OSError as error:
        raise SystemExit(f"--catalog postgres needs createdb from the PostgreSQL client tools: {error}") from None
    if process.returncode != 0:
        raise SystemExit(f"Failed to create the PostgreSQL database {name}: {process.stderr.strip()}")


def drop_database(name):
    # --force (PostgreSQL 13+) ends the sessions of a runner that was stopped in the middle of a statement
    command = ["dropdb", "--no-password", "--force", "--", name]
    try:
        process = subprocess.run(command, capture_output=True, text=True, errors="replace", check=False)
        if process.returncode == 0:
            return
        error = process.stderr.strip()
    except OSError as exception:
        error = str(exception)
    print(f"Warning: failed to drop the PostgreSQL database {name}: {error}", file=sys.stderr, flush=True)


def postgres_database_names(prefix):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", prefix):
        raise SystemExit("--postgres-database must contain only ASCII letters, digits, underscores or hyphens")
    # Keep the UUID and role suffix within PostgreSQL's 63-byte identifier limit.
    namespace = f"{prefix[:25]}_{uuid4().hex}"
    return f"{namespace}_base", f"{namespace}_pr"


def postgres_extensions(runner):
    """Returns the extension directory of a runner's build, which must hold postgres_scanner"""
    try:
        extensions = find_extension_directory(runner)
    except ValueError as error:
        raise SystemExit(str(error)) from None
    if not extensions or not os.path.isfile(os.path.join(extensions, "postgres_scanner.duckdb_extension")):
        raise SystemExit(f"{runner} was built without postgres_scanner; build it with ENABLE_POSTGRES_SCANNER=1")
    return extensions


def check_executable(directory):
    """Fails before any database exists when programs cannot run from a directory, as on a noexec mount"""
    probe = directory / "probe"
    probe.write_text("#!/bin/sh\n", encoding="utf-8")
    probe.chmod(0o755)
    try:
        subprocess.run([str(probe)], check=True)
    except (OSError, subprocess.CalledProcessError) as error:
        message = f"Cannot run the runner wrappers from {directory} ({error}); point TMPDIR elsewhere"
        raise SystemExit(message) from None


def postgres_runner(directory, runner, database):
    """Wraps a runner so that it loads its own build's extensions and keeps its metadata in its own database"""
    extensions = postgres_extensions(runner)
    wrapper = directory / database
    # The runner keeps the first value of a repeated argument, so later arguments cannot change the database
    wrapper.write_text(
        "#!/bin/sh\n"
        f"export {EXTENSION_DIRECTORY_ENV}={shlex.quote(extensions)}\n"
        f'exec {shlex.quote(runner)} --pg_database {shlex.quote(database)} "$@"\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return str(wrapper)


def postgres_runners(directory, cleanup, old, new, prefix):
    """Wraps both runners to use new databases of their own, which are dropped on cleanup"""
    databases = postgres_database_names(prefix)
    # test_runner.py looks for extensions two levels above each runner, where the wrappers have none
    runners = directory / "runners"
    runners.mkdir()
    check_executable(runners)
    wrappers = [postgres_runner(runners, runner, database) for runner, database in zip((old, new), databases)]
    print(f"PostgreSQL databases: {databases[0]} (base) and {databases[1]} (PR)", flush=True)
    for database in databases:
        create_database(database)
        cleanup.callback(drop_database, database)
    return wrappers


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmarks", required=True)
    parser.add_argument("--old", required=True)
    parser.add_argument("--new", required=True)
    parser.add_argument("--catalog", choices=("duckdb", "postgres"), default="duckdb")
    parser.add_argument(
        "--postgres-database",
        default="ducklake_regression",
        help="database prefix (ASCII letters, digits, _ or -; first 25 characters used before a unique suffix)",
    )
    parser.add_argument("--benchmark-argument", action="append", default=[], help="NAME=VALUE for both runners")
    known, passthrough = parser.parse_known_args(argv)
    for argument in known.benchmark_argument:
        if argument.split("=", 1)[0] == "pg_database":
            raise SystemExit("run.py selects the pg_database of each runner; do not pass it as a benchmark argument")
        passthrough += ["--benchmark-argument", argument]

    old, new = os.path.abspath(known.old), os.path.abspath(known.new)
    for label, runner in (("base", old), ("PR", new)):
        if not os.path.isfile(runner):
            raise SystemExit(f"Failed to find {label} runner {runner}")
    try:
        benchmarks = Path(known.benchmarks).read_text(encoding="utf-8").split()
    except OSError as error:
        raise SystemExit(f"Failed to read the benchmark list: {error}") from None
    suite = Path(known.benchmarks).stem
    if known.catalog != "duckdb":
        suite += "_" + known.catalog
    with tempfile.TemporaryDirectory(prefix="ducklake-regression-") as directory, ExitStack() as cleanup:
        # Unwind on SIGTERM and SIGHUP as on Ctrl-C, so the comparison is stopped and its databases are dropped
        for signum in (signal.SIGTERM, signal.SIGHUP):
            cleanup.callback(signal.signal, signum, signal.signal(signum, interrupt))
        directory = Path(directory)
        root = directory / "root"
        try:
            render_root(REPOSITORY, root, benchmarks, create_catalog(known.catalog, UNSET_DATABASE))
        except (OSError, ValueError) as error:
            raise SystemExit(f"Failed to render the benchmarks: {error}") from None
        if known.catalog == "postgres":
            old, new = postgres_runners(directory, cleanup, old, new, known.postgres_database)
        passthrough = ["--old", old, "--new", new, *passthrough]

        initial = directory / f"{suite}.csv"
        initial.write_text("\n".join(benchmarks) + "\n", encoding="utf-8")
        code, regressed = run(root, passthrough, initial)
        if code != 0 or not regressed:
            return code

        print("Rerunning the regressed benchmarks " + ", ".join(sorted(regressed)), flush=True)
        rerun = directory / f"{suite}_rerun.csv"
        rerun.write_text("\n".join(sorted(regressed)) + "\n", encoding="utf-8")
        code, again = run(root, passthrough, rerun)
    if code != 0:
        return code
    repeated = regressed & again
    for benchmark in sorted(repeated):
        print(f"::error::{benchmark} ({suite}) is 10% or more slower in two runs in a row", flush=True)
    return 1 if repeated else 0


if __name__ == "__main__":
    sys.exit(main())
