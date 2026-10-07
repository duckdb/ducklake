import io
import os
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import run


def fake_build(directory, extensions=("postgres_scanner",), versions=("v1",)):
    """Creates a release directory whose runner prints its extension directory and its arguments"""
    release = Path(directory).resolve() / "build" / "release"
    (release / "benchmark").mkdir(parents=True)
    for version in versions:
        platform = release / "repository" / version / "linux_amd64"
        platform.mkdir(parents=True)
        for extension in extensions:
            (platform / f"{extension}.duckdb_extension").touch()
    runner = release / "benchmark" / "benchmark_runner"
    runner.write_text('#!/bin/sh\necho "$DUCKDB_BENCHMARK_EXTENSION_DIRECTORY" "$@"\n')
    runner.chmod(0o755)
    return release, runner


def running(pid):
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return state != "" and not state.startswith("Z")


class PostgresRunnerTest(unittest.TestCase):
    def test_wrapper_selects_extensions_and_database(self):
        with tempfile.TemporaryDirectory() as directory:
            release, runner = fake_build(directory)
            wrapper = run.postgres_runner(Path(directory), str(runner), "bench_base")
            output = subprocess.run(
                [wrapper, "benchmark/x.benchmark", "--pg_database", "unowned"],
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertEqual(
                output.stdout.split(),
                [
                    str(release / "repository" / "v1" / "linux_amd64"),
                    "--pg_database",
                    "bench_base",
                    "benchmark/x.benchmark",
                    "--pg_database",
                    "unowned",
                ],
            )

    def test_build_without_postgres_scanner_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            _, runner = fake_build(directory, extensions=("ducklake",))
            with self.assertRaisesRegex(SystemExit, "built without postgres_scanner"):
                run.postgres_runner(Path(directory), str(runner), "bench_base")

    def test_several_extension_directories_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            _, runner = fake_build(directory, versions=("v1", "v2"))
            with self.assertRaisesRegex(SystemExit, "multiple extension directories"):
                run.postgres_runner(Path(directory), str(runner), "bench_base")

    def test_directory_without_execution_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(run.subprocess, "run", side_effect=PermissionError("noexec")):
                with self.assertRaisesRegex(SystemExit, "TMPDIR"):
                    run.check_executable(Path(directory))


class PostgresDatabaseTest(unittest.TestCase):
    def test_database_names_are_unique_and_fit_identifier_limit(self):
        for prefix in ("ducklake_regression", "x" * 100):
            with self.subTest(prefix=prefix):
                first = run.postgres_database_names(prefix)
                second = run.postgres_database_names(prefix)
                self.assertEqual(len(set(first + second)), 4)
                self.assertEqual(first[0].removesuffix("_base"), first[1].removesuffix("_pr"))
                for name in first + second:
                    self.assertLessEqual(len(name.encode("utf-8")), 63)
                    self.assertTrue(name.startswith(prefix[:25] + "_"))

    def test_database_prefix_must_be_safe_in_benchmark_arguments(self):
        for prefix in ("", "two words", "newline\n", "quote'", "é"):
            with self.subTest(prefix=prefix), self.assertRaises(SystemExit):
                run.postgres_database_names(prefix)

    def test_existing_database_is_not_adopted(self):
        result = subprocess.CompletedProcess([], 1, stderr='database "existing" already exists')
        with mock.patch.object(run.subprocess, "run", return_value=result), self.assertRaises(SystemExit):
            run.create_database("existing")

    def test_tools_never_prompt_for_a_password(self):
        result = subprocess.CompletedProcess([], 0, stderr="")
        with mock.patch.object(run.subprocess, "run", return_value=result) as tool:
            run.create_database("owned")
            run.drop_database("owned")
        self.assertEqual(
            [call.args[0] for call in tool.call_args_list],
            [["createdb", "--no-password", "--", "owned"], ["dropdb", "--no-password", "--force", "--", "owned"]],
        )

    def test_drop_failures_are_reported_without_raising(self):
        failure = subprocess.CompletedProcess([], 1, stderr="database is being accessed by other users")
        for outcome in (failure, FileNotFoundError("dropdb missing")):
            with self.subTest(outcome=outcome):
                with mock.patch.object(run.subprocess, "run", side_effect=[outcome]):
                    with mock.patch.object(run.sys, "stderr", new_callable=io.StringIO) as stderr:
                        run.drop_database("owned")
                self.assertIn("failed to drop the PostgreSQL database owned", stderr.getvalue())

    def test_undecodable_drop_errors_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            dropdb = Path(directory) / "dropdb"
            dropdb.write_bytes(b"#!/bin/sh\nprintf 'base de donn\\351es en cours' >&2\nexit 1\n")
            dropdb.chmod(0o755)
            with mock.patch.dict(os.environ, {"PATH": directory + os.pathsep + os.environ["PATH"]}):
                with mock.patch.object(run.sys, "stderr", new_callable=io.StringIO) as stderr:
                    run.drop_database("owned")
            self.assertIn("failed to drop the PostgreSQL database owned: base de donn", stderr.getvalue())


class ComparisonProcessTest(unittest.TestCase):
    def test_interrupt_stops_the_whole_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            comparison = Path(directory) / "comparison.py"
            comparison.write_text(
                "import os, subprocess, sys, time\n"
                "runner = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
                "print(os.getpid(), runner.pid, flush=True)\n"
                "time.sleep(60)\n"
            )
            lines = []

            def interrupted(line, **kwargs):
                lines.append(line)
                raise KeyboardInterrupt

            with mock.patch.object(run, "RUNNER", comparison), mock.patch("builtins.print", side_effect=interrupted):
                with self.assertRaises(KeyboardInterrupt):
                    run.run(directory, [], "micro.csv")
            pids = [int(pid) for pid in lines[0].split()]
            deadline = time.monotonic() + 10
            while any(running(pid) for pid in pids) and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertFalse([pid for pid in pids if running(pid)])


class RegressionRunTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        (self.directory / "micro.csv").write_text("benchmark/example.benchmark\n")
        for runner in ("old", "new"):
            (self.directory / runner).touch()
        self.real_drop_database = run.drop_database
        self.compare = mock.Mock(return_value=(0, set()))
        self.wrap = mock.Mock(side_effect=lambda directory, runner, database: str(directory / database))
        patcher = mock.patch.multiple(
            run,
            create_database=mock.DEFAULT,
            drop_database=mock.DEFAULT,
            check_executable=mock.DEFAULT,
            render_root=mock.DEFAULT,
            postgres_runner=self.wrap,
            run=self.compare,
        )
        self.mocks = patcher.start()
        self.addCleanup(patcher.stop)
        stdout = mock.patch.object(run.sys, "stdout", new_callable=io.StringIO)
        self.stdout = stdout.start()
        self.addCleanup(stdout.stop)

    def main(self, *arguments, catalog="postgres"):
        old, new = (str(self.directory / runner) for runner in ("old", "new"))
        benchmarks = str(self.directory / "micro.csv")
        return run.main(["--benchmarks", benchmarks, "--old", old, "--new", new, "--catalog", catalog, *arguments])

    def created(self):
        return [call.args[0] for call in self.mocks["create_database"].call_args_list]

    def dropped(self):
        return [call.args[0] for call in self.mocks["drop_database"].call_args_list]

    def test_independent_invocations_use_distinct_databases(self):
        self.assertEqual(self.main(), 0)
        self.assertEqual(self.main(), 0)
        self.assertEqual(len(set(self.created())), 4)
        self.assertEqual(sorted(self.dropped()), sorted(self.created()))

    def test_rendered_benchmarks_name_no_real_database(self):
        self.assertEqual(self.main(), 0)
        self.assertEqual(self.mocks["render_root"].call_args.args[3].database, run.UNSET_DATABASE)

    def test_partial_creation_failure_drops_only_owned_database(self):
        self.mocks["create_database"].side_effect = [None, SystemExit("creation failed")]
        with self.assertRaisesRegex(SystemExit, "creation failed"):
            self.main()
        self.assertEqual(self.dropped(), self.created()[:1])
        self.compare.assert_not_called()

    def test_missing_runner_is_reported_before_databases_exist(self):
        (self.directory / "old").unlink()
        with self.assertRaisesRegex(SystemExit, "Failed to find base runner"):
            self.main()
        self.mocks["create_database"].assert_not_called()

    def test_render_failure_is_reported_before_databases_exist(self):
        self.mocks["render_root"].side_effect = ValueError("benchmark/example.benchmark:3: bad operation")
        with self.assertRaisesRegex(SystemExit, "Failed to render the benchmarks: benchmark/example.benchmark:3"):
            self.main()
        self.mocks["create_database"].assert_not_called()

    def test_wrapper_failure_is_reported_before_databases_exist(self):
        self.wrap.side_effect = SystemExit("built without postgres_scanner")
        with self.assertRaisesRegex(SystemExit, "postgres_scanner"):
            self.main()
        self.mocks["create_database"].assert_not_called()

    def test_pg_database_benchmark_argument_is_rejected(self):
        with self.assertRaisesRegex(SystemExit, "selects the pg_database"):
            self.main("--benchmark-argument", "pg_database=shared")
        self.mocks["create_database"].assert_not_called()

    def test_other_arguments_reach_the_comparison(self):
        self.assertEqual(self.main("--threads", "2", "--benchmark-argument", "sf=1"), 0)
        self.assertEqual(self.compare.call_args.args[1][4:], ["--threads", "2", "--benchmark-argument", "sf=1"])

    def test_interrupt_drops_databases(self):
        self.compare.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.main()
        self.assertEqual(self.dropped(), self.created()[::-1])

    def test_sigterm_drops_databases_and_restores_the_handler(self):
        handler = signal.getsignal(signal.SIGTERM)
        self.compare.side_effect = lambda *arguments: os.kill(os.getpid(), signal.SIGTERM)
        with self.assertRaises(SystemExit) as raised:
            self.main()
        self.assertEqual(raised.exception.code, 128 + signal.SIGTERM)
        self.assertEqual(self.dropped(), self.created()[::-1])
        self.assertIs(signal.getsignal(signal.SIGTERM), handler)

    def test_databases_survive_until_confirmation_finishes(self):
        def compare(root, passthrough, benchmarks):
            self.mocks["drop_database"].assert_not_called()
            self.assertEqual(len(self.created()), 2)
            return 0, {"benchmark/example.benchmark"}

        self.compare.side_effect = compare
        self.assertEqual(self.main(), 1)
        self.assertEqual(self.compare.call_count, 2)
        self.assertEqual(self.compare.call_args_list[0].args[1], self.compare.call_args_list[1].args[1])
        self.assertEqual(self.dropped(), self.created()[::-1])
        self.assertIn(
            "::error::benchmark/example.benchmark (micro_postgres) is 10% or more slower in two runs in a row",
            self.stdout.getvalue(),
        )

    def test_cleanup_failure_preserves_result_and_attempts_both_drops(self):
        self.compare.return_value = (42, set())
        self.mocks["drop_database"].side_effect = self.real_drop_database
        with mock.patch.object(run.subprocess, "run", side_effect=OSError("drop failed")):
            with mock.patch.object(run.sys, "stderr", new_callable=io.StringIO) as stderr:
                self.assertEqual(self.main(), 42)
        self.assertEqual(self.dropped(), self.created()[::-1])
        self.assertEqual(stderr.getvalue().count("failed to drop the PostgreSQL database"), 2)

    def test_duckdb_does_not_create_postgres_databases(self):
        self.assertEqual(self.main(catalog="duckdb"), 0)
        self.mocks["create_database"].assert_not_called()
        self.mocks["drop_database"].assert_not_called()


if __name__ == "__main__":
    unittest.main()
