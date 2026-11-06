import io
import os
import random
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import compare
import run
from benchmark import BENCHMARK_NOT_FOUND_MESSAGE

test_runner = compare.test_runner


class FakeRunner:
    """Times a benchmark within 2% of a fixed duration, with noise that only depends on the seed"""

    def __init__(self, label, seconds, seed, failure=None):
        self.label = label
        self.seconds = seconds
        self.random = random.Random(seed)
        self.failure = failure
        self.calls = []

    def run(self, benchmark, timed_runs):
        self.calls.append(timed_runs)
        if self.failure:
            return None, self.failure
        return [self.seconds * self.random.uniform(0.98, 1.02) for _ in range(timed_runs)], None


def compare_benchmark(function, change, early_stop=True, old_failure=None, new_failure=None):
    """Compares a PR that takes change times as long as the base, and returns the result, output and runs"""
    old = FakeRunner("Base", 0.2, 1, old_failure)
    new = FakeRunner("PR", 0.2 * change, 2, new_failure)
    with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
        result = function(old, new, "benchmark/example.benchmark", early_stop, True)
    return result, stdout.getvalue(), (old.calls, new.calls)


class AdaptiveBenchmarkTest(unittest.TestCase):
    def test_every_change_ends_after_the_initial_pairs(self):
        cases = {
            "faster": (0.8, test_runner.BUCKET_FASTER, "faster"),
            "near faster": (0.95, test_runner.BUCKET_FASTER, "faster"),
            "unchanged": (1.0, test_runner.BUCKET_UNCHANGED, r"within ±3%; no change"),
            "slower": (1.05, test_runner.BUCKET_SLOWER, "slower"),
            "regression": (1.12, test_runner.BUCKET_REGRESSION, "regression"),
        }
        for name, (change, bucket, status) in cases.items():
            with self.subTest(name, change=change):
                result, output, runs = compare_benchmark(compare.run_adaptive_benchmark, change)
                self.assertEqual((result.initial_runs, result.confirmation_runs), (test_runner.INITIAL_RUNS, 0))
                self.assertEqual(test_runner.classify_result(result), bucket)
                self.assertEqual(runs, ([5, 5], [5, 5]))
                self.assertRegex(
                    output, r"^samples: benchmark/example\.benchmark: 10 pairs \| .* \| " + status + "$"
                )

    def test_duckdb_confirms_faster_changes(self):
        result, _, _ = compare_benchmark(compare.DUCKDB_ADAPTIVE_BENCHMARK, 0.8)
        self.assertEqual(result.confirmation_runs, test_runner.MIN_CONFIRMATION_RUNS)

    def test_failures_are_compared_as_by_duckdb(self):
        cases = {
            "failure": (dict(change=0.8, new_failure="PR crashed"), test_runner.BUCKET_FAILURE),
            "missing base": (dict(change=0.8, old_failure=BENCHMARK_NOT_FOUND_MESSAGE), None),
        }
        for name, (arguments, bucket) in cases.items():
            for early_stop in (False, True):
                with self.subTest(name, early_stop=early_stop):
                    expected = compare_benchmark(compare.DUCKDB_ADAPTIVE_BENCHMARK, early_stop=early_stop, **arguments)
                    actual = compare_benchmark(compare.run_adaptive_benchmark, early_stop=early_stop, **arguments)
                    self.assertEqual(actual, expected)
                    if bucket:
                        self.assertEqual(test_runner.classify_result(actual[0]), bucket)


def fake_runner(path, seconds):
    """Writes a benchmark runner program that reports fixed timings for each benchmark"""
    path.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        f"seconds = {seconds!r}[sys.argv[1]]\n"
        "runs = int(sys.argv[sys.argv.index('--timed-runs') + 1])\n"
        "print('name\\trun\\ttiming', file=sys.stderr)\n"
        "for run in range(runs):\n"
        "    print(f'{sys.argv[1]}\\t{run}\\t{seconds}', file=sys.stderr)\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return str(path)


class RegressionJobTest(unittest.TestCase):
    def test_a_regression_on_the_initial_pairs_fails_without_confirmation_runs(self):
        changes = {
            "benchmark/faster.benchmark": 0.8,
            "benchmark/slower.benchmark": 1.05,
            "benchmark/regressed.benchmark": 1.12,
        }
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            benchmarks = directory / "micro.csv"
            benchmarks.write_text("\n".join(changes) + "\n", encoding="utf-8")
            old = fake_runner(directory / "old", {benchmark: 0.1 for benchmark in changes})
            new = fake_runner(directory / "new", {benchmark: 0.1 * change for benchmark, change in changes.items()})
            arguments = ["--benchmarks", str(benchmarks), "--old", old, "--new", new, "--early-stop"]
            render = mock.patch.object(run, "render_root", lambda repository, root, benchmarks, catalog: root.mkdir())
            with render, mock.patch.dict(os.environ, {"CI": "false", "GITHUB_STEP_SUMMARY": ""}):
                with mock.patch.object(run.sys, "stdout", new_callable=io.StringIO) as stdout:
                    self.assertEqual(run.main(arguments), 1)
        output = stdout.getvalue()
        self.assertIn("samples: benchmark/faster.benchmark: 10 pairs | median change -20.0% | faster\n", output)
        self.assertIn("samples: benchmark/slower.benchmark: 10 pairs | median change +5.0% | slower\n", output)
        self.assertIn("samples: benchmark/regressed.benchmark: 10 pairs | median change +12.0% | regression\n", output)
        self.assertNotIn("confirm:", output)
        self.assertNotIn("Rerunning", output)
        self.assertIn("::error::benchmark/regressed.benchmark (micro) is 10% or more slower", output)


if __name__ == "__main__":
    unittest.main()
