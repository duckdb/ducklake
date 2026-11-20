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


def measurement(old_timing, new_timing):
    return test_runner.benchmark_measurement([old_timing] * 5, [new_timing] * 5)


class FreshBudgetTest(unittest.TestCase):
    """Starts every test with an unspent confirmation budget"""

    def setUp(self):
        patcher = mock.patch.object(compare, "_confirmation_seconds", 0.0)
        patcher.start()
        self.addCleanup(patcher.stop)


class FakeRunner:
    """Times a benchmark within 2% of a fixed duration, with noise that only depends on the seed"""

    def __init__(self, label, seconds, seed, failure=None):
        self.label = label
        self.seconds = seconds
        self.random = random.Random(seed)
        self.failure = failure

    def run(self, benchmark, timed_runs):
        if self.failure:
            return None, self.failure
        return [self.seconds * self.random.uniform(0.98, 1.02) for _ in range(timed_runs)], None


def compare_benchmark(change, seconds=0.2, old_failure=None, new_failure=None):
    """Compares a PR that takes change times as long as the base, and returns the result and output"""
    old = FakeRunner("Base", seconds, 1, old_failure)
    new = FakeRunner("PR", seconds * change, 2, new_failure)
    with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
        result = compare.run_adaptive_benchmark(old, new, "benchmark/example.benchmark", True, True)
    return result, stdout.getvalue()


class MeasurementOutcomeTest(unittest.TestCase):
    def test_regression_requires_ratio_and_floor(self):
        cases = {
            "both": (measurement(0.2, 0.24), test_runner.OUTCOME_REGRESSION),
            "just above the floor": (measurement(0.05, 0.056), test_runner.OUTCOME_REGRESSION),
            "exactly at the floor": (measurement(0.01, 0.015), test_runner.OUTCOME_REGRESSION),
            "at the floor despite rounding": (measurement(0.025, 0.03), test_runner.OUTCOME_REGRESSION),
            "at the floor from larger medians": (measurement(0.04, 0.045), test_runner.OUTCOME_REGRESSION),
            "below the floor": (measurement(0.03, 0.0345), test_runner.OUTCOME_NO_REGRESSION),
            "below the limit": (measurement(10.0, 10.5), test_runner.OUTCOME_NO_REGRESSION),
            "unchanged": (measurement(0.2, 0.2), test_runner.OUTCOME_NO_REGRESSION),
        }
        for name, (value, outcome) in cases.items():
            with self.subTest(name):
                self.assertEqual(compare.measurement_outcome(value), outcome)

    def test_status_reports_the_floor(self):
        cases = {
            "regression": (measurement(0.2, 0.24), "regression"),
            "below the floor": (measurement(0.03, 0.0345), compare.BELOW_FLOOR_STATUS),
            "slower": (measurement(0.2, 0.21), "slower"),
            "faster": (measurement(0.2, 0.18), "faster"),
        }
        for name, (value, status) in cases.items():
            with self.subTest(name):
                self.assertEqual(compare.measurement_status(value), status)
        self.assertNotRegex(compare.BELOW_FLOOR_STATUS, r"regression$")


class ConfirmationRunCountTest(FreshBudgetTest):
    def test_slow_benchmarks_confirm_on_fewer_pairs(self):
        cases = {
            "slow": (measurement(10.0, 11.5), 10),
            "medium": (measurement(0.2, 0.23), 15),
            "fast": (measurement(0.025, 0.03), 100),
        }
        for name, (value, count) in cases.items():
            with self.subTest(name):
                self.assertEqual(compare.confirmation_run_count(value), count)
                self.assertLessEqual(compare.confirmation_run_count(value), compare.DUCKDB_CONFIRMATION_RUN_COUNT(value))

    def test_spending_stops_at_the_budget_while_cheap_candidates_still_confirm(self):
        slow = measurement(30.0, 33.0)
        self.assertEqual(compare.confirmation_run_count(slow), 10)
        self.assertEqual(compare.confirmation_run_count(slow), 0)
        self.assertEqual(compare.confirmation_run_count(measurement(0.025, 0.03)), 100)
        self.assertLessEqual(compare._confirmation_seconds, compare.CONFIRMATION_BUDGET_SECONDS)

    def test_a_nearly_spent_budget_shrinks_confirmations_to_what_remains(self):
        compare._confirmation_seconds = compare.CONFIRMATION_BUDGET_SECONDS - 3.0
        self.assertEqual(compare.confirmation_run_count(measurement(0.025, 0.03)), 50)

    def test_a_denied_confirmation_reserves_nothing(self):
        self.assertEqual(compare.confirmation_run_count(measurement(200.0, 220.0)), 0)
        self.assertEqual(compare._confirmation_seconds, 0.0)


class AdaptiveBenchmarkTest(FreshBudgetTest):
    def test_unchanged_benchmarks_end_after_the_initial_pairs(self):
        result, output = compare_benchmark(1.0)
        self.assertEqual((result.initial_runs, result.confirmation_runs), (test_runner.INITIAL_RUNS, 0))
        self.assertEqual(test_runner.classify_result(result), test_runner.BUCKET_UNCHANGED)
        self.assertNotIn("confirm:", output)

    def test_a_confirmed_regression_fails_on_capped_pairs(self):
        result, output = compare_benchmark(1.12)
        self.assertLess(result.confirmation_runs, test_runner.MIN_CONFIRMATION_RUNS)
        self.assertGreaterEqual(result.confirmation_runs, compare.MIN_CONFIRMATION_RUNS)
        self.assertEqual(test_runner.classify_result(result), test_runner.BUCKET_REGRESSION)
        self.assertRegex(output, r"confirm: benchmark/example\.benchmark: \d+ pairs \| .* \| regression$")

    def test_a_regression_below_the_floor_is_only_slower(self):
        result, output = compare_benchmark(1.15, seconds=0.025)
        self.assertEqual(result.confirmation_runs, 100)
        self.assertEqual(test_runner.classify_result(result), test_runner.BUCKET_SLOWER)
        self.assertIn(compare.BELOW_FLOOR_STATUS, output)
        self.assertNotRegex(output, r"\| regression$")

    def test_a_spent_budget_decides_on_the_initial_pairs(self):
        compare._confirmation_seconds = compare.CONFIRMATION_BUDGET_SECONDS
        result, output = compare_benchmark(1.12)
        self.assertEqual((result.initial_runs, result.confirmation_runs), (test_runner.INITIAL_RUNS, 0))
        self.assertEqual(test_runner.classify_result(result), test_runner.BUCKET_REGRESSION)
        self.assertRegex(output, r"samples: benchmark/example\.benchmark: 10 pairs \| .* \| regression$")
        self.assertNotIn("confirm:", output)

    def test_failures_keep_duckdb_handling(self):
        result, _ = compare_benchmark(1.0, new_failure="PR crashed")
        self.assertEqual(result.outcome, test_runner.OUTCOME_FAILURE)
        self.assertEqual(test_runner.classify_result(result), test_runner.BUCKET_FAILURE)

    def test_missing_base_keeps_duckdb_handling(self):
        result, _ = compare_benchmark(1.0, old_failure=BENCHMARK_NOT_FOUND_MESSAGE)
        self.assertEqual(result.outcome, test_runner.OUTCOME_MISSING_BASE)


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
    def test_only_confirmed_regressions_beyond_the_floor_fail(self):
        changes = {
            "benchmark/faster.benchmark": (0.1, 0.8),
            "benchmark/slower.benchmark": (0.1, 1.05),
            "benchmark/regressed.benchmark": (0.1, 1.12),
            "benchmark/below_floor.benchmark": (0.04, 1.12),
        }
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            benchmarks = directory / "micro.csv"
            benchmarks.write_text("\n".join(changes) + "\n", encoding="utf-8")
            old = fake_runner(directory / "old", {benchmark: seconds for benchmark, (seconds, _) in changes.items()})
            new = fake_runner(
                directory / "new", {benchmark: seconds * change for benchmark, (seconds, change) in changes.items()}
            )
            arguments = ["--benchmarks", str(benchmarks), "--old", old, "--new", new, "--early-stop"]
            render = mock.patch.object(run, "render_root", lambda repository, root, benchmarks, catalog: root.mkdir())
            with render, mock.patch.dict(os.environ, {"CI": "false", "GITHUB_STEP_SUMMARY": ""}):
                with mock.patch.object(run.sys, "stdout", new_callable=io.StringIO) as stdout:
                    self.assertEqual(run.main(arguments), 1)
        output = stdout.getvalue()
        self.assertIn("confirm: benchmark/faster.benchmark: 40 pairs | median change -20.0% | faster\n", output)
        self.assertIn("confirm: benchmark/slower.benchmark: 30 pairs | median change +5.0% | slower\n", output)
        self.assertIn("confirm: benchmark/regressed.benchmark: 30 pairs | median change +12.0% | regression\n", output)
        self.assertIn(
            f"confirm: benchmark/below_floor.benchmark: 75 pairs | median change +12.0% | {compare.BELOW_FLOOR_STATUS}\n",
            output,
        )
        self.assertIn("::error::benchmark/regressed.benchmark (micro) is at least 10% and 5 ms slower", output)
        self.assertNotIn("::error::benchmark/below_floor.benchmark", output)
        self.assertNotIn("::error::benchmark/slower.benchmark", output)
        self.assertIn("below the floor reports as slower", output)


if __name__ == "__main__":
    unittest.main()
