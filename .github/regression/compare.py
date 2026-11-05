"""Runs DuckDB's benchmark comparison, deciding each benchmark on its initial pairs alone"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "duckdb" / "scripts" / "regression"))
import test_runner  # noqa: E402

DUCKDB_ADAPTIVE_BENCHMARK = test_runner.run_adaptive_benchmark


class RecordedRunner:
    """Runs a benchmark runner on new calls, and replays its earlier results in order after a rewind"""

    def __init__(self, runner):
        self.runner = runner
        self.label = runner.label
        self.results = []
        self.position = 0

    def run(self, benchmark, timed_runs):
        if self.position == len(self.results):
            self.results.append(self.runner.run(benchmark, timed_runs))
        self.position += 1
        return self.results[self.position - 1]

    def rewind(self):
        self.position = 0
        return self


def run_adaptive_benchmark(old_runner, new_runner, benchmark, early_stop, verbose):
    """Reports the initial pairs as the verdict, accepting false positives over confirmation runs"""
    old_runner, new_runner = RecordedRunner(old_runner), RecordedRunner(new_runner)
    old, new, old_failure, new_failure, _, _ = test_runner.run_paired_samples(
        old_runner, new_runner, benchmark, test_runner.INITIAL_RUNS, 0
    )
    if not old_failure and not new_failure:
        measurement = test_runner.benchmark_measurement(old, new)
        status = test_runner.measurement_status(measurement)
        if verbose:
            change = test_runner.format_ratio_change(measurement.ratio)
            print(f"samples: {benchmark}: {len(old)} pairs | median change {change} | {status}", flush=True)
        return test_runner.BenchmarkResult(
            benchmark=benchmark,
            initial_measurement=measurement,
            measurement=measurement,
            initial_runs=len(old),
            outcome=test_runner.measurement_outcome(measurement),
        )
    # a failed run keeps DuckDB's handling of missing and erroring benchmarks
    return DUCKDB_ADAPTIVE_BENCHMARK(old_runner.rewind(), new_runner.rewind(), benchmark, early_stop, verbose)


if __name__ == "__main__":
    test_runner.run_adaptive_benchmark = run_adaptive_benchmark
    sys.exit(test_runner.main())
