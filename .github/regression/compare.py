"""Runs DuckDB's benchmark comparison with an absolute regression floor and a strict confirmation budget"""

import math
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "duckdb" / "scripts" / "regression"))
import test_runner  # noqa: E402
from comparison import CONFIRMATION_TARGET_SECONDS, MAX_CONFIRMATION_RUNS, SAMPLE_BATCH_SIZE  # noqa: E402

# Runner jitter alone must not fail CI, so a regression must also lose this much time
REGRESSION_FLOOR_SECONDS = 0.005
# Subtracting float medians can fall a few ULPs short of a floor they meet exactly
FLOOR_TOLERANCE_SECONDS = 1e-9
# Two batches already double the initial pairs, and thirty would take minutes on the slowest benchmarks
MIN_CONFIRMATION_RUNS = 2 * SAMPLE_BATCH_SIZE
# Candidates share this estimated budget; once it cannot cover a minimum confirmation, the initial pairs decide
CONFIRMATION_BUDGET_SECONDS = 1200.0
BELOW_FLOOR_STATUS = f"slower (below the {round(REGRESSION_FLOOR_SECONDS * 1000)} ms regression floor)"

# Read the originals eagerly, so a submodule bump that renames a hook fails the import instead of going unpatched
DUCKDB_MEASUREMENT_OUTCOME = test_runner.measurement_outcome
DUCKDB_MEASUREMENT_STATUS = test_runner.measurement_status
DUCKDB_CONFIRMATION_RUN_COUNT = test_runner.confirmation_run_count
DUCKDB_RUN_ADAPTIVE_BENCHMARK = test_runner.run_adaptive_benchmark
DUCKDB_PRINT_BENCHMARK_REPORT = test_runner.print_benchmark_report

_confirmation_seconds = 0.0


def measurement_outcome(measurement):
    """Confirms a regression only when it also loses at least the absolute floor"""
    outcome = DUCKDB_MEASUREMENT_OUTCOME(measurement)
    delta = measurement.new_timing - measurement.old_timing
    if outcome == test_runner.OUTCOME_REGRESSION and delta < REGRESSION_FLOOR_SECONDS - FLOOR_TOLERANCE_SECONDS:
        return test_runner.OUTCOME_NO_REGRESSION
    return outcome


def measurement_status(measurement, stopped_early=False):
    status = DUCKDB_MEASUREMENT_STATUS(measurement, stopped_early)
    if status == "regression" and measurement_outcome(measurement) != test_runner.OUTCOME_REGRESSION:
        return BELOW_FLOOR_STATUS
    return status


def confirmation_run_count(measurement):
    """Keeps DuckDB's confirmation time target within the shared budget, or returns zero when it is spent"""
    global _confirmation_seconds
    faster_side_median = min(measurement.old_timing, measurement.new_timing)
    estimated_batches = math.ceil(CONFIRMATION_TARGET_SECONDS / faster_side_median / SAMPLE_BATCH_SIZE)
    runs = max(MIN_CONFIRMATION_RUNS, min(MAX_CONFIRMATION_RUNS, estimated_batches * SAMPLE_BATCH_SIZE))
    pair_seconds = measurement.old_timing + measurement.new_timing
    remaining = CONFIRMATION_BUDGET_SECONDS - _confirmation_seconds
    affordable = math.floor(max(remaining, 0.0) / pair_seconds / SAMPLE_BATCH_SIZE) * SAMPLE_BATCH_SIZE
    runs = min(runs, affordable)
    if runs < MIN_CONFIRMATION_RUNS:
        return 0
    # Early stops make a confirmation cheaper than its reservation, which only leaves budget unspent
    _confirmation_seconds += runs * pair_seconds
    return runs


def benchmark_result(benchmark, initial_measurement, measurement, initial_runs, confirmation_runs):
    return test_runner.BenchmarkResult(
        benchmark=benchmark,
        initial_measurement=initial_measurement,
        measurement=measurement,
        initial_runs=initial_runs,
        confirmation_runs=confirmation_runs,
        outcome=measurement_outcome(measurement),
    )


def run_adaptive_benchmark(old_runner, new_runner, benchmark, early_stop, verbose):
    """DuckDB's adaptive comparison, deciding on the initial pairs once the confirmation budget is spent"""
    old_initial, new_initial, old_failure, new_failure, batch_index, _ = test_runner.run_paired_samples(
        old_runner, new_runner, benchmark, test_runner.INITIAL_RUNS, 0
    )
    initial_count = min(len(old_initial), len(new_initial))
    if old_failure or new_failure:
        if test_runner.benchmark_not_found(old_failure) and not new_failure:
            return test_runner.missing_base_benchmark_result(benchmark, old_failure, len(new_initial))
        return test_runner.failed_benchmark_result(benchmark, old_failure, new_failure, None, initial_count, 0)

    initial_measurement = test_runner.benchmark_measurement(old_initial, new_initial)
    change = test_runner.format_ratio_change(initial_measurement.ratio)
    if test_runner.within_noise(initial_measurement):
        if verbose:
            print(f"initial: {benchmark}: {initial_count} pairs | median change {change} | within ±3%; done", flush=True)
        return benchmark_result(benchmark, initial_measurement, initial_measurement, initial_count, 0)
    requested = confirmation_run_count(initial_measurement)
    if not requested:
        # The samples line carries the verdict that run.py reads, like a fixed-sample comparison
        if verbose:
            status = measurement_status(initial_measurement)
            print(f"samples: {benchmark}: {initial_count} pairs | median change {change} | {status}", flush=True)
        return benchmark_result(benchmark, initial_measurement, initial_measurement, initial_count, 0)
    if verbose:
        print(f"initial: {benchmark}: {initial_count} pairs | median change {change} | confirming with {requested} pairs", flush=True)

    old_confirmation, new_confirmation, old_failure, new_failure, _, stopped_early = test_runner.run_paired_samples(
        old_runner, new_runner, benchmark, requested, batch_index, early_stop
    )
    confirmation_count = min(len(old_confirmation), len(new_confirmation))
    if old_failure or new_failure:
        return test_runner.failed_benchmark_result(
            benchmark, old_failure, new_failure, initial_measurement, initial_count, confirmation_count
        )
    measurement = test_runner.benchmark_measurement(old_confirmation, new_confirmation)
    if verbose:
        change = test_runner.format_ratio_change(measurement.ratio)
        status = measurement_status(measurement, stopped_early)
        print(f"confirm: {benchmark}: {confirmation_count} pairs | median change {change} | {status}", flush=True)
    return benchmark_result(benchmark, initial_measurement, measurement, initial_count, confirmation_count)


def print_benchmark_report(*arguments, **keywords):
    """DuckDB's report, followed by the gate of this wrapper, which its legend does not know about"""
    DUCKDB_PRINT_BENCHMARK_REPORT(*arguments, **keywords)
    floor = round(REGRESSION_FLOOR_SECONDS * 1000)
    print("", flush=True)
    print(test_runner.gray(f"gate: a query regression fails CI at ≥ +10.0% and ≥ +{floor} ms; below the floor reports as slower"), flush=True)


def apply():
    test_runner.measurement_outcome = measurement_outcome
    test_runner.measurement_status = measurement_status
    test_runner.confirmation_run_count = confirmation_run_count
    test_runner.run_adaptive_benchmark = run_adaptive_benchmark
    test_runner.print_benchmark_report = print_benchmark_report
    # The sampling description of the report reads the confirmation bounds from these globals
    test_runner.MIN_CONFIRMATION_RUNS = MIN_CONFIRMATION_RUNS


if __name__ == "__main__":
    apply()
    sys.exit(test_runner.main())
