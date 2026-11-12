"""Runs DuckDB's benchmark comparison with an absolute regression floor and time-capped confirmation runs"""

import math
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "duckdb" / "scripts" / "regression"))
import test_runner  # noqa: E402
from comparison import CONFIRMATION_TARGET_SECONDS, MAX_CONFIRMATION_RUNS, SAMPLE_BATCH_SIZE  # noqa: E402

# Runner jitter alone must not fail CI, so a regression must also lose this much time
REGRESSION_FLOOR_SECONDS = 0.005
# Two batches already double the initial pairs, and thirty would take minutes on the slowest benchmarks
MIN_CONFIRMATION_RUNS = 2 * SAMPLE_BATCH_SIZE
# Many candidates share this budget, so even a PR that regresses everything cannot time the job out
CONFIRMATION_BUDGET_SECONDS = 1200.0
BELOW_FLOOR_STATUS = f"slower (below the {round(REGRESSION_FLOOR_SECONDS * 1000)} ms regression floor)"

# Read the originals eagerly, so a submodule bump that renames a hook fails the import instead of going unpatched
DUCKDB_MEASUREMENT_OUTCOME = test_runner.measurement_outcome
DUCKDB_MEASUREMENT_STATUS = test_runner.measurement_status
DUCKDB_CONFIRMATION_RUN_COUNT = test_runner.confirmation_run_count

_confirmation_seconds = 0.0


def measurement_outcome(measurement):
    """Confirms a regression only when it also loses at least the absolute floor"""
    outcome = DUCKDB_MEASUREMENT_OUTCOME(measurement)
    below_floor = measurement.new_timing - measurement.old_timing < REGRESSION_FLOOR_SECONDS
    if outcome == test_runner.OUTCOME_REGRESSION and below_floor:
        return test_runner.OUTCOME_NO_REGRESSION
    return outcome


def measurement_status(measurement, stopped_early=False):
    status = DUCKDB_MEASUREMENT_STATUS(measurement, stopped_early)
    if status == "regression" and measurement_outcome(measurement) != test_runner.OUTCOME_REGRESSION:
        return BELOW_FLOOR_STATUS
    return status


def confirmation_run_count(measurement):
    """Keeps DuckDB's confirmation time target, capped per benchmark and by the shared budget"""
    global _confirmation_seconds
    faster_side_median = min(measurement.old_timing, measurement.new_timing)
    estimated_batches = math.ceil(CONFIRMATION_TARGET_SECONDS / faster_side_median / SAMPLE_BATCH_SIZE)
    runs = max(MIN_CONFIRMATION_RUNS, min(MAX_CONFIRMATION_RUNS, estimated_batches * SAMPLE_BATCH_SIZE))
    pair_seconds = measurement.old_timing + measurement.new_timing
    remaining = CONFIRMATION_BUDGET_SECONDS - _confirmation_seconds
    affordable = math.floor(remaining / pair_seconds / SAMPLE_BATCH_SIZE) * SAMPLE_BATCH_SIZE
    runs = max(MIN_CONFIRMATION_RUNS, min(runs, affordable))
    _confirmation_seconds += runs * pair_seconds
    return runs


def apply():
    test_runner.measurement_outcome = measurement_outcome
    test_runner.measurement_status = measurement_status
    test_runner.confirmation_run_count = confirmation_run_count
    # The sampling description of the report reads the confirmation bounds from these globals
    test_runner.MIN_CONFIRMATION_RUNS = MIN_CONFIRMATION_RUNS


if __name__ == "__main__":
    apply()
    sys.exit(test_runner.main())
