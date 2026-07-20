#!/usr/bin/env python3

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--scale", default="xlarge")
    args = parser.parse_args()

    groups = defaultdict(list)
    for path in args.inputs:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if row["scale"] == args.scale:
                    groups[(row["adapter"], row["operation"])].append(row)

    print("operation\tbaseline_s\tfast_s\tlatency_reduction\tspeedup")
    operations = sorted({operation for _, operation in groups})
    for operation in operations:
        baseline = groups.get(("postgres", operation), [])
        fast = groups.get(("postgres_fast", operation), [])
        if len(baseline) < 3 or len(fast) < 3:
            continue
        baseline_s = statistics.median(row["elapsed_seconds"] for row in baseline)
        fast_s = statistics.median(row["elapsed_seconds"] for row in fast)
        latency_reduction = 100 * (baseline_s - fast_s) / baseline_s
        speedup = baseline_s / fast_s
        print(
            f"{operation}\t{baseline_s:.6f}\t{fast_s:.6f}\t{latency_reduction:.2f}%\t{speedup:.2f}x"
        )


if __name__ == "__main__":
    main()
