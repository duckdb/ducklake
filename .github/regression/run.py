"""Runs DuckDB's regression runner and fails when any benchmark regresses in two runs in a row"""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

RUNNER = Path(__file__).resolve().parents[2] / "duckdb" / "scripts" / "regression" / "test_runner.py"
REGRESSED = re.compile(r"^(?:confirm|samples): (\S+): .*\| regression$", re.MULTILINE)
COLOR = re.compile(r"\x1b\[[0-9;]*m")


def run(passthrough, benchmarks):
    command = [sys.executable, str(RUNNER), *passthrough, "--verbose", "--nofail", "--benchmarks", benchmarks]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = []
    for line in process.stdout:
        print(line, end="", flush=True)
        output.append(line)
    code = process.wait()
    return code, set(REGRESSED.findall(COLOR.sub("", "".join(output))))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmarks", required=True)
    known, passthrough = parser.parse_known_args()

    code, regressed = run(passthrough, known.benchmarks)
    if code != 0 or not regressed:
        return code

    print("Rerunning the regressed benchmarks " + ", ".join(sorted(regressed)), flush=True)
    with tempfile.TemporaryDirectory() as directory:
        rerun = Path(directory) / "rerun.csv"
        rerun.write_text("\n".join(sorted(regressed)) + "\n", encoding="utf-8")
        code, again = run(passthrough, str(rerun))
    if code != 0:
        return code
    repeated = regressed & again
    for benchmark in sorted(repeated):
        print(f"::error::{benchmark} is 10% or more slower in two runs in a row", flush=True)
    return 1 if repeated else 0


if __name__ == "__main__":
    sys.exit(main())
