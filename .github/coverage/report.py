"""Renders the coverage report of a `make codecov` build with DuckDB's report generator, and compares the line
coverage of a report with the one of a baseline report"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
FILTERED = "filtered.info"
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
# GitHub shows at most 10 warning annotations per step
MAX_ANNOTATIONS = 10


def render(arguments):
    """Merges the profiles into an LCOV and an HTML report, like `test/run --coverage-report` does in DuckDB"""
    sys.path.insert(0, str(REPOSITORY / "duckdb" / "scripts" / "ci"))
    import run_tests

    unittest = arguments.build_dir / "test" / "unittest"
    result = run_tests.generate_coverage_report(REPOSITORY, str(unittest), arguments.profiles, arguments.output)
    if result != 0:
        return result
    # The check compares the same lines as the HTML report, so it applies the lcovrc and the genhtml filters too
    command = ["lcov", "--quiet", "--config-file", str(REPOSITORY / ".github" / "workflows" / "lcovrc")]
    command += ["--filter", "region,brace", "--ignore-errors", "inconsistent,inconsistent"]
    command += ["--ignore-errors", "unsupported,unsupported", "--ignore-errors", "mismatch,mismatch"]
    command += [
        "--add-tracefile",
        str(arguments.output / "lcov.info"),
        "--output-file",
        str(arguments.output / FILTERED),
    ]
    return subprocess.run(command).returncode


def read_lcov(path):
    """Returns the hit count of every instrumented line, per source file of an LCOV report"""
    files = {}
    lines = None
    for record in Path(path).read_text().splitlines():
        if record.startswith("SF:"):
            # reports hold absolute paths, and the baseline may come from a checkout in another directory
            source = record[3:]
            if source.startswith("/") and "/src/" in source:
                source = source[source.rindex("/src/") + 1 :]
            lines = files.setdefault(source, {})
        elif record.startswith("DA:") and lines is not None:
            number, hits = record[3:].split(",")[:2]
            lines[int(number)] = lines.get(int(number), 0) + int(hits)
        elif record == "end_of_record":
            lines = None
    return files


def coverage(files):
    """Returns the covered and the instrumented line counts of a report"""
    covered = sum(1 for lines in files.values() for hits in lines.values() if hits > 0)
    return covered, sum(len(lines) for lines in files.values())


def percentage(covered, instrumented):
    return 100 * covered / instrumented if instrumented else 100.0


def changed_lines(base):
    """Returns the lines that the commits since `base` added or changed, per source file"""
    command = ["git", "diff", "--unified=0", f"{base}...HEAD", "--", "src"]
    diff = subprocess.run(command, cwd=REPOSITORY, check=True, capture_output=True, text=True).stdout
    changes = {}
    path = None
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = line[len("+++ b/") :] if line.startswith("+++ b/") else None
        elif path is not None and HUNK.match(line):
            start, count = HUNK.match(line).groups()
            start, count = int(start), int(count or 1)
            changes.setdefault(path, set()).update(range(start, start + count))
    return changes


def ranges(lines):
    """Groups line numbers into [first, last] ranges of consecutive lines"""
    result = []
    for line in sorted(lines):
        if result and line == result[-1][1] + 1:
            result[-1][1] = line
        else:
            result.append([line, line])
    return result


def uncovered_lines(files):
    return {source: {line for line, hits in lines.items() if hits == 0} for source, lines in files.items()}


def compare(report, baseline, changes, threshold):
    """Returns the Markdown summary, the uncovered changed lines and whether the coverage regressed"""
    current = coverage(report)
    summary = [
        "## DuckLake code coverage",
        "",
        "| | Covered lines | Lines | Coverage |",
        "|---|---:|---:|---:|",
        f"| This change | {current[0]} | {current[1]} | {percentage(*current):.2f}% |",
    ]
    now = uncovered_lines(report)
    regressed = False
    if baseline is None:
        summary += ["", "There is no baseline report to compare with."]
    else:
        previous = coverage(baseline)
        difference = percentage(*current) - percentage(*previous)
        regressed = difference < -threshold
        summary += [
            f"| Baseline | {previous[0]} | {previous[1]} | {percentage(*previous):.2f}% |",
            "",
            f"The coverage changed by {difference:+.2f} percentage points, a drop of more than {threshold} fails.",
        ]
        before = uncovered_lines(baseline)
        grown = [(source, len(before.get(source, ())), len(lines)) for source, lines in sorted(now.items())]
        grown = [row for row in grown if row[2] > row[1]]
        if grown:
            summary += ["", "| File with more uncovered lines | Baseline | This change |", "|---|---:|---:|"]
            summary += [f"| `{source}` | {old} | {new} |" for source, old, new in grown]
    missed = {source: lines & now.get(source, set()) for source, lines in changes.items()}
    missed = {source: lines for source, lines in sorted(missed.items()) if lines}
    if missed:
        summary += ["", "Changed lines that no test covers:", ""]
        for source, lines in missed.items():
            spans = ", ".join(str(a) if a == b else f"{a}-{b}" for a, b in ranges(lines))
            summary.append(f"- `{source}`: {spans}")
    return "\n".join(summary) + "\n", missed, regressed


def check(arguments):
    report = read_lcov(arguments.report / FILTERED)
    baseline = arguments.baseline / FILTERED if arguments.baseline else None
    baseline = read_lcov(baseline) if baseline and baseline.is_file() else None
    changes = changed_lines(arguments.diff_base) if arguments.diff_base else {}
    summary, missed, regressed = compare(report, baseline, changes, arguments.threshold)
    if arguments.summary:
        with open(arguments.summary, "a") as output:
            output.write(summary)
    print(summary)
    spans = [(source, span) for source, lines in missed.items() for span in ranges(lines)]
    for source, (first, last) in spans[:MAX_ANNOTATIONS]:
        print(f"::warning file={source},line={first},endLine={last},title=Not covered::No test covers these lines")
    if regressed:
        print("::error title=Coverage regression::The line coverage dropped compared to the baseline")
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    html = subcommands.add_parser("render", help="merge the profiles of the test runs into an LCOV and an HTML report")
    html.add_argument("--build-dir", default=REPOSITORY / "build" / "codecov", type=Path)
    html.add_argument("--profiles", required=True, type=Path, help="directory with the .profraw files")
    html.add_argument("--output", required=True, type=Path, help="directory to write the report to")
    compared = subcommands.add_parser("check", help="fail when the line coverage dropped compared to a baseline")
    compared.add_argument("--report", required=True, type=Path, help="report directory of this change")
    compared.add_argument("--baseline", type=Path, help="report directory to compare with, if it exists")
    compared.add_argument("--diff-base", help="commit whose later changes to report when uncovered")
    compared.add_argument("--threshold", default=0.1, type=float, help="allowed drop, in percentage points")
    compared.add_argument("--summary", type=Path, help="file to append the Markdown summary to")
    arguments = parser.parse_args()
    if arguments.command == "render":
        return render(arguments)
    return check(arguments)


if __name__ == "__main__":
    sys.exit(main())
