import tempfile
import unittest
from pathlib import Path

import report


def lcov(files):
    """Renders an LCOV report with the given hit count per line, per source file"""
    records = []
    for source, lines in files.items():
        records.append(f"SF:{source}")
        records += [f"DA:{line},{hits}" for line, hits in lines.items()]
        records.append("end_of_record")
    return "\n".join(records) + "\n"


class ReadLcovTest(unittest.TestCase):
    def read(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lcov.info"
            path.write_text(text)
            return report.read_lcov(path)

    def test_sums_repeated_records(self):
        text = "SF:src/a.cpp\nDA:1,0\nDA:2,3,abc\nend_of_record\nSF:src/a.cpp\nDA:1,2\nend_of_record\n"
        self.assertEqual(self.read(text), {"src/a.cpp": {1: 2, 2: 3}})

    def test_paths_are_relative_to_the_checkout(self):
        text = lcov({"/home/runner/work/ducklake/ducklake/src/storage/a.cpp": {1: 1}})
        self.assertEqual(self.read(text), {"src/storage/a.cpp": {1: 1}})


class RangesTest(unittest.TestCase):
    def test_groups_consecutive_lines(self):
        self.assertEqual(report.ranges({7, 1, 2, 3, 5}), [[1, 3], [5, 5], [7, 7]])


class CompareTest(unittest.TestCase):
    def test_a_drop_beyond_the_threshold_regresses(self):
        baseline = {"src/a.cpp": {1: 1, 2: 1, 3: 1, 4: 1}}
        current = {"src/a.cpp": {1: 1, 2: 1, 3: 1, 4: 0}}
        summary, missed, regressed = report.compare(current, baseline, {"src/a.cpp": {4, 9}}, 0.1)
        self.assertTrue(regressed)
        self.assertEqual(missed, {"src/a.cpp": {4}})
        self.assertIn("| This change | 3 | 4 | 75.00% |", summary)
        self.assertIn("| Baseline | 4 | 4 | 100.00% |", summary)
        self.assertIn("| `src/a.cpp` | 0 | 1 |", summary)
        self.assertIn("- `src/a.cpp`: 4", summary)

    def test_a_drop_within_the_threshold_passes(self):
        baseline = {"src/a.cpp": {line: 1 for line in range(1000)}}
        current = {"src/a.cpp": {**baseline["src/a.cpp"], 0: 0}}
        self.assertFalse(report.compare(current, baseline, {}, 0.1)[2])

    def test_without_a_baseline_nothing_regresses(self):
        summary, _, regressed = report.compare({"src/a.cpp": {1: 0}}, None, {}, 0.1)
        self.assertFalse(regressed)
        self.assertIn("no baseline report", summary)


if __name__ == "__main__":
    unittest.main()
