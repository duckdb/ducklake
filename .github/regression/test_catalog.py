import shutil
import tempfile
import unittest
from pathlib import Path

from catalog import DuckDBCatalog, PostgresCatalog, expand, render, render_root


class CatalogOperationTest(unittest.TestCase):
    def test_duckdb_operations(self):
        catalog = DuckDBCatalog()
        self.assertEqual(
            expand(catalog, "attach", "IF NOT EXISTS ${lake}_pristine AS seed (READ_ONLY)"),
            ["ATTACH IF NOT EXISTS 'ducklake:${BENCHMARK_DIR}/${lake}_pristine.db' AS seed (READ_ONLY);"],
        )
        self.assertEqual(
            expand(catalog, "attach", "${work} AS lake"), ["ATTACH 'ducklake:${BENCHMARK_DIR}/${work}.db' AS lake;"]
        )
        self.assertEqual(
            expand(catalog, "copy", "${lake}_pristine TO ${work}"),
            [
                "COPY (SELECT content FROM read_blob('${BENCHMARK_DIR}/${lake}_pristine.db'))",
                "TO '${BENCHMARK_DIR}/${work}.db' (FORMAT blob);",
                "COPY (SELECT ''::BLOB AS content) TO '${BENCHMARK_DIR}/${work}.db.wal' (FORMAT blob);",
            ],
        )
        self.assertIn("TO '${BENCHMARK_DIR}/warm.db' (FORMAT blob);", expand(catalog, "create", "warm"))
        self.assertEqual(expand(catalog, "seal", "warm"), [])
        self.assertEqual(expand(catalog, "rollback", "${lake}_pristine TO ${work}"), ["ROLLBACK;"])

    def test_postgres_operations(self):
        catalog = PostgresCatalog("bench")
        self.assertEqual(
            expand(catalog, "attach", "warm AS lake (DATA_PATH '${BENCHMARK_DIR}/warm_files', READ_ONLY)"),
            [
                "ATTACH 'ducklake:postgres:dbname=${pg_database}' AS lake "
                "(METADATA_SCHEMA 'warm', DATA_PATH '${BENCHMARK_DIR}/warm_files', READ_ONLY);"
            ],
        )
        self.assertEqual(
            expand(catalog, "create", "warm")[1],
            """CALL postgres_execute('pg_admin', 'DROP SCHEMA IF EXISTS "warm" CASCADE', prepare = false);""",
        )
        copy = expand(catalog, "copy", "${lake}_pristine TO ${work}")
        self.assertEqual(copy[0], "ATTACH 'dbname=${pg_database}' AS pg_admin (TYPE postgres);")
        self.assertEqual(copy[2], "DETACH pg_admin;")
        self.assertTrue(
            copy[1].endswith(
                "SELECT pg_temp.ducklake_regression_copy(''${lake}_pristine'', ''${work}'')', prepare = false);"
            )
        )
        self.assertNotIn("\n", copy[1])
        self.assertNotIn("--", copy[1])
        self.assertIn("pg_temp.ducklake_regression_seal(''warm'')", expand(catalog, "seal", "warm")[1])
        self.assertEqual(expand(catalog, "rollback", "${lake}_pristine TO ${work}"), ["ROLLBACK;"] + copy)
        self.assertEqual(catalog.header(), ["require postgres_scanner", "", "argument pg_database bench", ""])

    def test_postgres_database_survives_the_runner_lowercasing_it(self):
        for database in ("MixedCase", "two words", "quote'"):
            with self.subTest(database=database), self.assertRaisesRegex(ValueError, "invalid PostgreSQL database"):
                PostgresCatalog(database)

    def test_invalid_operations(self):
        catalog = DuckDBCatalog()
        for line in ("@restore lake", "@copy a b", "@attach a", "@create a'b", "@", "ATTACH 'ducklake:x.db' AS lake;"):
            with self.subTest(line=line), self.assertRaisesRegex(ValueError, r"^x\.benchmark:2: "):
                render("x.benchmark", "load\n" + line + "\n", catalog, False)


class RenderRootTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.repository = Path(directory.name) / "repository"
        (self.repository / "benchmark" / "queries").mkdir(parents=True)
        (self.repository / "shared").mkdir()
        self.root = Path(directory.name) / "root"

    def write(self, path, text):
        (self.repository / path).write_text(text)

    def render(self, *benchmarks):
        render_root(self.repository, self.root, list(benchmarks), PostgresCatalog("bench"))

    def test_rendering_never_writes_outside_the_root(self):
        source = "reload\n@attach seed AS lake\n"
        self.write("shared/outside.benchmark.in", source)
        self.write("benchmark/common.inc", source)
        for include in ("shared/outside.benchmark.in", "benchmark/../shared/outside.benchmark.in"):
            with self.subTest(include=include):
                self.write("benchmark/a.benchmark", f"include {include}\n")
                with self.assertRaisesRegex(ValueError, "relative paths in benchmark/"):
                    self.render("benchmark/a.benchmark")
                shutil.rmtree(self.root)
        with self.assertRaisesRegex(ValueError, "relative paths in benchmark/"):
            self.render(str(self.repository / "benchmark" / "a.benchmark"))
        shutil.rmtree(self.root)
        self.write("benchmark/a.benchmark", "include benchmark/common.inc\n")
        self.render("benchmark/a.benchmark")
        self.assertFalse((self.root / "benchmark" / "common.inc").is_symlink())
        self.assertIn("METADATA_SCHEMA 'seed'", (self.root / "benchmark" / "common.inc").read_text())
        self.assertEqual((self.repository / "shared" / "outside.benchmark.in").read_text(), source)
        self.assertEqual((self.repository / "benchmark" / "common.inc").read_text(), source)

    def test_sql_files_read_verbatim_cannot_attach_a_lake(self):
        self.write("benchmark/queries/q1.sql", "ATTACH 'ducklake:other.db' AS lake;\n")
        self.write("benchmark/queries/q2.sql", "SELECT 2;\n")
        self.write("benchmark/a.benchmark", "run benchmark/queries/q2.sql\n")
        self.render("benchmark/a.benchmark")
        for block in ("load benchmark/queries/q1.sql", "run benchmark/queries/q${QUERY}.sql"):
            with self.subTest(block=block):
                shutil.rmtree(self.root)
                self.write("benchmark/a.benchmark", f"{block}\n")
                with self.assertRaisesRegex(ValueError, "q1.sql attaches a DuckLake directly"):
                    self.render("benchmark/a.benchmark")

    def test_renders_requested_benchmarks_and_their_includes(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory) / "repository"
            (repository / "benchmark" / "queries").mkdir(parents=True)
            (repository / "duckdb_benchmark_data").mkdir()
            (repository / "data.csv").write_text("1\n")
            (repository / "benchmark" / "queries" / "q.sql").write_text("SELECT 1;\n")
            (repository / "benchmark" / "fixture.benchmark.in").write_text("reload\n@attach seed AS lake\n")
            (repository / "benchmark" / "template.benchmark.in").write_text("include benchmark/fixture.benchmark.in\n")
            (repository / "benchmark" / "a.benchmark").write_text("template benchmark/template.benchmark.in\n")
            (repository / "benchmark" / "unused.benchmark").write_text("load\nATTACH 'ducklake:x.db' AS lake;\n")

            root = Path(directory) / "root"
            render_root(repository, root, ["benchmark/a.benchmark"], PostgresCatalog("bench"))

            self.assertTrue((root / "data.csv").is_symlink())
            self.assertTrue((root / "benchmark" / "queries" / "q.sql").is_symlink())
            self.assertFalse((root / "duckdb_benchmark_data").exists())
            self.assertFalse((root / "benchmark" / "unused.benchmark").exists())
            self.assertTrue((root / "benchmark" / "a.benchmark").read_text().startswith("require postgres_scanner\n"))
            self.assertEqual(
                (root / "benchmark" / "fixture.benchmark.in").read_text(),
                "reload\nATTACH 'ducklake:postgres:dbname=${pg_database}' AS lake (METADATA_SCHEMA 'seed');\n",
            )


if __name__ == "__main__":
    unittest.main()
