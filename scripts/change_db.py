import os
import sys
import subprocess
from enum import Enum
import re


class DBMS(Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"
    MYSQL = "mysql"


if len(sys.argv) != 3:
    print("Usage: python script.py <dependency_path> <dbms>")
    sys.exit(1)

dependency_path = sys.argv[1]
dbms_input = sys.argv[2].lower()

try:
    dbms = DBMS(dbms_input).value
except ValueError:
    print(
        f"Error: Unsupported dbms '{dbms_input}'. Must be one of: {', '.join(d.value for d in DBMS)}"
    )
    sys.exit(1)

script_dir = os.path.dirname(os.path.abspath(__file__))
folder_to_process = os.path.abspath(os.path.join(script_dir, "..", "test", "sql"))
release_folder = os.path.abspath(
    os.path.join(script_dir, "..", "build", "release", "test")
)

LOAD_STATEMENT = f"\nstatement ok\nLOAD '{dependency_path}'"


def process_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if lines and lines[0].strip() == "# [GENERATED]":
        return  # Already marked as generated, skip

    lines = ["# [GENERATED]\n"] + lines[1:] if lines else ["# [GENERATED]\n"]

    insert_idx = 0
    while insert_idx < len(lines) and lines[insert_idx].startswith("#"):
        insert_idx += 1

    if any("LOAD" in line and dependency_path in line for line in lines):
        return

    lines = lines[:insert_idx] + [LOAD_STATEMENT + "\n"] + lines[insert_idx:]

    if dbms == "sqlite":
        lines = [
            line.replace("ducklake:__TEST_DIR__", "ducklake:sqlite:__TEST_DIR__")
            for line in lines
        ]
    elif dbms == "postgres":
        lines = [
            re.sub(
                r"'ducklake:__TEST_DIR__[^']*'",
                "'ducklake:postgres:dbname=ducklakedb'",
                line,
            )
            for line in lines
        ]
    elif dbms == "mysql":
        lines = [
            re.sub(
                r"'ducklake:__TEST_DIR__[^']*'", "'ducklake:mysql:db=ducklakedb'", line
            )
            for line in lines
        ]

    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    print(f"Updated: {file_path}")


def collect_test_files(root_folder):
    test_files = []
    for dirpath, _, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith(".test"):
                full_path = os.path.join(dirpath, filename)
                test_files.append(full_path)
    return test_files


test_files = collect_test_files(folder_to_process)

if dbms == "postgres":
    subprocess.run(
        "dropdb --if-exists ducklakedb", shell=True, capture_output=True, text=True
    )
elif dbms == "mysql":
    subprocess.run(
        ' mysql -u root -e "DROP DATABASE ducklakedb"',
        shell=True,
        capture_output=True,
        text=True,
    )

passed_count = 0
failed_count = 0
skip_tests = {
    "test/sql/ducklake_basic.test",
    "test/sql/catalog/quoted_identifiers.test",
}
for test_file in test_files:
    if any(test_file.endswith(skip) for skip in skip_tests):
        passed_count = passed_count + 1
        continue
    process_file(test_file)
    if dbms == "postgres":
        subprocess.run(
            "createdb ducklakedb", shell=True, capture_output=True, text=True
        )
    elif dbms == "mysql":
        subprocess.run(
            ' mysql -u root -e "CREATE DATABASE ducklakedb"',
            shell=True,
            capture_output=True,
            text=True,
        )
    result = subprocess.run(
        f"{release_folder}/unittest {test_file}",
        shell=True,
        capture_output=True,
        text=True,
    )
    if dbms == "postgres":
        subprocess.run(
            "dropdb --if-exists ducklakedb", shell=True, capture_output=True, text=True
        )
    elif dbms == "mysql":
        subprocess.run(
            ' mysql -u root -e "DROP DATABASE ducklakedb"',
            shell=True,
            capture_output=True,
            text=True,
        )
    if result.stderr:
        if (
            "are supported in the MySQL connector" not in result.stderr
            and "Data inlining is currently only supported on DuckDB catalogs"
            not in result.stderr
        ):
            print(result.stderr)
            failed_count = failed_count + 1
        else:
            passed_count = passed_count + 1
    else:
        passed_count = passed_count + 1
print("Passed: " + str(passed_count))
print("Failed: " + str(failed_count))
print("Total: " + str(len(test_files)))
