#!/usr/bin/env python3
"""CI entry point: run every DuckLake test under test/sql/ against a fresh quack sidecar.

Before any test runs the script installs `quack` from the `core_nightly` extension
repository (DuckLake's CI builds against duckdb v1.5.2, which has a matching nightly
quack). If the install fails the script exits non-zero so CI surfaces it. Pass
`--no-install` to skip when quack is already available (e.g. statically linked via
ENABLE_QUACK=1 in a dev build).

For each test file, this script:
  1. Spawns a sibling `duckdb -unsigned` process running `quack_serve(...)`.
  2. Waits for the sidecar to start listening on :19999.
  3. Invokes `test/unittest --test-config test/configs/quack.json <test>`.
  4. Kills the sidecar so the next test starts with a clean DatabaseInstance.

Why per-test sidecar lifecycle:
  - Hosting `quack_serve` out-of-process is what breaks the connection-id self-RPC
    deadlock that triggers when DuckLake-on-quack and quack_serve share a
    DatabaseInstance.
  - Tearing down the sidecar between tests gives every test a guaranteed-clean
    server-side catalog, removing the need to hand-maintain a DROP-list in
    quack.json's on_cleanup.

Exit codes:
  0 — all tests passed
  1 — at least one test failed / timed out / had an infra error
  2 — pre-flight failed (missing binary, port in use, install failure)

Usage:
    scripts/run_quack_tests.py [--filter GLOB] [--slow] [--timeout SECS]
                               [--list] [--build-dir DIR] [--port PORT]
                               [--no-install]

Examples:
    scripts/run_quack_tests.py
    scripts/run_quack_tests.py --filter 'table_changes/*'
    scripts/run_quack_tests.py --filter '*deletions*' --timeout 60
    scripts/run_quack_tests.py --slow
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BUILD_DIR = REPO_ROOT / "build" / "release"
DEFAULT_PORT = 19999
DEFAULT_TOKEN = "ducklake-test-token"
DEFAULT_TIMEOUT = 120  # seconds per test
SIDECAR_STARTUP_TIMEOUT = 5.0  # seconds to wait for :PORT to listen


@dataclass
class TestResult:
    path: Path
    status: str  # "passed", "failed", "timeout", "skipped", "infra"
    duration: float
    detail: str = ""


def discover_tests(sql_dir: Path, include_slow: bool) -> list[Path]:
    patterns = ["*.test"]
    if include_slow:
        patterns.append("*.test_slow")
    found: list[Path] = []
    for pat in patterns:
        found.extend(sql_dir.rglob(pat))
    return sorted(found)


def install_quack(duckdb_bin: Path) -> tuple[bool, str]:
    # Force-install so a stale local quack from a previous build doesn't shadow the nightly
    # the CI was meant to test against. We run it via the same duckdb binary the sidecar
    # uses so both processes resolve to the same extension directory.
    sql = "FORCE INSTALL quack FROM core_nightly; LOAD quack; SELECT 1;"
    try:
        proc = subprocess.run(
            [str(duckdb_bin), "-unsigned", "-c", sql],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as ex:
        return False, f"failed to invoke duckdb for quack install: {ex}"
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip()
        return False, f"install/load returned {proc.returncode}:\n{tail}"
    return True, ""


def load_skip_set(config_path: Path) -> set[str]:
    cfg = json.loads(config_path.read_text())
    skip: set[str] = set()
    for entry in cfg.get("skip_tests", []):
        for path in entry.get("paths", []):
            skip.add(path)
    return skip


def port_in_use(port: int) -> bool:
    # quack_serve listens on the IPv6 wildcard, so probe via getaddrinfo and try
    # each candidate address family. AF_INET-only checks miss the listener.
    for family, _, _, _, sockaddr in socket.getaddrinfo(
        "localhost", port, type=socket.SOCK_STREAM
    ):
        with socket.socket(family, socket.SOCK_STREAM) as s:
            s.settimeout(0.1)
            try:
                s.connect(sockaddr)
                return True
            except (ConnectionRefusedError, socket.timeout, OSError):
                continue
    return False


def wait_for_port(port: int, timeout: float, sidecar: subprocess.Popen) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if port_in_use(port):
            return True
        if sidecar.poll() is not None:
            return False
        time.sleep(0.05)
    return False


def start_sidecar(duckdb_bin: Path, port: int, token: str, log_path: Path,
                  local_extension_repo: Path) -> subprocess.Popen:
    sql = (
        # quack is built loadable (not statically linked); install the locally-built copy
        # from the build repository so the sidecar's LOAD quack resolves without a network
        # fetch or a pre-seeded ~/.duckdb.
        f"FORCE INSTALL quack FROM '{local_extension_repo}';\n"
        "LOAD httpfs;\n"
        "LOAD quack;\n"
        f"SELECT * FROM quack_serve('quack://localhost:{port}/', token := '{token}');\n"
        # Long-running query that yields no rows — keeps the duckdb process alive
        # while the unittest runs against the server.
        "SELECT count(*) FROM range(1000000000000) WHERE range < 0;\n"
    )
    # Pipe the SQL in as stdin via a separate write; duckdb processes it and stays
    # in the long-running query until we SIGTERM the process group.
    log_file = open(log_path, "wb")
    proc = subprocess.Popen(
        [str(duckdb_bin), "-unsigned"],
        stdin=subprocess.PIPE,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid if os.name == "posix" else None,
    )
    assert proc.stdin is not None
    try:
        proc.stdin.write(sql.encode())
        proc.stdin.flush()
        # Note: deliberately leave stdin OPEN — closing it makes the CLI exit
        # once the keep-alive query yields its (empty) result set.
    except (BrokenPipeError, OSError):
        pass
    return proc


def stop_sidecar(sidecar: subprocess.Popen) -> None:
    if sidecar.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(os.getpgid(sidecar.pid), signal.SIGTERM)
        else:
            sidecar.terminate()
    except (ProcessLookupError, PermissionError):
        pass
    try:
        sidecar.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            if os.name == "posix":
                os.killpg(os.getpgid(sidecar.pid), signal.SIGKILL)
            else:
                sidecar.kill()
        except (ProcessLookupError, PermissionError):
            pass
        sidecar.wait()


def run_one_test(
    unittest_bin: Path,
    config_path: Path,
    test_path: Path,
    duckdb_bin: Path,
    port: int,
    token: str,
    timeout: float,
    local_extension_repo: Path,
) -> TestResult:
    rel = test_path.relative_to(REPO_ROOT)
    start = time.monotonic()

    if port_in_use(port):
        return TestResult(test_path, "infra", 0.0, f"port {port} already in use")

    sidecar_log = REPO_ROOT / "build" / "quack_sidecar.log"
    sidecar = start_sidecar(duckdb_bin, port, token, sidecar_log, local_extension_repo)
    try:
        if not wait_for_port(port, SIDECAR_STARTUP_TIMEOUT, sidecar):
            stop_sidecar(sidecar)
            log_tail = ""
            try:
                log_tail = sidecar_log.read_text(errors="replace").splitlines()[-10:]
                log_tail = "\n".join(log_tail)
            except OSError:
                pass
            return TestResult(
                test_path,
                "infra",
                time.monotonic() - start,
                f"sidecar failed to listen; log tail:\n{log_tail}",
            )

        try:
            proc = subprocess.run(
                [
                    str(unittest_bin),
                    "--test-config",
                    str(config_path),
                    str(rel),
                ],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as ex:
            parts = [f"hung > {timeout}s"]
            if ex.stdout:
                parts.append("--- stdout ---")
                parts.append((ex.stdout.decode(errors="replace") if isinstance(ex.stdout, bytes) else ex.stdout).rstrip())
            if ex.stderr:
                parts.append("--- stderr ---")
                parts.append((ex.stderr.decode(errors="replace") if isinstance(ex.stderr, bytes) else ex.stderr).rstrip())
            return TestResult(
                test_path,
                "timeout",
                time.monotonic() - start,
                "\n".join(parts),
            )

        duration = time.monotonic() - start
        if proc.returncode == 0:
            return TestResult(test_path, "passed", duration)
        # Capture full output so the user can see the actual failure.
        parts = []
        if proc.stdout:
            parts.append("--- stdout ---")
            parts.append(proc.stdout.rstrip())
        if proc.stderr:
            parts.append("--- stderr ---")
            parts.append(proc.stderr.rstrip())
        parts.append(f"--- exit code: {proc.returncode} ---")
        return TestResult(test_path, "failed", duration, "\n".join(parts))
    finally:
        stop_sidecar(sidecar)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("tests", nargs="*", help="specific test file(s) to run (e.g. test/sql/add_files/add_file_partitioned.test)")
    p.add_argument("--filter", default=None, help="only run tests whose path matches this glob (e.g. 'table_changes/*')")
    p.add_argument("--slow", action="store_true", help="also include *.test_slow files")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help=f"per-test timeout in seconds (default {DEFAULT_TIMEOUT})")
    p.add_argument("--list", action="store_true", help="list discovered tests and exit (after applying filter and skip_tests)")
    p.add_argument("--build-dir", default=str(DEFAULT_BUILD_DIR), help="path to build dir containing test/unittest and duckdb")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help="TCP port for the quack sidecar")
    p.add_argument("--token", default=DEFAULT_TOKEN, help="auth token for quack_serve")
    p.add_argument("--no-skip-list", action="store_true", help="ignore skip_tests in quack.json (run them anyway)")
    p.add_argument("--stop-on-fail", action="store_true", help="stop after the first failing/timeout test")
    p.add_argument("--no-install", action="store_true", help="skip the pre-flight 'INSTALL quack FROM core_nightly' (use when quack is already statically linked)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    build_dir = Path(args.build_dir).resolve()
    unittest_bin = build_dir / "test" / "unittest"
    duckdb_bin = build_dir / "duckdb"
    local_extension_repo = build_dir / "repository"
    config_path = REPO_ROOT / "test" / "configs" / "quack.json"
    sql_dir = REPO_ROOT / "test" / "sql"

    for path in (unittest_bin, duckdb_bin, config_path, sql_dir):
        if not path.exists():
            print(f"error: missing {path}", file=sys.stderr)
            return 2

    # quack is built as a loadable extension, and the unittest installs/loads extensions into a
    # per-test temp dir (not ~/.duckdb) while quack.json disables autoloading — so on_init's
    # `LOAD quack` can't find it. Bake an explicit install of the locally-built quack (from the
    # build repository) into on_init so each test resolves it into its own extension dir.
    effective_config_path = build_dir / "quack_config_effective.json"
    cfg = json.loads(config_path.read_text())
    cfg["on_init"] = f"FORCE INSTALL quack FROM '{local_extension_repo}'; " + cfg["on_init"]
    effective_config_path.write_text(json.dumps(cfg))

    if not args.no_install:
        print("installing quack from core_nightly...", flush=True)
        ok, detail = install_quack(duckdb_bin)
        if not ok:
            print(f"error: {detail}", file=sys.stderr)
            return 2
        print("  ok", flush=True)

    skip_set = set() if args.no_skip_list else load_skip_set(config_path)

    selected: list[Path] = []
    skipped_by_config = 0

    if args.tests:
        for raw in args.tests:
            p = Path(raw)
            if not p.is_absolute():
                p = (REPO_ROOT / p).resolve()
            else:
                p = p.resolve()
            if not p.exists():
                print(f"error: test not found: {raw}", file=sys.stderr)
                return 2
            try:
                p.relative_to(REPO_ROOT)
            except ValueError:
                print(f"error: test path is outside repo: {raw}", file=sys.stderr)
                return 2
            selected.append(p)
    else:
        all_tests = discover_tests(sql_dir, include_slow=args.slow)
        for t in all_tests:
            rel = str(t.relative_to(REPO_ROOT))
            if rel in skip_set:
                skipped_by_config += 1
                continue
            if args.filter:
                rel_under_sql = str(t.relative_to(sql_dir))
                if not (fnmatch.fnmatch(rel_under_sql, args.filter) or fnmatch.fnmatch(rel, args.filter)):
                    continue
            selected.append(t)

    if args.list:
        for t in selected:
            print(t.relative_to(REPO_ROOT))
        print(f"# {len(selected)} test(s); {skipped_by_config} skipped via quack.json", file=sys.stderr)
        return 0

    if port_in_use(args.port):
        print(
            f"error: port {args.port} already in use — stop the existing process first",
            file=sys.stderr,
        )
        return 2

    print(f"running {len(selected)} test(s); {skipped_by_config} skipped via quack.json", flush=True)
    print(f"build dir: {build_dir}", flush=True)
    print("-" * 72, flush=True)

    results: list[TestResult] = []
    counts = {"passed": 0, "failed": 0, "timeout": 0, "infra": 0, "skipped": 0}
    overall_start = time.monotonic()

    for idx, test_path in enumerate(selected, 1):
        rel = test_path.relative_to(REPO_ROOT)
        prefix = f"[{idx:>4}/{len(selected)}]"
        sys.stdout.write(f"{prefix} {rel} ... ")
        sys.stdout.flush()

        result = run_one_test(
            unittest_bin=unittest_bin,
            config_path=effective_config_path,
            test_path=test_path,
            duckdb_bin=duckdb_bin,
            port=args.port,
            token=args.token,
            timeout=args.timeout,
            local_extension_repo=local_extension_repo,
        )
        results.append(result)
        counts[result.status] = counts.get(result.status, 0) + 1

        marker = {
            "passed": "ok",
            "failed": "FAIL",
            "timeout": "TIMEOUT",
            "infra": "INFRA",
            "skipped": "skip",
        }.get(result.status, result.status)
        print(f"{marker} ({result.duration:.2f}s)", flush=True)
        if result.status != "passed" and result.detail:
            print("=" * 72, flush=True)
            print(f"FAILURE: {rel}", flush=True)
            print("-" * 72, flush=True)
            print(result.detail, flush=True)
            print("=" * 72, flush=True)

        if args.stop_on_fail and result.status in {"failed", "timeout", "infra"}:
            print("--stop-on-fail set; aborting after first failure", flush=True)
            break

    overall = time.monotonic() - overall_start
    total = len(results)
    failed_results = [r for r in results if r.status in {"failed", "timeout", "infra"}]

    print("=" * 72, flush=True)
    if failed_results:
        print(f"Failed tests ({len(failed_results)}):", flush=True)
        for r in failed_results:
            rel = r.path.relative_to(REPO_ROOT)
            print("-" * 72, flush=True)
            print(f"[{r.status.upper()}] {rel} ({r.duration:.2f}s)", flush=True)
            if r.detail:
                for line in r.detail.splitlines():
                    print(f"  {line}", flush=True)
        print("=" * 72, flush=True)

    print(
        f"summary: {counts['passed']} passed, {counts['failed']} failed, "
        f"{counts['timeout']} timeout, {counts['infra']} infra-error "
        f"in {overall:.1f}s",
        flush=True,
    )
    print(f"passed: {counts['passed']}/{total}", flush=True)

    if failed_results:
        print("\nFailed test list:", flush=True)
        for r in failed_results:
            print(f"  [{r.status}] {r.path.relative_to(REPO_ROOT)}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        sys.exit(130)
