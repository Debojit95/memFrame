#!/usr/bin/env python3
"""Single entry point for the memFrame test suite.

Runs tests grouped by database backend ("db by db") instead of launching a
separate pytest process per operation. The whole suite becomes a handful of
invocations:

    python tests/run_tests.py --backend duckdb            # ops + integration
    python tests/run_tests.py --backend duckdb,postgres   # pick backends
    python tests/run_tests.py --scope unit                # unit tests only
    python tests/run_tests.py --backend all --tox         # everything + tox

Exit code is non-zero if any test run fails.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env.test"

BACKENDS = ("duckdb", "postgres", "clickhouse")
SCOPES = ("unit", "ops", "integration")
UPLOAD_TYPES = ("csv", "parquet", "df")

PYTEST = [sys.executable, "-m", "pytest"]

ENV_PARAMS = {
    "duckdb": "DUCKDB_DB_PARAMS",
    "postgres": "POSTGRES_DB_PARAMS",
    "clickhouse": "CLICKHOUSE_DB_PARAMS",
}
ENV_UPLOAD = {
    "csv": "UPLOAD_CSV_FILEPATH",
    "parquet": "UPLOAD_PARQUET_FILEPATH",
}


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE lines from a dotenv file into os.environ.

    Existing environment variables take precedence; only missing keys are set.
    Values may be quoted with single or double quotes.
    """
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def parse_params(raw: str) -> dict:
    """Parse a JSON params string; empty means {}."""
    if not raw:
        return {}
    try:
        params = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--{raw=} is not valid JSON: {exc}")
    if not isinstance(params, dict):
        raise SystemExit("db params must decode to a JSON object")
    return params


def default_duckdb_params(label: str) -> dict:
    """A fresh temp duckdb file per backend run to avoid cross-run residue."""
    tmpdir = Path(os.getenv("COMMIT_CHECK_TMPDIR", tempfile.gettempdir()))
    return {"db_path": str(tmpdir / f"memframe-{label}-{uuid.uuid4().hex}.duckdb")}


def backend_params(backend: str, args, label: str) -> dict | None:
    cli = getattr(args, f"{backend}_params")
    env = os.getenv(ENV_PARAMS[backend])
    if cli or env:
        return parse_params(cli or env)
    if backend == "duckdb":
        return default_duckdb_params(label)
    return None


def schema_prefixed(params: dict, backend: str, prefix: str) -> dict:
    if prefix and backend != "duckdb":
        params = dict(params)
        params["schema_prefix"] = prefix
    return params


def upload_args(args, upload_type: str) -> list[str]:
    if upload_type == "df":
        return ["--upload-type", "df"]
    env_var = ENV_UPLOAD[upload_type]
    path = getattr(args, f"upload_{upload_type}", None) or os.getenv(env_var)
    if not path:
        if args.require_upload_file:
            raise SystemExit(f"--upload-{upload_type} (or {env_var}) is required for {upload_type} upload tests")
        return ["--upload-type", upload_type]
    return ["--upload-type", upload_type, "--filepath", str(path)]


def run(cmd: list[str], dry_run: bool) -> bool:
    print("==> " + " ".join(cmd))
    if dry_run:
        return True
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode == 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the memFrame test suite, grouped by database backend.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--backend",
        default="all",
        help="Comma-separated backends, or 'all'. One of: duckdb,postgres,clickhouse",
    )
    parser.add_argument(
        "--scope",
        default="all",
        help="Comma-separated test scopes, or 'all'. One of: unit,ops,integration",
    )
    parser.add_argument(
        "--upload-type",
        default="all",
        help="Comma-separated upload types to test, or 'all', or 'none'. One of: csv,parquet,df",
    )
    parser.add_argument(
        "--require-upload-file",
        action="store_true",
        help="Fail if an upload filepath is missing instead of skipping that upload test",
    )
    for b in BACKENDS:
        parser.add_argument(f"--{b}-params", help=f"JSON connection params for {b}")
    parser.add_argument("--upload-csv", help="Path to the CSV file used by upload tests")
    parser.add_argument("--upload-parquet", help="Path to the Parquet file used by upload tests")
    parser.add_argument("--schema-prefix", help="Schema prefix for postgres/clickhouse isolation")
    parser.add_argument("--tox", action="store_true", help="Also run tox across py310-py313")
    parser.add_argument("--tox-recreate", action="store_true", help="Recreate tox environments")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Pass -v to pytest (verbose output)",
    )
    parser.add_argument(
        "--save-to-file",
        action="store_true",
        help="Save expected-vs-actual PDF reports from integration tests",
    )
    parser.add_argument("--pytest-args", nargs=argparse.REMAINDER, help="Extra args passed to every pytest run")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them")
    return parser


def selected_backends(args) -> list[str]:
    raw = [b.strip().lower() for b in args.backend.split(",") if b.strip()]
    if "all" in raw:
        return list(BACKENDS)
    unknown = set(raw) - set(BACKENDS)
    if unknown:
        raise SystemExit(f"Unknown backend(s): {', '.join(sorted(unknown))}")
    return raw


def selected_scopes(args) -> list[str]:
    raw = [s.strip().lower() for s in args.scope.split(",") if s.strip()]
    if "all" in raw:
        return list(SCOPES)
    unknown = set(raw) - set(SCOPES)
    if unknown:
        raise SystemExit(f"Unknown scope(s): {', '.join(sorted(unknown))}")
    return raw


def selected_upload_types(args) -> list[str]:
    raw = [u.strip().lower() for u in args.upload_type.split(",") if u.strip()]
    if "none" in raw:
        return []
    if "all" in raw:
        return list(UPLOAD_TYPES)
    unknown = set(raw) - set(UPLOAD_TYPES)
    if unknown:
        raise SystemExit(f"Unknown upload type(s): {', '.join(sorted(unknown))}")
    return raw


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    load_env_file(ENV_FILE)

    if args.dry_run:
        print(f"(dry-run) root: {REPO_ROOT}")

    verbose = ["-v"] if args.verbose else []
    pytest_args = list(args.pytest_args or [])
    integration_args = ["--save-to-file"] if args.save_to_file else []
    scopes = selected_scopes(args)
    upload_types = selected_upload_types(args)
    failed: list[str] = []

    def check(label: str, cmd: list[str], run_args: list[str] | None = None):
        ok = run(cmd + verbose + (run_args or []) + pytest_args, args.dry_run)
        if not ok:
            failed.append(label)

    # Unit tests need no database; run once, not per backend.
    if "unit" in scopes:
        check("unit", PYTEST + ["tests/unit"])

    run_tox = args.tox and not args.dry_run

    backend_scopes = set(scopes) & {"ops", "integration"}
    if backend_scopes:
        for backend in selected_backends(args):
            label = f"{backend}"
            params = backend_params(backend, args, label)
            if params is None:
                print(f"==> Skipping {backend}: no params provided")
                continue

            def fresh_params() -> str:
                p = schema_prefixed(dict(params), backend, args.schema_prefix)
                if backend == "duckdb" and not (getattr(args, f"{backend}_params") or os.getenv(ENV_PARAMS[backend])):
                    p = default_duckdb_params(label)
                return json.dumps(p, separators=(",", ":"))

            targets = []
            if "ops" in scopes:
                targets.append("tests/integration/ops")
            if "integration" in scopes:
                targets.append("tests/integration")
            cmd = PYTEST + targets + ["--db-backend", backend, "--db-params", fresh_params()]
            check(f"ops+integration {backend}", cmd, integration_args)

            for upload_type in upload_types:
                filepath = getattr(args, f"upload_{upload_type}", None)
                if upload_type != "df":
                    filepath = filepath or os.getenv(ENV_UPLOAD[upload_type])
                if upload_type != "df" and not filepath:
                    if args.require_upload_file:
                        raise SystemExit(f"--upload-{upload_type} (or {ENV_UPLOAD[upload_type]}) is required")
                    print(f"==> Skipping upload {upload_type} {backend}: no source file")
                    continue
                cmd = (
                    PYTEST
                    + ["tests/integration/ops/test_upload.py"]
                    + upload_args(args, upload_type)
                    + ["--db-backend", backend, "--db-params", fresh_params()]
                )
                check(f"upload {upload_type} {backend}", cmd, integration_args)

    if run_tox:
        tox_args = ["uv", "run", "tox", "-p", "auto", "-e", "py310,py311,py312,py313"]
        if args.tox_recreate:
            tox_args.insert(3, "-r")
        check("tox py310 py311 py312 py313", tox_args)

    if failed:
        print("")
        print("Test runs failed:" if not args.dry_run else "Would run:")
        for f in failed:
            print(f"  - {f}")
        return 1 if not args.dry_run else 0
    print("")
    print("All test runs passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
