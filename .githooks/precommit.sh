#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

echo "Running commit checks. To bypass this hook, commit with: git commit --no-verify" >&2

echo "==> ruff check src/"
uv run ruff check src/ || exit 1

echo "==> std tests (unit)"
uv run python tests/run_tests.py --scope unit
rc=$?
if [ $rc -ne 0 ]; then
  echo "==> Commit checks FAILED (rc=$rc)" >&2
fi
exit $rc
