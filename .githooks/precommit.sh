#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

echo "Running commit checks. To bypass this hook, commit with: git commit --no-verify" >&2

echo "==> ruff check src/"
uv run ruff check src/ || exit 1

scripts/run-commit-checks.sh
