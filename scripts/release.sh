#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# Usage: scripts/release.sh <version> "<commit message>" [--dry-run]
#   version        e.g. 0.2.1 — must be newer than the current pyproject version.
#   commit message the message used for the release commit (passed to git commit).
#   --dry-run      bump version + run gates + build, but skip the git commit/tag/push.
#
# Version scheme:
#   0.X.0        feature addition (bump X, reset Y)
#   0.X.Y        bug fix only (bump Y)
#   0.X.Y (both) feature + bug fix in the same release
#
# First release: if <version> already matches pyproject.toml and no v* tags
# exist yet, the version bump is skipped (nothing to bump or commit) and the
# script goes straight to gates -> tag -> push.
#
# Publishing is NEVER done locally: this script bumps, gates, builds, commits,
# and pushes a v<version> tag. CI (release-testpypi.yml) publishes to TestPyPI;
# you promote to PyPI with the release-prod workflow_dispatch button.

new_version="${1:?usage: scripts/release.sh <version> \"<commit message>\" [--dry-run]}"
commit_message="${2:?usage: scripts/release.sh <version> \"<commit message>\" [--dry-run]}"
dry_run=false
if [[ "${3:-}" == "--dry-run" ]]; then
  dry_run=true
fi

cur_version="$(sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml | head -1)"
if [[ -z "$cur_version" ]]; then
  echo "Could not read current version from pyproject.toml" >&2
  exit 1
fi

if [[ ! "$new_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Version '$new_version' must be X.Y.Z" >&2
  exit 1
fi

tags_exist="$(git tag --list 'v*' | head -1)"
if [[ "$new_version" == "$cur_version" && -n "$tags_exist" ]]; then
  echo "Version '$new_version' already released (existing tag v$new_version) — refusing to republish." >&2
  exit 1
fi

skip_bump=false
if [[ "$new_version" == "$cur_version" ]]; then
  # First release bootstrap: version already matches, no tags yet.
  skip_bump=true
  echo "==> First release: version $cur_version already set in pyproject.toml (no bump needed)."
fi

# Abort if the working tree has changes other than pyproject.toml — a release
# should not sweep up unrelated work.
dirty="$(git status --porcelain | grep -v '^??' || true)"
if [[ -n "$dirty" ]]; then
  echo "Working tree is not clean — aborting. Uncommitted tracked changes:" >&2
  echo "$dirty" >&2
  echo "Commit or stash them first, then re-run." >&2
  exit 1
fi

if $skip_bump; then
  echo "==> Skipping version bump and release commit (bootstrap release)."
else
  echo "==> Bumping version: $cur_version -> $new_version"
  sed -i "s/^version = \"$cur_version\"/version = \"$new_version\"/" pyproject.toml
fi

echo "==> Gate: ruff check src/"
uv run ruff check src/

echo "==> Gate: full commit checks (unit + ops + integration + tox)"
echo "    postgres/clickhouse integration failures are non-blocking (CI publishes on tag push)"
INTEGRATION_MODE=warn bash scripts/run-commit-checks.sh

echo "==> Build + twine check"
rm -rf dist
uv build
uvx twine check dist/*

if $dry_run; then
  echo "(dry-run) Tagging and pushing skipped."
  if $skip_bump; then
    echo "(dry-run) Next: git tag v$new_version && git push origin main --tags"
  else
    echo "(dry-run) Next: git add pyproject.toml && git commit -m '$commit_message'"
    echo "(dry-run) Next: git tag v$new_version && git push origin main --tags"
  fi
  exit 0
fi

if ! $skip_bump; then
  git add pyproject.toml
  git commit -m "$commit_message"
fi
git tag "v$new_version"
git push origin main --tags

echo "==> Tag v$new_version pushed. CI will publish to TestPyPI."
echo "==> After verifying: pip install --index-url https://test.pypi.org/simple memframe"
echo "==> Promote to PyPI via the release-prod workflow."
