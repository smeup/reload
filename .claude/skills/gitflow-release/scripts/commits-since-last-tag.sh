#!/usr/bin/env bash
# Lists non-merge commits since the last release tag, for drafting a CHANGELOG entry.
set -euo pipefail

last_tag=$(git tag --sort=-v:refname | head -1)
if [ -z "$last_tag" ]; then
  echo "No tags found." >&2
  exit 1
fi

echo "Commits since $last_tag:" >&2
git log "${last_tag}..develop" --oneline --no-merges
