#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/seqout-sc-count" || exit 1

hash=$(md5sum pyproject.toml | cut -d' ' -f1)
saved=$(cat .deps-hash 2>/dev/null || echo "")
if [[ "$hash" == "$saved" ]]; then
  exit 0
fi

tmp=$(mktemp)
awk '
  /^\[/ { s = 1 }
  s && /^\[/ && !/^\[project\]/ { exit }
  { print }
' pyproject.toml > "$tmp" && mv "$tmp" pyproject.toml

export RPY2_CFFI_MODE=ABI
uv sync
echo "$hash" > .deps-hash
