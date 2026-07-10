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

if ! command -v uv &>/dev/null; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

uv sync
echo "$hash" > .deps-hash
