#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/seqout-sc-count" || exit 1

hash=$(md5sum pyproject.toml | cut -d' ' -f1)
saved=$(cat .deps-hash 2>/dev/null || echo "")
if [[ "$hash" == "$saved" ]]; then
  exit 0
fi

tmp=$(mktemp)
# Keep [project] and [project.optional-dependencies] (the "rds" extra --
# rpy2, pyreadr -- lives there); stop trimming at the next header after that
# (e.g. [tool.ruff], [dependency-groups]). A trim that stopped at
# [project.optional-dependencies] instead of past it silently deleted the
# rds extra before uv ever saw it, so every .rds study failed with an
# uncaught ImportError.
awk '
  /^\[/ { s = 1 }
  s && /^\[/ && !/^\[project\]/ && !/^\[project\.optional-dependencies\]/ { exit }
  { print }
' pyproject.toml > "$tmp" && mv "$tmp" pyproject.toml

export RPY2_CFFI_MODE=ABI

if ! command -v uv &>/dev/null; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

# --extra rds pulls pyreadr, which builds a C extension from source when no
# wheel matches this host's Python, and that build fails hard on hosts
# missing system headers (e.g. liblzma-dev). A failed `uv sync` never writes
# .deps-hash, so every relaunch re-attempts and re-fails the same build
# before processing anything. Fall back to a sync without it: pyreadr/rpy2
# import lazily in sc_enrich/deep/_process.py, so their absence only fails
# the specific .rds items that need them, not every item on the host.
if ! uv sync --extra rds; then
  echo "uv sync --extra rds failed -- falling back to uv sync without the rds extra." >&2
  echo "Only .rds items will fail on this host; everything else is unaffected." >&2
  uv sync
fi
echo "$hash" > .deps-hash
