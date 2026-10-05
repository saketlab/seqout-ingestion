#!/usr/bin/env bash
# Same as recipes/install_deps.sh, plus two fixes scoped to this job:
# 1. drop the [binary] extra from psycopg -- uv can't find a psycopg-binary
#    wheel for this host's Python (3.14t), and this job never touches
#    Postgres, so the pure-Python fallback is fine here.
# 2. keep [project.optional-dependencies] when trimming pyproject.toml, and
#    sync the "rds" extra (rpy2, pyreadr) -- the original trim stops at the
#    first non-[project] header, which is [project.optional-dependencies],
#    silently deleting the rds extra before uv ever sees it. Every .rds
#    study was failing with an uncaught ImportError as a result.
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
  s && /^\[/ && !/^\[project\]/ && !/^\[project\.optional-dependencies\]/ { exit }
  { print }
' pyproject.toml > "$tmp" && mv "$tmp" pyproject.toml

sed -i 's/psycopg\[binary\]/psycopg/' pyproject.toml

export RPY2_CFFI_MODE=ABI

if ! command -v uv &>/dev/null; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

# --extra rds pulls pyreadr, which builds a C extension (librdata) from
# source when no wheel matches this host's Python (e.g. the 3.14 free-
# threaded build) -- and that build fails hard on hosts missing liblzma-dev
# headers. A failed `uv sync` here never writes .deps-hash, so every future
# relaunch re-attempts and re-fails the same multi-minute build before ever
# reaching item 1 -- the whole per-relaunch time budget spent on a dependency
# that only .rds items need. Fall back to a sync without it: pyreadr/rpy2
# import lazily now (_process.py's _pyreadr()/_ro()), so their absence only
# fails the specific .rds items that need them, not every item on the host.
if ! uv sync --extra rds; then
  echo "uv sync --extra rds failed (pyreadr build likely missing system libs;" >&2
  echo "see uv's error above) -- falling back to uv sync without the rds extra." >&2
  echo "Only .rds items will fail on this host; everything else is unaffected." >&2
  uv sync
fi
echo "$hash" > .deps-hash
