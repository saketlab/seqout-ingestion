#!/usr/bin/env bash
# Cache purge is required because main.py caches growth after the matview rebuild.
set -euo pipefail

DIR="$(dirname "$0")"
bash "$DIR/create_database_volume_growth_matview.sh"

API_BASE="${SEQOUT_API_BASE:-https://seqout.org/api}"
if curl -fsS -X POST "$API_BASE/cache/purge" >/dev/null; then
  echo "Caches purged; volume growth is live."
else
  echo "WARNING: matview rebuilt but cache purge failed ($API_BASE/cache/purge)." >&2
  echo "Stale volume data may be served until the 7-day cache TTL expires." >&2
  exit 1
fi
