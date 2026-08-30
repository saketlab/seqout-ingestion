#!/usr/bin/env bash
# Load a weekly stage directory into Postgres, adding rows only.
#
# --apply ends by refreshing the matviews that read what it wrote. Without that
# the load lands and the site keeps serving the previous numbers, which looks
# exactly like the transfer never ran.
#
# The per-source *_DB_FILE variables are env overrides in the justfile, so
# pointing them at the stage directory reuses the normal recipes unchanged.
set -euo pipefail

STAGE_DIR=${STAGE_DIR:-/data/saket/cron_dump}
APPLY=${APPLY:-0}
[ "${1:-}" = "--apply" ] && APPLY=1

cd "$(dirname "$0")/.."
say() { echo "[$(date -u +%FT%TZ)] $*"; }

stage_db() {
    local src=$1 latest
    # date-named windows only: the "empty" stand-in sorts after any date and
    # would otherwise shadow a real ingest.
    latest=$(ls -1 "$STAGE_DIR/$src/db/" 2>/dev/null \
        | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' | sort -r | head -1) || return 0
    [ -n "$latest" ] || return 0
    local f="$STAGE_DIR/$src/db/$latest/${src}_metadata_$latest.db"
    [ -f "$f" ] && echo "$f"
}

for src in sra geo ena ae dra gea gsa gsa_human gsa_bioproject; do
    f=$(stage_db "$src") || true
    var=$(echo "$src" | tr '[:lower:]' '[:upper:]')_DB_FILE
    if [ -n "${f:-}" ]; then
        export "$var=$f"
        say "$var=$f ($(du -h "$f" | cut -f1))"
    else
        say "$var: no staged window, this source is skipped"
    fi
done

# built by weekly_enrich.sh; the transfer reads unified_metadata for countries,
# alias_all, publications and the geocode tables
export UNIFIED_CATALOG_DB_FILE=$STAGE_DIR/unified/unified_catalog.db
export UNIFIED_METADATA_DB_FILE=$STAGE_DIR/unified/unified_metadata.db
export PUBMED_METADATA_DB_FILE=$STAGE_DIR/pubmed/pubmed_metadata.db
for f in "$UNIFIED_METADATA_DB_FILE" "$PUBMED_METADATA_DB_FILE"; do
    [ -f "$f" ] || { say "missing $f; run recipes/weekly_enrich.sh first"; exit 1; }
done

if [ "$APPLY" != 1 ]; then
    say "dry run. Re-run with --apply to write to postgres."
    exit 0
fi

# the transfer itself. The batch modifiers run here, so organisms,
#    platforms, library_strategies, instrument_models, alias and bioproject_id
#    are computed for each new row rather than left NULL.
say "transfer postgres --insert-only"
just transfer_postgres --insert-only
say "transfer ddbj --insert-only"
just transfer_ddbj --insert-only

# organism facets for the sources that fill them from the loaded pg tables
for cmd in ena_organisms gea_organisms dra_organisms; do
    say "transfer $cmd"
    just "transfer_$cmd"
done
# no just recipe for this one
say "transfer gsa-organisms"
.venv/bin/python ./src/main.py transfer gsa-organisms --pg-url "$PG_URL"

say "refresh matviews"
.venv/bin/python recipes/refresh_matviews.py

say "done"
