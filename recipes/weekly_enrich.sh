#!/usr/bin/env bash
# Enrich a weekly stage directory: publications, geocoding, unification,
# single-cell detection. Everything after ingest and before transfer.
#
# Produces a self-contained unified SQLite under the stage directory. It writes
# no Postgres and runs no transfer; the last line prints the transfer command to
# run once the output has been reviewed.
#
#   recipes/weekly_stage.sh        # ingest the window
#   recipes/weekly_enrich.sh       # this script
#   recipes/weekly_transfer.sh --apply
#
# Every stage is resumable: rerunning skips nothing, but each underlying command
# is idempotent, so a rerun after a failure costs time rather than correctness.
set -euo pipefail

STAGE_DIR=${STAGE_DIR:-/data/saket/cron_dump}
ONLY=${ONLY:-}                       # optional: run one stage, e.g. ONLY=sc_db

cd "$(dirname "$0")/.."
say()  { echo "[$(date -u +%FT%TZ)] $*"; }
want() { [ -z "$ONLY" ] || [ "$ONLY" = "$1" ]; }

# newest window per source, matching the justfile's own discovery
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

# A source with no staged window gets an EMPTY database built from its schema,
# never the justfile default. The default resolves to the production file under
# DATA_DIR, and pm-ids writes to the source DBs, so falling through would have
# this script modify production archives it was never given.
empty_db() {
    local src=$1 out=$STAGE_DIR/$src/db/empty/${src}_metadata_empty.db
    mkdir -p "$(dirname "$out")"
    [ -f "$out" ] || .venv/bin/python -c "
import sqlite3,sys,pathlib
sql=pathlib.Path('sql/tables/$src.sql').read_text()
c=sqlite3.connect('$out'); c.executescript(sql); c.commit(); c.close()"
    echo "$out"
}

for src in sra geo ena ae dra gea gsa gsa_human; do
    f=$(stage_db "$src") || true
    var=$(echo "$src" | tr '[:lower:]' '[:upper:]')_DB_FILE
    if [ -n "${f:-}" ]; then
        export "$var=$f"; say "$var=$f"
    else
        f=$(empty_db "$src")
        export "$var=$f"; say "$var=(no window this run, empty schema) $f"
    fi
done

# derived outputs live beside the staged sources, so production is untouched
export UNIFIED_CATALOG_DB_FILE=$STAGE_DIR/unified/unified_catalog.db
export UNIFIED_METADATA_DB_FILE=$STAGE_DIR/unified/unified_metadata.db
export PUBMED_METADATA_DB_FILE=$STAGE_DIR/pubmed/pubmed_metadata.db
export SC_EVIDENCE_DB_FILE=$STAGE_DIR/sc/single_cell_evidence.db
export SC_DB_DIR=$STAGE_DIR/sc
export SC_DB_FILE=$STAGE_DIR/sc/single_cell.db
mkdir -p "$STAGE_DIR"/{unified,pubmed,sc}

# 1. publications. pm-ids writes pmids back into the staged source DBs;
#    pm-meta fetches the article records those pmids point at.
if want pmids; then
    # --batch-size is BOTH the pysradb call size and the commit interval, and a
    # worker holds one source's whole share when the share is under it: one
    # giant call, one commit at the end, nothing kept if the run dies. Keep it
    # well below the per-worker share so progress lands incrementally.
    say "backfill pm-ids"
    just backfill_pm_ids --batch-size 100
    say "backfill pm-meta"
    just backfill_pm_meta --output-dir "$STAGE_DIR/pubmed" --db-file "$PUBMED_METADATA_DB_FILE"
fi

# 2. unification. Both stages are CREATE TABLE IF NOT EXISTS + INSERT OR IGNORE,
#    so an existing catalogue gains the new accessions and keeps the old.
if want unify; then
    say "unify build-catalog"
    just unify_build_catalog --output-dir "$STAGE_DIR/unified" --db-file "$UNIFIED_CATALOG_DB_FILE"
    say "unify build-metadata"
    just unify_build_metadata --output-dir "$STAGE_DIR/unified" --db-file "$UNIFIED_METADATA_DB_FILE"
fi

# 3. geocoding fills unified_centers, which sra_studies.countries and the four
#    *_geocode tables read at transfer time. The cache is shared on purpose:
#    an institution resolved last week is not looked up again.
if want geocode; then
    say "geocode (missing only)"
    just geocode --mode missing
fi

# 4. single cell. classify reads study prose; sc_evidence derives one row per
#    (object, rule) from the archive tables; sc_db turns that into the work queue.
if want sc; then
    say "sc_enrich classify"
    just sc_enrich_classify
    say "sc_evidence build"
    just sc_evidence_build
    say "sc_evidence coverage"
    just sc_evidence_coverage
    say "sc_db build (postgres is read SELECT-only here)"
    just sc_db_build
    just sc_db_summary || true
fi

say "built, nothing transferred:"
for f in "$UNIFIED_CATALOG_DB_FILE" "$UNIFIED_METADATA_DB_FILE" \
         "$PUBMED_METADATA_DB_FILE" "$SC_EVIDENCE_DB_FILE" "$SC_DB_FILE"; do
    [ -f "$f" ] && say "  $(du -h "$f" | cut -f1)  $f"
done
cat <<EOF

Review the above, then transfer with:
    recipes/weekly_transfer.sh --apply
EOF
