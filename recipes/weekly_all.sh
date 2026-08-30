#!/usr/bin/env bash
# One entry point for the whole refresh. Stages differ in cadence and in blast
# radius, so the script runs the read-only half unattended and gates every
# Postgres write behind --apply.
#
#   recipes/weekly_all.sh            # ingest + enrich, writes no postgres
#   recipes/weekly_all.sh --apply    # the above, then transfer and embed
#
# Cadences, and why they differ:
#   geo ena ae dra gea   release-date window, overlapping, watermarked
#   gsa gsa_human        full sync every run: a study flips Control -> Open
#                        years after its release date, so a release window
#                        never selects it
#   sra                  NCBI publishes one Full dump a month, mid-month, so a
#                        weekly window usually contains none. Ingested only when
#                        a dump newer than the last one we hold appears.
set -euo pipefail

STAGE_DIR=${STAGE_DIR:-/data/saket/cron_dump}
APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

cd "$(dirname "$0")/.."
LOG_DIR=$STAGE_DIR/logs; mkdir -p "$LOG_DIR"
say() { echo "[$(date -u +%FT%TZ)] == $* =="; }

# newest Full dump we already hold, against what NCBI lists
sra_due() {
    local have listed
    have=$(ls -1 "$STAGE_DIR"/sra/data/NCBI_SRA_Metadata_Full_*.tar.gz 2>/dev/null \
        | sed -E 's/.*Full_([0-9]{8})\.tar\.gz/\1/' | sort -r | head -1)
    listed=$(.venv/bin/python - <<'PY' 2>/dev/null
import re, httpx
r = httpx.get("https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata",
              timeout=60, follow_redirects=True)
d = re.findall(r"NCBI_SRA_Metadata_Full_(\d{8})\.tar\.gz", r.text)
print(max(d) if d else "")
PY
)
    [ -n "$listed" ] || { echo "unknown"; return; }
    [ "$listed" != "$have" ] && echo "$listed" || echo ""
}

say "1/4 ingest"
bash recipes/weekly_stage.sh 2>&1 | tee -a "$LOG_DIR/all_stage.log" | tail -5

due=$(sra_due)
if [ "$due" = "unknown" ]; then
    say "sra: could not read the NCBI listing; skipped"
elif [ -n "$due" ]; then
    say "sra: Full_$due is newer than what we hold; ingesting"
    # the window must contain the dump's own date, not the refresh window
    .venv/bin/python ./src/main.py --log-level info ingest sra \
        --output-dir "$STAGE_DIR" \
        --start-date "${due:0:4}-${due:4:2}-${due:6:2}" --end-date "$(date -u +%F)" \
        --workers 8 --retries 10 --write-batch-size 50000 \
        >>"$LOG_DIR/all_sra.log" 2>&1 || say "sra FAILED, see $LOG_DIR/all_sra.log"
else
    say "sra: no dump newer than the one we hold; nothing to do"
fi

say "2/4 enrich (pmids, unify, geocode, single cell)"
STAGE_DIR="$STAGE_DIR" bash recipes/weekly_enrich.sh 2>&1 | tee -a "$LOG_DIR/all_enrich.log" | tail -8

if [ "$APPLY" != 1 ]; then
    cat <<EOF

Ingest and enrichment are done and nothing was written to Postgres.
Review $STAGE_DIR, then re-run with --apply to transfer and embed.
EOF
    exit 0
fi

say "3/4 transfer to postgres (insert-only), then refresh matviews"
STAGE_DIR="$STAGE_DIR" bash recipes/weekly_transfer.sh --apply 2>&1 | tee -a "$LOG_DIR/all_transfer.log" | tail -8

say "4/4 embed (similar-project search)"
just embed_run                 2>&1 | tail -3
just embed_coords              2>&1 | tail -3
just embed_cluster             2>&1 | tail -3
just embed_neighbors           2>&1 | tail -3
just transfer_embed            2>&1 | tail -3

say "done"
