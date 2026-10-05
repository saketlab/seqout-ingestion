#!/usr/bin/env bash
# Resume the GSA (CRA) ingest until the study-page scrape is complete.
#
# CNCB drops most requests under load, so one pass never finishes. Each study page is
# cached to data/study_meta/, so a re-run resumes; loop until the cache stops growing.
set -uo pipefail

DATA_DIR="${DATA_DIR:-/NAS/saket/seqout-ddbj}"
WORKERS="${WORKERS:-6}"
RETRIES="${RETRIES:-20}"
STALL_ROUNDS="${STALL_ROUNDS:-3}"
PYTHON="${PYTHON:-.venv/bin/python}"

cd "$(dirname "$0")/.." || exit 1
export PYTHON_GIL=0

meta_dir="$DATA_DIR/gsa/data/study_meta"
today="$(date -u +%F)"
stalled=0

while :; do
    before=$(find "$meta_dir" -name '*.json' 2>/dev/null | wc -l)

    # today..today is the full-sync sentinel (see gsa_common.date_filter_active)
    "$PYTHON" ./src/main.py --log-level info ingest gsa \
        --output-dir "$DATA_DIR" \
        --start-date "$today" --end-date "$today" \
        --workers "$WORKERS" --retries "$RETRIES"
    rc=$?

    after=$(find "$meta_dir" -name '*.json' 2>/dev/null | wc -l)
    echo "[$(date -u +%T)] pass rc=$rc; study_meta cached: $before -> $after"

    if [ "$rc" -eq 0 ]; then
        echo "ingest completed cleanly"
        exit 0
    fi

    if [ "$after" -le "$before" ]; then
        stalled=$((stalled + 1))
        echo "no progress ($stalled/$STALL_ROUNDS)"
        [ "$stalled" -ge "$STALL_ROUNDS" ] && {
            echo "giving up: $STALL_ROUNDS passes with no new pages"
            exit 1
        }
        sleep 300
    else
        stalled=0
        sleep 30
    fi
done
