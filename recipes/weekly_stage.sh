#!/usr/bin/env bash
# Weekly staging ingest. Fetches each archive's newest records into its own
# per-window SQLite and writes a report. It never transfers to Postgres: the
# user reviews the report and runs the transfer by hand.
#
# Usage:
#   bash recipes/weekly_stage.sh              # all sources due this run
#   ONLY=geo,ena bash recipes/weekly_stage.sh # just these
#   DRY_RUN=1 bash recipes/weekly_stage.sh    # print the plan, fetch nothing
#
# State (watermarks, lockfile, report) lives under STAGE_DIR, not DATA_DIR, so a
# timed run can never touch the hand-run pipeline's data.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
export PYTHON_GIL=0

STAGE_DIR=${STAGE_DIR:-/data/saket/cron_dump}
PY=${PY:-.venv/bin/python}
WORKERS=${WORKERS:-8}
RETRIES=${RETRIES:-20}

GSA_WORKERS=${GSA_WORKERS:-2}
ONLY=${ONLY:-}
DRY_RUN=${DRY_RUN:-}

# Archives back-date and revise records after release, so each window reaches
# back behind the watermark rather than starting where the last one stopped.
OVERLAP_DAYS=${OVERLAP_DAYS:-10}

# A source that has never run has no watermark; this is where it starts.
FIRST_WINDOW_DAYS=${FIRST_WINDOW_DAYS:-14}

# Below this the run stops: an ingest that fills the volume takes the box with it.
MIN_FREE_GB=${MIN_FREE_GB:-25}

# The weekly job looks at SRA but never downloads it; see sra_probe below.
SRA_LISTING_URL=${SRA_LISTING_URL:-https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata}

# This box sits behind a campus SSO portal whose session expires on its own. A
# captive portal answers with a real status code, so only the body proves reach.
NET_PROBE_ACC=${NET_PROBE_ACC:-SRR000001}
NET_PROBE_URL=${NET_PROBE_URL:-https://www.ebi.ac.uk/ena/portal/api/filereport}
NET_PROBE_FALLBACK_URL=${NET_PROBE_FALLBACK_URL:-https://s3.amazonaws.com/logan-pub/u/SRR000001/SRR000001.unitigs.fa.zst}
NET_LOGIN_URL=${NET_LOGIN_URL:-https://internet-sso.iitb.ac.in/login.php}
NET_RETRIES=${NET_RETRIES:-3}
NET_RETRY_SLEEP=${NET_RETRY_SLEEP:-15}

STATE_DIR=$STAGE_DIR/state
LOG_DIR=$STAGE_DIR/logs
REPORT=$STAGE_DIR/weekly_report.log
LOCK=$STATE_DIR/weekly.lock
mkdir -p "$STATE_DIR" "$LOG_DIR"

TODAY=$(date -u +%F)
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)

say() { echo "[$(date -u +%FT%TZ)] $*"; }
report() { echo "$RUN_ID $*" >>"$REPORT"; }

free_gb() { df -BG --output=avail "$STAGE_DIR" | tail -1 | tr -dc 0-9; }

# One run at a time. A weekly trigger must not land on top of a run still going.
exec 9>"$LOCK"
if ! flock -n 9; then
    say "another weekly run holds $LOCK; exiting"
    report "SKIPPED reason=locked"
    exit 0
fi
echo $$ >&9

# Two independent targets, because "ENA is down" and "we are behind a portal"
# must not look alike. Either one answering with real content proves reach.
net_probe() {
    local body code
    body=$(curl -fsS --max-time 45 \
        "$NET_PROBE_URL?accession=$NET_PROBE_ACC&result=read_run&fields=run_accession,fastq_ftp" \
        2>/dev/null)
    if printf '%s' "$body" | grep -q "$NET_PROBE_ACC" &&
       printf '%s' "$body" | grep -q "fastq"; then
        return 0
    fi
    # A ranged GET returns 206 from the real object store and never from a portal.
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 45 \
        -r 0-99 "$NET_PROBE_FALLBACK_URL" 2>/dev/null)
    [ "$code" = "206" ] || [ "$code" = "200" ]
}

# Credentials reach curl on stdin, never in argv, so they cannot surface in ps
# output or in a log line. Nothing here echoes either value.
sso_login() {
    [ -n "${IITB_SSO_USER:-}" ] && [ -n "${IITB_SSO_PASS:-}" ] || return 1
    printf 'user = "%s:%s"\nurl = "%s"\n' \
        "$IITB_SSO_USER" "$IITB_SSO_PASS" "$NET_LOGIN_URL" |
        curl --location-trusted --config - --silent --show-error \
            --max-time 60 -o /dev/null 2>/dev/null
}

NET_FAILED=
# Returns non-zero when the box cannot reach the archives. Callers must treat
# that as "do no work", never as "the archive was empty".
ensure_network() {
    net_probe && return 0
    local i creds=absent
    if [ -n "${IITB_SSO_USER:-}" ] && [ -n "${IITB_SSO_PASS:-}" ]; then
        creds=present
    fi
    if [ "$creds" = absent ]; then
        say "network probe failed and no SSO credentials are configured"
        report "NETWORK_UNAVAILABLE reason=probe_failed sso_credentials=absent set_IITB_SSO_USER_and_IITB_SSO_PASS_in_.env"
        NET_FAILED=1
        return 1
    fi
    # One login does not always take, so re-probe rather than trusting its exit.
    for i in $(seq 1 "$NET_RETRIES"); do
        say "network probe failed; attempting campus SSO login ($i/$NET_RETRIES)"
        sso_login || true
        sleep "$NET_RETRY_SLEEP"
        if net_probe; then
            report "NETWORK_RECOVERED after_sso_login_attempts=$i"
            return 0
        fi
    done
    say "network still unreachable after $NET_RETRIES SSO login attempts"
    report "NETWORK_UNAVAILABLE reason=probe_failed_after_sso sso_credentials=present login_attempts=$NET_RETRIES"
    NET_FAILED=1
    return 1
}

watermark_file() { echo "$STATE_DIR/$1.watermark"; }

read_watermark() {
    local f
    f=$(watermark_file "$1")
    if [ -s "$f" ]; then
        cat "$f"
    else
        date -u -d "$TODAY - $FIRST_WINDOW_DAYS days" +%F
    fi
}

window_start() {
    local wm
    wm=$(read_watermark "$1")
    date -u -d "$wm - $OVERLAP_DAYS days" +%F
}

selected() {
    [ -z "$ONLY" ] && return 0
    case ",$ONLY," in *,"$1",*) return 0 ;; *) return 1 ;; esac
}

# Rows written, so the report can say what a window actually produced.
db_rows() {
    local db=$1 total=0 n
    [ -f "$db" ] || { echo 0; return; }
    for t in $(sqlite3 "$db" "SELECT name FROM sqlite_master WHERE type='table';" 2>/dev/null); do
        n=$(sqlite3 "$db" "SELECT COUNT(*) FROM \"$t\";" 2>/dev/null || echo 0)
        total=$((total + n))
    done
    echo "$total"
}

newest_db() { ls -1t "$STAGE_DIR/$1"/db/*/*.db 2>/dev/null | head -1; }

run_source() {
    local name=$1 cmd=$2 start=$3 end=$4 workers=${5:-$WORKERS}
    shift 5 2>/dev/null || shift 4
    local log=$LOG_DIR/$name.log
    local free
    # The session can lapse partway through a run that takes hours.
    if ! ensure_network; then
        report "$name SKIPPED window=$start..$end reason=network_unavailable"
        return
    fi
    free=$(free_gb)
    if [ "$free" -lt "$MIN_FREE_GB" ]; then
        say "$name: only ${free}G free, below ${MIN_FREE_GB}G; skipping"
        report "$name SKIPPED window=$start..$end reason=low_disk free_gb=$free"
        return
    fi

    if [ -n "$DRY_RUN" ]; then
        say "$name: would ingest $start..$end"
        report "$name DRY_RUN window=$start..$end"
        return
    fi

    local after t0 t1 rc
    t0=$(date +%s)
    say "$name: ingesting $start..$end"
    {
        echo "=== $RUN_ID $name $start..$end ==="
        "$PY" ./src/main.py --log-level info ingest "$cmd" \
            --output-dir "$STAGE_DIR" \
            --start-date "$start" --end-date "$end" \
            --workers "$workers" --retries "$RETRIES" "$@"
    } >>"$log" 2>&1
    rc=$?
    t1=$(date +%s)
    after=$(db_rows "$(newest_db "$name")")

    if [ "$rc" -eq 0 ]; then
        # Advance only on success, so a failed week is retried instead of
        # leaving a window nothing ever covers.
        echo "$end" >"$(watermark_file "$name")"
        report "$name OK window=$start..$end rows=$after secs=$((t1-t0)) free_gb=$(free_gb)"
    else
        report "$name FAILED rc=$rc window=$start..$end secs=$((t1-t0)) log=$log"
    fi
}

say "weekly staging run $RUN_ID, free $(free_gb)G"
report "START free_gb=$(free_gb) overlap_days=$OVERLAP_DAYS"

# Preflight before any source runs. A captive portal must never be allowed to
# masquerade as an empty archive, so a failure here does no work at all and
# leaves every watermark where it was.
if [ -z "$DRY_RUN" ] && ! ensure_network; then
    report "END aborted=network_unavailable sources_run=0 watermarks_advanced=0"
    say "aborting: no usable network. No source ran and no watermark moved."
    exit 1
fi

# Sources where a release-date window is not enough. Both GSA halves fetch
# per-study detail only for studies the export marks Open, and a study can flip
# Control -> Open years after its release date, so a window keyed on release
# never selects it. gsa_common.date_filter_active treats today..today as
# "no window", which selects every open study instead. The downloaded workbooks
# live in <output>/<src>/data/studies, shared across runs, so a full sync after
# the first costs only the studies whose workbook is not already cached.
FULL_SYNC_SOURCES=${FULL_SYNC_SOURCES:-"gsa gsa_human"}

for src in geo ena ae dra gea gsa_human gsa; do
    selected "$src" || continue
    # State keys use underscores; the CLI subcommand uses hyphens.
    case $src in
        gsa|gsa_human) w=$GSA_WORKERS ;;
        *) w=$WORKERS ;;
    esac
    case " $FULL_SYNC_SOURCES " in
        *" $src "*) from=$TODAY ;;
        *)          from=$(window_start "$src") ;;
    esac
    run_source "$src" "${src//_/-}" "$from" "$TODAY" "$w"
done

# GSA BioProject has no date window of its own. It reads the two GSA SQLites and
# fetches the BioProjects they name, so it runs after them or not at all.
if selected gsa_bioproject; then
    gsa_db=$(newest_db gsa)
    gsa_human_db=$(newest_db gsa_human)
    bp_args=()
    [ -n "$gsa_db" ] && bp_args+=(--gsa-db-file "$gsa_db")
    [ -n "$gsa_human_db" ] && bp_args+=(--gsa-human-db-file "$gsa_human_db")
    if [ ${#bp_args[@]} -gt 0 ] && [ -z "$DRY_RUN" ]; then
        log=$LOG_DIR/gsa_bioproject.log
        t0=$(date +%s)
        {
            echo "=== $RUN_ID gsa_bioproject ==="
            "$PY" ./src/main.py --log-level info ingest gsa-bioproject \
                --output-dir "$STAGE_DIR" "${bp_args[@]}" \
                --workers "$WORKERS" --retries "$RETRIES"
        } >>"$log" 2>&1
        rc=$?
        t1=$(date +%s)
        if [ "$rc" -eq 0 ]; then
            report "gsa_bioproject OK rows=$(db_rows "$(newest_db gsa_bioproject)") secs=$((t1-t0))"
        else
            report "gsa_bioproject FAILED rc=$rc secs=$((t1-t0)) log=$log"
        fi
    else
        report "gsa_bioproject SKIPPED reason=no_gsa_db"
    fi
fi

# SRA is looked at every week but downloaded by hand, on its own monthly cadence.
# The probe reads the listing and nothing else, so a monthly ingest never becomes a
# blind spot: a stale copy nobody notices is the failure this exists to prevent.
sra_probe() {
    local listing rc newest_full newest_daily seen url size

    listing=$(curl -fsSL --max-time 120 --retry 2 "$SRA_LISTING_URL" 2>/dev/null)
    rc=$?
    if [ $rc -ne 0 ] || [ -z "$listing" ]; then
        report "sra PROBE_FAILED reason=unreachable curl_rc=$rc"
        return 0
    fi
    # NCBI serves its maintenance page with HTTP 200, so status alone cannot be trusted.
    if printf '%s' "$listing" | grep -qi "Service unavailable"; then
        report "sra PROBE_FAILED reason=ncbi_maintenance_page"
        return 0
    fi

    # Local patterns on purpose: this is a listing check, not the ingest path, and
    # SRA_TARBALL_RE_PAT stays as the ingest's own contract.
    newest_full=$(printf '%s' "$listing" |
        grep -oE 'NCBI_SRA_Metadata_Full_[0-9]{8}\.tar\.gz' | grep -oE '[0-9]{8}' |
        sort -u | tail -1)
    newest_daily=$(printf '%s' "$listing" |
        grep -oE 'NCBI_SRA_Metadata_[0-9]{8}\.tar\.gz' | grep -oE '[0-9]{8}' |
        sort -u | tail -1)

    if [ -z "$newest_full" ] && [ -z "$newest_daily" ]; then
        report "sra PROBE_FAILED reason=no_tarballs_parsed"
        return 0
    fi

    # Newest daily is the staleness signal: current means the archive is healthy,
    # days behind means something is wrong upstream.
    if [ -n "$newest_daily" ]; then
        local daily_age
        daily_age=$(( ( $(date -u -d "$TODAY" +%s) -
                        $(date -u -d "$newest_daily" +%s) ) / 86400 ))
        report "sra PROBE newest_daily=$newest_daily daily_age_days=$daily_age"
    fi

    seen=$(cat "$STATE_DIR/sra.watermark" 2>/dev/null || echo 00000000)
    if [ -n "$newest_full" ] && [ "$newest_full" \> "$seen" ]; then
        url="$SRA_LISTING_URL/NCBI_SRA_Metadata_Full_$newest_full.tar.gz"
        size=$(curl -fsIL --max-time 60 "$url" 2>/dev/null |
            tr -d '\r' | awk -F': ' 'tolower($1)=="content-length"{n=$2} END{print n+0}')
        report "sra ACTION_REQUIRED new_full=NCBI_SRA_Metadata_Full_$newest_full.tar.gz date=$newest_full bytes=$size last_ingested=$seen NOT_DOWNLOADED run_ingest_sra_by_hand"
        say "SRA: a new Full tarball is available ($newest_full). It was NOT downloaded."
    elif [ -n "$newest_full" ]; then
        report "sra PROBE newest_full=$newest_full already_ingested=yes"
    fi
    # The watermark is advanced by the operator after a real ingest, never here:
    # a probe that consumed its own signal would announce the gap only once.
}

if selected sra; then
    sra_probe
fi

report "END free_gb=$(free_gb)"
say "done; report at $REPORT"

# A run that lost the network partway through must not look like a clean run.
[ -n "$NET_FAILED" ] && exit 1
exit 0
