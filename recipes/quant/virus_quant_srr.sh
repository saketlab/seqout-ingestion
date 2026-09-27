#!/usr/bin/env bash
# Quantify viral/microbial content of one SRA run from Logan unitigs.
#
# Output is the requested two-column CSV: target name, reads mapping to it.
#
# How "reads" is derived: Logan unitigs carry mean k-mer abundance, so
# reads_i ~= ka_i * (len_i - 30) / (READ_LEN - 30), summed by target. This is an
# estimate; __read_length__ records the assumption. K-mer mass is emitted too and
# is better for run-to-run comparisons.
#
# Two counts are different and both are given:
#   __total_reads__   the run's real read count, from the ENA filereport API
#   reads column      estimated reads assigned to each reference
# A target's share of the library is (its reads) / __total_reads__.
#
# Only targets clearing the alignment filter are counted (nmatch >= MIN_NMATCH,
# identity >= MIN_ID). MIN_NMATCH sits above the spurious-alignment ceiling; see
# virus_scan_srr.sh for the calibration.
#
# Outputs -> OUT_DIR/<ACC>.quant.csv
#   virus,reads,kmer_mass,n_unitigs,ref_bp,class
#   __run__,<acc>,,,,
#   __total_reads__,<n from ENA>,,,,
#   __read_length__,<assumed>,,,,
#   virus|HPV16|K02718,3421,1.2e6,84,7906,virus
# and OUT_DIR/<ACC>.status  (ok | absent_from_logan | download_failed)
#
# Usage: ./virus_quant_srr.sh <ACC> [OUT_DIR]
#   REF=panel.mmi MIN_NMATCH=40 MIN_ID=0.95 READ_LEN=100 THREADS=4

set -uo pipefail


# Probe --retry-all-errors: older curl treats it as a hard usage error, while
# distro backports make version checks unreliable. Without it curl still retries,
# just not on every error class.
if curl --help all 2>/dev/null | grep -q -- --retry-all-errors; then
  RETRY_ALL_ERRORS=--retry-all-errors
else
  RETRY_ALL_ERRORS=
fi
ACC="${1:?usage: virus_quant_srr.sh <SRR accession> [OUT_DIR]}"
OUT="${2:-quant}"
REF="${REF:-$HOME/virus_scan/combined.mmi}"
MIN_NMATCH="${MIN_NMATCH:-40}"
MIN_ID="${MIN_ID:-0.95}"
READ_LEN="${READ_LEN:-100}"
THREADS="${THREADS:-4}"
K=31
URL="https://s3.amazonaws.com/logan-pub/u/${ACC}/${ACC}.unitigs.fa.zst"

# Shared exponential backoff for transient network failures. Jitter keeps hosts
# that started together from retrying in lockstep. RETRIES/BACKOFF_BASE/
# BACKOFF_CAP are overridable per host.
RETRIES="${RETRIES:-4}"
BACKOFF_BASE="${BACKOFF_BASE:-8}"
BACKOFF_CAP="${BACKOFF_CAP:-120}"
backoff() {                       # backoff <attempt-number, 1-based>
    _n=$1; _d=$BACKOFF_BASE
    while [ "$_n" -gt 1 ]; do _d=$(( _d * 2 )); _n=$(( _n - 1 )); done
    [ "$_d" -gt "$BACKOFF_CAP" ] && _d=$BACKOFF_CAP
    sleep $(( _d + RANDOM % 5 ))
}

mkdir -p "$OUT"
# Resume from the CSV alone and repair a missing sidecar. The CSV is the
# measurement; .status describes it and must not turn a complete CSV into
# "not measured" after a partial pull.
if [ -s "$OUT/${ACC}.quant.csv" ]; then
    [ -s "$OUT/${ACC}.status" ] || echo "ok" > "$OUT/${ACC}.status"
    exit 0
fi
command -v minimap2 >/dev/null || { echo "minimap2 not found" >&2; exit 2; }
[ -s "$REF" ] || { echo "missing index: $REF" >&2; exit 2; }

# One HEAD gives the status code and size. Parse raw headers, not
# `-w %{header_json}`, because older curl can silently omit the size and disable
# the disk guard. Retry the HEAD: throttled probes must not leave only .status
# when the done-list keys on .quant.csv.
code=""; CLEN=0
for _h in $(seq 1 "$RETRIES"); do
    hdr=$(curl -sI --max-time 60 -w '\nHTTPCODE:%{http_code}\n' "$URL" 2>/dev/null)
    code=$(printf '%s' "$hdr" | grep -o 'HTTPCODE:[0-9]*' | tail -1 | cut -d: -f2)
    CLEN=$(printf '%s' "$hdr" | tr -d '\r' | awk 'BEGIN{IGNORECASE=1} /^content-length:/{print $2}' | tail -1)
    case "$CLEN" in ''|*[!0-9]*) CLEN=0;; esac
    case "$code" in ''|000) [ "$_h" -lt "$RETRIES" ] && backoff "$_h";; *) break;; esac
done
if [ "$code" = "404" ]; then
    echo "absent_from_logan" > "$OUT/${ACC}.status"
    printf 'virus,reads,kmer_mass,n_unitigs,ref_bp,class\n__run__,%s,,,,\n__status__,absent_from_logan,,,,\n' "$ACC" \
        > "$OUT/${ACC}.quant.csv"
    exit 0
elif [ "$code" != "200" ]; then
    echo "download_failed http=$code" > "$OUT/${ACC}.status"; exit 1
fi

# The run's true read count is the denominator for abundance. Retry ENA and
# leave the field empty on failure, so missing has one representation. Remaining
# gaps are repaired later; never guess a count from file size.
TOTAL_READS=""
for _try in $(seq 1 "$RETRIES"); do
    TOTAL_READS=$(curl -s --max-time 45 \
      "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=${ACC}&result=read_run&fields=read_count&format=tsv" \
      2>/dev/null | awk -F'\t' 'NR==2{print $2}')
    case "$TOTAL_READS" in ''|*[!0-9]*) TOTAL_READS=""; [ "$_try" -lt "$RETRIES" ] && backoff "$_try";; *) break;; esac
done

TMPDIR_LOCAL="${TMPDIR_LOCAL:-${TMPDIR:-/tmp}}"
mkdir -p "$TMPDIR_LOCAL"

# Disk guard. The unitig file is written locally for the abundance pass. Require
# headroom before download; if it does not fit, leave no .quant.csv so the next
# scatter can send it to a host with space.
AVAIL=$(df -P --block-size=1 "$TMPDIR_LOCAL" 2>/dev/null | awk 'NR==2{print $4}')
AVAIL=${AVAIL:-0}
# Include current temp-dir usage plus a margin for this file. `du | cut` keeps a
# plain integer; awk numeric conversion can emit scientific notation that Bash
# arithmetic cannot parse, and mawk's %d can truncate multi-GB values.
INUSE=$(du -s --block-size=1 "$TMPDIR_LOCAL" 2>/dev/null | cut -f1)
case "$INUSE" in ''|*[!0-9]*) INUSE=0;; esac
NEED=$(( CLEN + INUSE + CLEN / 5 ))
if [ "$CLEN" -gt 0 ] && [ "$AVAIL" -gt 0 ] && [ "$NEED" -gt "$AVAIL" ]; then
    echo "too_large_for_host bytes=$CLEN avail=$AVAIL" > "$OUT/${ACC}.status"
    exit 1
fi
# Put all temp files under TMPDIR_LOCAL, which is what the disk guard measured.
paf="$(mktemp -p "$TMPDIR_LOCAL")"; ka="$(mktemp -p "$TMPDIR_LOCAL")"; zst="$(mktemp -p "$TMPDIR_LOCAL")"
trap 'rm -f "$paf" "$paf.csv" "$ka" "$ka.ids" "$zst"' EXIT

# DL_MAX_TIME bounds one accession. A capped failure leaves .status but no
# .quant.csv, so the next scatter can move it. Download to a file before
# processing: streaming through a pipe made mid-transfer drops non-resumable.
DL_RC=0
for _a in $(seq 1 "$RETRIES"); do
    curl -s --fail --max-time "${DL_MAX_TIME:-7200}" \
         --retry 5 --retry-delay 10 $RETRY_ALL_ERRORS \
         --speed-time 300 --speed-limit 1024 \
         -C - -o "$zst" "$URL" && { DL_RC=0; break; }
    DL_RC=$?
    # curl 35 is a TLS handshake failure and 56 a mid-transfer reset; both are
    # transient here and both are covered by --retry-all-errors inside curl, so
    # this outer loop only exists for the cases curl gives up on entirely
    [ "$_a" -lt "$RETRIES" ] && backoff "$_a"
done
# --speed-limit/--speed-time abort a transfer stalled under 1 KB/s for 5 min, so a
# dead connection fails fast instead of burning the full 2 h budget.
if [ "$DL_RC" -ne 0 ] || [ ! -s "$zst" ]; then
    echo "download_failed curl rc=$DL_RC" > "$OUT/${ACC}.status"; exit 1
fi
if ! zstd -dc "$zst" 2>/dev/null \
     | minimap2 -x sr -t "$THREADS" --secondary=no "$REF" - 2>/dev/null > "$paf"; then
    echo "download_failed decompress_or_map" > "$OUT/${ACC}.status"; exit 1
fi

# abundance only for unitigs that survive the filter
awk -F'\t' -v mn="$MIN_NMATCH" -v mi="$MIN_ID" \
    '$10>=mn && $11>0 && $10/$11>=mi {print $1}' "$paf" | sort -u > "$ka.ids"
# Check the abundance pass too. An empty $ka would make present organisms look
# measured-negative; exit without .quant.csv so the run stays unmeasured.
if [ -s "$ka.ids" ]; then
    if ! zstd -dc "$zst" | grep '^>' | awk -v ids="$ka.ids" '
        BEGIN { while ((getline i < ids) > 0) WANT[i]=1 }
        { id=substr($1,2); if (id in WANT) { split($2,a,":"); print id"\t"a[3] } }' > "$ka"
    then
        echo "abundance_pass_failed" > "$OUT/${ACC}.status"; exit 1
    fi
    [ -s "$ka" ] || { echo "abundance_pass_empty" > "$OUT/${ACC}.status"; exit 1; }
fi
rm -f "$zst" "$ka.ids"

awk -F'\t' -v acc="$ACC" -v mn="$MIN_NMATCH" -v mi="$MIN_ID" -v kaf="$ka" \
    -v rl="$READ_LEN" -v k="$K" -v tot="$TOTAL_READS" '
BEGIN { while ((getline l < kaf) > 0) { split(l,x,"\t"); KA[x[1]]=x[2]+0 }
        denom = rl - k + 1; if (denom < 1) denom = 1; B = 100
        # exact-breadth cutoff: the size class the >=0.50 rule is calibrated for
        SMALL_BP = 50000 }
$10 >= mn && $11 > 0 && $10/$11 >= mi {
    v=$6; u=$1
    if (!((v SUBSEP u) in seen)) {
        seen[v,u]=1; nu[v]++
        # unitig length is the query length in the PAF
        mass = KA[u] * ($2 - k + 1); if (mass < 0) mass = 0
        km[v] += mass
    }
    # Breadth is reference bases covered by any alignment. Abundance alone cannot
    # separate real hits from short conserved cross-mapping, so small references
    # use exact merged intervals. Bins are kept only for megabase refs, whose
    # breadth is never adjudicated.
    if ($7 <= SMALL_BP) { n = ++acnt[v]; AS[v,n] = $8+0; AE[v,n] = $9+0 }
    else { for (b = int($8/B); b <= int(($9-1)/B); b++)
               if (!((v SUBSEP b) in bin)) { bin[v,b]=1; nb[v]++ } }
    tlen[v]=$7
}
END {
    print "virus,reads,kmer_mass,n_unitigs,ref_bp,covered_bp,breadth_frac,class"
    printf "__run__,%s,,,,,,\n", acc
    printf "__total_reads__,%s,,,,,,\n", tot
    printf "__read_length__,%d,,,,,,\n", rl
    for (v in nu) {
        split(v, c, "|")
        if (tlen[v] <= SMALL_BP && (v in acnt)) {
            # exact coverage: sort intervals by start, merge overlaps, sum lengths
            m = acnt[v]
            for (i=1; i<=m; i++) { s[i]=AS[v,i]; e[i]=AE[v,i] }
            for (i=2; i<=m; i++) { ks=s[i]; ke=e[i]; j=i-1
                while (j>=1 && s[j]>ks) { s[j+1]=s[j]; e[j+1]=e[j]; j-- }
                s[j+1]=ks; e[j+1]=ke }
            cov=0; cs=s[1]; ce=e[1]
            for (i=2; i<=m; i++) {
                if (s[i] <= ce) { if (e[i] > ce) ce=e[i] }
                else { cov += ce-cs; cs=s[i]; ce=e[i] } }
            cov += ce-cs
            delete s; delete e
        } else {
            cov = nb[v] * B
        }
        if (cov > tlen[v]) cov = tlen[v]
        bf  = (tlen[v] > 0) ? cov / tlen[v] : 0
        printf "%s,%d,%.1f,%d,%d,%d,%.4f,%s\n", v, int(km[v]/denom + 0.5), km[v],
               nu[v], tlen[v], cov, bf, c[1]
    }
}' "$paf" > "$paf.csv"
# Sort the body while keeping metadata rows on top. Read from a file: piping to
# `{ head; tail | sort; }` lets head consume buffered data before tail sees it.
{ head -4 "$paf.csv"; tail -n +5 "$paf.csv" | sort -t, -k2,2nr; } > "$OUT/${ACC}.quant.csv.tmp"
rm -f "$paf.csv"
mv "$OUT/${ACC}.quant.csv.tmp" "$OUT/${ACC}.quant.csv"
echo "ok" > "$OUT/${ACC}.status"
echo "[$ACC] targets=$(( $(wc -l < "$OUT/${ACC}.quant.csv") - 4 )) total_reads=$TOTAL_READS"
