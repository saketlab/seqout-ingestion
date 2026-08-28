#!/usr/bin/env bash
# yuj work loop: quantify every accession in this host's slice.
#
# PREFLIGHT IS FATAL BY DESIGN. virus_quant_srr.sh exits per accession when a
# tool is missing and xargs still returns 0, so an unchecked loop reports clean
# while quantifying nothing. minimap2 ships in the payload as a static x86_64
# binary; it is not installed per host.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export PATH="$PWD:$PATH"          # payload minimap2 wins over any system copy

# SINGLE-INSTANCE GUARD. yuj installs a watchdog *and* two cron lines and the
# wrapper does no mutual exclusion, so every cron tick starts another full loop.
# The workload is bandwidth-bound, so extra streams split the same pipe and push
# every file toward the DL_MAX_TIME cap. Guarding here rather than in the wrapper
# means it holds no matter who starts us.
# $PWD, not dirname "$BASH_SOURCE" a second time: line 11 already cd'd there, so
# a RELATIVE source path resolves against the new cwd. That open fails, flock -n
# on the bad fd fails, and the script exits 0 announcing another instance holds
# the lock. Only invocations by absolute path escape it.
exec 9>"$PWD/.run_quant.lock"
if ! flock -n 9; then
  echo "[$(date '+%F %T')] $(hostname): another run_quant.sh holds the lock; exiting"
  exit 0
fi

IN="${IN:-accessions.csv}"
OUT="${OUT:-results}"

# PER-HOST TUNING. fleet.csv has no `parallel` column, and one global PARALLEL
# is wrong on a fleet whose hosts differ by orders of magnitude in free disk.
# Slot count is bounded by DISK, not cores: the unitig files reach several GB
# and each in-flight run tees one to local disk.
# host.env is written per host at deploy time; absent, the old default applies.
[ -f "$PWD/host.env" ] && . "$PWD/host.env"
PARALLEL="${PARALLEL:-4}"
export REF="${REF:-$PWD/liver_panel.mmi}"
export THREADS="${THREADS:-2}"
export TMPDIR_LOCAL="${TMPDIR_LOCAL:-$PWD/tmp}"
# The disk guard in virus_quant_srr.sh reserves headroom for DISK_SLOTS
# concurrent downloads. A fixed default lets each run's check pass on its own
# while the slots collectively blow past free space, so tie it to the slot
# count rather than to cores.
export DISK_SLOTS="${DISK_SLOTS:-$((PARALLEL + 2))}"
mkdir -p "$OUT" "$TMPDIR_LOCAL"

fail=0
for t in minimap2 zstd curl; do
  command -v "$t" >/dev/null || { echo "FATAL: $t not found on $(hostname)" >&2; fail=1; }
done

# Check curl against the bucket, not just PATH. A shadowed curl can exist but
# fail every TLS handshake, yielding empty success. Retry before aborting a host.
if command -v curl >/dev/null; then
  probe=""
  for _p in 1 2 3; do
    probe=$(curl -sI --max-time 30 -o /dev/null -w '%{http_code}' \
            https://s3.amazonaws.com/logan-pub/ 2>/dev/null)
    case "$probe" in ''|000) sleep $(( _p * _p * 5 ));; *) break;; esac
  done
  case "$probe" in
    ''|000) echo "FATAL: curl ($(command -v curl)) cannot reach logan-pub on $(hostname) after 3 tries; TLS or network" >&2; fail=1;;
  esac
fi

# Build the index locally from the FASTA if it is not already here. The FASTA is
# the smaller file and `minimap2 -d` rebuilds the index in seconds, so ship the
# FASTA and build per host. It also removes a hazard: a truncated .mmi loads
# without complaining, giving the host a silently broken reference.
REF_FA="${REF%.mmi}.fa"
if [ ! -s "$REF" ] && [ -s "$REF_FA" ]; then
  echo "[$(hostname)] building index from $(basename "$REF_FA")..."
  minimap2 -x sr -d "$REF" "$REF_FA" 2>/dev/null || { echo "FATAL: index build failed" >&2; fail=1; }
fi
[ -s "$REF" ] || { echo "FATAL: missing index $REF and FASTA $REF_FA on $(hostname)" >&2; fail=1; }
[ -s "$IN" ]  || { echo "FATAL: missing work list $IN on $(hostname)" >&2; fail=1; }
[ "$fail" -eq 0 ] || exit 3

tail -n +2 "$IN" | grep -E '^[SDE]RR[0-9]+' \
  | xargs -P "$PARALLEL" -I{} ./virus_quant_srr.sh {} "$OUT"
echo "[$(date '+%F %T')] $(hostname): $(ls "$OUT"/*.quant.csv 2>/dev/null | wc -l) runs quantified"
