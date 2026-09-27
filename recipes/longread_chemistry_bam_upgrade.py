#!/usr/bin/env python
"""
Upgrade longread_chemistry rows from bucket to exact, where a submitted BAM
carries the vendor chemistry tag in its header (see longread_chemistry_pilot.py
for how this was validated: only a minority of runs carry one).

Resumable by design: every row processed gets bam_checked_at set regardless of
outcome, so re-running only picks up rows this process (or a prior kill) never
reached. Safe to kill and rerun; safe to run repeatedly with --limit to work
through the corpus over several sessions instead of one long-lived call.

Two network calls per candidate: ENA filereport for submitted_ftp (find a
.bam), then a Range GET on that BAM's header. Both go through helpers.send_req
so a transient failure retries instead of permanently marking a run
"no chemistry": a run whose header we never successfully read must stay
unchecked, not be recorded as evidence-free.

Usage:
  python recipes/longread_chemistry_bam_upgrade.py --db FILE [--limit N] [--platform PACBIO_SMRT]
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from helpers import send_req  # noqa: E402

FILEREPORT = "https://www.ebi.ac.uk/ena/portal/api/filereport"
BAM_HEADER_BYTE_CAP = 512_000

ONT_MODEL_RE = re.compile(r"basecall_model=(\S+)")
# dna_r10.4.1_e8.2_400bps_sup@v5.2.0: not every submitter's string matches this
# shape exactly (rna_ prefix, older kits without a speed segment), so the split
# is best-effort on top of the always-kept full string.
ONT_MODEL_SPLIT_RE = re.compile(
    r"^(?P<moltype>dna|rna)_r(?P<pore>[\d.]+)_(?P<kit>e[\d.]+)_"
    r"(?:(?P<speed>\d+bps)_)?(?P<tier>\w+)@(?P<basecall_model_version>v[\d.]+)$"
)
ONT_PU_RE = re.compile(r"[\t ]PU:(\S+)")
ONT_PG_RE = re.compile(r"@PG\t.*\bPN:(dorado|guppy|bonito)\b.*?\bVN:(\S+)")

PB_TAGS_RE = re.compile(
    r"(BINDINGKIT|SEQUENCINGKIT|BASECALLERVERSION|SMRTCELLKIT)=([^;]+)"
)
PB_PM_RE = re.compile(r"[\t ]PM:(\S+)")
PB_CM_RE = re.compile(r"[\t ]CM:(\S+)")
PB_PG_RE = re.compile(r"@PG\t.*\bPN:(ccs|bam2bam|baz2bam)\b.*?\bVN:(\S+)")

# New evidence columns, added to longread_chemistry alongside the original ones.
EVIDENCE_COLS = (
    "basecaller_software",
    "basecaller_software_version",
    "flow_cell_id",
    "ont_pore",
    "ont_kit",
    "ont_speed_bps",
    "ont_model_tier",
    "ont_basecall_model_version",
    "pacbio_platform_model",
    "pacbio_chemistry_code",
    "pacbio_binding_kit",
    "pacbio_sequencing_kit",
    "pacbio_smrtcell_kit",
    "pacbio_basecaller_version",
)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(longread_chemistry)")}
    if "bam_checked_at" not in cols:
        conn.execute("ALTER TABLE longread_chemistry ADD COLUMN bam_checked_at TEXT")
    for col in EVIDENCE_COLS:
        if col not in cols:
            conn.execute(f"ALTER TABLE longread_chemistry ADD COLUMN {col} TEXT")  # noqa: S608
    conn.commit()


def _submitted_bam(client: httpx.Client, run_acc: str) -> str | None:
    resp = send_req(
        "GET",
        FILEREPORT,
        params={"accession": run_acc, "result": "read_run", "fields": "submitted_ftp"},
        timeout=30,
        num_retries=4,
        max_wait=20,
        client=client,
    )
    lines = resp.text.strip().splitlines()
    if len(lines) < 2:
        return None
    field = lines[1].split("\t", 1)[-1] if "\t" in lines[1] else ""
    bams = [f for f in field.split(";") if f.lower().endswith(".bam")]
    return bams[0] if bams else None


def _parse_header(header: str) -> dict[str, str] | None:
    """Every field this header's @RG/@PG lines yield, plus 'chemistry' and
    'raw_evidence' summary strings. None if neither platform's tag is present."""
    ont = ONT_MODEL_RE.search(header)
    if ont:
        model_str = ont.group(1)
        fields: dict[str, str] = {}
        m = ONT_MODEL_SPLIT_RE.match(model_str)
        if m:
            fields["ont_pore"] = f"r{m.group('pore')}"
            fields["ont_kit"] = m.group("kit")
            if m.group("speed"):
                fields["ont_speed_bps"] = m.group("speed").removesuffix("bps")
            fields["ont_model_tier"] = m.group("tier")
            fields["ont_basecall_model_version"] = m.group("basecall_model_version")
        pu = ONT_PU_RE.search(header)
        if pu:
            fields["flow_cell_id"] = pu.group(1)
        pg = ONT_PG_RE.search(header)
        if pg:
            fields["basecaller_software"] = pg.group(1)
            fields["basecaller_software_version"] = pg.group(2)
        chemistry = f"ONT {model_str}"
        return {**fields, "chemistry": chemistry, "raw_evidence": model_str}

    pb = dict(PB_TAGS_RE.findall(header))
    if pb:
        fields = {}
        if "BINDINGKIT" in pb:
            fields["pacbio_binding_kit"] = pb["BINDINGKIT"]
        if "SEQUENCINGKIT" in pb:
            fields["pacbio_sequencing_kit"] = pb["SEQUENCINGKIT"]
        if "SMRTCELLKIT" in pb:
            fields["pacbio_smrtcell_kit"] = pb["SMRTCELLKIT"]
        if "BASECALLERVERSION" in pb:
            fields["pacbio_basecaller_version"] = pb["BASECALLERVERSION"]
        pm = PB_PM_RE.search(header)
        if pm:
            fields["pacbio_platform_model"] = pm.group(1)
        cm = PB_CM_RE.search(header)
        if cm:
            fields["pacbio_chemistry_code"] = cm.group(1)
        pg = PB_PG_RE.search(header)
        if pg:
            fields["basecaller_software"] = pg.group(1)
            fields["basecaller_software_version"] = pg.group(2)
        raw = ";".join(f"{k}={v}" for k, v in sorted(pb.items()))
        chemistry = f"PacBio {raw}"
        return {**fields, "chemistry": chemistry, "raw_evidence": raw}
    return None


def _chemistry_from_bam(client: httpx.Client, url: str) -> dict[str, str] | None:
    full_url = url if url.startswith("http") else f"https://{url}"
    resp = send_req(
        "GET",
        full_url,
        headers={"Range": f"bytes=0-{BAM_HEADER_BYTE_CAP}"},
        timeout=30,
        num_retries=4,
        max_wait=20,
        client=client,
    )
    proc = subprocess.run(
        ["samtools", "view", "-H", "-"],
        input=resp.content,
        capture_output=True,
        timeout=30,
        check=False,
    )
    header = proc.stdout.decode("utf-8", errors="replace")
    return _parse_header(header)


def process_one(
    client: httpx.Client, run_acc: str
) -> tuple[str, dict[str, str] | None]:
    """(run_acc, fields_or_None). Raises on network failure (caller decides
    whether to mark checked)."""
    bam_url = _submitted_bam(client, run_acc)
    if not bam_url:
        return run_acc, None
    return run_acc, _chemistry_from_bam(client, bam_url)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--platform", choices=["PACBIO_SMRT", "OXFORD_NANOPORE"])
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    _ensure_schema(conn)

    where = "bam_checked_at IS NULL"
    params: list[str] = []
    if args.platform:
        where += " AND instrument_platform = ?"
        params.append(args.platform)
    # PacBio first by default: validated yield is 12% vs ONT's 2%.
    order = "instrument_platform DESC" if not args.platform else "run_accession"
    rows = conn.execute(
        f"SELECT run_accession FROM longread_chemistry WHERE {where} "  # noqa: S608
        f"ORDER BY {order} LIMIT ?",
        (*params, args.limit),
    ).fetchall()
    run_accs = [r[0] for r in rows]
    print(f"checking {len(run_accs)} runs (limit={args.limit})", flush=True)

    t0 = time.time()
    upgraded = checked = failed = 0
    with httpx.Client() as client, ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(process_one, client, r): r for r in run_accs}
        for fut in as_completed(futs):
            run_acc = futs[fut]
            try:
                _, fields = fut.result()
            except Exception as exc:  # noqa: BLE001 (leave unchecked, retried next run)
                failed += 1
                print(f"  {run_acc}: FAILED, left unchecked ({exc})", flush=True)
                continue
            now = time.strftime("%Y-%m-%dT%H:%M:%S")
            if fields:
                cols = [
                    "chemistry",
                    "chemistry_confidence",
                    "chemistry_source",
                    *EVIDENCE_COLS,
                ]
                values = [
                    fields["chemistry"],
                    "exact",
                    "bam_header",
                    *(fields.get(c) for c in EVIDENCE_COLS),
                ]
                # raw_evidence is not in EVIDENCE_COLS (it predates this pass).
                cols.append("raw_evidence")
                values.append(fields["raw_evidence"])
                cols.append("bam_checked_at")
                values.append(now)
                sets = ", ".join(f"{c} = ?" for c in cols)
                conn.execute(
                    f"UPDATE longread_chemistry SET {sets} WHERE run_accession = ?",  # noqa: S608
                    (*values, run_acc),
                )
                upgraded += 1
            else:
                conn.execute(
                    "UPDATE longread_chemistry SET bam_checked_at = ? WHERE run_accession = ?",
                    (now, run_acc),
                )
            checked += 1
            if checked % 200 == 0:
                conn.commit()
                print(
                    f"  ... {checked}/{len(run_accs)} checked, {upgraded} upgraded",
                    flush=True,
                )
    conn.commit()
    conn.close()
    print(
        f"\ndone: {checked} checked, {upgraded} upgraded to exact, "
        f"{failed} left unchecked (network failure) in {time.time() - t0:.0f}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
