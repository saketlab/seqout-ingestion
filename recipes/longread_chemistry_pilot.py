#!/usr/bin/env python
"""
Pilot for the long-read chemistry plan, step 2: for a stratified sample of
ONT/PacBio runs, check ENA's submitted_ftp/submitted_format (not ingested
anywhere in this pipeline; sra_run_download_links.fastq_ftp cannot answer
this, see longread_chemistry_scoping.py) for a native BAM, and for a small
subset of BAM hits confirm the chemistry tag actually parses out of the
header.

Sample is capped at one run per study per instrument_model stratum, so one
prolific study cannot masquerade as broad coverage.

Usage:
  python recipes/longread_chemistry_pilot.py --db /tmp/longread_download_links.db
"""

from __future__ import annotations

import argparse
import random
import re
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from helpers import send_req  # noqa: E402

FILEREPORT = "https://www.ebi.ac.uk/ena/portal/api/filereport"
PER_STRATUM = 40
HEADER_FETCH_CAP_PER_PLATFORM = 15
BAM_HEADER_BYTE_CAP = 512_000

ONT_MODEL_RE = re.compile(r"basecall_model=(\S+)")
PB_TAGS_RE = re.compile(r"(BINDINGKIT|SEQUENCINGKIT|BASECALLERVERSION)=([^;]+)")


def sample_runs(db: Path) -> list[tuple[str, str, str, str]]:
    """One run per (instrument_model, study_accession), capped per stratum."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    by_model: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)
    seen_study_per_model: dict[str, set[str]] = defaultdict(set)
    rows = conn.execute(
        "SELECT run_accession, study_accession, instrument_platform, "
        "instrument_model FROM longread_download_links ORDER BY RANDOM()"
    ).fetchall()
    conn.close()
    for run_acc, study_acc, platform, model in rows:
        model = model or "unspecified"
        if study_acc in seen_study_per_model[model]:
            continue
        if len(by_model[model]) >= PER_STRATUM:
            continue
        seen_study_per_model[model].add(study_acc)
        by_model[model].append((run_acc, study_acc, platform, model))
    sample = [r for group in by_model.values() for r in group]
    random.shuffle(sample)
    return sample


def fetch_submitted_format(
    client: httpx.Client, run_acc: str
) -> tuple[str, str | None]:
    resp = send_req(
        "GET",
        FILEREPORT,
        params={
            "accession": run_acc,
            "result": "read_run",
            "fields": "submitted_ftp",
        },
        timeout=30,
        num_retries=3,
        max_wait=10,
        client=client,
    )
    lines = resp.text.strip().splitlines()
    if len(lines) < 2:
        return run_acc, None
    return run_acc, lines[1].split("\t", 1)[-1] if "\t" in lines[1] else None


def bam_urls(submitted_ftp: str | None) -> list[str]:
    if not submitted_ftp:
        return []
    return [f for f in submitted_ftp.split(";") if f.lower().endswith(".bam")]


def fetch_chemistry_tag(url: str) -> str | None:
    """Range-GET the front of a remote BAM and pull the @RG DS: chemistry tag."""
    full_url = url if url.startswith("http") else f"https://{url}"
    try:
        resp = httpx.get(
            full_url,
            headers={"Range": f"bytes=0-{BAM_HEADER_BYTE_CAP}"},
            timeout=30,
            follow_redirects=True,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        return f"fetch_failed: {exc}"

    proc = subprocess.run(
        ["samtools", "view", "-H", "-"],
        input=resp.content,
        capture_output=True,
        timeout=30,
        check=False,
    )
    header = proc.stdout.decode("utf-8", errors="replace")
    if not header:
        return "header_over_cap_or_unparseable"

    ont = ONT_MODEL_RE.search(header)
    if ont:
        return f"ont:{ont.group(1)}"
    pb = dict(PB_TAGS_RE.findall(header))
    if pb:
        return "pacbio:" + ";".join(f"{k}={v}" for k, v in sorted(pb.items()))
    return "no_chemistry_tag_in_header"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="from longread_chemistry_scoping.py")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()
    random.seed(args.seed)

    sample = sample_runs(Path(args.db))
    print(f"sampled {len(sample)} runs across {len({r[3] for r in sample})} strata")

    results: dict[str, tuple[str, str, str, str | None]] = {}
    with httpx.Client() as client, ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(fetch_submitted_format, client, r[0]): r for r in sample}
        for fut in as_completed(futs):
            run_acc, study_acc, platform, model = futs[fut]
            try:
                _, submitted_ftp = fut.result()
            except Exception as exc:  # noqa: BLE001 (pilot script, report and move on)
                print(f"  {run_acc}: filereport call failed: {exc}")
                submitted_ftp = None
            results[run_acc] = (platform, model, study_acc, submitted_ftp)

    per_platform = Counter()
    bam_per_platform = Counter()
    bam_candidates: list[tuple[str, str, str]] = []  # run_acc, platform, url
    for run_acc, (platform, _model, _study, submitted_ftp) in results.items():
        per_platform[platform] += 1
        urls = bam_urls(submitted_ftp)
        if urls:
            bam_per_platform[platform] += 1
            bam_candidates.append((run_acc, platform, urls[0]))

    print("\n=== submitted_ftp: BAM presence by platform ===")
    for platform, total in per_platform.items():
        bam_n = bam_per_platform[platform]
        print(f"{platform:>16}: {bam_n}/{total} runs have a submitted .bam")

    random.shuffle(bam_candidates)
    by_platform_cap: Counter = Counter()
    to_check = []
    for run_acc, platform, url in bam_candidates:
        if by_platform_cap[platform] >= HEADER_FETCH_CAP_PER_PLATFORM:
            continue
        by_platform_cap[platform] += 1
        to_check.append((run_acc, platform, url))

    print(
        f"\n=== confirming chemistry tag parses on {len(to_check)} BAM "
        f"headers (cap {HEADER_FETCH_CAP_PER_PLATFORM}/platform) ==="
    )
    outcomes = Counter()
    for run_acc, platform, url in to_check:
        tag = fetch_chemistry_tag(url)
        outcomes[tag.split(":", 1)[0] if tag else "none"] += 1
        print(f"  {run_acc} ({platform}): {tag}")

    print("\n=== header-parse outcome summary ===")
    for outcome, n in outcomes.most_common():
        print(f"  {outcome}: {n}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
