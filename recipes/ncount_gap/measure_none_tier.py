#!/usr/bin/env python3
"""
Measure how many `evidence=none` studies have a count matrix.

## Why this exists

`gap_studies.csv` grades every uncounted study strong / weak / none using the
filename regexes in `../seqout-paper-plots/nm-figures/14_ncount_gap.py`. The
`none` tier -- 15,025 studies -- is documented as "no processed data found by
any route", and is excluded from both target lists. It is therefore never
queued, never counted, and gets no pentimento row.

That grading is filename-only, and the patterns have a blind spot. GSE150050
deposits `GSE150050_all_cells_STAR_raw_counts.csv.tar.gz` -- an unambiguous
count matrix -- and is graded `none`, because:

  * strong wants a container extension (.mtx/.h5ad/.loom/...); a .csv.tar.gz
    is not one.
  * weak has `count[_.]?matrix`, which does not match `raw_counts`.

So the study is real, its matrix is real, and nothing will ever fetch it. This
script sizes that class rather than guessing at it.

This measures rather than fixes: it writes a candidate list and a breakdown by
which pattern caught each study, so the recovery can be judged before any of it
is queued. Patterns that fire on very few studies, or that look like they would
also catch metadata tables, are visible in the breakdown rather than buried in a
single total.

## Honesty about what a filename proves

A hit here is EVIDENCE, not proof, and the tiers are reported separately so any
one of them can be rejected without rejecting the total.

Normalized tables (TPM/CPM/FPKM/logTPM) ARE counted. The counter measures matrix
DIMENSIONS -- cells x genes -- and a TPM matrix has the same shape as the raw
matrix it came from, so the normalization is irrelevant to what is being
extracted. Excluding them would throw away a usable matrix over a property
nothing in this pipeline reads. They are graded under their own
`normalized_matrix` tier so the choice stays visible.

`metadata`, `readme`, QC and clustering tables remain excluded: those cannot
carry a cells x genes shape at all.

Read-only against production Postgres.

Usage:
    .venv/bin/python recipes/ncount_gap/measure_none_tier.py [--out DIR]
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from collections import Counter
from pathlib import Path

import psycopg

REPO = Path(__file__).resolve().parent.parent.parent
GAP_CSV = REPO / "recipes/ncount_gap/gap_studies.csv"

#: Verbatim from 14_ncount_gap.py. Duplicated rather than imported because that
#: file is in a sibling repo and runs its own Postgres session on import; if the
#: patterns there change, this comment is the reminder to re-sync.
STRONG = (
    r"(\.mtx|_bc_matrix|\.h5ad|\.h5seurat|\.loom|\.cloupe"
    r"|filtered_feature_bc|raw_feature_bc|cellranger|\.h5mu|\.zarr)"
)
WEAK = (
    r"(\.h5|\.hdf5|\.rds|\.rdata|\.npz|barcodes\.tsv|features\.tsv"
    r"|genes\.tsv|expression[_.]?matrix|umi[_.]?count|count[_.]?matrix|dge)"
)

#: The proposed additions, kept SEPARATE and named so each can be accepted or
#: rejected on its own evidence. Ordered most- to least-defensible.
EXTRA = {
    # "raw counts" / "STAR counts" / "gene counts": says counts, in any order,
    # without requiring the literal word "matrix". GSE150050 is the case.
    "raw_counts": r"raw[_.\- ]?counts?|counts?[_.\- ]?raw|star[_.\- ]?counts?",
    # a bare counts table: "..._counts.csv", "..._counts.txt", "..._counts.tsv"
    "counts_table": r"[_.\- ]counts?\.(csv|tsv|txt|mtx)",
    # 10x triplet member names that the strong pattern only catches when the
    # directory form `_bc_matrix` is present
    "tenx_parts": r"matrix\.mtx|barcodes\.(tsv|csv)|features\.(tsv|csv)",
    # explicit UMI/count matrices written with other separators
    "umi_matrix": r"umi[_.\- ]?matri|count[_.\- ]?tabl",
    # Normalized tables are not raw counts, but they keep the cells x genes
    # shape needed for ncount. The normalization method is irrelevant here.
    "normalized_matrix": r"tpm|cpm|fpkm|rpkm|logtpm|normali[sz]ed|expr(ession)?[_.\- ]?(mat|tab|val)",
    # single-cell experiment containers not in STRONG
    "sce_containers": r"\.h5ad\.gz|\.anndata|\.seurat|singlecellexperiment",
}

#: Excluded even when a positive pattern fires. Keep this narrow: only files
#: that cannot carry a cells x genes shape belong here.
NEGATIVE = r"(metadata|readme|annotation|_qc\.|clustering|\.pdf|\.docx?|filelist)"


def _none_tier() -> dict[str, str]:
    """accession -> geo_target, for evidence == 'none'."""
    out = {}
    with GAP_CSV.open(newline="") as fh:
        for row in csv.DictReader(fh):
            if row["evidence"] == "none":
                out[row["accession"]] = row["geo_target"] or row["accession"]
    return out


def _supp_names(cur, accs: list[str]) -> dict[str, list[str]]:
    """GEO series accession -> supplementary filenames."""
    names: dict[str, list[str]] = {}
    for i in range(0, len(accs), 5000):
        cur.execute(
            "SELECT accession, supplementary_data FROM geo_series "
            "WHERE accession = ANY(%s)",
            (accs[i : i + 5000],),
        )
        for acc, supp in cur.fetchall():
            if not supp:
                continue
            files = []
            for item in supp if isinstance(supp, list) else [supp]:
                txt = item.get("#text") if isinstance(item, dict) else str(item)
                if txt and txt.upper() != "NONE":
                    files.append(txt.rsplit("/", 1)[-1])
            if files:
                names[acc] = files
    return names


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=REPO / "recipes/ncount_gap")
    ap.add_argument("--pg-url", default=os.environ.get("PG_URL"))
    args = ap.parse_args()
    if not args.pg_url:
        # .env is the project convention; read it rather than demanding a flag
        for line in (REPO / ".env").read_text().splitlines():
            if line.startswith("PG_URL"):
                args.pg_url = line.split("=", 1)[1].strip().strip('"')
    if not args.pg_url:
        ap.error("--pg-url or PG_URL required")

    none_tier = _none_tier()
    print(f"evidence=none studies      : {len(none_tier):,}")

    targets = sorted({v for v in none_tier.values() if v.startswith("GSE")})
    print(f"  ... with a GEO target    : {len(targets):,}")

    with psycopg.connect(args.pg_url, connect_timeout=60) as pg, pg.cursor() as cur:
        supp = _supp_names(cur, targets)
    print(f"  ... with supplementary   : {len(supp):,}")

    strong_re = re.compile(STRONG, re.I)
    weak_re = re.compile(WEAK, re.I)
    neg_re = re.compile(NEGATIVE, re.I)
    extra_re = {k: re.compile(v, re.I) for k, v in EXTRA.items()}

    caught: dict[str, str] = {}
    by_pattern: Counter[str] = Counter()
    already: Counter[str] = Counter()

    for acc, files in supp.items():
        # a file only counts if it matches AND is not a normalized/metadata table
        usable = [f for f in files if not neg_re.search(f)]
        if not usable:
            continue
        if any(strong_re.search(f) for f in usable):
            already["would_be_STRONG"] += 1
            continue
        if any(weak_re.search(f) for f in usable):
            already["would_be_WEAK"] += 1
            continue
        for name, rx in extra_re.items():
            if any(rx.search(f) for f in usable):
                caught[acc] = name
                by_pattern[name] += 1
                break

    print("\n-- already matched by the EXISTING patterns (regrade, not new) --")
    for k, v in already.most_common():
        print(f"  {k:20s} {v:6,}")

    print("\n-- newly caught by EXTRA patterns --")
    for k, v in by_pattern.most_common():
        print(f"  {k:20s} {v:6,}")
    print(f"  {'TOTAL':20s} {len(caught):6,}")

    args.out.mkdir(parents=True, exist_ok=True)
    dest = args.out / "none_tier_recoverable.csv"
    with dest.open("w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["geo_target", "pattern"])
        w.writerows(sorted(caught.items()))
    print(f"\nwrote {dest} ({len(caught):,} rows)")

    denom = len(none_tier)
    print(
        f"\nrecoverable share of the none tier: "
        f"{len(caught)}/{denom} = {100 * len(caught) / denom:.1f}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
