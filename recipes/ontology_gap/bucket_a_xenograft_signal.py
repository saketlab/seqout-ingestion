"""
Bucket A evidence collection (NOT a clearing gate -- read this before using
the output for anything). Of the samples where a pinned cell line (Hela/
K562/Mcf7/Hepg2/Jurkat/A549/Hct116) sits on a mouse/rat/zebrafish organism --
deferred by guard_cell_line_species / fix_cell_line_species_mismatch.py
because xenograft and EV-transfer experiments using these exact lines into
these organisms are a real, confirmed pattern -- what does the enrichment
text actually say?

A Codex review of an earlier version of this script (see
/data/saket/ontology_gap/codex_review_bucket_a/result.txt) found it unsafe
to treat "no matched text" as "safe to clear": confirmed genuine
cross-species samples (HeLa mixed with mouse stromal cells, A549 co-cultured
with rat pericytes, an HCT116 xenograft host liver, ...) had no matching
text in the 6 fields this script queries, because the real evidence lives in
raw SRA/GEO/ENA sample attributes this pipeline never ingested -- 198,587 of
276,730 rows do not even have a study title joined. It also found: a
field-concatenation bug in the previous version let a regex span across the
" | " join between fields, manufacturing false hits; the EV-abbreviation
regex matched "EV71" (enterovirus) and "empty vector" and missed plural
"EVs" and hyphenated "extracellular-vesicle"; and generic "donor cells"/
"transplant" language matched unrelated bone-marrow and germ-cell-transfer
studies with no connection to any of the 7 pinned lines.

So this version reports three outcomes per row, not two:
  - cross_species_evidence: text suggests a real xenograft/EV-transfer/
    co-culture/mixture experiment (weak evidence -- a lead for review, not
    a verdict; per Codex, even sample_type="Xenograft" pairs with a
    cell_line the LLM invented, so this alone does not prove the pinned
    name is correct either).
  - cell_line_identity_confirmed_in_text: the raw text itself names the
    pinned line in cell-culture terms (e.g. "HeLa cell", "A549 cells") --
    evidence the enrichment call may be right and the ORGANISM annotation
    is what needs review, not the cell line.
  - unresolved: neither signal found. Given how much of Bucket A lacks a
    title at all, this will be most rows -- that is the honest answer with
    the fields available, not a finding that they are safe to clear.

This script writes no Postgres changes and this data must not gate a
backfill on its own. Real disambiguation needs source-level SRA/GEO/ENA
sample attributes and growth protocols, which is separate, larger work.

Usage:
    .venv/bin/python recipes/ontology_gap/bucket_a_xenograft_signal.py
    .venv/bin/python recipes/ontology_gap/bucket_a_xenograft_signal.py --out-dir /data/saket/ontology_gap
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "recipes"))
sys.path.insert(0, str(REPO / "src"))

from pg_connect import connect  # noqa: E402

from enrich.guards import ALLOWED_CELL_LINES  # noqa: E402

BUCKET_A_TAXIDS = ("10090", "10116", "7955")  # mouse, rat, zebrafish

# per Codex's fix: word-boundary "ev"/"evs" that isn't itself part of a
# longer alnum token (blocks "EV71", "EV-D68"); the caller still excludes
# "empty vector" separately since this boundary alone cannot.
_EV = r"(?<![A-Za-z0-9_])evs?(?![A-Za-z0-9_])"

# each category: (label, regex). Applied to ONE field's raw text at a time
# -- never to fields joined together, which let a previous version's
# ".{0,N}" patterns span across unrelated fields (Codex found this
# manufactured false "injection ... cell" hits by matching "Injection" in
# one field and "...cell" from "Hepatocellular" in a different field).
CROSS_SPECIES_CATEGORIES = [
    (
        "xenograft_direct",
        r"(?i)\bxeno[\s-]?graft(?:s|ed|ing)?\b|\b(?:pdx|cdx)s?\b",
    ),
    (
        "vesicle_or_exosome",
        r"(?i)\bextracellular[\s-]+vesicles?\b|\bmicrovesicles?\b|\bexosom(?:es?|al)\b",
    ),
    (
        "ev_abbreviation_near_transfect_or_rna",
        rf"(?i)(?:{_EV}.{{0,30}}(?:transfect|rna)|(?:transfect|rna).{{0,30}}{_EV})",
    ),
    (
        "co_culture_or_conditioned_medium",
        r"(?i)\bco[\s-]?cultur(?:e[ds]?|ing)\b|\btranswell\b"
        r"|\bconditioned[\s-]+medi(?:um|a)\b",
    ),
    (
        "mixture_or_spike_in",
        r"(?i)\bmixed[\s-]+sample\b|\bspike[\s-]?in\b",
    ),
]

# text that, if present alongside an EV hit, means it's a known false
# positive (enterovirus, plasmid) rather than extracellular-vesicle biology.
_EV_FALSE_POSITIVE = re.compile(
    r"(?i)\bev[\s-]?(?:71|d68|a71)\b|\bempty[\s-]+vector\b"
)

# text confirming the pinned line's own identity in cell-culture terms --
# the OPPOSITE signal: organism metadata may be wrong, not the cell line.
CELL_LINE_NAME_PATTERNS = {
    "hela": r"(?i)\bhela\b",
    "k562": r"(?i)\bk[\s-]?562\b",
    "mcf7": r"(?i)\bmcf[\s-]?7\b",
    "hepg2": r"(?i)\bhep[\s-]?g2\b",
    "jurkat": r"(?i)\bjurkat\b",
    "a549": r"(?i)\ba[\s-]?549\b",
    "hct116": r"(?i)\bhct[\s-]?116\b",
}
CELL_CULTURE_CONTEXT_RE = re.compile(
    r"(?i)\bcell[s]?\b|\bcell[\s-]?line\b|\bcancer[\s-]?cells?\b|\bcell[\s-]?cultur"
)

FIELDS = ("treatment", "disease", "phenotype", "genetic_modification", "sample_type", "title")


_COMPILED_CROSS_SPECIES = [(label, re.compile(p)) for label, p in CROSS_SPECIES_CATEGORIES]


def classify(row: dict, cell_key: str) -> tuple[str, str]:
    """(outcome, matched_categories) for one row."""
    field_texts = {f: str(row.get(f) or "") for f in FIELDS}

    cross_hits = []
    for label, rx in _COMPILED_CROSS_SPECIES:
        for text in field_texts.values():
            if not text or not rx.search(text):
                continue
            if "ev_abbreviation" in label and _EV_FALSE_POSITIVE.search(text):
                continue
            cross_hits.append(label)
            break

    name_pattern = CELL_LINE_NAME_PATTERNS.get(cell_key)
    identity_hit = False
    if name_pattern:
        name_rx = re.compile(name_pattern)
        for text in field_texts.values():
            if text and name_rx.search(text) and CELL_CULTURE_CONTEXT_RE.search(text):
                identity_hit = True
                break

    if cross_hits and identity_hit:
        return "cross_species_evidence+identity_confirmed", ";".join(cross_hits)
    if cross_hits:
        return "cross_species_evidence", ";".join(cross_hits)
    if identity_hit:
        return "cell_line_identity_confirmed_in_text", ""
    return "unresolved", ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="/data/saket/ontology_gap")
    args = ap.parse_args()

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select e.study_accession, e.sample, e.cell_line, o.organism,
                   e.treatment, e.disease, e.phenotype, e.genetic_modification,
                   e.sample_type, s.title
            from enriched_samples_v5 e
            join ontology_samples_v5 o using (study_accession, sample)
            left join enriched_studies_v5 s on s.accession = e.study_accession
            where lower(regexp_replace(e.cell_line, '[^a-zA-Z0-9]', '', 'g')) = any(%s)
              and o.taxid = any(%s)
            """,
            (list(ALLOWED_CELL_LINES), list(BUCKET_A_TAXIDS)),
        )
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    counts: dict[str, int] = {}
    no_title = 0
    out_rows = []
    for row in rows:
        if not row.get("title"):
            no_title += 1
        cell_key = re.sub(r"[^a-zA-Z0-9]", "", row["cell_line"]).lower()
        outcome, categories = classify(row, cell_key)
        counts[outcome] = counts.get(outcome, 0) + 1
        out_rows.append(
            {
                "study_accession": row["study_accession"],
                "sample": row["sample"],
                "cell_line": row["cell_line"],
                "organism": row["organism"],
                "outcome": outcome,
                "categories": categories,
            }
        )

    total = len(rows)
    print(f"Bucket A total: {total:,}")
    pct_no_title = 100 * no_title / total
    print(f"rows with no study title joined: {no_title:,} ({pct_no_title:.1f}%)")
    print("\noutcomes (NOT a clearing gate -- see module docstring):")
    for outcome, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {outcome}: {n:,} ({100 * n / total:.1f}%)")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "bucket_a_xenograft_signal.csv"
    fieldnames = [
        "study_accession",
        "sample",
        "cell_line",
        "organism",
        "outcome",
        "categories",
    ]
    with out_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)
    print(f"\nfull classification ({len(out_rows):,} rows) -> {out_path}")


if __name__ == "__main__":
    main()
