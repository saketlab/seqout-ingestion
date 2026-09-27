"""
Audit: for the "Uterus, Cervix" / HeLa-family cluster in *_v5 (796,786 rows
per Codex's independent count), join each row to its RAW source record
(sra_samples / ena_samples / dra_samples / geo_samples -- not the *_v5
enrichment output) and check whether the source actually says HeLa.

Motivation: a Codex investigation
(/data/saket/ontology_gap/codex_review_hela_cervix/result.txt) estimated
~88% of this cluster is likely misattributed -- cell_line='Hela' came via
`llm_fallback` for 95.9% of rows, and 99.085% have no HeLa/cervical/HPV
corroboration anywhere in the OTHER enriched fields or joined study title.
That investigation used raw SRA/GEO sample records in Postgres for ground
truth (`sra_samples`, `ena_samples`, `dra_samples`, `geo_samples` --
distinct from the *_v5 enrichment tables, and NOT the "new source-attribute
ingestion" Bucket A's writeup assumed would be needed; the data was already
there, just in different tables). This script does the same join
systematically over the whole cluster, not just a sample.

Per-row evidence, by which raw table the sample accession belongs to:
  - ERS (ena_samples): `cell_line` is a submitter-DECLARED column --
    directly comparable, no text parsing needed.
  - SRS/DRS (sra_samples/dra_samples): `attributes_json` is a dict of
    submitter-supplied key/value attributes; look for a 'cell line' or
    'cell_line' key directly (confirmed present in real data, e.g.
    SRS18730180: {'cell line': 'primary culture cell', 'cell type':
    'Neonatal rat cardiomyocytes', ...} -- structured, not free text).
  - GSM (geo_samples): no structured cell-line column; falls back to a
    HeLa-family regex over title + description.
  - Any other prefix (SAM/ERR/free-text names/...): left UNRESOLVED, no
    raw table to join against.

Classification per row:
  - CONTRADICTED: the raw source has a structured cell-line-ish field
    (ENA `cell_line`, or an SRA/DRA attributes_json cell-line key) that is
    non-blank and does NOT match the HeLa-family pattern. Strongest
    signal -- the submitter said something else.
  - SUPPORTED: the raw source's cell-line field (structured or, for GEO,
    title/description text) DOES match the HeLa-family pattern.
  - UNRESOLVED: no raw table joined, or the raw source has no cell-line
    information at all (structured field absent/blank and no text match).

This script writes nothing -- it is read-only, sizing the CONTRADICTED
bucket precisely before any backfill is designed against it.

Usage:
    .venv/bin/python recipes/ontology_gap/audit_hela_cervix_cluster.py
    .venv/bin/python recipes/ontology_gap/audit_hela_cervix_cluster.py --out-dir /data/saket/ontology_gaps
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

HELA_RE = re.compile(r"(?i)\bhela(?:[\s-]?(?:s3|kyoto|229|ccl-?2))?\b")

CELL_LINE_ATTR_KEYS = ("cell line", "cell_line", "cellline")


def classify_ena(cell_line: str | None) -> str:
    if not cell_line or not cell_line.strip():
        return "unresolved"
    return "supported" if HELA_RE.search(cell_line) else "contradicted"


def classify_attrs_json(attrs: dict | None) -> str:
    if not isinstance(attrs, dict):
        return "unresolved"
    val = None
    for k, v in attrs.items():
        if str(k).strip().lower() in CELL_LINE_ATTR_KEYS:
            val = v
            break
    if val is None or not str(val).strip():
        return "unresolved"
    return "supported" if HELA_RE.search(str(val)) else "contradicted"


def classify_text(*texts: str | None) -> str:
    joined = " ".join(t for t in texts if t)
    if not joined.strip():
        return "unresolved"
    return "supported" if HELA_RE.search(joined) else "unresolved"


def fetch_cluster(cur) -> list[dict]:
    cur.execute(
        """
        select e.study_accession, e.sample, e.cell_line
        from ontology_samples_v5 o
        join enriched_samples_v5 e using (study_accession, sample)
        where o.tissue = 'Uterus, Cervix'
          and e.cell_line ~* '^hela'
        """
    )
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def batch_join(cur, table: str, key_col: str, extra_cols: tuple[str, ...], samples: list[str]) -> dict[str, tuple]:
    if not samples:
        return {}
    cols = ", ".join(extra_cols)
    cur.execute(
        f"""
        select {key_col}, {cols} from {table}
        where {key_col} = any(%s)
        """,  # noqa: S608
        (samples,),
    )
    return {row[0]: row[1:] for row in cur.fetchall()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="/data/saket/ontology_gaps")
    args = ap.parse_args()

    with connect() as conn, conn.cursor() as cur:
        rows = fetch_cluster(cur)
        print(f"cluster total: {len(rows):,}")

        by_prefix: dict[str, list[dict]] = {}
        for r in rows:
            prefix = r["sample"][:3]
            by_prefix.setdefault(prefix, []).append(r)

        ers = by_prefix.get("ERS", [])
        srs = by_prefix.get("SRS", [])
        drs = by_prefix.get("DRS", [])
        gsm = by_prefix.get("GSM", [])
        other = [r for p, rs in by_prefix.items() if p not in ("ERS", "SRS", "DRS", "GSM") for r in rs]

        ena_map = batch_join(
            cur,
            "ena_samples",
            "secondary_sample_accession",
            ("cell_line", "cell_type", "sample_title", "tissue_type"),
            [r["sample"] for r in ers],
        )
        sra_map = batch_join(cur, "sra_samples", "accession", ("attributes_json",), [r["sample"] for r in srs])
        dra_map = batch_join(cur, "dra_samples", "accession", ("attributes_json",), [r["sample"] for r in drs])
        geo_map = batch_join(cur, "geo_samples", "accession", ("title", "description"), [r["sample"] for r in gsm])

        results = []
        for r in ers:
            v = ena_map.get(r["sample"])
            if v and v[0] and str(v[0]).strip():
                outcome = classify_ena(v[0])
                evidence = v[0]
            elif v:
                outcome = classify_text(v[1], v[2], v[3])
                evidence = " | ".join(str(x) for x in v[1:] if x)
            else:
                outcome = "unresolved"
                evidence = None
            results.append({**r, "source_table": "ena_samples", "outcome": outcome, "evidence": evidence})
        for r in srs:
            v = sra_map.get(r["sample"])
            outcome = classify_attrs_json(v[0]) if v else "unresolved"
            ev = None
            if v and isinstance(v[0], dict):
                for k, val in v[0].items():
                    if str(k).strip().lower() in CELL_LINE_ATTR_KEYS:
                        ev = f"{k}={val}"
                        break
            results.append({**r, "source_table": "sra_samples", "outcome": outcome, "evidence": ev})
        for r in drs:
            v = dra_map.get(r["sample"])
            outcome = classify_attrs_json(v[0]) if v else "unresolved"
            ev = None
            if v and isinstance(v[0], dict):
                for k, val in v[0].items():
                    if str(k).strip().lower() in CELL_LINE_ATTR_KEYS:
                        ev = f"{k}={val}"
                        break
            results.append({**r, "source_table": "dra_samples", "outcome": outcome, "evidence": ev})
        for r in gsm:
            v = geo_map.get(r["sample"])
            outcome = classify_text(*v) if v else "unresolved"
            results.append({**r, "source_table": "geo_samples", "outcome": outcome, "evidence": (v[0] if v else None)})
        for r in other:
            results.append({**r, "source_table": None, "outcome": "unresolved", "evidence": None})

        counts: dict[str, int] = {}
        for r in results:
            counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
        print("\noutcomes:")
        for k, n in sorted(counts.items(), key=lambda kv: -kv[1]):
            print(f"  {k}: {n:,} ({100 * n / len(results):.2f}%)")

        print("\nby source table:")
        for src in ("ena_samples", "sra_samples", "dra_samples", "geo_samples", None):
            sub = [r for r in results if r["source_table"] == src]
            if not sub:
                continue
            sub_counts: dict[str, int] = {}
            for r in sub:
                sub_counts[r["outcome"]] = sub_counts.get(r["outcome"], 0) + 1
            print(f"  {src}: {len(sub):,} rows -> {sub_counts}")

        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "hela_cervix_cluster_audit.csv"
        fieldnames = ["study_accession", "sample", "cell_line", "source_table", "outcome", "evidence"]
        with out_path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(results)
        print(f"\nfull audit ({len(results):,} rows) -> {out_path}")


if __name__ == "__main__":
    main()
