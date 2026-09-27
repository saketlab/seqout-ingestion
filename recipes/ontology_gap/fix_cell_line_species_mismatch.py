"""
Backfill: clear cell_line + its Cellosaurus-donated fields for a pinned,
verified-unambiguous allowlist of cell-line names crossed with a pinned,
verified allowlist of organisms with no xenograft/co-culture/EV-transfer link
to any of them. Uses the exact same two allowlists and the exact same
donation-value gating as enrich.guards.guard_cell_line_species (imported
directly, not restated), so the live guard and this backfill can never
diverge on what counts as safe.

Applied directly to the already-transferred *_v5 Postgres tables, keyed
throughout on the real primary key (study_accession, sample) -- `sample`
alone is reused across an estimated 1.38M different studies in this data and
is NOT a safe join/update key by itself.

A field is only cleared in a given table when, in THAT table specifically:
  (a) its <field>_source in provenance_samples_v5 still reads 'cellosaurus', and
  (b) its current value there still equals exactly what the pinned entry
      donated (so a later correction, or a value that only ever lived in one
      of the three tables, is left alone).
`cell_line` itself is only cleared in a table when that table's own
cell_line value still normalizes to the same pinned key -- a table where it
was already manually corrected is left alone.

Read-only until --apply; --apply runs everything in one transaction.

Usage:
    .venv/bin/python recipes/ontology_gap/fix_cell_line_species_mismatch.py
    .venv/bin/python recipes/ontology_gap/fix_cell_line_species_mismatch.py --apply
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "recipes"))
sys.path.insert(0, str(REPO / "src"))

from pg_connect import connect  # noqa: E402

from enrich.cellosaurus import load_lookup, normalize_cell_line_name  # noqa: E402
from enrich.guards import (  # noqa: E402
    ALLOWED_CELL_LINES,
    SPECIES_SAFE_TAXIDS,
    _entry_donations,
    _matches_donation,
)

OUTPUT_DIR = "/data/saket/ontology_gap"

GATED_FIELDS = ("tissue", "disease", "sample_type", "age", "sex", "ethnicity")
SITE_FIELDS = ("tissue_primary_site", "tissue_site_type")
AGE_DERIVATIVES = ("age_days", "age_group", "prenatal", "age_raw")
ONTOLOGY_SUFFIXES = (
    "_ontology_id",
    "_ontology_name",
    "_ontology_score",
    "_match_method",
    "_ancestors",
)
TABLES = ("enriched_samples_v5", "ontology_samples_v5", "cellosaurus_samples_v5")
TABLE_FIELDS = {
    "enriched_samples_v5": GATED_FIELDS,
    "ontology_samples_v5": (*GATED_FIELDS, *SITE_FIELDS),
    "cellosaurus_samples_v5": (*GATED_FIELDS, *SITE_FIELDS),
}
Key = tuple[str, str]


def find_candidates(cur: psycopg.Cursor, lookup: dict) -> list[dict]:
    """One dict per (study_accession, sample) on the pinned x safe-organism grid."""
    cur.execute(
        """
        select e.study_accession, e.sample, e.cell_line, o.taxid, o.organism
        from enriched_samples_v5 e
        join ontology_samples_v5 o using (study_accession, sample)
        where e.cell_line is not null and e.cell_line != ''
          and o.taxid = any(%s)
        """,
        (list(SPECIES_SAFE_TAXIDS),),
    )
    out = []
    for study_accession, sample, cell_line, taxid, organism in cur.fetchall():
        key = normalize_cell_line_name(cell_line)
        pinned_cvcl = ALLOWED_CELL_LINES.get(key)
        if pinned_cvcl is None:
            continue
        entry = lookup.get(key)
        if entry is None or entry.get("cvcl_id") != pinned_cvcl:
            continue  # lookup no longer agrees with the pin: abstain
        out.append(
            {
                "study_accession": study_accession,
                "sample": sample,
                "cell_line": cell_line,
                "key": key,
                "taxid": taxid,
                "organism": organism,
                "entry": entry,
            }
        )
    return out


def fetch_current(
    cur: psycopg.Cursor, keys: list[Key], fields: tuple[str, ...], table: str
) -> dict[Key, dict]:
    if not keys or not fields:
        return {}
    studies = [k[0] for k in keys]
    samples = [k[1] for k in keys]
    cols = ", ".join(fields)
    cur.execute(
        f"""
        select t.study_accession, t.sample, {cols}
        from {table} t
        join (select unnest(%s::text[]) as study_accession,
                     unnest(%s::text[]) as sample) k
          on t.study_accession = k.study_accession and t.sample = k.sample
        """,  # noqa: S608
        (studies, samples),
    )
    return {
        (sa, sm): dict(zip(fields, vals, strict=True))
        for sa, sm, *vals in cur.fetchall()
    }


def clear_composite(
    cur: psycopg.Cursor, table: str, field: str, keys: list[Key]
) -> int:
    if not keys:
        return 0
    studies = [k[0] for k in keys]
    samples = [k[1] for k in keys]
    cur.execute(
        f"""
        update {table} t set {field} = null
        from (select unnest(%s::text[]) as study_accession,
                     unnest(%s::text[]) as sample) k
        where t.study_accession = k.study_accession and t.sample = k.sample
        """,  # noqa: S608
        (studies, samples),
    )
    return cur.rowcount


def clear_field(cur: psycopg.Cursor, table: str, field: str, keys: list[Key]) -> int:
    n = clear_composite(cur, table, field, keys)
    if table == "ontology_samples_v5" and field in ("tissue", "disease"):
        for suf in ONTOLOGY_SUFFIXES:
            clear_composite(cur, table, f"{field}{suf}", keys)
    if table == "ontology_samples_v5" and field == "age":
        for dep in AGE_DERIVATIVES:
            clear_composite(cur, table, dep, keys)
    return n


def build_eligibility(
    cur: psycopg.Cursor, candidates: list[dict]
) -> tuple[dict[str, list[Key]], dict[str, dict[str, list[Key]]]]:
    """Per-table cell_line keys, and per-table-per-field donated-value-clear keys."""
    keys = [(c["study_accession"], c["sample"]) for c in candidates]
    prov = fetch_current(
        cur, keys, tuple(f"{f}_source" for f in GATED_FIELDS), "provenance_samples_v5"
    )
    current = {t: fetch_current(cur, keys, TABLE_FIELDS[t], t) for t in TABLES}
    cur_cell_line = {t: fetch_current(cur, keys, ("cell_line",), t) for t in TABLES}

    cell_line_eligible = {t: [] for t in TABLES}
    field_eligible = {t: {f: [] for f in TABLE_FIELDS[t]} for t in TABLES}

    for c in candidates:
        key = (c["study_accession"], c["sample"])
        donate = _entry_donations(c["entry"])
        p = prov.get(key, {})
        for t in TABLES:
            cl = cur_cell_line[t].get(key, {}).get("cell_line")
            if cl is not None and normalize_cell_line_name(cl) == c["key"]:
                cell_line_eligible[t].append(key)
            cur_vals = current[t].get(key, {})
            for field in TABLE_FIELDS[t]:
                gate_field = "tissue" if field in SITE_FIELDS else field
                if p.get(f"{gate_field}_source") != "cellosaurus":
                    continue
                if not _matches_donation(cur_vals.get(field), donate.get(field)):
                    continue
                field_eligible[t][field].append(key)
    return cell_line_eligible, field_eligible


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write; default is dry-run")
    ap.add_argument("--manifest-dir", default=OUTPUT_DIR)
    args = ap.parse_args()

    lookup = load_lookup()
    if not lookup:
        print("Cellosaurus lookup table not readable; aborting.")
        return

    with connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            candidates = find_candidates(cur, lookup)
            print(f"pinned cell lines: {sorted(ALLOWED_CELL_LINES)}")
            print(f"safe organisms: {sorted(SPECIES_SAFE_TAXIDS.values())}")
            print(f"candidate (study_accession, sample) rows: {len(candidates):,}")

            manifest_dir = Path(args.manifest_dir)
            manifest_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            manifest_path = manifest_dir / f"cell_line_species_fix_{ts}.csv"
            with manifest_path.open("w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(
                    ["study_accession", "sample", "cell_line", "row_taxid", "organism"]
                )
                for c in candidates:
                    w.writerow(
                        [
                            c["study_accession"],
                            c["sample"],
                            c["cell_line"],
                            c["taxid"],
                            c["organism"],
                        ]
                    )
            print(f"manifest ({len(candidates):,} rows) -> {manifest_path}")

            cell_line_eligible, field_eligible = build_eligibility(cur, candidates)
            for t in TABLES:
                print(f"  {t}.cell_line eligible: {len(cell_line_eligible[t]):,}")
                for f in TABLE_FIELDS[t]:
                    print(f"  {t}.{f} eligible: {len(field_eligible[t][f]):,}")

            if not args.apply:
                print("\nDry run only. Re-run with --apply to write.")
                conn.rollback()
                return

            for t in TABLES:
                n = clear_composite(cur, t, "cell_line", cell_line_eligible[t])
                print(f"{t}.cell_line cleared: {n:,}")
                for f in TABLE_FIELDS[t]:
                    n = clear_field(cur, t, f, field_eligible[t][f])
                    print(f"{t}.{f} cleared: {n:,}")
            conn.commit()
            print("\ncommitted.")


if __name__ == "__main__":
    main()
