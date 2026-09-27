"""
Backfill: clear cell_line="Hela"(-family) + its Cellosaurus-donated fields
for rows where the RAW submitter record (sra_samples / ena_samples /
dra_samples -- not the *_v5 enrichment output) explicitly declares a
DIFFERENT, specific cell-line/cell-type identity. This is the
"contradicted" tier from `audit_hela_cervix_cluster.py`: 14,089 of the
799,484-row HeLa/cervix cluster, per a fresh Codex investigation
(/data/saket/ontology_gap/codex_review_hela_cervix/result.txt) that
estimated ~88% of the whole cluster is likely misattributed via
`llm_fallback`, with only ~2.6% independently source-supported and the
remaining ~95.6% simply unresolved (no evidence either way -- NOT
clearable on this evidence, left untouched here, same as Bucket A's
"unresolved" rows).

Unlike Bucket A/B, this bucket has POSITIVE contradicting evidence, not
just ambiguity or organism disagreement -- e.g. `ERP125378/ERS5394598`'s
ENA record says "renal proximal tubule epithelial cells hTert1
immortalized"; `ERP125250/ERS5364967` says "Cryopreserved primary human
hepatocytes". Given that, `cell_line` itself is cleared here (unlike
Bucket B, where no correct alternate was derivable and the raw label was
plausibly still genuine) -- the raw record directly says this row is not
HeLa.

Donated fields (tissue, disease, sample_type, age, sex, ethnicity) are
still gated the way the Bucket B rewrite established, after Codex's review
of an earlier, broken version of that gate
(/data/saket/ontology_gaps/codex_review_bucket_b/result.txt): a field is
only cleared when (a) provenance's <field>_source is 'cellosaurus', (b)
<field>_original_source IS NULL -- Cellosaurus was the sole source, no
independent value is at risk of a coincidental match, and (c) the current
value in that specific table matches HeLa's live Cellosaurus donation
under case/whitespace-insensitive comparison (not literal byte equality --
submitted values carry casing/whitespace noise the pipeline doesn't
normalize before storage). Dependent columns (ontology suffixes for
tissue/disease, age derivatives for age) are cleared alongside their
parent, but ONLY for the keys the parent's checked UPDATE actually matched
(via `UPDATE ... RETURNING`) -- a concurrent correction between planning
and this write leaves both the parent and its dependents alone, per a
Codex review of the same race in the Bucket B script
(/data/saket/ontology_gaps/codex_review_bucket_b_v2/result.txt).

A Codex review of this script
(/data/saket/ontology_gaps/codex_review_hela_contradicted/result.txt)
found clearing `enriched_samples_v5.cell_line` unconditionally overreaches:
a raw record contradicting HeLa is strong evidence against the DERIVED
label, but does not prove every independently-populated
`enriched_samples_v5.cell_line='Hela'` value is wrong (mixed-population
samples, aliquot/channel semantics, or a separate study-level source could
all still be legitimately HeLa). So `cell_line` is only cleared in
`ontology_samples_v5`/`cellosaurus_samples_v5` -- the downstream,
Cellosaurus-facing representations -- never in `enriched_samples_v5`,
which is preserved as raw/audit evidence, exactly like Bucket B's
treatment of that table.

Site fields are not touched, same reasoning as the Bucket B rewrite: the
real pipeline derives a sample's primary site from disease, not directly
from the Cellosaurus entry, so a value-matching approach can't reliably
attribute a site value to this donation.

Keyed on (study_accession, sample). Every UPDATE re-checks its own
expected-value WHERE clause at write time, not just the earlier SELECT.
Source-table join coverage (ENA/SRA/DRA rows that don't join at all) and a
near-miss attribute-key count (keys containing "cell" and "line" that
don't match `CELL_LINE_ATTR_KEYS`) are printed on every run per Codex's
review, so silent coverage gaps or key-spelling misses are visible before
`--apply`, not just assumed.

No Postgres write happens until --apply (a dry run still writes a local
manifest CSV to --manifest-dir, same as every other script in this
investigation -- an audit trail of what WOULD be touched, not a database
write). --apply runs every UPDATE in one transaction.

A Codex review of this version
(/data/saket/ontology_gaps/codex_review_hela_contradicted_v2/result.txt)
noted one residual, narrower race: a correction landing between a parent
field's checked UPDATE...RETURNING and its dependent column's UPDATE could
still have that dependent metadata cleared. This is the same category of
residual documented and accepted for the Bucket B script's equivalent fix
(/data/saket/ontology_gaps/codex_review_bucket_b_v3/result.txt) -- the
planning-to-write race the review originally flagged is closed; this
narrower, sub-second window between two immediately sequential statements
in the same transaction is accepted rather than solved with a stronger
lock, consistent with that precedent.

Usage:
    .venv/bin/python recipes/ontology_gap/fix_hela_source_contradicted.py
    .venv/bin/python recipes/ontology_gap/fix_hela_source_contradicted.py --apply
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "recipes"))
sys.path.insert(0, str(REPO / "src"))

from pg_connect import connect  # noqa: E402

from enrich.cellosaurus import load_lookup  # noqa: E402
from enrich.guards import _blank, _entry_donations  # noqa: E402

OUTPUT_DIR = "/data/saket/ontology_gap"

HELA_RE = re.compile(r"(?i)\bhela(?:[\s-]?(?:s3|kyoto|229|ccl-?2))?\b")
CELL_LINE_ATTR_KEYS = ("cell line", "cell_line", "cellline", "cell-line")
_NEAR_MISS_RE = re.compile(r"(?i)cell.*line")

GATED_FIELDS = ("tissue", "disease", "sample_type", "age", "sex", "ethnicity")
AGE_DERIVATIVES = ("age_days", "age_group", "prenatal", "age_raw")
ONTOLOGY_SUFFIXES = (
    "_ontology_id",
    "_ontology_name",
    "_ontology_score",
    "_match_method",
    "_ancestors",
)
TABLES = ("enriched_samples_v5", "ontology_samples_v5", "cellosaurus_samples_v5")
CELL_LINE_CLEAR_TABLES = ("ontology_samples_v5", "cellosaurus_samples_v5")
Key = tuple[str, str]


def classify_ena(cell_line, cell_type, sample_title, tissue_type) -> str:
    text = (cell_line or "").strip()
    if text:
        return "supported" if HELA_RE.search(text) else "contradicted"
    joined = " ".join(t for t in (cell_type, sample_title, tissue_type) if t)
    return "supported" if HELA_RE.search(joined) else "unresolved" if joined.strip() else "unresolved"


def classify_attrs_json(attrs) -> tuple[str, str | None]:
    if not isinstance(attrs, dict):
        return "unresolved", None
    for k, v in attrs.items():
        if str(k).strip().lower() in CELL_LINE_ATTR_KEYS and str(v).strip():
            outcome = "supported" if HELA_RE.search(str(v)) else "contradicted"
            return outcome, f"{k}={v}"
    return "unresolved", None


def find_contradicted(cur: psycopg.Cursor) -> tuple[list[dict], dict]:
    """Re-derive the 'contradicted' tier fresh from Postgres (same logic as
    audit_hela_cervix_cluster.py, restated here so this backfill has no
    dependency on a stale CSV). Also returns join-coverage and near-miss
    attribute-key diagnostics (Codex review: coverage gaps and key-spelling
    misses must be visible, not assumed)."""
    cur.execute(
        """
        select e.study_accession, e.sample, e.cell_line
        from ontology_samples_v5 o
        join enriched_samples_v5 e using (study_accession, sample)
        where o.tissue = 'Uterus, Cervix' and e.cell_line ~* '^hela'
        """
    )
    cols = [d.name for d in cur.description]
    rows = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    by_prefix: dict[str, list[dict]] = {}
    for r in rows:
        by_prefix.setdefault(r["sample"][:3], []).append(r)

    out = []
    diagnostics = {"coverage": {}, "near_miss_keys": {}}

    ers = by_prefix.get("ERS", [])
    if ers:
        samples = [r["sample"] for r in ers]
        cur.execute(
            """
            select secondary_sample_accession, cell_line, cell_type,
                   sample_title, tissue_type
            from ena_samples where secondary_sample_accession = any(%s)
            """,
            (samples,),
        )
        ena_map = {row[0]: row[1:] for row in cur.fetchall()}
        diagnostics["coverage"]["ena_samples"] = (len(ena_map), len(ers))
        for r in ers:
            v = ena_map.get(r["sample"])
            if v and classify_ena(*v) == "contradicted":
                out.append({**r, "evidence": v[0] or " | ".join(str(x) for x in v[1:] if x)})

    for table, prefix in (("sra_samples", "SRS"), ("dra_samples", "DRS")):
        subset = by_prefix.get(prefix, [])
        if not subset:
            continue
        samples = [r["sample"] for r in subset]
        cur.execute(
            f"select accession, attributes_json from {table} where accession = any(%s)",  # noqa: S608
            (samples,),
        )
        attr_map = {row[0]: row[1] for row in cur.fetchall()}
        diagnostics["coverage"][table] = (len(attr_map), len(subset))
        near_miss = 0
        for r in subset:
            attrs = attr_map.get(r["sample"])
            if isinstance(attrs, dict):
                for k in attrs:
                    kl = str(k).strip().lower()
                    if kl not in CELL_LINE_ATTR_KEYS and _NEAR_MISS_RE.search(kl):
                        near_miss += 1
            outcome, evidence = classify_attrs_json(attrs)
            if outcome == "contradicted":
                out.append({**r, "evidence": evidence})
        diagnostics["near_miss_keys"][table] = near_miss

    return out, diagnostics


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


def clear_composite_checked(
    cur: psycopg.Cursor, table: str, field: str, plan: list[tuple[Key, str]]
) -> list[Key]:
    """Clear ``field`` only where it STILL matches ``expected`` (normalized)
    at write time. Returns exactly the keys the UPDATE matched, via
    RETURNING, so dependent columns are only cleared for rows the parent
    update actually touched (closes the race a Codex review found in the
    Bucket B script's equivalent function)."""
    if not plan:
        return []
    studies = [k[0] for k, _ in plan]
    samples = [k[1] for k, _ in plan]
    expected = [v for _, v in plan]
    cur.execute(
        f"""
        update {table} t set {field} = null
        from (select unnest(%s::text[]) as study_accession,
                     unnest(%s::text[]) as sample,
                     unnest(%s::text[]) as expected) k
        where t.study_accession = k.study_accession and t.sample = k.sample
          and lower(trim(t.{field})) = lower(trim(k.expected))
        returning t.study_accession, t.sample
        """,  # noqa: S608
        (studies, samples, expected),
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def clear_dependents(cur: psycopg.Cursor, table: str, field: str, matched: list[Key]) -> None:
    if table != "ontology_samples_v5" or not matched:
        return
    studies = [k[0] for k in matched]
    samples = [k[1] for k in matched]
    deps = [f"{field}{suf}" for suf in ONTOLOGY_SUFFIXES] if field in ("tissue", "disease") else (
        list(AGE_DERIVATIVES) if field == "age" else []
    )
    for dep in deps:
        cur.execute(
            f"""
            update {table} t set {dep} = null
            from (select unnest(%s::text[]) as study_accession,
                         unnest(%s::text[]) as sample) k
            where t.study_accession = k.study_accession and t.sample = k.sample
            """,  # noqa: S608
            (studies, samples),
        )


def clear_cell_line_checked(cur: psycopg.Cursor, table: str, plan: list[Key]) -> int:
    """Clear cell_line only if it STILL normalizes to a HeLa-family name at
    write time (re-checked, not trusted from the earlier SELECT). Only
    called for CELL_LINE_CLEAR_TABLES -- `enriched_samples_v5` is preserved
    as raw/audit evidence, per Codex's review."""
    if not plan:
        return 0
    studies = [k[0] for k in plan]
    samples = [k[1] for k in plan]
    cur.execute(
        f"""
        update {table} t set cell_line = null
        from (select unnest(%s::text[]) as study_accession,
                     unnest(%s::text[]) as sample) k
        where t.study_accession = k.study_accession and t.sample = k.sample
          and t.cell_line ~* '^hela'
        """,  # noqa: S608
        (studies, samples),
    )
    return cur.rowcount


def build_plan(
    cur: psycopg.Cursor, candidates: list[dict], donate: dict
) -> tuple[list[Key], dict[str, dict[str, list[tuple[Key, str]]]]]:
    keys = [(c["study_accession"], c["sample"]) for c in candidates]
    prov_cols = tuple(f"{f}_source" for f in GATED_FIELDS) + tuple(
        f"{f}_original_source" for f in GATED_FIELDS
    )
    prov = fetch_current(cur, keys, prov_cols, "provenance_samples_v5")
    current = {t: fetch_current(cur, keys, GATED_FIELDS, t) for t in TABLES}

    field_plan: dict[str, dict[str, list[tuple[Key, str]]]] = {
        t: {f: [] for f in GATED_FIELDS} for t in TABLES
    }
    for key in keys:
        p = prov.get(key, {})
        for field in GATED_FIELDS:
            donated = donate.get(field)
            if _blank(donated):
                continue
            if p.get(f"{field}_source") != "cellosaurus":
                continue
            if p.get(f"{field}_original_source") is not None:
                continue
            for t in TABLES:
                cur_val = current[t].get(key, {}).get(field)
                if _blank(cur_val):
                    continue
                if str(cur_val).strip().lower() == str(donated).strip().lower():
                    field_plan[t][field].append((key, str(donated)))
    return keys, field_plan


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write; default is dry-run")
    ap.add_argument("--manifest-dir", default=OUTPUT_DIR)
    args = ap.parse_args()

    lookup = load_lookup()
    hela_entry = lookup.get("hela")
    if not hela_entry or hela_entry.get("cvcl_id") != "CVCL_0030":
        print("Live lookup no longer pins 'hela' to CVCL_0030; aborting.")
        return
    donate = _entry_donations(hela_entry)

    with connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            candidates, diagnostics = find_contradicted(cur)
            print(f"source-contradicted candidate rows: {len(candidates):,}")
            print("\nsource-table join coverage (joined / total-in-prefix):")
            for table, (joined, total) in diagnostics["coverage"].items():
                print(f"  {table}: {joined:,} / {total:,}")
            near_miss_total = sum(diagnostics["near_miss_keys"].values())
            if near_miss_total:
                print(f"\nWARNING: {near_miss_total:,} near-miss attribute keys "
                      f"(contain 'cell'+'line' but not an exact CELL_LINE_ATTR_KEYS match): "
                      f"{diagnostics['near_miss_keys']}")
            else:
                print("\nno near-miss attribute keys found")

            keys_seen = {(c["study_accession"], c["sample"]) for c in candidates}
            assert len(keys_seen) == len(candidates), (
                f"duplicate candidate keys: {len(candidates)} rows, {len(keys_seen)} unique"
            )

            keys, field_plan = build_plan(cur, candidates, donate)

            manifest_dir = Path(args.manifest_dir)
            manifest_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            manifest_path = manifest_dir / f"hela_source_contradicted_{ts}.csv"
            with manifest_path.open("w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["study_accession", "sample", "cell_line", "evidence"])
                for c in candidates:
                    w.writerow(
                        [c["study_accession"], c["sample"], c["cell_line"], c["evidence"]]
                    )
            print(f"manifest ({len(candidates):,} rows) -> {manifest_path}")

            print(f"\ncell_line eligible (all {len(keys):,} candidates, re-checked at write):")
            for t in CELL_LINE_CLEAR_TABLES:
                print(f"  {t}.cell_line")
            print(f"  enriched_samples_v5.cell_line: preserved (raw/audit evidence, not cleared)")
            print("\ndonated-field eligible:")
            for t in TABLES:
                for f in GATED_FIELDS:
                    print(f"  {t}.{f}: {len(field_plan[t][f]):,}")

            if not args.apply:
                print("\nDry run only. Re-run with --apply to write.")
                conn.rollback()
                return

            for t in CELL_LINE_CLEAR_TABLES:
                n = clear_cell_line_checked(cur, t, keys)
                print(f"{t}.cell_line cleared: {n:,}")
            for t in TABLES:
                for f in GATED_FIELDS:
                    matched = clear_composite_checked(cur, t, f, field_plan[t][f])
                    print(f"{t}.{f} cleared: {len(matched):,}")
                    clear_dependents(cur, t, f, matched)
            conn.commit()
            print("\ncommitted.")


if __name__ == "__main__":
    main()
