"""
Backfill: clear the Cellosaurus-donated fields for a pinned set of cell-line
names that `enrich.cellosaurus.load_lookup()` cannot resolve unambiguously --
each name has multiple real, distinct registered lines sharing one short
name, and the flat name -> one-entry lookup always returns the same wrong
default regardless of which study is asking (confirmed per-name by checking
each default entry's canonical_name/tissue/disease directly). Whether every
name in the list has NO correct registered entry at all (as originally
assumed for MEF/N2) or simply resolves to the wrong one of several real
candidates does not matter for this script: either way, the live default is
demonstrably not the one this row means.

A Codex review of the first version of this script (see
/data/saket/ontology_gaps/codex_review_bucket_b/result.txt) found it unsafe
to apply: `provenance_samples_v5.<field>_source` is not per-table, so
"current value equals what the wrong entry would donate" was often
coincidence, not causation -- e.g. an independently, correctly extracted
`disease="Glioblastoma"` (source `llm_fallback`) happened to also equal one
ambiguous name's wrong donated disease, and the first version would have
deleted it. Concretely: of the fields that matched under that gate, every
single `sample_type` value and the overwhelming majority of `disease`/
`tissue` values had a real `<field>_original_source` (llm_fallback,
llm_primary, embedding, ...), meaning the field held independently-sourced
content the Cellosaurus donation was laid on top of, not created from
scratch. Value equality alone cannot tell the two cases apart, since the
DB never stored which CVCL accession actually produced a given value.

So this version narrows to the one case that IS provably safe: a field
whose `<field>_original_source` is NULL. That means Cellosaurus was the
SOLE source that ever populated the field for this row -- there is no prior
independent value being overwritten, coincidentally or otherwise, so
clearing a Cellosaurus-sourced field with no other source of information
cannot delete anything except the (already-established-wrong) donation
itself. This shrinks the safe set considerably (from ~62k eligible rows in
the first version to a few thousand per field) but each remaining row is
now defensible field by field, not merely by value comparison.

Also dropped from the first version, per the same review:
  - `tissue_primary_site`/`tissue_site_type`: the real pipeline derives a
    metastatic sample's primary site FROM disease (extract.py), not
    directly from the Cellosaurus entry's own site fields the way
    `_entry_donations()` predicts -- so a value-matching approach cannot
    reliably tell whether a stored site value came from the wrong entry or
    from this separate disease-driven derivation. Not attempted here.
  - Any field whose non-blank donation happens to equal a non-blank current
    value is still required (`_matches_donation`), but a BLANK donation is
    no longer treated as eligible even when the current value is also
    blank -- clearing a blank field that was never truly informative is a
    no-op at best and, per the review, could still touch orphaned ontology
    annotation columns for no reason.

Does NOT clear `cell_line` or `organism`/`taxid`: no per-row column tracks
organism's source at all (confirmed against `information_schema.columns`),
so there's no way to gate a clear on it safely. `cell_line` is left alone
too -- not because it is certainly a raw submitted label (the review found
counterexamples: `SRP351393/SRS11341134`'s "B16" was extracted from an
antibody name, and destination tables can retain a donated canonical name
like "F12-3" rather than the raw label) but because, absent that
provenance, there is no safe way to tell which case applies for a given row
either.

Keyed throughout on (study_accession, sample); `sample` alone is reused
across an estimated 1.38M different studies. Every UPDATE re-checks the
expected current value in its own WHERE clause (not just in the earlier
SELECT), closing the check-then-act race the review flagged for a
READ COMMITTED connection.

Read-only until --apply; --apply runs everything in one transaction.

Usage:
    .venv/bin/python recipes/ontology_gap/fix_ambiguous_cell_line_donation.py
    .venv/bin/python recipes/ontology_gap/fix_ambiguous_cell_line_donation.py --apply
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
from enrich.guards import _blank, _entry_donations  # noqa: E402

OUTPUT_DIR = "/data/saket/ontology_gap"

# Every one of these keys currently resolves, in the live lookup, to a
# different real cell line than the one actually meant for this data (see
# module docstring). Whether the correct identity is also registered in
# Cellosaurus under a different key is irrelevant to this script.
AMBIGUOUS_NAMES = (
    "mef",
    "s2",
    "e14",
    "j1",
    "n2",
    "f123",
    "kh2",
    "b16",
    "r1",
    "p19",
    "hm1",
    "sk1",
    "cl",
)

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
Key = tuple[str, str]


def find_candidates(cur: psycopg.Cursor, lookup: dict) -> list[dict]:
    """One dict per (study_accession, sample) whose cell_line normalizes to
    one of the ambiguous names, carrying that name's current (wrong) entry."""
    cur.execute(
        """
        select e.study_accession, e.sample, e.cell_line
        from enriched_samples_v5 e
        where e.cell_line is not null and e.cell_line != ''
        """
    )
    out = []
    for study_accession, sample, cell_line in cur.fetchall():
        key = normalize_cell_line_name(cell_line)
        if key not in AMBIGUOUS_NAMES:
            continue
        entry = lookup.get(key)
        if entry is None:
            continue  # lookup no longer has this key: nothing to gate against
        out.append(
            {
                "study_accession": study_accession,
                "sample": sample,
                "cell_line": cell_line,
                "key": key,
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


def clear_composite_checked(
    cur: psycopg.Cursor,
    table: str,
    field: str,
    plan: list[tuple[Key, str]],
) -> list[Key]:
    """Clear ``field`` only where it STILL equals ``expected`` at write time
    (re-checked in the UPDATE's own WHERE, not just in the earlier SELECT --
    closes the check-then-act race a READ COMMITTED connection allows).
    Returns exactly the keys the UPDATE actually matched, via RETURNING, so
    a dependent column (ontology suffix, age derivative) is only cleared
    alongside a parent value that was truly touched -- a concurrent
    correction between planning and this UPDATE now leaves BOTH the parent
    and its dependents alone, not just the parent."""
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


def clear_field(
    cur: psycopg.Cursor, table: str, field: str, plan: list[tuple[Key, str]]
) -> int:
    matched = clear_composite_checked(cur, table, field, plan)
    if table == "ontology_samples_v5" and field in ("tissue", "disease"):
        for suf in ONTOLOGY_SUFFIXES:
            clear_unconditional(cur, table, f"{field}{suf}", matched)
    if table == "ontology_samples_v5" and field == "age":
        for dep in AGE_DERIVATIVES:
            clear_unconditional(cur, table, dep, matched)
    return len(matched)


def clear_unconditional(
    cur: psycopg.Cursor, table: str, field: str, keys: list[Key]
) -> int:
    """Dependent columns (ontology suffixes, age derivatives) have no
    donation value of their own to re-check; clear alongside their parent
    once the parent's own re-check has passed."""
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


def build_plan(
    cur: psycopg.Cursor, candidates: list[dict]
) -> dict[str, dict[str, list[tuple[Key, str]]]]:
    """Per-table-per-field (key, expected_current_value) clear plan.

    A field is only planned for clearing when, for that row:
      (a) provenance's <field>_source is exactly 'cellosaurus', AND
      (b) provenance's <field>_original_source is NULL -- Cellosaurus was
          the only thing that ever populated this field, so there is no
          independent value a coincidental match could be shadowing, AND
      (c) the ambiguous name's current (wrong) entry actually donates a
          non-blank value for this field, AND
      (d) that table's current value for the field equals it exactly.
    """
    keys = [(c["study_accession"], c["sample"]) for c in candidates]
    prov_cols = tuple(f"{f}_source" for f in GATED_FIELDS) + tuple(
        f"{f}_original_source" for f in GATED_FIELDS
    )
    prov = fetch_current(cur, keys, prov_cols, "provenance_samples_v5")
    current = {t: fetch_current(cur, keys, GATED_FIELDS, t) for t in TABLES}

    plan: dict[str, dict[str, list[tuple[Key, str]]]] = {
        t: {f: [] for f in GATED_FIELDS} for t in TABLES
    }

    for c in candidates:
        key = (c["study_accession"], c["sample"])
        donate = _entry_donations(c["entry"])
        p = prov.get(key, {})
        for field in GATED_FIELDS:
            donated = donate.get(field)
            if _blank(donated):
                continue  # nothing to attribute the current value to
            if p.get(f"{field}_source") != "cellosaurus":
                continue
            if p.get(f"{field}_original_source") is not None:
                continue  # a real prior source existed: match could be coincidence
            for t in TABLES:
                cur_val = current[t].get(key, {}).get(field)
                if _blank(cur_val):
                    continue
                if str(cur_val).strip().lower() == str(donated).strip().lower():
                    plan[t][field].append((key, str(donated)))
    return plan


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
            print(f"ambiguous names: {sorted(AMBIGUOUS_NAMES)}")
            print(f"candidate (study_accession, sample) rows: {len(candidates):,}")

            plan = build_plan(cur, candidates)

            manifest_dir = Path(args.manifest_dir)
            manifest_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            manifest_path = manifest_dir / f"ambiguous_cell_line_fix_{ts}.csv"
            with manifest_path.open("w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["table", "field", "study_accession", "sample", "expected_current_value"])
                for t in TABLES:
                    for f in GATED_FIELDS:
                        for (sa, sm), expected in plan[t][f]:
                            w.writerow([t, f, sa, sm, expected])
            n_planned = sum(
                len(plan[t][f]) for t in TABLES for f in GATED_FIELDS
            )
            print(f"manifest ({n_planned:,} field-level clears) -> {manifest_path}")

            for t in TABLES:
                for f in GATED_FIELDS:
                    print(f"  {t}.{f} eligible: {len(plan[t][f]):,}")

            if not args.apply:
                print("\nDry run only. Re-run with --apply to write.")
                conn.rollback()
                return

            for t in TABLES:
                for f in GATED_FIELDS:
                    n = clear_field(cur, t, f, plan[t][f])
                    print(f"{t}.{f} cleared: {n:,}")
            conn.commit()
            print("\ncommitted.")


if __name__ == "__main__":
    main()
