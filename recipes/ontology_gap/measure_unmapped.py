"""
Measure the tissue/disease -> ontology mapping gap directly from Postgres.

Read-only. For each of {tissue, disease}, reports:
  - total samples with non-empty raw text
  - how many have no *_ontology_id
  - the top N unmapped raw strings by sample count (the patterns worth
    designing a matching-rule fix around)

disease additionally excludes "Healthy"/"Control" (not diseases, correctly
unmapped) from the "real gap" count, but still lists them if they show up in
the top-N dump so nothing is hidden.

Usage:
    .venv/bin/python recipes/ontology_gap/measure_unmapped.py --top 1000
    .venv/bin/python recipes/ontology_gap/measure_unmapped.py --field tissue --out /tmp/tissue.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pg_connect import connect  # noqa: E402

NOT_DISEASE = {"healthy", "control"}

FIELDS = {
    "tissue": ("tissue", "tissue_ontology_id"),
    "disease": ("disease", "disease_ontology_id"),
}


def measure(cur, field: str, raw_col: str, id_col: str, top: int) -> dict:
    cur.execute(
        f"""
        select count(*) filter (where {raw_col} is not null and {raw_col} != '')
        from ontology_samples_v5
        """,  # noqa: S608
    )
    total = cur.fetchone()[0]

    cur.execute(
        f"""
        select count(*)
        from ontology_samples_v5
        where {raw_col} is not null and {raw_col} != '' and {id_col} is null
        """,  # noqa: S608
    )
    unmapped = cur.fetchone()[0]

    cur.execute(
        f"""
        select {raw_col}, count(*) as n
        from ontology_samples_v5
        where {raw_col} is not null and {raw_col} != '' and {id_col} is null
        group by {raw_col}
        order by n desc
        limit %s
        """,  # noqa: S608
        (top,),
    )
    rows = cur.fetchall()

    real_gap = unmapped
    if field == "disease":
        real_gap = unmapped - sum(n for v, n in rows if v.strip().lower() in NOT_DISEASE)
        # rows beyond `top` that are Healthy/Control would also need excluding,
        # so pull their exact counts directly instead of trusting the top-N dump.
        cur.execute(
            f"""
            select count(*)
            from ontology_samples_v5
            where {raw_col} is not null and {id_col} is null
              and lower(trim({raw_col})) = any(%s)
            """,  # noqa: S608
            (list(NOT_DISEASE),),
        )
        excluded = cur.fetchone()[0]
        real_gap = unmapped - excluded

    return {
        "field": field,
        "total_with_text": total,
        "unmapped": unmapped,
        "real_gap": real_gap,
        "top_rows": rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--field", choices=sorted(FIELDS), help="default: both")
    ap.add_argument("--top", type=int, default=1000)
    ap.add_argument("--out-dir", default="/data/saket/ontology_gap")
    args = ap.parse_args()

    fields = [args.field] if args.field else list(FIELDS)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with connect() as conn, conn.cursor() as cur:
        for field in fields:
            raw_col, id_col = FIELDS[field]
            result = measure(cur, field, raw_col, id_col, args.top)

            print(f"\n== {field} ==")
            print(f"total with non-empty text: {result['total_with_text']:,}")
            print(f"unmapped:                  {result['unmapped']:,}")
            print(f"real gap (excl noise):     {result['real_gap']:,}")

            out_path = out_dir / f"unmapped_{field}.csv"
            with out_path.open("w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow([raw_col, "n_samples"])
                w.writerows(result["top_rows"])
            print(f"top {args.top} unmapped strings -> {out_path}")


if __name__ == "__main__":
    main()
