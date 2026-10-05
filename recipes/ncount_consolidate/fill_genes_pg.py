#!/usr/bin/env python3
"""
Fill `pentimento_sample.genes` where Postgres holds NULL and staging has a value.

Strictly additive, twice over:
  * the WHERE clause requires `genes IS NULL`, so a populated value is never
    touched -- not even by an identical write
  * the staged value must be non-NULL, so this can never introduce a NULL

That double guard is the point. `transfer/pentimento.py` may write NULLs because
pentimento is rebuilt whole; this scoped patch against a live table follows the
`non_nulling_update` rule instead.

--apply writes; default is a dry run.
"""

from __future__ import annotations

import argparse
import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pg_connect import connect

BATCH = 500


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("db", type=pathlib.Path)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    staged = [
        (s, a, g)
        for s, a, g in con.execute(
            "SELECT study_accession, sample_accession, genes FROM ncount_sample "
            "WHERE scope='sample' AND genes IS NOT NULL"
        )
    ]
    print(f"staged rows with a genes value: {len(staged)}")

    with connect() as pg:
        pg.execute("SET statement_timeout='30min'")
        targets = []
        for i in range(0, len(staged), BATCH):
            chunk = staged[i : i + BATCH]
            live = dict(
                pg.execute(
                    "SELECT sample_accession, genes FROM pentimento_sample "
                    "WHERE sample_accession = ANY(%s)",
                    ([a for _, a, _ in chunk],),
                ).fetchall()
            )
            targets += [(a, g) for _, a, g in chunk if a in live and live[a] is None]
        print(f"pg rows with genes IS NULL that we can fill: {len(targets)}")
        if not targets:
            print("nothing to do")
            return 0
        if not args.apply:
            for a, g in targets[:10]:
                print(f"  {a}  NULL -> {g}")
            print("\nDRY RUN -- nothing written. Re-run with --apply.")
            return 0
        n = 0
        for a, g in targets:
            cur = pg.execute(
                "UPDATE pentimento_sample SET genes = %s "
                "WHERE sample_accession = %s AND genes IS NULL",
                (g, a),
            )
            n += cur.rowcount
        pg.commit()
        print(f"filled {n} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
