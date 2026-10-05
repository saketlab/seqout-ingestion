"""One-shot loader: local supplementary_file_gene_corruption staging db -> pg.

Brand-new table (created via sql/tables/excel_gene_corruption_pg.sql), so this
is a plain bulk upsert, not a non-nulling merge -- there is nothing existing to
protect yet. Safe to re-run: ON CONFLICT (accession, url) DO UPDATE keeps it
idempotent for the follow-up load once the requeue scan finishes.

  python transfer_pg.py --db /data/saket/excel_errors/excel_gene_corruption.db
"""

from __future__ import annotations

import argparse
import os
import sqlite3

import psycopg

COLS = [
    "accession",
    "source",
    "url",
    "n_hits",
    "fmt",
    "rows_scanned",
    "truncated",
    "kinds",
    "examples",
    "checked_at",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    rows = [
        (*row[:6], bool(row[6]), *row[7:])
        for row in con.execute(
            f"SELECT {','.join(COLS)} FROM supplementary_file_gene_corruption"
        ).fetchall()
    ]

    with psycopg.connect(os.environ["PG_URL"]) as pg, pg.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO supplementary_file_gene_corruption ({",".join(COLS)})
            VALUES ({",".join(["%s"] * len(COLS))})
            ON CONFLICT (accession, url) DO UPDATE SET
                n_hits=EXCLUDED.n_hits, fmt=EXCLUDED.fmt,
                rows_scanned=EXCLUDED.rows_scanned, truncated=EXCLUDED.truncated,
                kinds=EXCLUDED.kinds, examples=EXCLUDED.examples,
                checked_at=EXCLUDED.checked_at
            """,
            rows,
        )
        pg.commit()
    print(f"upserted {len(rows)} rows into pg:supplementary_file_gene_corruption")


if __name__ == "__main__":
    main()
