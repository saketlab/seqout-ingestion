"""Load merge_results.py's excel_gene_errors.csv into supplementary_file_gene_corruption.

  python recipes/excel_genes/load_corruption_table.py excel_gene_errors.csv \
      --db excel_gene_corruption.db [--checked-at 2026-09-16]

Applies sql/tables/excel_gene_corruption.sql if the table doesn't exist yet,
then upserts one row per (accession, url). `source` is read off the
accession's prefix (see PREFIX_SOURCE) -- everything in the CSV is one of
geo/arrayexpress/gea/gsa, per the scan's own scope.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import sqlite3
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCHEMA_FILE = HERE.parent.parent / "sql" / "tables" / "excel_gene_corruption.sql"

PREFIX_SOURCE = [
    ("GSE", "geo"),
    ("E-GEAD-", "gea"),
    ("E-", "arrayexpress"),  # E-MTAB-, E-MEXP-, E-TABM-, E-SMDB-, E-PROT-, E-SNGR-, ...
    ("CRA", "gsa"),
]


def source_for(accession: str) -> str:
    for prefix, source in PREFIX_SOURCE:
        if accession.startswith(prefix):
            return source
    msg = f"unrecognized accession prefix: {accession!r}"
    raise ValueError(msg)


def parse_kinds(raw: str) -> str | None:
    if not raw:
        return None
    kinds = {}
    for part in raw.split(";"):
        if not part:
            continue
        k, _, v = part.partition("=")
        kinds[k] = int(v)
    return json.dumps(kinds)


def parse_examples(raw: str) -> str | None:
    if not raw:
        return None
    pairs = []
    for part in raw.split(";"):
        if not part:
            continue
        value, _, count = part.rpartition("x")
        pairs.append([value, int(count)])
    return json.dumps(pairs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_file", type=Path)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--checked-at", default=datetime.date.today().isoformat())
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.executescript(SCHEMA_FILE.read_text())

    n = 0
    with args.csv_file.open(newline="") as fh:
        for row in csv.DictReader(fh):
            con.execute(
                """
                INSERT INTO supplementary_file_gene_corruption
                    (accession, source, url, n_hits, fmt, rows_scanned,
                     truncated, kinds, examples, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (accession, url) DO UPDATE SET
                    n_hits=excluded.n_hits, fmt=excluded.fmt,
                    rows_scanned=excluded.rows_scanned, truncated=excluded.truncated,
                    kinds=excluded.kinds, examples=excluded.examples,
                    checked_at=excluded.checked_at
                """,
                (
                    row["accession"],
                    source_for(row["accession"]),
                    row["url"],
                    int(row["n_hits"]),
                    row["fmt"] or None,
                    int(row["rows_scanned"]) if row["rows_scanned"] else None,
                    row["truncated"] == "True",
                    parse_kinds(row["kinds"]),
                    parse_examples(row["examples"]),
                    args.checked_at,
                ),
            )
            n += 1
    con.commit()
    print(f"loaded {n} rows into {args.db}:supplementary_file_gene_corruption")


if __name__ == "__main__":
    main()
