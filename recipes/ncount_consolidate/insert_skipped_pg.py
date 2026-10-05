#!/usr/bin/env python3
"""
Land the rows `insert_samples_pg.py` skipped, without breaking the GSM uniqueness rule.

Two disjoint groups in skipped_ambiguous.csv:

  multi_study_in_staging
      The same GSM appears under a SubSeries and its SuperSeries. Collapse to one
      row per GSM by information content: genes beats no genes, then higher cells,
      then higher nnz.

  already_in_pg
      The GSM already exists under a different study. Do not insert a second row;
      only fill NULL measurements without touching study attribution.

--apply writes; default is a dry run.
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import re

import psycopg

COLS = (
    "study_accession",
    "sample_accession",
    "cells",
    "genes",
    "nnz",
    "file_format",
    "unfiltered",
)


def pg_url() -> str:
    for line in pathlib.Path(".env").read_text().splitlines():
        if re.match(r"^\s*(?:export\s+)?PG_URL\s*=", line):
            return line.split("=", 1)[1].strip().strip("\"'")
    raise SystemExit("PG_URL not found in .env")


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def rank(r: dict) -> tuple:
    """More information wins. Never let a blanker row displace a fuller one."""
    return (
        as_int(r["genes"]) is not None,
        as_int(r["nnz"]) is not None,
        as_int(r["cells"]) or 0,
        as_int(r["nnz"]) or 0,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_file", type=pathlib.Path)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    rows = list(csv.DictReader(args.csv_file.open()))
    multi = [r for r in rows if r["reason"] == "multi_study_in_staging"]
    inpg = [r for r in rows if r["reason"] != "multi_study_in_staging"]

    best: dict[str, dict] = {}
    for r in multi:
        gsm = r["sample_accession"]
        if gsm not in best or rank(r) > rank(best[gsm]):
            best[gsm] = r
    print(f"multi_study: {len(multi)} rows -> {len(best)} unique GSMs")

    conn = psycopg.connect(pg_url(), connect_timeout=30)
    before = conn.execute("SELECT count(*) FROM pentimento_sample").fetchone()[0]
    print(f"pentimento_sample before {before}")

    ins = (
        f"INSERT INTO pentimento_sample ({','.join(COLS)}) "
        f"VALUES ({','.join(['%s'] * len(COLS))}) "
        "ON CONFLICT (study_accession, sample_accession) DO NOTHING"
    )
    payload = [
        (
            r["study_accession"],
            r["sample_accession"],
            as_int(r["cells"]),
            as_int(r["genes"]),
            as_int(r["nnz"]),
            r["file_format"] or None,
            str(r["unfiltered"]) in ("1", "True", "true"),
        )
        for r in best.values()
    ]

    # fill-only: a NULL in pg becomes our value; a value in pg is never touched
    upd = (
        "UPDATE pentimento_sample SET "
        "cells = COALESCE(cells, %s), genes = COALESCE(genes, %s), "
        "nnz = COALESCE(nnz, %s) "
        "WHERE sample_accession = %s "
        "AND (cells IS NULL OR genes IS NULL OR nnz IS NULL)"
    )
    fills = [
        (
            as_int(r["cells"]),
            as_int(r["genes"]),
            as_int(r["nnz"]),
            r["sample_accession"],
        )
        for r in inpg
    ]

    if not args.apply:
        print(f"would INSERT {len(payload)} new GSMs")
        print(f"would attempt fill-only UPDATE on {len(fills)} existing GSMs")
        print("\nDRY RUN -- nothing written. Re-run with --apply.")
        conn.close()
        return 0

    with conn.cursor() as cur:
        for i in range(0, len(payload), 2000):
            cur.executemany(ins, payload[i : i + 2000])
            conn.commit()
        mid = conn.execute("SELECT count(*) FROM pentimento_sample").fetchone()[0]
        print(f"inserted: +{mid - before}")
        filled = 0
        for i in range(0, len(fills), 2000):
            cur.executemany(upd, fills[i : i + 2000])
            filled += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            conn.commit()
    after = conn.execute("SELECT count(*) FROM pentimento_sample").fetchone()[0]
    print(f"pentimento_sample after  {after}  (+{after - before})")
    print(f"fill-only updates applied (last batch rowcount only): {filled}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
