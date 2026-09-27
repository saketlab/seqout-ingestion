#!/usr/bin/env python3
"""
Insert new ncount samples into pentimento_sample, additively.

`pentimento_sample` is unique on `sample_accession`, because cohort search joins
counts by sample. GEO SuperSeries/SubSeries can place one GSM under more than
one GSE, so this inserts only rows that are new to Postgres and unique in
staging. Skipped rows go to skipped_ambiguous.csv.

--apply writes; default is a dry run.
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import re
import sqlite3

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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db", type=pathlib.Path)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    lite = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    rows = lite.execute(
        "SELECT study_accession, sample_accession, cells, genes, nnz, "
        "file_format, unfiltered FROM ncount_sample"
    ).fetchall()

    seen: dict[str, int] = {}
    for r in rows:
        seen[r[1]] = seen.get(r[1], 0) + 1
    multi = {a for a, n in seen.items() if n > 1}

    conn = psycopg.connect(pg_url(), connect_timeout=30)
    accs = sorted(seen)
    live: dict[str, str] = {}
    for i in range(0, len(accs), 10000):
        vals = ",".join("('" + a.replace("'", "''") + "')" for a in accs[i : i + 10000])
        for a, st in conn.execute(
            f"SELECT p.sample_accession, p.study_accession FROM pentimento_sample p "  # noqa: S608
            f"JOIN (VALUES {vals}) v(a) ON v.a = p.sample_accession"
        ):
            live[a] = st

    clean, skipped = [], []
    for r in rows:
        gsm = r[1]
        if gsm in multi:
            skipped.append((*r, "multi_study_in_staging"))
        elif gsm in live:
            skipped.append((*r, f"already_in_pg_under_{live[gsm]}"))
        else:
            clean.append(r)

    print(f"staged rows            {len(rows)}")
    print(f"  insertable (new GSM) {len(clean)}")
    print(f"  skipped ambiguous    {len(skipped)}")
    out = args.db.with_name("skipped_ambiguous.csv")
    with out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([*COLS, "reason"])
        w.writerows(skipped)
    print(f"  -> {out}")

    before = conn.execute("SELECT count(*) FROM pentimento_sample").fetchone()[0]
    print(f"pentimento_sample before {before}")

    if args.apply:
        ins = (
            f"INSERT INTO pentimento_sample ({','.join(COLS)}) "
            f"VALUES ({','.join(['%s'] * len(COLS))}) "
            "ON CONFLICT (study_accession, sample_accession) DO NOTHING"
        )
        with conn.cursor() as cur:
            for i in range(0, len(clean), 5000):
                cur.executemany(
                    ins,
                    [(*r[:6], bool(r[6])) for r in clean[i : i + 5000]],
                )
                conn.commit()
                if (i + 5000) % 20000 == 0:
                    print(f"  inserted through {i + 5000}")
        after = conn.execute("SELECT count(*) FROM pentimento_sample").fetchone()[0]
        print(f"pentimento_sample after  {after}  (+{after - before})")
    else:
        print("\nDRY RUN -- nothing written. Re-run with --apply.")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
