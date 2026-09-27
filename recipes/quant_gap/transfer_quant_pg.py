#!/usr/bin/env python3
"""
Scoped, additive transfer of quant results into Postgres.

This is not `transfer pentimento`: the staging DB was built only from the quant
dir, so its preflightx columns are NULL artifacts. It touches only
pentimento_microbe and pentimento_run.quant_status/total_reads, never deletes,
never writes NULL, and never touches preflightx columns or cells/genes.

--apply writes; default is a dry run that reports the same counts.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sqlite3
import sys

import psycopg
from psycopg import sql

BATCH = 5000


def pg_url() -> str:
    for line in pathlib.Path(".env").read_text().splitlines():
        if re.match(r"^\s*(?:export\s+)?PG_URL\s*=", line):
            return line.split("=", 1)[1].strip().strip("\"'")
    msg = "PG_URL not found in .env"
    raise SystemExit(msg)


MICROBE_COLS = (
    "run_accession",
    "organism",
    "class",
    "reads",
    "kmer_mass",
    "n_unitigs",
    "ref_bp",
    "covered_bp",
    "breadth_frac",
    "is_validated_viral",
    "is_viral_evidence",
    "is_validated_bacterial",
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db", type=pathlib.Path)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument(
        "--skip-microbe",
        action="store_true",
        help="microbe rows already committed; do the run columns only",
    )
    args = ap.parse_args()

    lite = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn = psycopg.connect(pg_url(), connect_timeout=30)
    conn.autocommit = False

    before = conn.execute("SELECT count(*) FROM pentimento_microbe").fetchone()[0]
    print(f"pentimento_microbe before: {before}")

    # pentimento_microbe: additive upsert
    ins = sql.SQL(
        "INSERT INTO pentimento_microbe ({cols}) VALUES ({ph}) "
        "ON CONFLICT (run_accession, organism) DO UPDATE SET {sets}"
    ).format(
        cols=sql.SQL(",").join(map(sql.Identifier, MICROBE_COLS)),
        ph=sql.SQL(",").join(sql.Placeholder() * len(MICROBE_COLS)),
        sets=sql.SQL(",").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
            for c in MICROBE_COLS[2:]
        ),
    )
    if args.skip_microbe:
        print("microbe phase skipped (already committed)")
        src = iter(())
    else:
        src = lite.execute(
            "SELECT run_accession, organism, class, reads, kmer_mass, n_unitigs, "
            "ref_bp, covered_bp, breadth_frac, is_validated_viral, "
            "is_viral_evidence, is_validated_bacterial FROM microbe_quant"
        )
    n = 0
    with conn.cursor() as cur:
        while chunk := (src.fetchmany(BATCH) if hasattr(src, "fetchmany") else []):
            rows = [(*r[:9], bool(r[9]), bool(r[10]), bool(r[11])) for r in chunk]
            if args.apply:
                cur.executemany(ins, rows)
            n += len(rows)
            if n % 100_000 == 0:
                print(f"  microbe: {n} rows staged")
                if args.apply:
                    conn.commit()
    if args.apply:
        conn.commit()
    print(f"microbe rows sent: {n}")

    # pentimento_run: quant columns only, never nulling
    # No `%s IS NOT NULL` guard: a bare parameter there has no type context and
    # Postgres rejects it ("could not determine data type of parameter"). The SELECT
    # below already excludes NULL status, and COALESCE keeps a NULL total_reads from
    # overwriting. The IS DISTINCT FROM clause makes a re-run a no-op.
    upd = (
        "UPDATE pentimento_run SET quant_status = %s, "
        "total_reads = COALESCE(%s, total_reads) "
        "WHERE run_accession = %s "
        "AND (quant_status IS DISTINCT FROM %s "
        "OR total_reads IS DISTINCT FROM COALESCE(%s, total_reads))"
    )
    prov = lite.execute(
        "SELECT run_accession, status, total_reads FROM quant_provenance "
        "WHERE status IS NOT NULL"
    )
    m = 0
    with conn.cursor() as cur:
        while chunk := prov.fetchmany(BATCH):
            if args.apply:
                cur.executemany(upd, [(s, t, r, s, t) for r, s, t in chunk])
            m += len(chunk)
            if args.apply and m % 20_000 == 0:
                conn.commit()
                print(f"  run: {m} rows applied")
    if args.apply:
        conn.commit()
    print(f"run quant rows sent: {m}")

    after = conn.execute("SELECT count(*) FROM pentimento_microbe").fetchone()[0]
    print(f"pentimento_microbe after: {after}  (delta {after - before})")
    if not args.apply:
        conn.rollback()
        print("\nDRY RUN -- nothing written. Re-run with --apply.")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
