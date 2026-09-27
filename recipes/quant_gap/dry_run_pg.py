#!/usr/bin/env python3
"""
Report what quant + ncount staging DBs would change in Postgres.

Read-only: every statement is a SELECT. Reports new, fill, agree, conflict, and
would_null buckets for microbe detections, run provenance, and sample counts;
conflict needs review, and would_null must not be written.

Usage: dry_run_pg.py <quant.db> <ncount.db>
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

SSH = [
    "ssh",
    "-i",
    "/home/saket/.ssh/id_ed25519",
    "-o",
    "BatchMode=yes",
    "ubuntu@167.114.208.177",
]


def psql(sql: str) -> list[list[str]]:
    out = subprocess.run(
        [*SSH, f"sudo -u postgres psql -d pysradb --csv -t -c {json.dumps(sql)}"],
        capture_output=True,
        text=True,
        timeout=3600,
        check=True,
    ).stdout
    return [r for r in csv.reader(io.StringIO(out)) if r]


def ro(p: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{p}?mode=ro", uri=True)


def main() -> int:
    qdb, ndb = ro(Path(sys.argv[1])), ro(Path(sys.argv[2]))

    # pentimento_microbe
    staged = {
        (r, o)
        for r, o in qdb.execute("SELECT run_accession, organism FROM microbe_quant")
    }
    live = {
        (r[0], r[1])
        for r in psql("SELECT run_accession, organism FROM pentimento_microbe")
    }
    print("=== pentimento_microbe (run x organism) ===")
    print(f"  staged {len(staged)}  live {len(live)}")
    print(f"  new                {len(staged - live)}")
    print(f"  already present    {len(staged & live)}")
    print(f"  live-only (kept, never deleted) {len(live - staged)}")

    # pentimento_run.quant_status
    sq = {
        r: (s, t)
        for r, s, t in qdb.execute(
            "SELECT run_accession, status, total_reads FROM quant_provenance"
        )
    }
    lq = {
        r[0]: (r[1] or None, r[2] or None)
        for r in psql(
            "SELECT run_accession, COALESCE(quant_status,''), COALESCE(total_reads::text,'') "
            "FROM pentimento_run WHERE quant_status IS NOT NULL"
        )
    }
    v: Counter[str] = Counter()
    conflicts = []
    for run, (st, _tr) in sq.items():
        cur = lq.get(run)
        if cur is None:
            v["run absent or quant_status NULL -> fill"] += 1
        elif not st and cur[0]:
            v["would_null"] += 1
        elif st == cur[0]:
            v["agree"] += 1
        elif st and cur[0]:
            v["CONFLICT"] += 1
            if len(conflicts) < 6:
                conflicts.append((run, cur[0], st))
    print("\n=== pentimento_run.quant_status ===")
    print(f"  staged {len(sq)}  live rows with a status {len(lq)}")
    for k in sorted(v):
        print(f"  {k:38s} {v[k]}")
    for run, old, new in conflicts:
        print(f"    {run}: {old[:30]} -> {new[:30]}")

    # pentimento_sample cells/genes
    sn = {
        (s, a): (c, g)
        for s, a, c, g in ndb.execute(
            "SELECT study_accession, sample_accession, cells, genes FROM ncount_sample"
        )
    }
    ln = {
        (r[0], r[1]): (r[2] or None, r[3] or None)
        for r in psql(
            "SELECT study_accession, sample_accession, COALESCE(cells::text,''), "
            "COALESCE(genes::text,'') FROM pentimento_sample"
        )
    }
    w: Counter[str] = Counter()
    for key, (c, g) in sn.items():
        cur = ln.get(key)
        if cur is None:
            w["new sample (needs INSERT via build)"] += 1
            continue
        for col, mine, theirs in (("cells", c, cur[0]), ("genes", g, cur[1])):
            theirs = int(theirs) if theirs else None
            if mine is None and theirs is not None:
                w[f"{col}:would_null"] += 1
            elif mine is not None and theirs is None:
                w[f"{col}:fill"] += 1
            elif mine == theirs:
                w[f"{col}:agree"] += 1
            else:
                w[f"{col}:CONFLICT"] += 1
    print("\n=== pentimento_sample (cells/genes) ===")
    print(f"  staged {len(sn)}  live {len(ln)}")
    for k in sorted(w):
        print(f"  {k:38s} {w[k]}")

    print("\nNOTHING WAS WRITTEN. This script only runs SELECTs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
