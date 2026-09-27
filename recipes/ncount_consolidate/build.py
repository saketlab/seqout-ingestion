#!/usr/bin/env python3
"""
Consolidate the ncount and ncount_assay pull dirs into one SQLite.

Both jobs emit the same per-study JSON, so this is the cells+genes merge the
handoff has been asking for. It is a staging artifact only -- nothing here writes
to Postgres.

Two things the merge has to get right:

1. **scope.** ncount rows are `scope="sample"`, keyed by a real GSM. Assay
   rows can be `scope="study"`; keep and count them separately, but do not
   transfer them as samples.

2. **precedence on overlap.** A study counted by both jobs keeps the row carrying
   more information: genes beats NULL, and sample-scope beats study-scope. Never
   let a later NULL overwrite an earlier value.

Usage: build.py <out.db> <pull_dir> [<pull_dir> ...]
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

STUDY_COLS = (
    "accession",
    "source",
    "aliases",
    "single_cell_count",
    "single_cell_file_format",
    "n_samples",
    "n_gsm_samples",
    "unfiltered",
    "has_metadata",
    "has_celltype",
    "has_donor",
    "has_demographics",
    "origin",
)
SAMPLE_COLS = (
    "study_accession",
    "sample_accession",
    "scope",
    "cells",
    "genes",
    "nnz",
    "file_format",
    "unfiltered",
    "origin",
)


def better(new: tuple, old: tuple) -> bool:
    """True if `new` should replace `old`. Information never decreases."""
    # (study, sample, scope, cells, genes, nnz, fmt, unfiltered, origin)
    _, _, n_scope, n_cells, n_genes, n_nnz, *_ = new
    _, _, o_scope, o_cells, o_genes, o_nnz, *_ = old

    def rank(scope, cells, genes, nnz):
        return (scope == "sample", genes is not None, nnz is not None, cells or 0)

    return rank(n_scope, n_cells, n_genes, n_nnz) > rank(
        o_scope, o_cells, o_genes, o_nnz
    )


def main() -> int:
    out, dirs = Path(sys.argv[1]), [Path(p) for p in sys.argv[2:]]
    studies: dict[str, tuple] = {}
    samples: dict[tuple[str, str], tuple] = {}
    stats = {"files": 0, "unparsable": 0, "empty": 0, "study_scope_dropped": 0}

    for d in dirs:
        origin = d.name
        for f in sorted(d.rglob("*.json")):
            stats["files"] += 1
            try:
                j = json.loads(f.read_text())
            except (json.JSONDecodeError, UnicodeDecodeError):
                stats["unparsable"] += 1
                continue
            acc = j.get("accession") or f.stem
            row = tuple(j.get(c) for c in STUDY_COLS[:-1]) + (origin,)
            # a study with counts beats one without
            prev = studies.get(acc)
            if prev is None or (j.get("single_cell_count") or 0) > (prev[3] or 0):
                studies[acc] = row
            per = j.get("per_sample") or []
            if not per:
                stats["empty"] += 1
            for s in per:
                # scope="study" repeats the study accession, not a per-sample ID.
                # Drop it here so rebuilds cannot invent downstream samples; the
                # count remains in ncount_study.single_cell_count.
                if s.get("scope") == "study" or (s.get("sample") or acc) == acc:
                    stats["study_scope_dropped"] += 1
                    continue
                key = (acc, s.get("sample") or acc)
                new = (
                    acc,
                    s.get("sample") or acc,
                    s.get("scope"),
                    s.get("cells"),
                    s.get("genes"),
                    s.get("nnz"),
                    s.get("file_format"),
                    s.get("unfiltered"),
                    origin,
                )
                if key not in samples or better(new, samples[key]):
                    samples[key] = new

    out.unlink(missing_ok=True)
    con = sqlite3.connect(out)
    con.execute(
        f"CREATE TABLE ncount_study ({','.join(STUDY_COLS)}, PRIMARY KEY (accession))"
    )
    con.execute(
        f"CREATE TABLE ncount_sample ({','.join(SAMPLE_COLS)}, "
        "PRIMARY KEY (study_accession, sample_accession))"
    )
    con.executemany(
        f"INSERT INTO ncount_study VALUES ({','.join('?' * len(STUDY_COLS))})",
        studies.values(),
    )
    con.executemany(
        f"INSERT INTO ncount_sample VALUES ({','.join('?' * len(SAMPLE_COLS))})",
        samples.values(),
    )
    con.commit()

    q = lambda s: con.execute(s).fetchone()
    print(f"files read        {stats['files']}  (unparsable {stats['unparsable']})")
    print(f"studies           {len(studies)}")
    print(f"  with no samples {stats['empty']}")
    print(f"samples           {len(samples)}")
    print(f"  study-scope dropped {stats['study_scope_dropped']}")
    print(
        f"  scope=sample    {q('SELECT count(*) FROM ncount_sample WHERE scope="sample"')[0]}"
    )
    print(
        f"  scope=study     {q('SELECT count(*) FROM ncount_sample WHERE scope="study"')[0]}"
    )
    print(f"  with cells      {q('SELECT count(cells) FROM ncount_sample')[0]}")
    print(f"  with genes      {q('SELECT count(genes) FROM ncount_sample')[0]}")
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
