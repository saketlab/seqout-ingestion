#!/usr/bin/env python3
"""
Build a quant-only SQLite from a pulled quant dir.

`pentimento build` loads these same tables, but it also requires the full
preflightx inputs. This uses the same `pentimento.sources` loaders for quant-only
refreshes.

`is_background` is not set here. The full build flags near-universal taxa across
the corpus, which needs more than one quant dir. 0 means "not assessed", not
"not background".

Usage: build_quant_sqlite.py <quant_dir> <out.db>
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from helpers import exec_query_from_file  # noqa: E402
from pentimento import sources  # noqa: E402

# load_microbe_quant appends total_reads LAST, but the table has it 11th. Naming
# the columns is not style here: a positional insert shifts every is_* flag by one.
MQ_COLS = (
    "run_accession",
    "class",
    "organism",
    "ref_accession",
    "reads",
    "kmer_mass",
    "n_unitigs",
    "ref_bp",
    "covered_bp",
    "breadth_frac",
    "is_validated_viral",
    "is_viral_evidence",
    "is_bacterial_evidence",
    "is_validated_bacterial",
    "total_reads",
)
PROV_COLS = (
    "run_accession",
    "status",
    "total_reads",
    "read_length",
    "source_url",
    "kmer_size",
    "min_nmatch",
    "min_identity",
    "aligner",
    "panel_ref",
    "viral_gate",
    "bacterial_gate",
    "quantified_at",
)


def main() -> int:
    quant_dir, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.unlink(missing_ok=True)
    con = sqlite3.connect(out)
    exec_query_from_file("tables/pentimento.sql", conn=con)

    con.executemany(
        f"INSERT OR REPLACE INTO microbe_quant ({','.join(MQ_COLS)}) "
        f"VALUES ({','.join('?' * len(MQ_COLS))})",
        sources.load_microbe_quant(quant_dir),
    )
    con.commit()

    prov = list(sources.load_quant_provenance(quant_dir))
    known = {r[0] for r in prov}
    # runs with a .status but no CSV: keeps "not measured" distinct from
    # "measured, nothing found"
    prov += list(sources.load_status_only(quant_dir, known))
    con.executemany(
        f"INSERT OR REPLACE INTO quant_provenance ({','.join(PROV_COLS)}) "
        f"VALUES ({','.join('?' * len(PROV_COLS))})",
        prov,
    )
    con.commit()

    q = lambda s: con.execute(s).fetchone()[0]
    print(
        f"microbe_quant     {q('SELECT count(*) FROM microbe_quant')} rows, "
        f"{q('SELECT count(DISTINCT run_accession) FROM microbe_quant')} runs"
    )
    print(f"quant_provenance  {q('SELECT count(*) FROM quant_provenance')} rows")
    print(
        f"  validated viral {q('SELECT count(*) FROM microbe_quant WHERE is_validated_viral=1')}"
    )
    print(
        f"  viral evidence  {q('SELECT count(*) FROM microbe_quant WHERE is_viral_evidence=1')}"
    )
    print(
        f"  bact evidence   {q('SELECT count(*) FROM microbe_quant WHERE is_bacterial_evidence=1')}"
    )
    print("  provenance by status:")
    for st, n in con.execute(
        "SELECT COALESCE(status,'(null)'), count(*) FROM quant_provenance "
        "GROUP BY 1 ORDER BY 2 DESC LIMIT 8"
    ):
        print(f"    {st[:38]:40s} {n}")
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
