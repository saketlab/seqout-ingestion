#!/usr/bin/env python3
"""01b: Archive study cross-links for the local copy, without Postgres.

The pipeline resolves SRP<->GSE through sra_studies.alias and PRJEB<->ERP
through ena_studies.secondary_study_accession (recall_rules.TWINS_SQL). With
Postgres unavailable, the same links come from NCBI's SRA_Accessions.tab, which
the SRA ingest keeps on local disk: for Type=STUDY rows, `Alias` is the GEO
series for GEO submissions and `BioProject` is the PRJNA/PRJEB/PRJDB project.

Writes table `twins(a, b, kind)` into the local copy, both directions.

Usage: 01b_twins_from_sra_accessions.py [--apply]
"""

from __future__ import annotations

import argparse
import re
import subprocess

from _db import local

TAB = "/data/share/seqout/sra/SRA_Accessions.tab"
GSE = re.compile(r"^GSE\d+$")
PRJ = re.compile(r"^PRJ[A-Z]{1,2}\d+$")


def main(apply: bool) -> None:
    # awk is ~10x faster than Python over 74M lines; it only filters STUDY rows.
    awk = [
        "awk",
        "-F\t",
        '$7=="STUDY" && $1 ~ /^[SED]RP[0-9]+$/ {print $1"\t"$10"\t"$19}',
        TAB,
    ]
    print("scanning", TAB, "(STUDY rows only)")
    if not apply:
        print("DRY RUN: would write table twins into the local copy")
        return
    out = subprocess.run(
        awk, capture_output=True, text=True, check=True, env={"LC_ALL": "C"}
    ).stdout
    rows: set[tuple[str, str, str]] = set()
    for line in out.splitlines():
        acc, alias, bioproject = (line.split("\t") + ["", ""])[:3]
        if GSE.match(alias):
            rows |= {(acc, alias, "sra_alias_gse"), (alias, acc, "sra_alias_gse")}
        if PRJ.match(bioproject):
            rows |= {(acc, bioproject, "bioproject"), (bioproject, acc, "bioproject")}
    con = local()
    con.execute("DROP TABLE IF EXISTS twins")
    con.execute("CREATE TABLE twins (a TEXT, b TEXT, kind TEXT)")
    con.executemany("INSERT INTO twins VALUES (?,?,?)", sorted(rows))
    con.execute("CREATE INDEX twins_a ON twins(a)")
    con.commit()
    for kind, n in con.execute("SELECT kind, COUNT(*) FROM twins GROUP BY 1"):
        print(f"  {kind}: {n:,} directed links")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--apply", action="store_true")
    main(ap.parse_args().apply)
