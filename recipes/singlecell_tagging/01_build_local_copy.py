#!/usr/bin/env python3
"""01: Build the LOCAL SQLite copy this recipe works on. Reads only.

Nothing here touches production Postgres (it was in crash recovery on
2026-09-29 and the coordinator froze all tunnel reads), and nothing writes the
pipeline's own files. Every source is opened read-only:

  unified_metadata.db   /data/share/seqout/unified_metadata/unified_metadata.db
                        (the pipeline's unify output, mode=ro): canonical rows,
                        aliases, title/description, library strategies
  recall_fix.db         /data/saket/sqlites/recall_fix.db (mode=ro): the
                        pipeline's CURRENT staged single-cell verdicts, built
                        2026-09-26 and never transferred to Postgres
  evidence extracts     read-only production pulls already on disk, made on
                        2026-09-28/29 by seqout-paper/scripts/singlecell_coverage_gaps
                        (02_*/03_* CSVs) and singlecell_audit (01_catalogue.parquet
                        = the live singlecell_study, 42,033 rows)
  external truth        scBaseCount study map (07) and Census collection map (09)

Writes /NAS/saket/github/seqout-ingestion-sctag-local/sctag_local.db (replaced
whole on each run; that file is the only thing this recipe writes besides out/).

Usage: 01_build_local_copy.py            (dry run: prints what it would copy)
       01_build_local_copy.py --apply    (builds the local copy)
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path

import pandas as pd

from _db import EXT, GAPS, LOCAL_DB, LOCAL_DIR, PAPER, PIPELINE_STAGING

UM_DB = Path("/data/share/seqout/unified_metadata/unified_metadata.db")
AUDIT = PAPER / "scripts" / "singlecell_audit" / "out"

CSV_TABLES = {
    # table                  file                               provenance
    "ev_libsource": (
        GAPS / "02_libsource.csv",
        "PG sra/ena/gsa/dra_experiments + arrayexpress_samples, library_source ~ SINGLE CELL, per raw study",
    ),
    "ev_exp_text": (
        GAPS / "02_exp_text.csv",
        "PG sra/dra/ena experiment text, BROAD_PG prefilter, top-3 longest distinct texts per raw study, studies outside singlecell_study",
    ),
    "ev_gsm_text": (
        GAPS / "02_gsm_text.csv",
        "PG geo_samples title/description/channels (Extract-Protocol), BROAD_PG prefilter, top-2 per GSE, outside singlecell_study",
    ),
    "ev_pent": (
        GAPS / "03_pent.csv",
        "PG pentimento_run per raw study, outside singlecell_study",
    ),
    "ev_matrix": (
        GAPS / "03_matrix.csv",
        "PG pentimento_study / pentimento_sample / sc_stats_study counted matrices, outside singlecell_study",
    ),
    "ev_onto": (
        GAPS / "03_onto.csv",
        "PG ontology_samples_v5 single-cell EFO assay, outside singlecell_study",
    ),
    "prod_um_status": (
        GAPS / "03_um_status.csv",
        "PG unified_metadata rows with a non-not_screened status (production flags)",
    ),
    "ext_scbasecount": (
        GAPS / "07_scbasecount_studies.csv",
        "Arc scBaseCount 2026-01-12 study -> seqout canonical and coverage status",
    ),
    "ext_census": (
        GAPS / "09_census_collection_coverage.csv",
        "CZI CELLxGENE Census collections -> seqout canonical and coverage verdict",
    ),
    "ext_scbasecount_srx": (
        EXT / "scbasecount_studies.csv",
        "Arc scBaseCount SRX -> study",
    ),
}


def build(apply: bool) -> None:
    for table, (path, prov) in CSV_TABLES.items():
        print(
            f"{table:22s} <- {path.name:40s} {path.stat().st_size / 1e6:8.1f} MB  {prov[:70]}"
        )
    print(f"{'um':22s} <- {UM_DB} (mode=ro)")
    print(f"{'staged_pipeline':22s} <- {PIPELINE_STAGING}:study_single_cell (mode=ro)")
    print(f"{'prod_singlecell_study':22s} <- {AUDIT / '01_catalogue.parquet'}")
    print(f"{'pg_um_text':22s} <- {GAPS / '03_study_text.csv'} (fld = 'um')")
    if not apply:
        print(f"DRY RUN: would (re)build {LOCAL_DB}")
        return

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    tmp = LOCAL_DB.with_suffix(".building")
    tmp.unlink(missing_ok=True)
    con = sqlite3.connect(tmp)
    con.execute("PRAGMA journal_mode=OFF")
    con.execute("PRAGMA synchronous=OFF")
    t = time.time()

    con.execute(f"ATTACH 'file:{UM_DB}?mode=ro' AS src_um")
    con.execute("""CREATE TABLE um AS SELECT canonical_accession, source, title, description, aliases,
                   dominant_scientific_name, dominant_assay_l1, dominant_assay_l2, library_strategy_counts,
                   n_experiments, n_samples, first_published, pmid FROM src_um.unified_metadata""")
    con.execute("DETACH src_um")
    con.execute("CREATE UNIQUE INDEX um_pk ON um(canonical_accession)")
    # alias -> canonical, the same shape as the matview's alias_map (min canonical wins)
    con.execute("""CREATE TABLE alias_map AS
                   SELECT alias, MIN(canonical_accession) AS canonical_accession FROM (
                       SELECT canonical_accession, canonical_accession AS alias FROM um
                       UNION
                       SELECT u.canonical_accession, j.value FROM um u, json_each(u.aliases) j
                        WHERE u.aliases IS NOT NULL AND j.value <> '' AND j.type = 'text'
                   ) GROUP BY alias""")
    con.execute("CREATE UNIQUE INDEX alias_map_pk ON alias_map(alias)")
    print(f"um + alias_map: {time.time() - t:.0f}s")

    con.execute(f"ATTACH 'file:{PIPELINE_STAGING}?mode=ro' AS src_st")
    con.execute(
        "CREATE TABLE staged_pipeline AS SELECT * FROM src_st.study_single_cell"
    )
    con.execute("DETACH src_st")
    con.execute(
        "CREATE UNIQUE INDEX staged_pipeline_pk ON staged_pipeline(study_accession)"
    )

    cat = pd.read_parquet(
        AUDIT / "01_catalogue.parquet",
        columns=[
            "study_accession",
            "source",
            "single_cell_status",
            "ev_matrix",
            "ev_barcode",
            "ev_r1len",
            "ev_um",
            "dominant_assay_l2",
            "title",
        ],
    )
    cat.to_sql("prod_singlecell_study", con, index=False)
    con.execute(
        "CREATE UNIQUE INDEX prod_sc_pk ON prod_singlecell_study(study_accession)"
    )

    for table, (path, _) in CSV_TABLES.items():
        df = pd.read_csv(path, low_memory=False)
        df.to_sql(table, con, index=False, chunksize=50_000)
        print(f"  {table}: {len(df):,} rows")
    for table, col in [
        ("ev_libsource", "raw_study"),
        ("ev_exp_text", "raw_study"),
        ("ev_gsm_text", "raw_study"),
        ("ev_pent", "raw_study"),
        ("ev_matrix", "raw_study"),
        ("ev_onto", "raw_study"),
        ("prod_um_status", "raw_study"),
        ("ext_scbasecount", "canonical"),
    ]:
        con.execute(f"CREATE INDEX {table}_{col} ON {table}({col})")

    # Postgres title|description for studies outside singlecell_study whose text
    # hits the broad prefilter (a superset of SC_TEXT_REGEX). The text pass runs
    # on this, not on the local unify output, which is newer than Postgres.
    parts = []
    for chunk in pd.read_csv(GAPS / "03_study_text.csv", chunksize=200_000):
        parts.append(chunk[chunk.fld == "um"][["raw_study", "txt"]])
    pd.concat(parts).to_sql("pg_um_text", con, index=False, chunksize=50_000)
    con.execute("CREATE INDEX pg_um_text_k ON pg_um_text(raw_study)")

    con.execute("CREATE TABLE provenance (tbl TEXT, source TEXT, note TEXT)")
    rows = [(k, str(p), n) for k, (p, n) in CSV_TABLES.items()]
    rows += [
        ("um", str(UM_DB), "pipeline unify output, read-only"),
        (
            "staged_pipeline",
            str(PIPELINE_STAGING),
            "pipeline staging study_single_cell, read-only copy",
        ),
        (
            "prod_singlecell_study",
            str(AUDIT / "01_catalogue.parquet"),
            "live singlecell_study 2026-09-29",
        ),
        (
            "pg_um_text",
            str(GAPS / "03_study_text.csv"),
            "PG unified_metadata title|description, BROAD_PG hits,"
            " studies outside singlecell_study",
        ),
    ]
    con.executemany("INSERT INTO provenance VALUES (?,?,?)", rows)
    con.commit()
    con.close()
    tmp.replace(LOCAL_DB)
    print(
        f"built {LOCAL_DB} ({LOCAL_DB.stat().st_size / 1e9:.2f} GB) in {time.time() - t:.0f}s"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--apply", action="store_true", help="build the local copy (default: dry run)"
    )
    build(ap.parse_args().apply)
