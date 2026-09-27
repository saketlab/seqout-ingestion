#!/usr/bin/env python
"""
Find read-called single-cell samples absent from metadata calls.

The read call comes from `pentimento_sample.assay_is_single_cell`; the stated
assay comes from `enriched_samples_v4.assay_category`. Report NULL/empty stated
assays as `unstated`, and non-single-cell stated assays as `contradicted` so a
reviewer can distinguish missing metadata from disagreement.

Spatial Transcriptomics is barcoded but not counted as stated single-cell for
search; it stays in `contradicted`. `assay_is_single_cell IS NULL` means
unmeasured and is excluded from both buckets.

Scope defaults to the GARD rare-disease subset; pass --all-diseases for the
whole corpus.

    uv run python rare_disease/assay_recovered.py
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from find_rare_disease_datasets import LOCAL_PORT, PG_DB, SSH_HOST, ensure_tunnel

from rare_disease.join import mondo_ids, with_subset

logger = logging.getLogger("rare_disease.assay_recovered")

# Closed-vocabulary values treated as stated single-cell.
STATED_SINGLE_CELL = (
    "Single-cell Transcriptomics",
    "Single-cell Epigenomics",
    "Single-cell Multi-omics",
    "Single-cell Genomics",
)

# %(mondo)s is NULL when --all-diseases, which switches the disease filter off
# rather than requiring a second copy of the query.
RECOVERED = with_subset("""
SELECT os.study_accession,
       os.sample,
       COALESCE(os.disease_ontology_name, os.disease, '')     AS disease,
       COALESCE(os.disease_ontology_id, '')                   AS mondo_id,
       COALESCE(nullif(btrim(os.assay_category), ''), '')     AS stated_assay_category,
       COALESCE(nullif(btrim(os.assay), ''), '')              AS stated_assay,
       CASE WHEN nullif(btrim(os.assay_category), '') IS NULL
            THEN 'unstated' ELSE 'contradicted' END           AS mismatch_case,
       COALESCE(p.assay_display, '')                          AS reads_assay,
       COALESCE(p.assay, '')                                  AS reads_assay_raw,
       p.cells,
       p.genes,
       p.sex_verdict,
       p.species_called
FROM ontology_samples_v4 os
JOIN pentimento_sample p ON p.sample_accession = os.sample
WHERE p.assay_is_single_cell IS TRUE
  AND (nullif(btrim(os.assay_category), '') IS NULL
       OR btrim(os.assay_category) <> ALL(%(single_cell)s))
  AND (%(mondo)s::text[] IS NULL OR {MONDO_SUBSET})
ORDER BY os.study_accession, os.sample
""")

HEADER = (
    "study_accession",
    "sample",
    "disease",
    "mondo_id",
    "stated_assay_category",
    "stated_assay",
    "mismatch_case",
    "reads_assay",
    "reads_assay_raw",
    "cells",
    "genes",
    "sex_verdict",
    "species_called",
)


def main() -> int:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--gard-db-file",
        type=Path,
        default=Path("/data/share/seqout/gard/gard.db"),
        help="gard.db from `gard fetch`",
    )
    p.add_argument("--out", type=Path, default=here / "assay_recovered.csv")
    p.add_argument("--all-diseases", action="store_true", help="ignore the GARD subset")
    p.add_argument("--port", type=int, default=LOCAL_PORT)
    p.add_argument("--ssh-host", default=SSH_HOST)
    p.add_argument("--log-level", default="info")
    args = p.parse_args()
    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s"
    )

    ensure_tunnel(args.port, args.ssh_host, timeout=60)
    dsn = f"postgresql://sramap@127.0.0.1:{args.port}/{PG_DB}"

    with psycopg.connect(dsn) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            mondo = None
            if not args.all_diseases:
                if not args.gard_db_file.exists():
                    logger.error(
                        "%s not found -- run `just gard_fetch` first", args.gard_db_file
                    )
                    return 1
                mondo = mondo_ids(args.gard_db_file)
                logger.info("GARD subset: %d MONDO ids", len(mondo))

            cur.execute(
                RECOVERED,
                {"single_cell": list(STATED_SINGLE_CELL), "mondo": mondo},
            )
            rows = cur.fetchall()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        w.writerows(rows)

    # r[6] is mismatch_case, by SELECT position
    unstated = sum(1 for r in rows if r[6] == "unstated")
    logger.info(
        "wrote %s: %d samples the reads call single-cell but the text does not",
        args.out,
        len(rows),
    )
    logger.info("  unstated (no assay_category):        %d", unstated)
    logger.info("  contradicted (non-single-cell text): %d", len(rows) - unstated)
    logger.info("  distinct studies: %d", len({r[0] for r in rows}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
