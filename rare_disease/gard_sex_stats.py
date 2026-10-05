#!/usr/bin/env python
"""
Subset the corpus to GARD's rare diseases and report sex composition per disease.

Everything comes from Postgres. Two sex columns live there, and they are separate
measurements, so this reports them separately and then against each other:

  - `ontology_samples_v4.sex` is STATED sex, curated from submitted metadata.
  - `pentimento_sample.sex_verdict` is READ-DERIVED sex, called by preflightx off
    Y-unique and XIST read counts.

Stated sex comes from ontology_samples_v4, not enriched_samples_v4, because it
has broader coverage. Revisit the source choice if the preflight backfill makes
read-derived sex dense.

Keep them apart: collapsing the columns hides potential mislabels. Agreement
counts only where both sides made a call; unstated sex and preflightx no-calls
are abstentions.

Subset by MONDO id, not disease text; see gard.join. Rows group by the resolved
ontology term, so a disease's spellings collapse into one row.

Forwards to the server's unix socket: peer-authenticated, so no password.
Read-only; nothing here writes.

    uv run python rare_disease/gard_sex_stats.py
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

from rare_disease.join import gard_labels, mondo_ids, with_subset

logger = logging.getLogger("rare_disease.gard_sex")

# Stated sex is free text; these are the spellings worth folding. Anything else
# that is non-empty is counted as `stated_other` rather than silently dropped.
MALE = ("male", "m", "man", "boy")
FEMALE = ("female", "f", "woman", "girl")

# preflightx verdicts that are real calls. 'unknown'/'uncertain' are abstentions.
CALLED = ("male", "female")

STATS = with_subset("""
SELECT COALESCE(os.disease_ontology_id, '')                       AS mondo_id,
       COALESCE(os.disease_ontology_name, os.disease, '')         AS disease,
       count(*)                                                   AS n_samples,
       count(DISTINCT os.study_accession)                         AS n_studies,
       count(*) FILTER (WHERE lower(btrim(os.sex)) = ANY(%(male)s))   AS stated_male,
       count(*) FILTER (WHERE lower(btrim(os.sex)) = ANY(%(female)s)) AS stated_female,
       count(*) FILTER (WHERE nullif(btrim(os.sex), '') IS NOT NULL
                          AND lower(btrim(os.sex)) <> ALL(%(male)s)
                          AND lower(btrim(os.sex)) <> ALL(%(female)s)) AS stated_other,
       count(*) FILTER (WHERE nullif(btrim(os.sex), '') IS NULL)      AS stated_missing,
       count(*) FILTER (WHERE p.sex_verdict = 'male')                 AS reads_male,
       count(*) FILTER (WHERE p.sex_verdict = 'female')               AS reads_female,
       count(*) FILTER (WHERE p.sex_verdict IN ('unknown', 'uncertain'))
                                                                      AS reads_nocall,
       count(*) FILTER (WHERE nullif(btrim(p.sex_verdict), '') IS NULL)
                                                                      AS reads_missing,
       count(*) FILTER (WHERE p.sex_verdict = ANY(%(called)s)
                          AND (lower(btrim(os.sex)) = ANY(%(male)s)
                            OR lower(btrim(os.sex)) = ANY(%(female)s))
                          AND p.sex_verdict = CASE
                                WHEN lower(btrim(os.sex)) = ANY(%(male)s) THEN 'male'
                                ELSE 'female' END)                    AS agree,
       count(*) FILTER (WHERE p.sex_verdict = ANY(%(called)s)
                          AND (lower(btrim(os.sex)) = ANY(%(male)s)
                            OR lower(btrim(os.sex)) = ANY(%(female)s))
                          AND p.sex_verdict <> CASE
                                WHEN lower(btrim(os.sex)) = ANY(%(male)s) THEN 'male'
                                ELSE 'female' END)                    AS disagree,
       sum(COALESCE(os.cell_count, 0))                                AS cells
FROM ontology_samples_v4 os
LEFT JOIN pentimento_sample p ON p.sample_accession = os.sample
WHERE {MONDO_SUBSET}
GROUP BY 1, 2
""")

# Studies must be counted DISTINCT in one pass. Summing the per-disease n_studies
# double-counts every study carrying two GARD diseases, which is common: one
# study annotated with both a syndrome and its subtype.
DISTINCT_STUDIES = with_subset("""
SELECT count(DISTINCT os.study_accession) FROM ontology_samples_v4 os
WHERE {MONDO_SUBSET}
""")

HEADER = (
    "mondo_id",
    "disease",
    "gard_id",
    "gard_name",
    "n_studies",
    "n_samples",
    "cells",
    "stated_male",
    "stated_female",
    "stated_other",
    "stated_missing",
    "reads_male",
    "reads_female",
    "reads_nocall",
    "reads_missing",
    "both_called",
    "agree",
    "disagree",
    "pct_agree",
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
    p.add_argument("--out", type=Path, default=here / "gard_sex_stats.csv")
    p.add_argument("--port", type=int, default=LOCAL_PORT)
    p.add_argument("--ssh-host", default=SSH_HOST)
    p.add_argument("--log-level", default="info")
    args = p.parse_args()
    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s"
    )

    if not args.gard_db_file.exists():
        logger.error("%s not found -- run `just gard_fetch` first", args.gard_db_file)
        return 1

    mondo = mondo_ids(args.gard_db_file)
    labels = gard_labels(args.gard_db_file)
    logger.info("%d MONDO ids from %s", len(mondo), args.gard_db_file)

    ensure_tunnel(args.port, args.ssh_host, timeout=60)
    dsn = f"postgresql://sramap@127.0.0.1:{args.port}/{PG_DB}"

    with psycopg.connect(dsn) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute(
                STATS,
                {
                    "mondo": mondo,
                    "male": list(MALE),
                    "female": list(FEMALE),
                    "called": list(CALLED),
                },
            )
            stats = cur.fetchall()

            cur.execute(DISTINCT_STUDIES, {"mondo": mondo})
            n_studies_distinct = cur.fetchone()[0]

    rows = []
    for (
        mondo_id,
        disease,
        n_samples,
        n_studies,
        s_m,
        s_f,
        s_o,
        s_na,
        r_m,
        r_f,
        r_nc,
        r_na,
        agree,
        disagree,
        cells,
    ) in stats:
        # A row reached through an ANCESTOR has its own MONDO id, which is not in
        # the GARD list; label it as unlabelled rather than inventing a parent.
        gard_id, gard_name = labels.get(mondo_id, ("", ""))
        both = agree + disagree
        rows.append(
            (
                mondo_id,
                disease,
                gard_id,
                gard_name,
                n_studies,
                n_samples,
                cells or 0,
                s_m,
                s_f,
                s_o,
                s_na,
                r_m,
                r_f,
                r_nc,
                r_na,
                both,
                agree,
                disagree,
                f"{100 * agree / both:.1f}" if both else "",
            )
        )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        w.writerows(sorted(rows, key=lambda x: -x[5]))

    def tot(i: int) -> int:
        return sum(x[i] for x in rows)

    both, agree = tot(15), tot(16)
    logger.info(
        "wrote %s: %d MONDO terms with data, %d distinct studies "
        "(%d study/disease pairs), %d samples",
        args.out,
        len(rows),
        n_studies_distinct,
        tot(4),
        tot(5),
    )
    logger.info(
        "stated sex:       male=%d female=%d other=%d missing=%d",
        tot(7),
        tot(8),
        tot(9),
        tot(10),
    )
    logger.info(
        "read-derived sex: male=%d female=%d no_call=%d not_measured=%d",
        tot(11),
        tot(12),
        tot(13),
        tot(14),
    )
    logger.info(
        "both called: %d, agree %d (%.1f%%), disagree %d",
        both,
        agree,
        100 * agree / both if both else 0.0,
        tot(17),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
