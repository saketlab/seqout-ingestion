"""
Populate gea_experiments.organisms from gea_samples.

GEA project pages otherwise show only the single experiment-level organism.

gea_samples.experiment_accession joins to gea_experiments.accession (the
study surface), so one GROUP BY. Small archive: 733 studies, 14 multi-organism
(measured 2026-07-25).

No GIN index: GEA is not part of the federated organism search (no search
branch), so nothing filters on this column -- the project page just reads the
array. Add an index here and a search branch in queries.py together, if GEA ever
joins the facet. No blanking; idempotent; re-runnable.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)

_ADD_COLUMN = "ALTER TABLE gea_experiments ADD COLUMN IF NOT EXISTS organisms text[]"

_POPULATE_FROM_SAMPLES = """
UPDATE gea_experiments g
SET organisms = sub.orgs
FROM (
    SELECT experiment_accession, array_agg(DISTINCT organism) AS orgs
    FROM gea_samples
    WHERE organism IS NOT NULL AND organism <> ''
    GROUP BY experiment_accession
) sub
WHERE g.accession = sub.experiment_accession
"""

_POPULATE_FALLBACK = """
UPDATE gea_experiments
SET organisms = ARRAY[organism]
WHERE organisms IS NULL AND NULLIF(btrim(organism), '') IS NOT NULL
"""


def gea_organisms_transfer(pg_url: str) -> None:
    """Fill gea_experiments.organisms from gea_samples. Idempotent; re-runnable."""
    with psycopg.connect(pg_url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(_ADD_COLUMN)
            logger.info("populating gea_experiments.organisms from samples ...")
            cur.execute(_POPULATE_FROM_SAMPLES)
            logger.info("  %s rows from samples", cur.rowcount)
            cur.execute(_POPULATE_FALLBACK)
            logger.info("  %s rows from organism fallback", cur.rowcount)
    logger.info("gea_organisms transfer done")
