"""
Populate ``dra_studies_search.organisms`` (text[]) so the federated organism
facet includes DRA. Unlike the other sources DRA needs the SRA-shape hop:
``dra_experiments.samples[]`` (an array of sample accessions) -> ``dra_samples``.

Only the search surface is populated. The DRA *project page* already derives its
organisms live in PROJECT_DRA_SQL via the same hop, so there is no column on
dra_studies to maintain -- this is facet-only. dra_studies_search is the
DDBJ-exclusive twin (1,178 rows), so the aggregation is cheap.

No GIN index: the organism facet filters the materialized `matched` temp table,
not this base table (same as the ENA/GSA search columns). Idempotent; re-runnable.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)

_ADD_COLUMN = "ALTER TABLE dra_studies_search ADD COLUMN IF NOT EXISTS organisms text[]"

_POPULATE = """
UPDATE dra_studies_search dss
SET organisms = sub.orgs
FROM (
    SELECT e.study, array_agg(DISTINCT sm.scientific_name) AS orgs
    FROM dra_experiments e
    JOIN dra_samples sm ON sm.accession = ANY(e.samples)
    WHERE sm.scientific_name IS NOT NULL AND sm.scientific_name <> ''
    GROUP BY e.study
) sub
WHERE dss.accession = sub.study
"""


def dra_organisms_transfer(pg_url: str) -> None:
    """Fill dra_studies_search.organisms from the experiment->sample hop."""
    with psycopg.connect(pg_url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(_ADD_COLUMN)
            logger.info("populating dra_studies_search.organisms ...")
            cur.execute(_POPULATE)
            logger.info("  %s rows populated", cur.rowcount)
    logger.info("dra_organisms transfer done")
