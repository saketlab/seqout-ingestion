"""
Populate gsa_studies.organisms from gsa_samples.

GSA project pages and the organism search facet otherwise see only the single
study-level scientific_name. gsa_samples.study_accession joins 1:1 to
gsa_studies.study_accession (both CRA*), so this is one GROUP BY with no
experiments hop. Measured on prod 2026-07-25: 19,182 studies with samples,
1,194 multi-organism.

gsa_studies is a Postgres-side derived table (study_accession PK + embedding
columns), not built from a static DDL, so this step owns the column via
ADD COLUMN IF NOT EXISTS. No blanking: a study with no sample organism keeps its
scientific_name. Idempotent and re-runnable.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)

_ADD_COLUMN = "ALTER TABLE gsa_studies ADD COLUMN IF NOT EXISTS organisms text[]"

_POPULATE_FROM_SAMPLES = """
UPDATE gsa_studies gs
SET organisms = sub.orgs
FROM (
    SELECT study_accession, array_agg(DISTINCT scientific_name) AS orgs
    FROM gsa_samples
    WHERE scientific_name IS NOT NULL AND scientific_name <> ''
    GROUP BY study_accession
) sub
WHERE gs.study_accession = sub.study_accession
"""

_POPULATE_FALLBACK = """
UPDATE gsa_studies
SET organisms = ARRAY[scientific_name]
WHERE organisms IS NULL AND NULLIF(btrim(scientific_name), '') IS NOT NULL
"""

_CREATE_GIN = (
    "CREATE INDEX IF NOT EXISTS idx_gsa_studies_organisms_gin "
    "ON gsa_studies USING gin(organisms)"
)

# The federated search + organism facet read gsa_studies_search.organisms, not
# gsa_studies. Mirror the multi-valued set there (it was previously single-valued)
# -- see _SEARCH_MATCH_CTE in the server's queries.py.
_COPY_TO_SEARCH = """
UPDATE gsa_studies_search gss
SET organisms = gs.organisms
FROM gsa_studies gs
WHERE gs.study_accession = gss.accession
"""


def gsa_organisms_transfer(pg_url: str) -> None:
    """Fill gsa_studies.organisms from gsa_samples. Idempotent; safe to re-run."""
    with psycopg.connect(pg_url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(_ADD_COLUMN)
            logger.info("populating gsa_studies.organisms from samples ...")
            cur.execute(_POPULATE_FROM_SAMPLES)
            logger.info("  %s rows from samples", cur.rowcount)
            cur.execute(_POPULATE_FALLBACK)
            logger.info("  %s rows from scientific_name fallback", cur.rowcount)
            cur.execute(_CREATE_GIN)
            cur.execute(_COPY_TO_SEARCH)
            logger.info("  %s rows copied to gsa_studies_search", cur.rowcount)
    logger.info("gsa_organisms transfer done")
