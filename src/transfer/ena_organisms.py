"""
Populate ena_studies.organisms from ena_samples.

ENA project pages and the organism search facet otherwise see only the single
study-level scientific_name. There is no experiments hop as for SRA/DRA:
ena_samples.study_accession joins 1:1 to ena_studies.study_accession (both PRJ*),
so this is one GROUP BY. Measured on prod 2026-07-25: 663,705 study groups, max
6,875 organisms, ~5s over the whole 36M-row samples table.

A study with no sample-derived organism keeps its own scientific_name, so the
column is never worse than the scalar it replaces, the same guard the SRA
study-facet backfill uses.

Re-runnable: the sample populate SETs rather than appends and the fallback only
fills rows still NULL. Doubles as the one-off prod backfill, creating the column
and GIN index if missing.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)

_ADD_COLUMN = "ALTER TABLE ena_studies ADD COLUMN IF NOT EXISTS organisms text[]"

_POPULATE_FROM_SAMPLES = """
UPDATE ena_studies es
SET organisms = sub.orgs
FROM (
    SELECT study_accession, array_agg(DISTINCT scientific_name) AS orgs
    FROM ena_samples
    WHERE scientific_name IS NOT NULL AND scientific_name <> ''
    GROUP BY study_accession
) sub
WHERE es.study_accession = sub.study_accession
"""

_POPULATE_FALLBACK = """
UPDATE ena_studies
SET organisms = ARRAY[scientific_name]
WHERE organisms IS NULL AND NULLIF(btrim(scientific_name), '') IS NOT NULL
"""

# Matches sql/indexes/ena.sql so a fresh build and this backfill agree.
_CREATE_GIN = (
    "CREATE INDEX IF NOT EXISTS idx_ena_studies_organisms_gin "
    "ON ena_studies USING gin(organisms)"
)

# The federated search + organism facet read ena_studies_search (the tsv surface,
# a clean subset of ena_studies), NOT ena_studies. Mirror the column there so the
# facet sees ENA organisms -- see _SEARCH_MATCH_CTE in the server's queries.py.
_ADD_SEARCH_COLUMN = (
    "ALTER TABLE ena_studies_search ADD COLUMN IF NOT EXISTS organisms text[]"
)
_COPY_TO_SEARCH = """
UPDATE ena_studies_search ess
SET organisms = es.organisms
FROM ena_studies es
WHERE es.study_accession = ess.accession AND es.organisms IS NOT NULL
"""


def ena_organisms_transfer(pg_url: str) -> None:
    """Fill ena_studies.organisms from ena_samples. Idempotent; safe to re-run."""
    with psycopg.connect(pg_url) as conn:
        # autocommit: each step is its own statement; the sample populate is one
        # ~591k-row UPDATE we don't want wrapped in a long-held transaction.
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(_ADD_COLUMN)
            logger.info("populating ena_studies.organisms from samples ...")
            cur.execute(_POPULATE_FROM_SAMPLES)
            logger.info("  %s rows from samples", cur.rowcount)
            cur.execute(_POPULATE_FALLBACK)
            logger.info("  %s rows from scientific_name fallback", cur.rowcount)
            cur.execute(_CREATE_GIN)
            cur.execute(_ADD_SEARCH_COLUMN)
            cur.execute(_COPY_TO_SEARCH)
            logger.info("  %s rows copied to ena_studies_search", cur.rowcount)
    logger.info("ena_organisms transfer done")
