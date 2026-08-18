"""
Rebuild the GSA search table so its tsv indexes metadata one join away.

The open GSA (CRA*) study record is almost always empty: ~93% have no
study_title of their own. The descriptive text sits on the linked BioProject
(gsa_projects), the organism on gsa_studies.scientific_name, and the biology on
gsa_samples, none of which were searchable before.

A GENERATED column cannot reference other tables, so the cross-table text is
materialized onto gsa_studies_search first and the tsv reads it from there:
project_title and project_summary from gsa_projects by project_name, organisms
from gsa_studies.scientific_name, and sample_tsv aggregated per study from the
gsa_samples title, description, tissue, disease and cell_type at weight D.

This module is the canonical schema: the Postgres search table was built ad-hoc
and has no DDL file, and sql/tables/gsa.sql covers only the SQLite side.

  gsa_studies_search(
    accession PK, study_title, study_description, study_name, center_name,
    project_name, project_title, project_summary, organisms text[],
    last_updated, first_public, sample_tsv tsvector,
    tsv tsvector GENERATED ALWAYS AS (<_TSV_EXPR>) STORED
  )  -- GIN index gsa_studies_search_fts_idx on (tsv)

Re-runnable: materialized columns are SET rather than appended, and the redefine
step is guarded so it runs only when the live tsv is out of date.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)

_FTS_INDEX = "gsa_studies_search_fts_idx"


# compound names ("HepG2/C3A", "Hep-G2") tokenize as one path-like lexeme, so a
# query for a part never matches; '/' and '-' become spaces before to_tsvector,
# the same rule as transfer.sample_tsv
def _norm(text_expr: str) -> str:
    return f"translate({text_expr}, '/-', '  ')"


_ENSURE_COLUMNS = """
ALTER TABLE gsa_studies_search
    ADD COLUMN IF NOT EXISTS project_title   text,
    ADD COLUMN IF NOT EXISTS project_summary text,
    ADD COLUMN IF NOT EXISTS organisms       text[],
    ADD COLUMN IF NOT EXISTS sample_tsv       tsvector
"""

_POPULATE_PROJECT = """
UPDATE gsa_studies_search s
SET project_title   = pr.title,
    project_summary = pr.summary,
    organisms       = CASE
        WHEN COALESCE(NULLIF(btrim(gs.scientific_name), ''),
                      NULLIF(btrim(pr.organism), '')) IS NOT NULL
        THEN ARRAY[COALESCE(NULLIF(btrim(gs.scientific_name), ''),
                            NULLIF(btrim(pr.organism), ''))]
    END
FROM gsa_studies gs
LEFT JOIN gsa_projects pr ON pr.accession = gs.project_name
WHERE s.accession = gs.study_accession
"""

_POPULATE_SAMPLE_TSV = (
    "UPDATE gsa_studies_search s SET sample_tsv = sub.stsv FROM ("
    " SELECT study_accession,"
    "        setweight(to_tsvector('english', "
    + _norm(
        "left(coalesce(string_agg(DISTINCT concat_ws(' ', sample_title,"
        " sample_description, tissue, disease, cell_type), ' '), ''), 400000)"
    )
    + "), 'D') AS stsv"
    " FROM gsa_samples"
    " GROUP BY study_accession"
    ") sub WHERE s.accession = sub.study_accession"
)


# generated tsv: study text, BioProject text and organism, sample text at D.
# concat_ws is only STABLE and a GENERATED column expression must be IMMUTABLE,
# hence || COALESCE.
def _join(*cols: str) -> str:
    return " || ' ' || ".join(f"COALESCE({c}, '')" for c in cols)


_TSV_EXPR = (
    "setweight(to_tsvector('english', "
    + _norm(_join("study_title", "project_title"))
    + "), 'A') || "
    "setweight(to_tsvector('english', "
    + _norm(_join("study_description", "project_summary"))
    + "), 'B') || "
    "setweight(to_tsvector('english', "
    + _norm(
        # array_to_string is only STABLE and fails immutability; GSA studies
        # carry a single organism, and subscripting is immutable
        _join("study_name", "project_name", "center_name", "organisms[1]")
    )
    + "), 'C') || "
    "coalesce(sample_tsv, ''::tsvector)"
)


def _tsv_up_to_date(cur: psycopg.Cursor) -> bool:
    """
    True once the generated tsv folds in both BioProject title and sample text.

    Both markers must be present, so a re-run of an up-to-date DB is a no-op.
    """
    cur.execute(
        "SELECT generation_expression FROM information_schema.columns"
        " WHERE table_name = 'gsa_studies_search' AND column_name = 'tsv'"
    )
    row = cur.fetchone()
    expr = (row[0] or "") if row else ""
    return "project_title" in expr and "sample_tsv" in expr


def _redefine_tsv(cur: psycopg.Cursor) -> None:
    """
    Re-add the GENERATED tsv with the enriched expression.

    PG16 has no ALTER ... SET EXPRESSION, and DROP COLUMN takes the dependent
    GIN index with it, so the index is recreated by name.
    """
    if _tsv_up_to_date(cur):
        logger.info("gsa_studies_search.tsv already up to date; skipping redefine")
        return
    logger.info("redefining gsa_studies_search.tsv (rewrites table)")
    cur.execute("ALTER TABLE gsa_studies_search DROP COLUMN IF EXISTS tsv")
    cur.execute(
        "ALTER TABLE gsa_studies_search ADD COLUMN tsv tsvector"
        f" GENERATED ALWAYS AS ({_TSV_EXPR}) STORED"
    )
    cur.execute(f"CREATE INDEX {_FTS_INDEX} ON gsa_studies_search USING gin (tsv)")


def gsa_search_tsv_transfer(pg_url: str) -> None:
    """
    Materialize BioProject, organism and sample text and rebuild the search tsv.

    Depends on gsa_studies_search, gsa_studies, gsa_projects, and gsa_samples
    already being loaded. Heavy (aggregates gsa_samples); statement_timeout
    is disabled for the duration.
    """
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")

            cur.execute(_ENSURE_COLUMNS)
            conn.commit()

            logger.info("populating BioProject title/summary + organism ...")
            cur.execute(_POPULATE_PROJECT)
            logger.info("  %s rows updated", cur.rowcount)
            conn.commit()

            logger.info("populating sample_tsv from gsa_samples ...")
            cur.execute(_POPULATE_SAMPLE_TSV)
            logger.info("  %s rows updated", cur.rowcount)
            conn.commit()

            _redefine_tsv(cur)
            conn.commit()
    logger.info("gsa search tsv transfer done")
