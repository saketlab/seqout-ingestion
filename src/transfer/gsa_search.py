"""
Rebuild the GSA search table (``gsa_studies_search``) so its ``tsv`` indexes the
rich metadata that lives one join away from the thin study record.

The open GSA (CRA*) study record is almost always empty: ~93% have no
``study_title`` of their own. The descriptive text sits on the linked BioProject
(``gsa_projects``), the organism on ``gsa_studies.scientific_name``, and the
biology on the samples (``gsa_samples``). None of that was searchable before.

A GENERATED column can't reference other tables, so the cross-table text is
materialized into columns on ``gsa_studies_search`` first, then the ``tsv`` reads
them:

  project_title / project_summary  <- gsa_projects (BioProject), by project_name
  organisms                        <- gsa_studies.scientific_name (also returned
                                       on result cards by the API)
  sample_tsv                       <- gsa_samples title/description/tissue/
                                       disease/cell_type, aggregated per study at
                                       weight D (recall without outranking titles),
                                       mirroring transfer.sample_tsv for the other
                                       sources.

Canonical schema (this module is the source of truth — the Postgres search table
was built ad-hoc and has no DDL file; sql/tables/gsa.sql is the SQLite schema):

  gsa_studies_search(
    accession PK, study_title, study_description, study_name, center_name,
    project_name, project_title, project_summary, organisms text[],
    last_updated, first_public, sample_tsv tsvector,
    tsv tsvector GENERATED ALWAYS AS (<_TSV_EXPR>) STORED
  )  -- GIN index gsa_studies_search_fts_idx on (tsv)

Re-runnable: materialized columns are SET (not appended) and the redefine step is
guarded so it runs only when the live tsv is out of date.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)

_FTS_INDEX = "gsa_studies_search_fts_idx"


# Compound names ("HepG2/C3A", "Hep-G2") tokenize as one path-like lexeme, so a
# query for a part never matches. Normalize '/' and '-' to spaces before
# to_tsvector (same rule as transfer.sample_tsv).
def _norm(text_expr: str) -> str:
    return f"translate({text_expr}, '/-', '  ')"


# New columns materialized onto gsa_studies_search for the tsv to read.
_ENSURE_COLUMNS = """
ALTER TABLE gsa_studies_search
    ADD COLUMN IF NOT EXISTS project_title   text,
    ADD COLUMN IF NOT EXISTS project_summary text,
    ADD COLUMN IF NOT EXISTS organisms       text[],
    ADD COLUMN IF NOT EXISTS sample_tsv       tsvector
"""

# BioProject title/summary + organism, by the 1:1 study PK. project_name on the
# search row is the PRJCA; gsa_projects.accession is the same PRJCA.
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

# Per-study sample text at weight D. string_agg(DISTINCT ...) collapses replicate
# samples; left(..., 400000) caps the aggregate well under the 1 MB tsvector limit
# (a few studies have tens of thousands of samples). Matches transfer.sample_tsv.
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


# Generated tsv: study text + BioProject text + organism, sample text at D.
# Uses `|| COALESCE` rather than concat_ws because concat_ws is only STABLE, and
# a GENERATED column expression must be IMMUTABLE.
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
        # organisms[1], not array_to_string(organisms, ' '): array_to_string is
        # only STABLE (fails immutability), and GSA studies carry a single
        # organism. Subscripting is immutable.
        _join("study_name", "project_name", "center_name", "organisms[1]")
    )
    + "), 'C') || "
    "coalesce(sample_tsv, ''::tsvector)"
)


def _tsv_up_to_date(cur: psycopg.Cursor) -> bool:
    """True once the generated tsv already folds in the BioProject title and the
    sample text (both markers), so a re-run of an up-to-date DB is a no-op."""
    cur.execute(
        "SELECT generation_expression FROM information_schema.columns"
        " WHERE table_name = 'gsa_studies_search' AND column_name = 'tsv'"
    )
    row = cur.fetchone()
    expr = (row[0] or "") if row else ""
    return "project_title" in expr and "sample_tsv" in expr


def _redefine_tsv(cur: psycopg.Cursor) -> None:
    """Drop + re-add the GENERATED tsv with the enriched expression and rebuild
    its GIN index. PG16 has no ALTER ... SET EXPRESSION; DROP COLUMN also drops
    the dependent index, so it's recreated by name after."""
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
    """Materialize BioProject/organism/sample text into ``gsa_studies_search`` and
    rebuild its search ``tsv``. Idempotent and re-runnable.

    Depends on gsa_studies_search, gsa_studies, gsa_projects, and gsa_samples
    already being loaded. Heavy (aggregates gsa_samples); ``statement_timeout``
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
