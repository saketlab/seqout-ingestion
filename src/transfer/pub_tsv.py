"""
Fold linked publication titles into each study's search tsv.

A paper title and its dataset title are usually worded differently, so pasting a
paper title into the search box used to miss the dataset (13 of 60 sampled GEO
papers were findable by their own title, 2026-07-24).

A generated column cannot reference other tables, so titles are materialized
into a per-study pub_tsv column that the study tsv appends. sample_tsv.py owns
both tsv expressions, so the two steps cannot fight over the column.

Source is study_publications, whose accessions join 1:1 to the study tables. DRA
and GEA have no rows there and instead resolve the pmid each study already
carries against pubmed_metadata (_PMID_TARGETS). ENA and GSA stay uncovered:
only 1,123 ENA studies are exclusive rather than mirrors, too few to justify
rewriting a 311k-row generated column, and GSA has no resolvable pmid at all.

Weight C: a paper title describes the study about as well as its overall design,
but must not outrank a direct hit on the dataset's own title or abstract. Side
effect: studies with a linked publication drift up on ordinary keyword queries
(27 of 40 top-5 slots held on an 8-query sample), with no new irrelevant rows.
"""

import logging
from typing import NamedTuple

import psycopg

from transfer.sample_tsv import (
    _AE_TSV_EXPR,
    _GEO_TRIGGER_FN,
    _SRA_TSV_EXPR,
    _ensure_sample_tsv_columns,
    _redefine_generated_tsv,
)

logger = logging.getLogger(__name__)


# 'lax $[*].title' tolerates publications being an array, a lone object, or a
# row whose first publication has no title. #>> '{}' unwraps
# the jsonb string to text without its quotes.
# sample_tsv's translate('/-') is omitted here: it splits compound cell-line
# names like HepG2/C3A, but also destroys the hyphenated lexeme that
# plainto_tsquery still emits and requires on the query side. Raw text takes
# hyphenated paper titles from 33/60 to 60/60 on a random GEO sample.
_PUB_TITLES = """
    left(coalesce((
        SELECT string_agg(DISTINCT t #>> '{}', ' ')
        FROM jsonb_path_query(sp.publications, 'lax $[*].title') t
    ), ''), 100000)
"""

_PUB_TSV_EXPR = "setweight(to_tsvector('english', " + _PUB_TITLES + "), 'C')"


def _populate(table: str, sources: tuple[str, ...]) -> str:
    """SET (not append) => re-runnable."""
    return f"""
    UPDATE {table} t
    SET pub_tsv = sub.ptsv
    FROM (
        SELECT sp.accession, {_PUB_TSV_EXPR} AS ptsv
        FROM study_publications sp
        WHERE sp.source = ANY(%(sources)s)
    ) sub
    WHERE t.accession = sub.accession
    """


_TARGETS = (
    ("geo_series", ("geo",)),
    ("sra_studies", ("sra",)),
    ("arrayexpress_experiments", ("arrayexpress", "ae")),
)


# many SRA studies carry a resolvable pmid but no study_publications row.
# Guarded on pub_tsv IS NULL so the richer source still wins. GEO's pmid is a
# jsonb array and takes its own path.
_PMID_FALLBACK_TARGETS = (
    ("sra_studies", "pubmed_id_pysradb"),
    ("arrayexpress_experiments", "pubmed_id_pysradb"),
)


def _pmid_fallback_populate(table: str, pmid_col: str) -> str:
    """Fill pub_tsv from the study's own pmid where study_publications did not."""
    return f"""
    UPDATE {table} t
    SET pub_tsv = setweight(to_tsvector('english', p.title), 'C')
    FROM pubmed_metadata p
    WHERE p.pmid = t.{pmid_col}
      AND p.title IS NOT NULL
      AND t.pub_tsv IS NULL
    """


_GEO_PMID_FALLBACK_POPULATE = """
UPDATE geo_series t
SET pub_tsv = sub.ptsv
FROM (
    SELECT gp.accession,
           setweight(to_tsvector('english',
               string_agg(DISTINCT p.title, ' ')), 'C') AS ptsv
    FROM (
        SELECT g.accession, unnest(g.pubmed_id_citation) AS pmid
        FROM geo_series g
        WHERE g.pub_tsv IS NULL AND g.pubmed_id_citation IS NOT NULL
        UNION
        SELECT g.accession, g.pubmed_id_pysradb AS pmid
        FROM geo_series g
        WHERE g.pub_tsv IS NULL
          AND g.pubmed_id_pysradb IS NOT NULL AND g.pubmed_id_pysradb <> ''
    ) gp
    JOIN pubmed_metadata p ON p.pmid = gp.pmid
    WHERE p.title IS NOT NULL
    GROUP BY gp.accession
) sub
WHERE t.accession = sub.accession AND t.pub_tsv IS NULL
"""


# DRA and GEA. Both pmid columns hold a single bare numeric value (verified),
# so an equality join against pubmed_metadata is enough.
#
# Their tsv is GENERATED ALWAYS like SRA/AE but omits the translate('/-')
# normalization, so each base expression below is reproduced verbatim from the
# live schema rather than rebuilt with _weighted(). Having no sample_tsv column,
# they also miss sample_tsv's up-to-date guard; _pmid_tsv_up_to_date replaces it.

_DRA_BASE_TSV = (
    "setweight(to_tsvector('english', COALESCE(study_title, '')), 'A')"
    " || setweight(to_tsvector('english', COALESCE(study_description, '')), 'B')"
    " || setweight(to_tsvector('english', COALESCE(study_name, '')), 'C')"
    " || setweight(to_tsvector('english', COALESCE(project_name, '')), 'C')"
)

_GEA_BASE_TSV = (
    "setweight(to_tsvector('english', COALESCE(title, '')), 'A')"
    " || setweight(to_tsvector('english', COALESCE(description, '')), 'B')"
    " || setweight(to_tsvector('english', COALESCE(organism, '')), 'C')"
    " || setweight(to_tsvector('english', COALESCE(study_type, '')), 'C')"
)

_PUB_TAIL = " || coalesce(pub_tsv, ''::tsvector)"


class _PmidTarget(NamedTuple):
    table: str  # table whose tsv is rebuilt (the search surface)
    pmid_table: str  # table carrying the pmid (same table for GEA)
    pmid_col: str
    join_col: str  # accession column on pmid_table
    base_tsv: str
    index_name: str


_PMID_TARGETS = (
    # dra_studies_search holds only the DDBJ-exclusive studies; the pmid lives on
    # dra_studies, so the populate joins across
    _PmidTarget(
        table="dra_studies_search",
        pmid_table="dra_studies",
        pmid_col="pubmed_id_pysradb",
        join_col="accession",
        base_tsv=_DRA_BASE_TSV,
        index_name="dra_studies_search_fts_idx",
    ),
    # gea_experiments is itself the search table; its pubmed_id_pysradb is
    # entirely NULL, so only pubmed_id is used
    _PmidTarget(
        table="gea_experiments",
        pmid_table="gea_experiments",
        pmid_col="pubmed_id",
        join_col="accession",
        base_tsv=_GEA_BASE_TSV,
        index_name="gea_experiments_fts_idx",
    ),
)


def _pmid_populate(t: _PmidTarget) -> str:
    """SET (not append) => re-runnable. string_agg handles multi-title studies."""
    return f"""
    UPDATE {t.table} tgt
    SET pub_tsv = sub.ptsv
    FROM (
        SELECT s.{t.join_col} AS accession,
               setweight(to_tsvector('english',
                   string_agg(DISTINCT p.title, ' ')), 'C') AS ptsv
        FROM {t.pmid_table} s
        JOIN pubmed_metadata p ON p.pmid = s.{t.pmid_col}
        WHERE p.title IS NOT NULL
        GROUP BY s.{t.join_col}
    ) sub
    WHERE tgt.accession = sub.accession
    """


def _pmid_tsv_up_to_date(cur: psycopg.Cursor, table: str) -> bool:
    cur.execute(
        "SELECT generation_expression FROM information_schema.columns"
        " WHERE table_name = %s AND column_name = 'tsv'",
        (table,),
    )
    row = cur.fetchone()
    return "pub_tsv" in ((row[0] or "") if row else "")


def pub_tsv_transfer(pg_url: str) -> None:
    """
    Populate per-study pub_tsv and wire it into each study's search tsv.

    Run after the pubmed backfill and after sample_tsv; both redefine the same
    generated columns, and the guard makes whichever runs second a no-op.
    """
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")

            _ensure_sample_tsv_columns(cur)
            conn.commit()

            # GEO trigger goes first, so the populate step that fires it rebuilds
            # tsv with the paper titles in a single pass
            cur.execute(_GEO_TRIGGER_FN)
            conn.commit()

            for table, sources in _TARGETS:
                logger.info("populating pub_tsv for %s ...", table)
                cur.execute(_populate(table, sources), {"sources": list(sources)})
                logger.info("  %s rows updated", cur.rowcount)
                conn.commit()

            logger.info("pmid-fallback pub_tsv for geo_series ...")
            cur.execute(_GEO_PMID_FALLBACK_POPULATE)
            logger.info("  %s rows updated", cur.rowcount)
            conn.commit()

            # runs after the study_publications populate, so pub_tsv IS NULL
            # means "not covered" and never "not tried yet"
            for table, pmid_col in _PMID_FALLBACK_TARGETS:
                logger.info("pmid-fallback pub_tsv for %s ...", table)
                cur.execute(_pmid_fallback_populate(table, pmid_col))
                logger.info("  %s rows updated", cur.rowcount)
                conn.commit()

            _redefine_generated_tsv(
                cur, "sra_studies", _SRA_TSV_EXPR, "sra_studies_fts_idx"
            )
            conn.commit()
            _redefine_generated_tsv(
                cur, "arrayexpress_experiments", _AE_TSV_EXPR, "ae_experiments_fts_idx"
            )
            conn.commit()

            for t in _PMID_TARGETS:
                cur.execute(
                    f"ALTER TABLE {t.table} ADD COLUMN IF NOT EXISTS pub_tsv tsvector"
                )
                conn.commit()

                logger.info("populating pub_tsv for %s ...", t.table)
                cur.execute(_pmid_populate(t))
                logger.info("  %s rows updated", cur.rowcount)
                conn.commit()

                if _pmid_tsv_up_to_date(cur, t.table):
                    logger.info("%s.tsv already up to date; skipping redefine", t.table)
                    continue
                logger.info("redefining %s.tsv to include pub_tsv", t.table)
                cur.execute(f"ALTER TABLE {t.table} DROP COLUMN tsv")
                cur.execute(
                    f"ALTER TABLE {t.table} ADD COLUMN tsv tsvector"
                    f" GENERATED ALWAYS AS ({t.base_tsv}{_PUB_TAIL}) STORED"
                )
                cur.execute(f"CREATE INDEX {t.index_name} ON {t.table} USING gin (tsv)")
                conn.commit()
    logger.info("pub_tsv transfer done")
