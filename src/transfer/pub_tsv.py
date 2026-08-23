"""
Fold linked publication titles into each study's search ``tsv``, so a paper
title pasted into the search box finds the dataset.

A generated column cannot reference other tables, so titles are materialised
into a per-study ``pub_tsv`` column that the study ``tsv`` reads. sample_tsv.py
owns the tsv expressions and appends both tails, so the two steps cannot fight
over the column.

  geo_series               tsv via TRIGGER  -> appends coalesce(pub_tsv,'')
  sra_studies              tsv GENERATED    -> appends coalesce(pub_tsv,'')
  arrayexpress_experiments tsv GENERATED    -> appends coalesce(pub_tsv,'')

Source is ``study_publications`` (accession, source, publications jsonb), whose
accessions join 1:1 to the study tables. DRA and GEA have no rows there and take
a second path: the pmid each study carries, resolved against pubmed_metadata
(see _PMID_TARGETS).

ENA and GSA are out of scope. Most ENA studies are mirrors whose SRA or GEO twin
already carries the title, and search dedups mirrors. GSA has no pmid linkage to
fold, so it needs a backfill first.

Weight ``C``: a paper title describes the study about as well as its overall
design, and must not outrank a direct hit on the dataset's own title or
abstract. Lower it to ``D`` to damp the reordering this causes, or drop the
step to remove it.
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
# row whose first publication has no title. `#>> '{}'` unwraps the jsonb string
# to text without its quotes, and DISTINCT collapses duplicate titles when a
# study lists the same paper twice.
# sample_tsv's translate('/-') is omitted here. It splits compound cell-line
# names like HepG2/C3A, but also destroys the hyphenated lexeme
# ('prader-willi') that plainto_tsquery emits and requires on the query side.
# left() guards against a study listing many papers.
_PUB_TITLES = """
    left(coalesce((
        SELECT string_agg(DISTINCT t #>> '{}', ' ')
        FROM jsonb_path_query(sp.publications, 'lax $[*].title') t
    ), ''), 100000)
"""

_PUB_TSV_EXPR = "setweight(to_tsvector('english', " + _PUB_TITLES + "), 'C')"


def _populate(table: str, sources: tuple[str, ...]) -> str:
    """SET (not append) => re-runnable.

    The ``IS DISTINCT FROM`` guard is not an optimisation, same as
    ``pg_fields.non_nulling_update``: without it every row with a publication is
    rewritten into a dead tuple on every run, and on GEO it also re-fires the tsv
    trigger for 232k rows. That is tolerable quarterly and not weekly -- see
    refresh/publications.py, which calls this on a schedule.
    """
    return f"""
    UPDATE {table} t
    SET pub_tsv = sub.ptsv
    FROM (
        SELECT sp.accession, {_PUB_TSV_EXPR} AS ptsv
        FROM study_publications sp
        WHERE sp.source = ANY(%(sources)s)
    ) sub
    WHERE t.accession = sub.accession
      AND t.pub_tsv IS DISTINCT FROM sub.ptsv
    """


_TARGETS = (
    ("geo_series", ("geo",)),
    ("sra_studies", ("sra",)),
    # 'ae' is a legacy label for the same 19 ArrayExpress rows.
    ("arrayexpress_experiments", ("arrayexpress", "ae")),
)


# The two linkage sources barely overlap: most SRA studies with a paper carry
# it as a bare pmid in pubmed_id_pysradb rather than a study_publications row.
# This applies the pmid->pubmed_metadata path DRA/GEA use as a fallback, guarded
# by pub_tsv IS NULL so the richer study_publications source still wins.
# GEO is excluded because its pmid is a jsonb array, a different shape.
_PMID_FALLBACK_TARGETS = (
    ("sra_studies", "pubmed_id_pysradb"),
    ("arrayexpress_experiments", "pubmed_id_pysradb"),
)


def _pmid_fallback_populate(table: str, pmid_col: str) -> str:
    """Fill pub_tsv from the study's own pmid where study_publications did not.
    Re-runnable: only touches rows whose pub_tsv is still NULL."""
    return f"""
    UPDATE {table} t
    SET pub_tsv = setweight(to_tsvector('english', p.title), 'C')
    FROM pubmed_metadata p
    WHERE p.pmid = t.{pmid_col}
      AND p.title IS NOT NULL
      AND t.pub_tsv IS NULL
    """


# GEO has its own fallback: unlike SRA/AE, its pmid columns are text[] arrays
# (pubmed_id, pubmed_id_citation) plus the scalar pubmed_id_pysradb. Native
# pubmed_id is already covered via study_publications (~97.5%), so only the two
# backfilled sources are folded here. Setting pub_tsv fires the GEO tsv trigger,
# so tsv is rebuilt in the same pass, with no generated-column redefine.
# To narrow to citation-only, drop the pubmed_id_pysradb UNION arm below.
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


# --------------------------------------------------------------------------
# DRA and GEA. These have no study_publications rows at all, so the title comes
# from pubmed_metadata via the pmid the study already carries. Both columns hold
# a single bare numeric pmid (verified: zero non-numeric values), so an equality
# join is enough, with no splitting.
#
# Their tsv is GENERATED ALWAYS like SRA/AE, but is built WITHOUT the
# translate('/-') normalization that geo/sra/ae use, so each base expression is
# reproduced verbatim from the live schema rather than rebuilt with _weighted().
# They also have no sample_tsv column, so sample_tsv's up-to-date guard (which
# requires both sample_tsv and translate markers) does not apply. See
# _pmid_tsv_up_to_date below.
# --------------------------------------------------------------------------

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
    # dra_studies, so the populate joins across.
    _PmidTarget(
        table="dra_studies_search",
        pmid_table="dra_studies",
        pmid_col="pubmed_id_pysradb",
        join_col="accession",
        base_tsv=_DRA_BASE_TSV,
        index_name="dra_studies_search_fts_idx",
    ),
    # gea_experiments IS the search table and carries its own pmid.
    # pubmed_id_pysradb is entirely NULL here, so only pubmed_id is used.
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
    """SET (not append) => re-runnable. string_agg guards against a study
    resolving to more than one title. Same no-op guard as ``_populate``."""
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
      AND tgt.pub_tsv IS DISTINCT FROM sub.ptsv
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
    """Populate per-study ``pub_tsv`` from study_publications and wire it into
    each study's search ``tsv``. Idempotent and re-runnable.

    Run after the pubmed backfill, and after ``sample_tsv`` (both redefine the
    same generated columns; the guard makes whichever runs second a no-op).
    """
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")

        # 1) sample_tsv + pub_tsv columns on every study table.
        _ensure_sample_tsv_columns(cur)
        conn.commit()

        # 2) GEO: update the trigger FIRST so the populate step (which fires
        #    it) rebuilds tsv with the paper titles in a single pass.
        cur.execute(_GEO_TRIGGER_FN)
        conn.commit()

        # 3) Populate pub_tsv per source.
        for table, sources in _TARGETS:
            logger.info("populating pub_tsv for %s ...", table)
            cur.execute(_populate(table, sources), {"sources": list(sources)})
            logger.info("  %s rows updated", cur.rowcount)
            conn.commit()

        # 3b) GEO series whose paper arrived after their last tsv rebuild are
        #     covered by 3); series with no publication never fire the
        #     trigger, and need no rebuild (pub_tsv stays NULL).
        #
        #     GEO pmid fallback: for series study_publications missed, fold the
        #     title resolved from pubmed_id_citation / pubmed_id_pysradb. The
        #     UPDATE fires the trigger, so tsv is rebuilt in the same pass.
        logger.info("pmid-fallback pub_tsv for geo_series ...")
        cur.execute(_GEO_PMID_FALLBACK_POPULATE)
        logger.info("  %s rows updated", cur.rowcount)
        conn.commit()

        # 3c) SRA/AE fallback: fold the pmid-resolved title for rows that
        #     study_publications missed. Runs after 3) so pub_tsv IS NULL
        #     means "not covered", not "not tried yet".
        for table, pmid_col in _PMID_FALLBACK_TARGETS:
            logger.info("pmid-fallback pub_tsv for %s ...", table)
            cur.execute(_pmid_fallback_populate(table, pmid_col))
            logger.info("  %s rows updated", cur.rowcount)
            conn.commit()

        # 4) SRA/AE: fold pub_tsv into the generated tsv (guarded).
        _redefine_generated_tsv(
            cur, "sra_studies", _SRA_TSV_EXPR, "sra_studies_fts_idx"
        )
        conn.commit()
        _redefine_generated_tsv(
            cur, "arrayexpress_experiments", _AE_TSV_EXPR, "ae_experiments_fts_idx"
        )
        conn.commit()

        # 5) DRA/GEA resolve pmid -> pubmed_metadata.title. Both tables are
        #    small, so the generated-column rewrite is cheap here.
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
