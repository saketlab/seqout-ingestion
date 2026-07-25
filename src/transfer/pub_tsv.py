"""
Fold the title of each study's linked publication(s) into that study's search
``tsv``, so pasting a paper title into the search box finds the dataset.

Measured on prod (2026-07-24): only 13/60 sampled GEO papers were already
findable by their own title, because a paper title and its dataset title are
usually worded differently. This closes that gap.

Same mechanism as ``sample_tsv``: a generated column can't reference other
tables, so the titles are materialized into a per-study ``pub_tsv`` column that
the study ``tsv`` reads (see sample_tsv.py, which owns the tsv expressions --
both tails are appended there so the two steps can't fight over the column).

  geo_series               tsv via TRIGGER  -> appends coalesce(pub_tsv,'')
  sra_studies              tsv GENERATED    -> appends coalesce(pub_tsv,'')
  arrayexpress_experiments tsv GENERATED    -> appends coalesce(pub_tsv,'')

Source is ``study_publications`` (accession, source, publications jsonb), which
search already joins for the journal facet. Its accessions join 1:1 to the study
tables.

DRA and GEA have no study_publications rows, so they take a second path: the
pmid each study already carries, resolved against pubmed_metadata (see
_PMID_TARGETS). Small but cheap -- both tables are tiny, so the rewrite is
trivial (measured 2026-07-24: DRA 28 of 1,178 searchable rows, GEA 47 of 735).

ENA and GSA are deliberately NOT covered:

  ENA   21,632 studies have a resolvable title, but only 1,123 are rows in
        ena_studies_search -- that twin holds the ENA-exclusive studies, and the
        rest are mirrors whose SRA/GEO twin already carries the title (and search
        dedups mirrors anyway). 1,123 does not justify rewriting a 311k-row
        generated column; if it is ever wanted, prefer a standalone pub_tsv
        column + CREATE INDEX CONCURRENTLY and an OR in the ENA search arm,
        which needs no rewrite.
  GSA   pubmed_id_pysradb is entirely NULL and gsa_projects.pubmed_id resolves to
        nothing, so there is no linkage to fold. Needs a pmid backfill first.

Coverage is backfill-limited everywhere, not linkage-limited: 1,866 ENA, 98 DRA
and 135 GEA studies carry a pmid that is simply absent from pubmed_metadata.
Backfilling those roughly triples DRA/GEA and ENA alike.

Weight ``C``: a paper title describes the study about as well as its overall
design, but must not outrank a direct hit on the dataset's own title/abstract.
Measured C vs D: identical recall, identical ranking on the samples tried --
matching is weight-independent, so the constant is a cheap knob, not a fix.

Known side effect: ordinary keyword queries reorder somewhat (27/40 of top-5
slots held on an 8-query sample). No new irrelevant rows appear -- the shift is
that a study whose paper title repeats the query terms now scores higher, so
datasets with a linked publication drift up. Same trade sample_tsv already
makes. Lower the weight to 'D' to damp it; drop this step to remove it.
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
# row whose first publication has no title (15,488 such rows). `#>> '{}'`
# unwraps the jsonb string to text without its quotes. DISTINCT collapses the
# duplicate titles that appear when a study lists the same paper twice.
# NO translate('/-') here, unlike sample_tsv: that exists to split compound
# cell-line names (HepG2/C3A), but it also destroys the whole hyphenated lexeme
# ('prader-willi') that plainto_tsquery still emits AND requires on the query
# side. Keeping the raw text takes hyphenated paper titles from miss to hit
# (measured on a random GEO sample: 33/60 with translate, 60/60 without).
# left() is a guard only -- titles are short, but a study may list many papers.
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
    # 'ae' is a legacy label for the same 19 ArrayExpress rows.
    ("arrayexpress_experiments", ("arrayexpress", "ae")),
)


# SRA/AE fold pub_tsv only from study_publications, but the two linkage sources
# barely overlap: most SRA studies with a paper carry it as a bare pmid in
# pubmed_id_pysradb, not a study_publications row. Measured 2026-07-25: 112,496
# SRA studies have a pmid that resolves to a pubmed_metadata title yet pub_tsv is
# still NULL (only 12,026 of the resolvable ones were already covered). This is
# the same pmid->pubmed_metadata path DRA/GEA use, applied as a fallback so the
# richer study_publications source still wins (guarded by pub_tsv IS NULL).
# GEO is excluded: its pmid is a jsonb array, a different shape, and unmeasured.
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


# --------------------------------------------------------------------------
# DRA and GEA. These have no study_publications rows at all, so the title comes
# from pubmed_metadata via the pmid the study already carries. Both columns hold
# a single bare numeric pmid (verified: zero non-numeric values), so an equality
# join is enough -- no splitting.
#
# Their tsv is GENERATED ALWAYS like SRA/AE, but is built WITHOUT the
# translate('/-') normalization that geo/sra/ae use, so each base expression is
# reproduced verbatim from the live schema rather than rebuilt with _weighted().
# They also have no sample_tsv column, so sample_tsv's up-to-date guard (which
# requires both sample_tsv and translate markers) does not apply -- see
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
    resolving to more than one title."""
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
    """Populate per-study ``pub_tsv`` from study_publications and wire it into
    each study's search ``tsv``. Idempotent and re-runnable.

    Run after the pubmed backfill, and after ``sample_tsv`` (both redefine the
    same generated columns; the guard makes whichever runs second a no-op).
    """
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
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

            # 5) DRA/GEA: pmid -> pubmed_metadata.title instead of
            #    study_publications. Both tables are tiny (1,178 and 735 rows),
            #    so the generated-column rewrite is cheap here.
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
                cur.execute(
                    f"CREATE INDEX {t.index_name} ON {t.table} USING gin (tsv)"
                )
                conn.commit()
    logger.info("pub_tsv transfer done")
