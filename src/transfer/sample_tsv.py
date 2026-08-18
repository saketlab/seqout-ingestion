"""
Fold sample-level text into each study's search tsv.

A study is otherwise unfindable by terms appearing only in its samples. Sample
text is deduped and added at weight D, surfacing a study without outranking a
real title hit.

A generated column cannot reference other tables, so sample text is materialized
into a per-study sample_tsv column that the study tsv appends: via trigger for
geo_series, via the generated expression for sra_studies and
arrayexpress_experiments.

sample_tsv joins:
  GEO  geo_samples.accession = ANY(geo_series.samples_ref)
  SRA  sra_experiments.study = sra_studies.accession
       AND sra_samples.accession = ANY(sra_experiments.samples)      (two-hop)
  AE   arrayexpress_samples.experiment_accession = arrayexpress_experiments.accession

Re-runnable: sample_tsv is SET rather than appended, and the schema steps are
guarded so they run at most once.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)


# compound cell-line names ("HepG2/C3A", "Hep-G2") parse as a single path-like
# lexeme, so a query for "hepg2" never matches. Translating '/' and '-' to
# spaces before to_tsvector makes the parts their own lexemes. The query side is
# untranslated, but the english config already splits hyphenated query terms.
def _norm(text_expr: str) -> str:
    return f"translate({text_expr}, '/-', '  ')"


# accession-only titles carry no search value and are dropped, keeping the
# description. string_agg(DISTINCT ...) collapses replicate samples so
# to_tsvector's position lists stay bounded, and left() caps the aggregate at
# 400 KB: a few studies have tens of thousands of unique samples whose combined
# text builds a tsvector past the hard 1 MB limit.
_SAMPLE_TSV_EXPR = (
    "setweight(to_tsvector('english', "
    + _norm(
        "left(coalesce(string_agg(DISTINCT concat_ws(' ', {title_expr}, {desc}),"
        " ' '), ''), 400000)"
    )
    + "), 'D')"
)

_GEO_POPULATE = """
UPDATE geo_series g
SET sample_tsv = sub.stsv
FROM (
    SELECT ser.accession,
           {expr} AS stsv
    FROM geo_series ser
    JOIN geo_samples s ON s.accession = ANY(ser.samples_ref)
    GROUP BY ser.accession
) sub
WHERE g.accession = sub.accession
""".format(
    expr=_SAMPLE_TSV_EXPR.format(
        title_expr="CASE WHEN btrim(s.title) = s.accession THEN NULL ELSE s.title END",
        # 'lax $[*]' tolerates Characteristics being an array, a lone object, or
        # absent. Protocol fields carry incidental mentions ("HepG2 conditioned
        # media") rather than the subject, so they are left out.
        desc=(
            "concat_ws(' ', s.description,"
            " (SELECT string_agg(DISTINCT concat_ws(' ', ch->>'Source',"
            "     (SELECT string_agg(DISTINCT e->>'#text', ' ')"
            "      FROM jsonb_path_query(ch->'Characteristics', 'lax $[*]') e)), ' ')"
            "  FROM unnest(s.channels) ch))"
        ),
    )
)

_SRA_POPULATE = """
UPDATE sra_studies st
SET sample_tsv = sub.stsv
FROM (
    SELECT e.study AS accession,
           {expr} AS stsv
    FROM sra_experiments e
    JOIN sra_samples s ON s.accession = ANY(e.samples)
    WHERE e.study IS NOT NULL
    GROUP BY e.study
) sub
WHERE st.accession = sub.accession
""".format(
    expr=_SAMPLE_TSV_EXPR.format(
        title_expr=(
            "CASE WHEN btrim(s.title) IN (s.accession, coalesce(s.alias,''))"
            " THEN NULL ELSE s.title END"
        ),
        desc="s.description",
    )
)

_AE_POPULATE = """
UPDATE arrayexpress_experiments ex
SET sample_tsv = sub.stsv
FROM (
    SELECT s.experiment_accession AS accession,
           {expr} AS stsv
    FROM arrayexpress_samples s
    WHERE s.experiment_accession IS NOT NULL
    GROUP BY s.experiment_accession
) sub
WHERE ex.accession = sub.accession
""".format(
    expr=_SAMPLE_TSV_EXPR.format(
        title_expr="s.sample_title",
        desc="s.sample_description",
    )
)

# GEO tsv is trigger-built, so the same-row sample_tsv is appended here; the
# body must match the live trigger plus the final sample line
_GEO_TRIGGER_FN = """
CREATE OR REPLACE FUNCTION public.geo_series_tsv_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.tsv :=
          setweight(to_tsvector('english', translate(coalesce(NEW.title,''), '/-', '  ')), 'A')
      ||  setweight(to_tsvector('english', translate(coalesce(NEW.summary,''), '/-', '  ')), 'B')
      ||  setweight(to_tsvector('english', translate(coalesce(NEW.overall_design,''), '/-', '  ')), 'C')
      ||  setweight(to_tsvector('english', coalesce(array_to_string(NEW.contributors,' '),'')), 'D')
      ||  setweight(to_tsvector('english', coalesce(array_to_string(NEW.departments,' '),'')), 'D')
      ||  setweight(to_tsvector('english', coalesce(array_to_string(NEW.organizations,' '),'')), 'D')
      ||  setweight(to_tsvector('english', coalesce(array_to_string(NEW.countries,' '),'')), 'D')
      ||  coalesce(NEW.sample_tsv, ''::tsvector)
      ||  coalesce(NEW.pub_tsv, ''::tsvector);
    RETURN NEW;
END $$;
"""


# SRA/AE tsv are GENERATED ALWAYS and PG16 has no ALTER ... SET EXPRESSION, so
# the column is dropped and re-added, taking its GIN index with it. The base
# expression must match the live schema exactly, plus the sample_tsv tail.
def _weighted(col: str, weight: str) -> str:
    """One tsv term: setweight(to_tsvector(normalized COALESCE(col)), weight)."""
    col_expr = _norm("COALESCE(" + col + ", '')")
    return "setweight(to_tsvector('english', " + col_expr + "), '" + weight + "')"


_SRA_TSV_EXPR = (
    "((("
    + _weighted("title", "A")
    + " || "
    + _weighted("abstract", "B")
    + ") || "
    + _weighted("center_project_name", "C")
    + ") || "
    + _weighted("study_type", "D")
    + ") || coalesce(sample_tsv, ''::tsvector)"
    + " || coalesce(pub_tsv, ''::tsvector)"
)
_AE_TSV_EXPR = (
    "((("
    + _weighted("title", "A")
    + " || "
    + _weighted("description", "B")
    + ") || "
    + _weighted("organizations_text", "C")
    + ") || "
    + _weighted("authors_text", "D")
    + ") || coalesce(sample_tsv, ''::tsvector)"
    + " || coalesce(pub_tsv, ''::tsvector)"
)

_STUDY_TABLES = ("geo_series", "sra_studies", "arrayexpress_experiments")


def _ensure_sample_tsv_columns(cur: psycopg.Cursor) -> None:
    """
    Add both extra tsv tail columns, sample_tsv and pub_tsv.

    Created together because the tsv expressions below reference both, so either
    step may run first.
    """
    for table in _STUDY_TABLES:
        for column in ("sample_tsv", "pub_tsv"):
            cur.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} tsvector",
            )


def _tsv_up_to_date(cur: psycopg.Cursor, table: str) -> bool:
    """
    True once the generated tsv folds in sample_tsv, pub_tsv and translate().

    All three markers must be present, so a table left at an older definition is
    redefined.
    """
    cur.execute(
        "SELECT generation_expression FROM information_schema.columns"
        " WHERE table_name = %s AND column_name = 'tsv'",
        (table,),
    )
    row = cur.fetchone()
    expr = (row[0] or "") if row else ""
    return "sample_tsv" in expr and "pub_tsv" in expr and "translate" in expr


def _redefine_generated_tsv(
    cur: psycopg.Cursor, table: str, tsv_expr: str, index_name: str
) -> None:
    """
    Re-add the GENERATED tsv column with the sample_tsv tail.

    Rebuilds its GIN index and rewrites the study table, which is small; never
    the sample tables.
    """
    if _tsv_up_to_date(cur, table):
        logger.info("%s.tsv already up to date; skipping redefine", table)
        return
    logger.info("redefining %s.tsv to include sample_tsv (rewrites table)", table)
    # DROP COLUMN takes the dependent GIN index with it, so recreate it by name
    cur.execute(f"ALTER TABLE {table} DROP COLUMN tsv")
    cur.execute(
        f"ALTER TABLE {table} ADD COLUMN tsv tsvector"
        f" GENERATED ALWAYS AS ({tsv_expr}) STORED",
    )
    cur.execute(f"CREATE INDEX {index_name} ON {table} USING gin (tsv)")


def sample_tsv_transfer(pg_url: str) -> None:
    """
    Populate per-study sample_tsv and wire it into each study's search tsv.

    Heavy: aggregates over geo_samples, sra_samples and arrayexpress_samples,
    with statement_timeout disabled for the duration. Run as a one-time
    migration, then as an ingest step whenever samples change.
    """
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")

            _ensure_sample_tsv_columns(cur)
            conn.commit()

            # GEO trigger goes first, so the populate step that fires it rebuilds
            # tsv with sample text in a single pass
            cur.execute(_GEO_TRIGGER_FN)
            conn.commit()

            for name, sql in (
                ("geo_series", _GEO_POPULATE),
                ("sra_studies", _SRA_POPULATE),
                ("arrayexpress_experiments", _AE_POPULATE),
            ):
                logger.info("populating sample_tsv for %s ...", name)
                cur.execute(sql)
                logger.info("  %s rows updated", cur.rowcount)
                conn.commit()

            # the populate skipped GEO series with no matching samples, so the
            # translate-normalized trigger never re-fired for them
            logger.info("touching sample-less geo_series to rebuild tsv ...")
            cur.execute(
                "UPDATE geo_series SET sample_tsv = sample_tsv WHERE sample_tsv IS NULL"
            )
            logger.info("  %s rows touched", cur.rowcount)
            conn.commit()

            _redefine_generated_tsv(
                cur, "sra_studies", _SRA_TSV_EXPR, "sra_studies_fts_idx"
            )
            conn.commit()
            _redefine_generated_tsv(
                cur, "arrayexpress_experiments", _AE_TSV_EXPR, "ae_experiments_fts_idx"
            )
            conn.commit()
    logger.info("sample_tsv transfer done")
