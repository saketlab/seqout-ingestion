"""
Fold sample-level title/description into each study's search ``tsv`` so a study
is findable by terms that only appear in its samples (recall). Sample text is
deduped, position-stripped-by-dedup, and added at weight ``D`` so it surfaces a
study without outranking a real title/abstract hit.

Mechanism (a generated column can't reference other tables, so sample text is
materialized into a per-study ``sample_tsv`` column that the study ``tsv`` reads):

  geo_series               tsv via TRIGGER  -> trigger fn appends coalesce(sample_tsv,'')
  sra_studies              tsv GENERATED    -> generated expr appends coalesce(sample_tsv,'')
  arrayexpress_experiments tsv GENERATED    -> generated expr appends coalesce(sample_tsv,'')

sample_tsv joins:
  GEO  geo_samples.accession = ANY(geo_series.samples_ref)
  SRA  sra_experiments.study = sra_studies.accession
       AND sra_samples.accession = ANY(sra_experiments.samples)      (two-hop)
  AE   arrayexpress_samples.experiment_accession = arrayexpress_experiments.accession

Titles that are just the sample's own accession are dropped (noise); the
description is always kept. AE samples have no accession column of their own, so
their title is used as-is. Re-runnable: ``sample_tsv`` is SET (not appended),
and the schema steps are guarded so they run at most once.
"""

import logging

import psycopg

logger = logging.getLogger(__name__)


# Compound cell-line names ("HepG2/C3A", "Hep-G2") get parsed as a single
# path-like lexeme, so a query for "hepg2" never matches. Normalize '/' and '-'
# to spaces before to_tsvector so the parts become their own lexemes (recall).
# The query side isn't translated, but the english config already splits
# hyphenated query terms into their parts, so both sides still meet.
def _norm(text_expr: str) -> str:
    return f"translate({text_expr}, '/-', '  ')"


# Accession-only titles carry no search value; drop them but keep description.
# concat_ws skips NULLs, and string_agg(DISTINCT ...) collapses replicate samples
# (identical title/description) so to_tsvector's position lists stay bounded.
# left(...) caps the aggregated text: a few studies have tens of thousands of
# unique samples whose combined text builds a tsvector past the hard 1 MB limit.
# fixed 400 KB cap, well under the 1 MB tsvector limit.
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
        # channels holds the sample's cell-line / annotation metadata that native
        # GEO matches on: Source (source_name free text) plus the #text VALUES of
        # each Characteristics entry (e.g. "HepG2" from {"@tag":"cell line",
        # "#text":"HepG2"}). Tag names are skipped (ubiquitous labels, no signal).
        # 'lax $[*]' tolerates Characteristics being an array, a lone object, or
        # absent. Protocol fields (Growth/Treatment/Extract) are left out: they
        # carry incidental mentions ("HepG2 conditioned media"), not the subject.
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

# AE samples have no accession column, so the title is kept as-is (no filter).
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

# GEO tsv is trigger-built; append the (same-row) sample_tsv. CREATE OR REPLACE is
# idempotent. Must match the existing trigger body plus the final sample line.
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
      ||  coalesce(NEW.sample_tsv, ''::tsvector);
    RETURN NEW;
END $$;
"""


# SRA/AE tsv are GENERATED ALWAYS. PG16 has no ALTER ... SET EXPRESSION, so the
# column is dropped and re-added (which also drops/rebuilds its GIN index). The
# base expression must match the live schema exactly, plus the sample_tsv tail.
def _weighted(col: str, weight: str) -> str:
    """setweight(to_tsvector(normalized COALESCE(col)), weight) — a tsv term."""
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
)

_STUDY_TABLES = ("geo_series", "sra_studies", "arrayexpress_experiments")


def _ensure_sample_tsv_columns(cur: psycopg.Cursor) -> None:
    for table in _STUDY_TABLES:
        cur.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sample_tsv tsvector",
        )


def _tsv_up_to_date(cur: psycopg.Cursor, table: str) -> bool:
    """True once the generated tsv expression folds in sample_tsv AND applies
    the translate() normalization. Both markers must be present, so a table
    left at an older definition (samples but no normalization) is redefined."""
    cur.execute(
        "SELECT generation_expression FROM information_schema.columns"
        " WHERE table_name = %s AND column_name = 'tsv'",
        (table,),
    )
    row = cur.fetchone()
    expr = (row[0] or "") if row else ""
    return "sample_tsv" in expr and "translate" in expr


def _redefine_generated_tsv(
    cur: psycopg.Cursor, table: str, tsv_expr: str, index_name: str
) -> None:
    """Drop + re-add the GENERATED tsv column with the sample_tsv tail, then
    rebuild its GIN index. No-op if already done. Rewrites the study table
    (studies only — small — not the sample tables)."""
    if _tsv_up_to_date(cur, table):
        logger.info("%s.tsv already up to date; skipping redefine", table)
        return
    logger.info("redefining %s.tsv to include sample_tsv (rewrites table)", table)
    # DROP COLUMN also drops the dependent GIN index; recreate it by name after.
    cur.execute(f"ALTER TABLE {table} DROP COLUMN tsv")
    cur.execute(
        f"ALTER TABLE {table} ADD COLUMN tsv tsvector"
        f" GENERATED ALWAYS AS ({tsv_expr}) STORED",
    )
    cur.execute(f"CREATE INDEX {index_name} ON {table} USING gin (tsv)")


def sample_tsv_transfer(pg_url: str) -> None:
    """Populate per-study ``sample_tsv`` from the sample tables and wire it into
    each study's search ``tsv``. Idempotent and re-runnable.

    Heavy: aggregates over geo_samples / sra_samples / arrayexpress_samples. Run
    as a one-time migration and thereafter as an ingest step whenever samples
    change. ``statement_timeout`` is disabled for the duration.
    """
    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")

            # 1) sample_tsv column on every study table.
            _ensure_sample_tsv_columns(cur)
            conn.commit()

            # 2) GEO: update the trigger FIRST so the populate step (which fires
            #    it) rebuilds tsv with sample text in a single pass.
            cur.execute(_GEO_TRIGGER_FN)
            conn.commit()

            # 3) Populate sample_tsv per source (SET => re-runnable).
            for name, sql in (
                ("geo_series", _GEO_POPULATE),
                ("sra_studies", _SRA_POPULATE),
                ("arrayexpress_experiments", _AE_POPULATE),
            ):
                logger.info("populating sample_tsv for %s ...", name)
                cur.execute(sql)
                logger.info("  %s rows updated", cur.rowcount)
                conn.commit()

            # 3b) GEO series with no matching samples were skipped above, so the
            #     updated (translate-normalized) trigger never re-fired for them.
            #     Touch them so their study-text tsv is rebuilt too.
            logger.info("touching sample-less geo_series to rebuild tsv ...")
            cur.execute(
                "UPDATE geo_series SET sample_tsv = sample_tsv WHERE sample_tsv IS NULL"
            )
            logger.info("  %s rows touched", cur.rowcount)
            conn.commit()

            # 4) SRA/AE: fold sample_tsv into the generated tsv (guarded).
            _redefine_generated_tsv(
                cur, "sra_studies", _SRA_TSV_EXPR, "sra_studies_fts_idx"
            )
            conn.commit()
            _redefine_generated_tsv(
                cur, "arrayexpress_experiments", _AE_TSV_EXPR, "ae_experiments_fts_idx"
            )
            conn.commit()
    logger.info("sample_tsv transfer done")
