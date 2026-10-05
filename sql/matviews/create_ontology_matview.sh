#!/bin/bash
set -e

# Ontology-term enrichment behind /tissue/{term}/* and /disease/{term}/*.
# One script for both entities: tissue (UBERON) and disease (MONDO) share the
# exact same shape, so only the ontology, thresholds, view names and column
# names differ, set by the case block below.
#
# Two views per entity, both one row keyed differently:
#   <term view>: one row per (study, ontology id). The id and its display
#     name are unambiguous here, and n_samples is scoped to that one id. Use
#     this for a count matching a resolved id set: SUM(n_samples) WHERE
#     <id col> = ANY(ids) GROUP BY study_accession.
#   <study view>: one row per study, with the array of every id it matched
#     (for the && filter) plus study-level attributes. It carries no name
#     array and no per-term count: array_agg(DISTINCT name) next to
#     array_agg(id) cannot be paired by position (dedup can shorten one array
#     and not the other), and a study-wide sample sum silently answers a
#     different question than "how many samples matched the queried term."
#     Look names and per-term counts up in the term view.
#
# A request resolves free text to an id set (matching ontology_terms rows plus
# every ontology_closure descendant) and filters the study view with
# <ids col> && ids. See queries.py's tissue/disease term resolution.
#
# Per sample, ontology_samples_v5 (the LLM pipeline) wins whenever it has
# processed that sample at all; ontology_samples_fast (embedding-only) fills
# in only the samples v5 has not reached yet, whichever pipeline reached the
# sample first.
#
# Joins longread_study, so a longread_matview rebuild (DROP ... CASCADE) takes
# both views in this script down with it. refresh_technology_matviews rebuilds
# this script after longread for that reason.
#
# unified_metadata is not deduped (a GSE and its linked SRP each get a row),
# so ontology_samples_v5/_fast rows for the same study can carry either
# accession. study_canonical (build_study_canonical.py) maps both to one
# winner before grouping; an accession missing from it (not yet in
# unified_metadata) keeps its own value rather than being dropped.

ENTITY="${1:?usage: create_ontology_matview.sh <tissue|disease>}"
case "$ENTITY" in
    tissue)
        ONTOLOGY=uberon
        MIN_ROWS=10000
        ID_COL=tissue_ontology_id
        NAME_COL=tissue_ontology_name
        IDS_COL=tissue_ontology_ids
        N_MATCHED_COL=n_samples_with_tissue
        TERM_VIEW=tissue_study_term
        STUDY_VIEW=tissue_study
        ;;
    disease)
        ONTOLOGY=mondo
        MIN_ROWS=100000
        ID_COL=disease_ontology_id
        NAME_COL=disease_ontology_name
        IDS_COL=disease_ontology_ids
        N_MATCHED_COL=n_samples_with_disease
        TERM_VIEW=disease_study_ontology_term
        STUDY_VIEW=disease_study_ontology
        ;;
    *)
        echo "unknown entity '$ENTITY' (expected tissue or disease)" >&2
        exit 1
        ;;
esac

# Exported DATABASE_URL wins; just tissue_matview/disease_ontology_matview
# pass a peer-authenticated socket.
DATABASE_URL="${DATABASE_URL:-$(grep '^DATABASE_URL=' .env | cut -d= -f2-)}"

echo "Creating $TERM_VIEW, $STUDY_VIEW ..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<EOF
DO \$\$
BEGIN
    IF (SELECT count(*) FROM ontology_closure WHERE ontology = '$ONTOLOGY') < $MIN_ROWS
    THEN
        RAISE EXCEPTION
            'ontology_closure has too few $ONTOLOGY rows; refusing to build a view '
            'whose descendant expansion would silently be empty.';
    END IF;
END \$\$;

DROP MATERIALIZED VIEW IF EXISTS $STUDY_VIEW;
DROP MATERIALIZED VIEW IF EXISTS $TERM_VIEW;

CREATE MATERIALIZED VIEW $TERM_VIEW AS
WITH term_rows AS (
    SELECT coalesce(sc.canonical_accession, v5.study_accession) AS study_accession,
           v5.sample, v5.$ID_COL, v5.$NAME_COL
    FROM ontology_samples_v5 v5
    LEFT JOIN study_canonical sc ON sc.accession = v5.study_accession
    WHERE v5.$ID_COL IS NOT NULL
    UNION ALL
    SELECT coalesce(sc.canonical_accession, f.study_accession) AS study_accession,
           f.sample, f.$ID_COL, f.$NAME_COL
    FROM ontology_samples_fast f
    LEFT JOIN study_canonical sc ON sc.accession = f.study_accession
    WHERE f.$ID_COL IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM ontology_samples_v5 v5
           WHERE v5.study_accession = f.study_accession AND v5.sample = f.sample
      )
)
SELECT study_accession,
       $ID_COL,
       -- most frequent observed label for this id in this study; ties break
       -- on collation order, deterministic but not label-preferring
       mode() WITHIN GROUP (ORDER BY $NAME_COL) AS $NAME_COL,
       count(*) AS n_samples
FROM term_rows
GROUP BY 1, 2;

CREATE UNIQUE INDEX idx_${TERM_VIEW}_pk ON $TERM_VIEW (study_accession, $ID_COL);
CREATE INDEX idx_${TERM_VIEW}_id ON $TERM_VIEW ($ID_COL);

CREATE MATERIALIZED VIEW $STUDY_VIEW AS
WITH rolled AS (
    SELECT study_accession,
           array_agg($ID_COL) AS $IDS_COL,
           sum(n_samples)     AS $N_MATCHED_COL
    FROM $TERM_VIEW
    GROUP BY 1
)
-- study_download_summary/longread_study/study_pub_dates are keyed by raw
-- accession, same as ontology_samples_*; a study's download links or pub date
-- can sit under an alias other than the winner picked above, so the LATERAL
-- joins below look them up across every accession study_canonical maps to
-- that winner. They join study_canonical directly (indexed on
-- canonical_accession) rather than through a CTE, which Postgres materializes
-- without an index and turns each correlated lookup into a full scan.
SELECT r.study_accession,
       u.title,
       u.dominant_scientific_name AS organism,
       u.dominant_assay_l1        AS assay_l1,
       u.dominant_assay_l2        AS assay_l2,
       CASE u.source
           WHEN 'ae' THEN 'ArrayExpress'
           WHEN 'geo' THEN 'GEO'
           WHEN 'sra' THEN 'SRA'
           WHEN 'ena' THEN 'ENA'
           WHEN 'gsa' THEN 'GSA'
           WHEN 'dra' THEN 'DRA'
           WHEN 'gea' THEN 'GEA'
           ELSE upper(u.source)
       END                        AS source,
       u.journal,
       u.country_code_iso2,
       pd.pub_date,
       u.n_samples,
       u.n_experiments,
       u.is_single_cell,
       u.single_cell_modality,
       r.$IDS_COL,
       r.$N_MATCHED_COL,
       dl.has_fastq, dl.has_sra, dl.n_runs, dl.n_fastq_runs, dl.n_sra_runs,
       coalesce(lr.is_long_read, false) AS is_long_read
FROM rolled r
LEFT JOIN unified_metadata u ON u.canonical_accession = r.study_accession
-- study_download_summary already sums run counts across every alias IT knows
-- of a study (its own alias_map, built the same way this table is); its row
-- for each of a study's accessions is already the full total. Summing those
-- already-total rows again here would double (or triple) it. MAX picks the
-- most complete one instead of adding equals together; it can only
-- under-report if two aliases' own alias-maps disagree, never over-report.
LEFT JOIN LATERAL (
    SELECT bool_or(d.has_fastq) AS has_fastq, bool_or(d.has_sra) AS has_sra,
           max(d.n_runs) AS n_runs, max(d.n_fastq_runs) AS n_fastq_runs,
           max(d.n_sra_runs) AS n_sra_runs
    FROM study_canonical sc
    JOIN study_download_summary d ON d.study_accession = sc.accession
    WHERE sc.canonical_accession = r.study_accession
) dl ON true
-- longread_study canonicalizes aliases with its own inline CTE (bioproject
-- included), which can pick a different winner than study_canonical for the
-- same multi-alias study; where they disagree, this join misses and
-- is_long_read under-reports. See build_study_canonical.py's module docstring.
LEFT JOIN LATERAL (
    SELECT bool_or(true) AS is_long_read
    FROM study_canonical sc
    JOIN longread_study lr2 ON lr2.study_accession = sc.accession
    WHERE sc.canonical_accession = r.study_accession
) lr ON true
LEFT JOIN LATERAL (
    SELECT min(sp.pub_date) AS pub_date
    FROM study_canonical sc
    JOIN study_pub_dates sp ON sp.accession = sc.accession
    WHERE sc.canonical_accession = r.study_accession
) pd ON true;

CREATE UNIQUE INDEX idx_${STUDY_VIEW}_accession ON $STUDY_VIEW (study_accession);
CREATE INDEX idx_${STUDY_VIEW}_ids ON $STUDY_VIEW USING gin ($IDS_COL);
CREATE INDEX idx_${STUDY_VIEW}_organism ON $STUDY_VIEW (organism);
CREATE INDEX idx_${STUDY_VIEW}_assay_l1 ON $STUDY_VIEW (assay_l1);
CREATE INDEX idx_${STUDY_VIEW}_source ON $STUDY_VIEW (source);
CREATE INDEX idx_${STUDY_VIEW}_fastq ON $STUDY_VIEW (has_fastq) WHERE has_fastq;
CREATE INDEX idx_${STUDY_VIEW}_sra ON $STUDY_VIEW (has_sra) WHERE has_sra;
CREATE INDEX idx_${STUDY_VIEW}_long_read ON $STUDY_VIEW (is_long_read) WHERE is_long_read;
CREATE INDEX idx_${STUDY_VIEW}_single_cell ON $STUDY_VIEW (is_single_cell) WHERE is_single_cell;
-- Matches build_ontology_term_projects_sql's sort_val expression: coalesce
-- keeps the cursor comparison NULL-free, and the query sorts/tiebreaks DESC
-- by default regardless of requested direction (Postgres walks a DESC index
-- backwards for ASC), so one index per sortable column serves both.
CREATE INDEX idx_${STUDY_VIEW}_sort_pub_date
    ON $STUDY_VIEW (coalesce(pub_date, '1970-01-01'::date) DESC, study_accession DESC);
CREATE INDEX idx_${STUDY_VIEW}_sort_n_samples
    ON $STUDY_VIEW (coalesce(n_samples, 0) DESC, study_accession DESC);
CREATE INDEX idx_${STUDY_VIEW}_sort_n_experiments
    ON $STUDY_VIEW (coalesce(n_experiments, 0) DESC, study_accession DESC);
ANALYZE $TERM_VIEW;
ANALYZE $STUDY_VIEW;
EOF

echo "Done."
