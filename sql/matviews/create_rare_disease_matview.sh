#!/bin/bash
set -e

# Study-level rollup behind /disease/{collection}/*. Run once per catalogue:
#
#     bash scripts/create_rare_disease_matview.sh gard   -> rare_disease_{study,facet}
#     bash scripts/create_rare_disease_matview.sh nord   -> nord_disease_{study,facet}
#
# Both catalogues are keyed by MONDO, so one script builds both; only the
# catalogue table, its name column and its facetable tags differ.
#
# Precomputed because membership is "this sample's disease, or any of its MONDO
# ancestors, is in the catalogue", which is too slow to resolve per request.

COLLECTION="${1:-gard}"
case "$COLLECTION" in
    gard)
        CATALOGUE=gard_disease
        NAME_COL=gard_name
        PREFIX=rare_disease
        LABEL_COL=gard_names
        # facet_name:view_column:set-yielding expression over alias cat
        TAGS="category:categories:cat.categories specialty:specialties:cat.specialties group:groups:cat.groups"
        ;;
    nord)
        CATALOGUE=nord_disease
        NAME_COL=nord_name
        PREFIX=nord_disease
        LABEL_COL=nord_names
        # NORD has no category scheme; array the scalar source collection axis.
        TAGS="nord_type:nord_types:ARRAY[cat.nord_type]"
        ;;
    *)
        echo "unknown collection '$COLLECTION' (expected gard or nord)" >&2
        exit 1
        ;;
esac

# Exported DATABASE_URL wins; just gard_matview passes a peer-authenticated socket.
DATABASE_URL="${DATABASE_URL:-$(grep '^DATABASE_URL=' .env | cut -d= -f2-)}"

TAG_SELECT=""
TAG_FACETS=""
TAG_INDEXES=""
for spec in $TAGS; do
    facet="${spec%%:*}"
    rest="${spec#*:}"
    col="${rest%%:*}"
    expr="${rest#*:}"
    TAG_SELECT="$TAG_SELECT
       (SELECT array_agg(DISTINCT t) FROM $CATALOGUE cat
          CROSS JOIN LATERAL unnest($expr) t
         WHERE cat.mondo_id = ANY(array_agg(e.mondo_id)))     AS $col,"
    TAG_FACETS="$TAG_FACETS
    SELECT '$facet' AS facet, unnest($expr) AS value, sm.study_accession
    FROM study_mondo sm JOIN $CATALOGUE cat ON cat.mondo_id = sm.mondo_id
    UNION ALL"
    TAG_INDEXES="$TAG_INDEXES
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_$col ON ${PREFIX}_study USING gin ($col);"
done

echo "Creating ${PREFIX}_study from $CATALOGUE ..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<EOF
-- An empty closure degrades expanded to the bare catalogue ids: the subtype
-- rollup vanishes while the build reports success.
DO \$\$
BEGIN
    IF (SELECT count(*) FROM ontology_closure WHERE ontology = 'mondo') < 100000
    THEN
        RAISE EXCEPTION
            'ontology_closure has too few mondo rows; refusing to build a view '
            'whose subtype rollup would silently be empty. Rebuild it first '
            '(pysradb-ingestion/enrichment/build_ontology_closure.py).';
    END IF;
END \$\$;

-- Dependent view first; dropping the base first fails on the dependency.
DROP MATERIALIZED VIEW IF EXISTS ${PREFIX}_facet;
DROP MATERIALIZED VIEW IF EXISTS ${PREFIX}_study;

CREATE MATERIALIZED VIEW ${PREFIX}_study AS
WITH expanded AS MATERIALIZED (
    -- catalogue ids plus every descendant; self-pairs are not stored, hence UNION
    SELECT descendant_id AS mondo_id FROM ontology_closure
     WHERE ontology = 'mondo'
       AND ancestor_id IN (SELECT mondo_id FROM $CATALOGUE WHERE mondo_id IS NOT NULL)
    UNION
    SELECT mondo_id FROM $CATALOGUE WHERE mondo_id IS NOT NULL
),
matched AS (
    -- arm 1: resolved id is in the catalogue or a descendant of one
    SELECT os.study_accession, os.sample, os.disease_ontology_id AS mondo_id,
           ARRAY[os.disease_ontology_id] AS matched_ids,
           os.disease_ontology_name,
           os.sex, os.assay, os.assay_category, os.tissue,
           os.organism, os.sample_type, os.ethnicity
    FROM ontology_samples_v4 os
    WHERE os.disease_ontology_id IN (SELECT mondo_id FROM expanded)
    UNION ALL  -- disjoint: arm 1 needs a non-null id, arm 2 an IS NULL
    -- arm 2: no resolved id, but disease_ancestors names one
    SELECT os.study_accession, os.sample, os.disease_ontology_id,
           -- The ancestors that hit the catalogue. Arm 2 has no resolved id, so
           -- without this the study lands with mondo_ids NULL and no disease
           -- filter returns it. expanded is MATERIALIZED so this scans it once.
           -- = ANY of a parenthesised subquery parses as ANY(subquery) and fails
           -- on text = text[]. No backticks: the heredoc is unquoted.
           ARRAY(SELECT a FROM unnest(mondo_ancestor_ids(os.disease_ancestors)) a
                  WHERE a IN (SELECT mondo_id FROM expanded)) AS matched_ids,
           os.disease_ontology_name,
           os.sex, os.assay, os.assay_category, os.tissue,
           os.organism, os.sample_type, os.ethnicity
    FROM ontology_samples_v4 os
    WHERE os.disease_ontology_id IS NULL
      AND os.disease_ancestors IS NOT NULL
      AND os.disease_ancestors <> ''
      AND mondo_ancestor_ids(os.disease_ancestors)
          && (SELECT array_agg(mondo_id) FROM expanded)
),
org_union AS (
    -- Every organism any table records for these studies, unioned. The
    -- enrichment column can be blank; these joins are unique, so they do not
    -- fan out.
    SELECT s.study_accession,
           array_agg(DISTINCT o) FILTER (WHERE nullif(btrim(o), '') IS NOT NULL)
               AS organisms
    FROM (SELECT DISTINCT study_accession FROM matched) s
    LEFT JOIN unified_metadata         u  ON u.canonical_accession = s.study_accession
    LEFT JOIN sra_studies              ss ON ss.accession          = s.study_accession
    LEFT JOIN ena_studies              es ON es.study_accession    = s.study_accession
    LEFT JOIN geo_series               gs ON gs.accession          = s.study_accession
    LEFT JOIN arrayexpress_experiments ae ON ae.accession          = s.study_accession
    LEFT JOIN gea_experiments          ge ON ge.accession          = s.study_accession
    LEFT JOIN gsa_studies              ga ON ga.study_accession    = s.study_accession
    LEFT JOIN dra_studies_search       dr ON dr.accession          = s.study_accession
    CROSS JOIN LATERAL unnest(
        ARRAY[u.dominant_scientific_name, es.scientific_name, ga.scientific_name]
        || coalesce(ss.organisms, '{}') || coalesce(es.organisms, '{}')
        || coalesce(gs.organisms, '{}') || coalesce(ae.organisms, '{}')
        || coalesce(ge.organisms, '{}') || coalesce(ga.organisms, '{}')
        || coalesce(dr.organisms, '{}')
    ) AS o
    GROUP BY 1
),
resolved AS (
    -- one row per study: every catalogue id its samples matched on
    SELECT study_accession, array_agg(DISTINCT m) AS resolved_mondo_ids
    FROM matched, unnest(matched_ids) AS m
    WHERE m IS NOT NULL
    GROUP BY 1
),
classified AS (
    -- One scope verdict per sample. sample_type is free text, so cell line and
    -- model match by pattern: alongside 'Cell Line' sit 'Cell Culture',
    -- 'Lymphoblastoid Cell Line' and 'Cell Cuture'.
    SELECT e.*,
           ou.organisms,
           u.dominant_scientific_name,
           es.title AS enriched_title,
           u.title AS unified_title,
           p.cells, p.sex_verdict, p.assay_is_single_cell,
           -- The sample's own organism wins, then the study's dominant, then
           -- the union. A flat union would admit a mouse-only study the moment
           -- any table mentioned a human.
           (CASE
              WHEN nullif(e.organism, '') IS NOT NULL
                  -- a sample may claim human only if some study-level source
                  -- agrees or none knows; per-sample mislabels are common
                  THEN e.organism = 'Homo sapiens'
                       AND (ou.organisms IS NULL
                            OR 'Homo sapiens' = ANY(ou.organisms))
              WHEN u.dominant_scientific_name IS NOT NULL
                  THEN u.dominant_scientific_name = 'Homo sapiens'
              ELSE 'Homo sapiens' = ANY(coalesce(ou.organisms, '{}'))
            END) AS is_human,
           (nullif(e.organism, '') IS NOT NULL
            OR u.dominant_scientific_name IS NOT NULL
            OR ou.organisms IS NOT NULL) AS organism_known,
           (lower(coalesce(e.sample_type, '')) ~ '(cell line|cell culture|cell cuture|cell cultrue)')
               AS is_cell_line,
           (lower(coalesce(e.sample_type, '')) ~ '(organoid|xenograft|ipsc|hipsc|mipsc|hpsc|esc-derived|hesc|mesc|induced pluripotent|embryonic stem)')
               AS is_model
    FROM matched e
    LEFT JOIN org_union ou ON ou.study_accession = e.study_accession
    LEFT JOIN unified_metadata u ON u.canonical_accession = e.study_accession
    LEFT JOIN enriched_studies_v4 es ON es.accession = e.study_accession
    LEFT JOIN pentimento_sample p ON p.sample_accession = e.sample
)
SELECT e.study_accession,
       coalesce(e.unified_title, e.enriched_title)          AS title,
       -- Sort and compare on the scalar, read the union.
       coalesce(e.dominant_scientific_name, e.organisms[1]) AS organism,
       e.organisms                                          AS organisms,
       count(*)                                             AS n_samples,
       count(DISTINCT e.mondo_id)                           AS n_diseases,
       array_agg(DISTINCT e.mondo_id) FILTER (WHERE e.mondo_id IS NOT NULL)
                                                            AS mondo_ids,
       -- The ids a disease filter tests. Equal to mondo_ids for arm 1; for arm 2
       -- the ancestor terms, which mondo_ids cannot hold. Aggregated in its own
       -- CTE because array_agg over per-row text[] of differing lengths is not a
       -- legal multidimensional array.
       res.resolved_mondo_ids,
       -- what the study itself is annotated with
       array_agg(DISTINCT e.disease_ontology_name)
           FILTER (WHERE e.disease_ontology_name IS NOT NULL) AS disease_names,
       -- direct catalogue hits; the ancestral roll-up is resolved per request
       (SELECT array_agg(DISTINCT cat.$NAME_COL) FROM $CATALOGUE cat
         WHERE cat.mondo_id = ANY(array_agg(e.mondo_id)))    AS $LABEL_COL,
       -- denormalized so projects filters with an indexed @>$TAG_SELECT
       -- disease_inheritance_all already merges HPO, Orphanet and GARD's modes.
       (SELECT array_agg(DISTINCT m) FROM disease_inheritance_all di
          CROSS JOIN LATERAL unnest(di.modes) m
         WHERE di.mondo_id = ANY(array_agg(e.mondo_id)))     AS inheritance,
       -- from study_download_summary (scripts/create_study_download_matview.sh).
       -- NULL means absent from the links table, so the file count is unknown.
       dl.has_fastq                                         AS has_fastq,
       dl.has_sra                                           AS has_sra,
       dl.n_fastq_runs                                      AS n_fastq_runs,
       dl.n_sra_runs                                        AS n_sra_runs,
       -- plain sum(): NULL survives to mean "not measured"
       sum(e.cells)                                    AS cells,
       -- Counters: /projects defaults to human primary material and the excluded
       -- rows stay inspectable.
       count(*) FILTER (WHERE e.is_human AND NOT e.is_cell_line
                          AND NOT e.is_model)                AS n_human_primary,
       count(*) FILTER (WHERE e.is_human AND e.is_model
                          AND NOT e.is_cell_line)            AS n_model,
       count(*) FILTER (WHERE e.is_cell_line)                AS n_cell_line,
       count(*) FILTER (WHERE NOT e.is_human
                          AND e.organism_known)              AS n_nonhuman,
       count(*) FILTER (WHERE NOT e.organism_known)          AS n_unknown_organism,
       sum(e.cells) FILTER (WHERE e.is_human AND NOT e.is_cell_line
                              AND NOT e.is_model)            AS cells_human_primary,
       mode() WITHIN GROUP (ORDER BY e.assay_category)      AS assay_category,
       -- stated and read-derived stay apart; the disagreements flag mislabels
       count(*) FILTER (WHERE lower(btrim(e.sex)) = 'male')        AS stated_male,
       count(*) FILTER (WHERE lower(btrim(e.sex)) = 'female')      AS stated_female,
       count(*) FILTER (WHERE nullif(btrim(e.sex), '') IS NULL)    AS stated_missing,
       -- Submitter-reported ethnicity: the values mix race, ethnicity and
       -- nationality, and stay verbatim.
       array_agg(DISTINCT btrim(e.ethnicity))
           FILTER (WHERE nullif(btrim(e.ethnicity), '') IS NOT NULL) AS ancestries,
       count(*) FILTER (WHERE e.sex_verdict = 'male')              AS reads_male,
       count(*) FILTER (WHERE e.sex_verdict = 'female')            AS reads_female,
       count(*) FILTER (WHERE e.assay_is_single_cell)              AS reads_single_cell
FROM classified e
LEFT JOIN resolved res ON res.study_accession = e.study_accession
LEFT JOIN study_download_summary dl ON dl.study_accession = e.study_accession
GROUP BY 1, 2, 3, 4, res.resolved_mondo_ids,
         dl.has_fastq, dl.has_sra, dl.n_fastq_runs, dl.n_sra_runs;

-- Precomputed; the live join is too slow.
CREATE MATERIALIZED VIEW ${PREFIX}_facet AS
WITH study_mondo AS (
    SELECT study_accession, unnest(mondo_ids) AS mondo_id FROM ${PREFIX}_study
),
tagged AS ($TAG_FACETS
    -- From the rollup: these describe the studies behind the disease.
    SELECT 'assay_category', assay_category, study_accession
    FROM ${PREFIX}_study WHERE assay_category IS NOT NULL
    UNION ALL
    SELECT 'organism', unnest(organisms), study_accession
    FROM ${PREFIX}_study WHERE organisms IS NOT NULL
    UNION ALL
    SELECT 'ancestry', unnest(ancestries), study_accession
    FROM ${PREFIX}_study WHERE ancestries IS NOT NULL
    UNION ALL
    SELECT 'inheritance', unnest(inheritance), study_accession
    FROM ${PREFIX}_study WHERE inheritance IS NOT NULL
)
SELECT facet, value, count(DISTINCT study_accession) AS studies
FROM tagged WHERE value <> '' GROUP BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_${PREFIX}_facet_pk ON ${PREFIX}_facet (facet, value);

CREATE UNIQUE INDEX IF NOT EXISTS idx_${PREFIX}_study_pk ON ${PREFIX}_study (study_accession);
-- One per sortable column, matching its ORDER BY exactly including the
-- tiebreak: a bare DESC is NULLS FIRST, and no unique tiebreak pages wrong.
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_cells
    ON ${PREFIX}_study (cells DESC NULLS LAST, study_accession);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_samples
    ON ${PREFIX}_study (n_samples DESC NULLS LAST, study_accession);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_diseases
    ON ${PREFIX}_study (n_diseases DESC NULLS LAST, study_accession);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_title
    ON ${PREFIX}_study (title, study_accession);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_assay ON ${PREFIX}_study (assay_category);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_organism ON ${PREFIX}_study (organism);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_organisms
    ON ${PREFIX}_study USING gin (organisms);
-- partial: /projects defaults to scope=human_primary
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_human_primary
    ON ${PREFIX}_study (cells DESC NULLS LAST, study_accession)
    WHERE n_human_primary > 0;
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_model
    ON ${PREFIX}_study (n_model) WHERE n_model > 0;
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_ancestries
    ON ${PREFIX}_study USING gin (ancestries);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_inheritance
    ON ${PREFIX}_study USING gin (inheritance);
CREATE INDEX IF NOT EXISTS idx_${PREFIX}_study_fastq
    ON ${PREFIX}_study (has_fastq) WHERE has_fastq;
$TAG_INDEXES
EOF

# No REFRESH: CREATE ... AS defaults to WITH DATA.

echo "Done."
