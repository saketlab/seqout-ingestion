-- Long-read collection: one row per study with at least one PacBio or Oxford
-- Nanopore experiment, plus a facet rollup. Backs /longread/{summary,facets,
-- projects} and the long_read search filter.
--
-- Platform comes from the experiment tables, not the study-level arrays: only
-- SRA and GEO carry those, and GEO's platforms[] holds GPL ids. GSA stores
-- model names where the others store the platform vocabulary, so it is mapped
-- the way create_platform_matviews.sh maps it. GEO series enter through their
-- instrument names AND through their linked SRP, and the two are merged by the
-- unified_metadata alias map so a study counts once. Onso is PacBio's
-- short-read instrument and is excluded even though SRA files it under
-- PACBIO_SMRT. Synthetic-Long-Read and linked-read strategies are Illumina and
-- stay out.
--
-- DROP plus CREATE, not IF NOT EXISTS, so a definition change is applied.
--
-- singlecell_study and country_study (sql/matviews/singlecell_pg.sql,
-- country_pg.sql) both join longread_study, so dropping it CASCADEs those
-- away too (found live 2026-09-14: a bare DROP failed with "other objects
-- depend on it"). Run `just refresh_technology_matviews` for the full
-- longread -> singlecell -> country rebuild in dependency order, not this
-- file alone, unless you are prepared to also re-run the other two.
--
-- Postgres only; run with `just longread_matview`. Not part of the SQLite
-- schema test, which is why it lives outside sql/tables.

DROP MATERIALIZED VIEW IF EXISTS longread_facet;
DROP MATERIALIZED VIEW IF EXISTS longread_study CASCADE;

CREATE MATERIALIZED VIEW longread_study AS
WITH lr AS (
    -- one row per long-read experiment (SRA, DRA, GEO) or run (ENA, GSA)
    SELECT e.study AS raw_accession, 'sra'::text AS archive, e.accession AS experiment,
           e.platform, e.instrument_model, e.library_strategy
    FROM sra_experiments e
    WHERE e.platform IN ('PACBIO_SMRT', 'OXFORD_NANOPORE')
      AND e.instrument_model IS DISTINCT FROM 'Onso'
    UNION ALL
    SELECT e.study_accession, 'ena', e.experiment_accession,
           e.instrument_platform, e.instrument_model, e.library_strategy
    FROM ena_experiments e
    WHERE e.instrument_platform IN ('PACBIO_SMRT', 'OXFORD_NANOPORE')
      AND e.instrument_model IS DISTINCT FROM 'Onso'
    UNION ALL
    SELECT e.study, 'dra', e.accession, e.platform, e.instrument_model, e.library_strategy
    FROM dra_experiments e
    WHERE e.platform IN ('PACBIO_SMRT', 'OXFORD_NANOPORE')
      AND e.instrument_model IS DISTINCT FROM 'Onso'
    UNION ALL
    SELECT e.study_accession, 'gsa', e.experiment_accession,
           CASE WHEN e.instrument_platform ILIKE 'PacBio %' THEN 'PACBIO_SMRT'
                ELSE 'OXFORD_NANOPORE' END,
           coalesce(nullif(e.instrument_model, ''), e.instrument_platform),
           e.library_strategy
    FROM gsa_experiments e
    WHERE e.instrument_platform ILIKE 'PacBio %'
       OR e.instrument_platform ILIKE 'OXFORD_NANOPORE%'
    UNION ALL
    -- GEO has no experiment rows; the series lists instrument names
    SELECT g.accession, 'geo', g.accession || ':' || m,
           CASE WHEN m ~* '(pacbio|sequel|revio|smrt)' THEN 'PACBIO_SMRT'
                ELSE 'OXFORD_NANOPORE' END,
           m, NULL::text
    FROM geo_series g, unnest(g.instrument_models) m
    WHERE m ~* '(pacbio|sequel|revio|smrt|minion|gridion|promethion|flongle|nanopore)'
      AND m !~* 'onso'
),
alias_map AS (
    -- every accession a study is known by, pointing at its canonical form;
    -- min() because an ENA project id can be an alias of both a GSE and its SRP
    SELECT alias, min(canonical_accession) AS canonical_accession FROM (
        SELECT canonical_accession, canonical_accession AS alias FROM unified_metadata
        UNION
        SELECT u.canonical_accession, a.value
        FROM unified_metadata u, jsonb_each_text(u.aliases) AS a
        WHERE a.value <> ''
    ) x
    GROUP BY 1
),
canon AS (
    SELECT coalesce(m.canonical_accession, lr.raw_accession) AS study_accession, lr.*
    FROM lr LEFT JOIN alias_map m ON m.alias = lr.raw_accession
    WHERE lr.raw_accession IS NOT NULL  -- an experiment with no study is unplaceable
),
agg AS (
    SELECT study_accession,
           array_agg(DISTINCT raw_accession)                       AS accessions,
           array_agg(DISTINCT archive)                             AS archives,
           array_agg(DISTINCT CASE platform WHEN 'PACBIO_SMRT' THEN 'PacBio'
                                            ELSE 'Oxford Nanopore' END) AS technologies,
           array_agg(DISTINCT platform)                            AS platforms,
           array_agg(DISTINCT instrument_model)
               FILTER (WHERE nullif(instrument_model, '') IS NOT NULL) AS instrument_models,
           array_agg(DISTINCT library_strategy)
               FILTER (WHERE nullif(library_strategy, '') IS NOT NULL) AS library_strategies,
           count(DISTINCT experiment)                              AS n_experiments
    FROM canon
    GROUP BY 1
),
-- every experiment of these studies, any platform, to tell long-read-only from hybrid
all_exp AS (
    SELECT e.study AS raw_accession, e.accession AS experiment FROM sra_experiments e
    WHERE e.study IN (SELECT raw_accession FROM lr WHERE archive = 'sra')
    UNION ALL
    SELECT e.study_accession, e.experiment_accession FROM ena_experiments e
    WHERE e.study_accession IN (SELECT raw_accession FROM lr WHERE archive = 'ena')
    UNION ALL
    SELECT e.study, e.accession FROM dra_experiments e
    WHERE e.study IN (SELECT raw_accession FROM lr WHERE archive = 'dra')
    UNION ALL
    SELECT e.study_accession, e.experiment_accession FROM gsa_experiments e
    WHERE e.study_accession IN (SELECT raw_accession FROM lr WHERE archive = 'gsa')
),
totals AS (
    SELECT coalesce(m.canonical_accession, x.raw_accession) AS study_accession,
           count(DISTINCT x.experiment) AS n_experiments_total
    FROM all_exp x LEFT JOIN alias_map m ON m.alias = x.raw_accession
    GROUP BY 1
),
chem AS (
    -- longread_chemistry keys run_accession -> its own study_accession, not
    -- necessarily canonical, so resolve through the same alias_map as lr.
    SELECT coalesce(m.canonical_accession, lc.study_accession) AS study_accession,
           array_agg(DISTINCT lc.chemistry)
               FILTER (WHERE lc.chemistry IS NOT NULL) AS chemistries,
           count(DISTINCT lc.run_accession) AS n_chemistry_runs,
           count(DISTINCT lc.run_accession)
               FILTER (WHERE lc.chemistry_confidence = 'exact') AS n_chemistry_exact
    FROM longread_chemistry lc
    LEFT JOIN alias_map m ON m.alias = lc.study_accession
    GROUP BY 1
),
org_union AS (
    SELECT a.study_accession,
           array_agg(DISTINCT o) FILTER (WHERE nullif(btrim(o), '') IS NOT NULL) AS organisms
    FROM agg a
    LEFT JOIN unified_metadata   u  ON u.canonical_accession = a.study_accession
    LEFT JOIN sra_studies        ss ON ss.accession          = a.study_accession
    LEFT JOIN LATERAL (
        SELECT organisms FROM ena_studies_search x
        WHERE x.accession = a.study_accession ORDER BY first_public LIMIT 1
    ) es ON TRUE
    LEFT JOIN geo_series         gs ON gs.accession          = a.study_accession
    LEFT JOIN gsa_studies_search ga ON ga.accession          = a.study_accession
    LEFT JOIN dra_studies_search dr ON dr.accession          = a.study_accession
    CROSS JOIN LATERAL unnest(
        ARRAY[u.dominant_scientific_name]
        || coalesce(ss.organisms, '{}') || coalesce(es.organisms, '{}')
        || coalesce(gs.organisms, '{}') || coalesce(ga.organisms, '{}')
        || coalesce(dr.organisms, '{}')
    ) AS o
    GROUP BY 1
)
SELECT a.study_accession,
       a.accessions, a.archives, a.technologies, a.platforms,
       a.instrument_models, a.library_strategies,
       a.n_experiments,
       t.n_experiments_total,
       -- NULL total means no experiment rows (GEO-only), so hybrid is unknown
       CASE WHEN t.n_experiments_total IS NULL THEN NULL
            ELSE a.n_experiments >= t.n_experiments_total END AS long_read_only,
       coalesce(u.title, ss.title, es.study_title, gs.title, ga.study_title, dr.study_title) AS title,
       coalesce(u.dominant_scientific_name, ou.organisms[1])  AS organism,
       ou.organisms,
       u.n_samples,
       u.dominant_assay_l1                                     AS assay_l1,
       u.is_single_cell,
       u.country,
       u.pmid,
       pub.first_published,
       extract(year FROM pub.first_published)::int             AS year,
       dl.has_fastq, dl.has_sra, dl.n_runs, dl.n_fastq_runs, dl.n_sra_runs,
       c.chemistries, c.n_chemistry_runs, c.n_chemistry_exact,
       coalesce(c.n_chemistry_exact, 0) > 0 AS has_exact_chemistry
FROM agg a
LEFT JOIN totals t              ON t.study_accession  = a.study_accession
LEFT JOIN chem c                ON c.study_accession  = a.study_accession
LEFT JOIN org_union ou          ON ou.study_accession = a.study_accession
LEFT JOIN unified_metadata u    ON u.canonical_accession = a.study_accession
LEFT JOIN sra_studies ss        ON ss.accession = a.study_accession
-- ena_studies_search repeats an accession per project alias; take one row
LEFT JOIN LATERAL (
    SELECT study_title, first_public FROM ena_studies_search x
    WHERE x.accession = a.study_accession ORDER BY first_public LIMIT 1
) es ON TRUE
LEFT JOIN geo_series gs         ON gs.accession = a.study_accession
LEFT JOIN gsa_studies_search ga ON ga.accession = a.study_accession
LEFT JOIN dra_studies_search dr ON dr.accession = a.study_accession
LEFT JOIN study_download_summary dl ON dl.study_accession = a.study_accession
-- unified_metadata.first_published is text; only an ISO prefix is trusted
CROSS JOIN LATERAL (
    SELECT coalesce(
        CASE WHEN u.first_published ~ '^\d{4}-\d{2}-\d{2}'
             THEN left(u.first_published, 10)::date END,
        ss.published_at, es.first_public, gs.published_at, ga.first_public, dr.first_public
    ) AS first_published
) pub;

CREATE MATERIALIZED VIEW longread_facet AS
WITH tagged AS (
    SELECT 'technology' AS facet, unnest(technologies) AS value, study_accession FROM longread_study
    UNION ALL
    SELECT 'platform', unnest(platforms), study_accession FROM longread_study
    UNION ALL
    SELECT 'instrument_model', unnest(instrument_models), study_accession FROM longread_study
    UNION ALL
    SELECT 'library_strategy', unnest(library_strategies), study_accession FROM longread_study
    UNION ALL
    SELECT 'organism', unnest(organisms), study_accession FROM longread_study
    UNION ALL
    SELECT 'archive', unnest(archives), study_accession FROM longread_study
    UNION ALL
    SELECT 'chemistry', unnest(chemistries), study_accession FROM longread_study
    UNION ALL
    SELECT 'has_exact_chemistry',
           CASE WHEN has_exact_chemistry THEN 'true' ELSE 'false' END, study_accession
    FROM longread_study
    UNION ALL
    SELECT 'assay_l1', assay_l1, study_accession FROM longread_study WHERE assay_l1 IS NOT NULL
    UNION ALL
    SELECT 'year', year::text, study_accession FROM longread_study WHERE year IS NOT NULL
    UNION ALL
    SELECT 'long_read_only', CASE WHEN long_read_only THEN 'true' ELSE 'false' END, study_accession
    FROM longread_study WHERE long_read_only IS NOT NULL
)
SELECT facet, value, count(DISTINCT study_accession) AS studies
FROM tagged WHERE value <> '' GROUP BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_longread_facet_pk ON longread_facet (facet, value);
CREATE UNIQUE INDEX IF NOT EXISTS idx_longread_study_pk ON longread_study (study_accession);
CREATE INDEX IF NOT EXISTS idx_longread_study_accessions ON longread_study USING gin (accessions);
CREATE INDEX IF NOT EXISTS idx_longread_study_technologies ON longread_study USING gin (technologies);
CREATE INDEX IF NOT EXISTS idx_longread_study_platforms ON longread_study USING gin (platforms);
CREATE INDEX IF NOT EXISTS idx_longread_study_instruments ON longread_study USING gin (instrument_models);
CREATE INDEX IF NOT EXISTS idx_longread_study_strategies ON longread_study USING gin (library_strategies);
CREATE INDEX IF NOT EXISTS idx_longread_study_organisms ON longread_study USING gin (organisms);
CREATE INDEX IF NOT EXISTS idx_longread_study_archives ON longread_study USING gin (archives);
-- One per sortable column, with the tiebreak, so pages do not repeat rows.
CREATE INDEX IF NOT EXISTS idx_longread_study_experiments
    ON longread_study (n_experiments DESC NULLS LAST, study_accession);
CREATE INDEX IF NOT EXISTS idx_longread_study_samples
    ON longread_study (n_samples DESC NULLS LAST, study_accession);
CREATE INDEX IF NOT EXISTS idx_longread_study_runs
    ON longread_study (n_runs DESC NULLS LAST, study_accession);
CREATE INDEX IF NOT EXISTS idx_longread_study_published
    ON longread_study (first_published DESC NULLS LAST, study_accession);
CREATE INDEX IF NOT EXISTS idx_longread_study_title ON longread_study (title, study_accession);
CREATE INDEX IF NOT EXISTS idx_longread_study_assay ON longread_study (assay_l1);
CREATE INDEX IF NOT EXISTS idx_longread_study_year ON longread_study (year);
CREATE INDEX IF NOT EXISTS idx_longread_study_fastq ON longread_study (has_fastq) WHERE has_fastq;
CREATE INDEX IF NOT EXISTS idx_longread_study_chemistries ON longread_study USING gin (chemistries);
CREATE INDEX IF NOT EXISTS idx_longread_study_exact_chem
    ON longread_study (has_exact_chemistry) WHERE has_exact_chemistry;
