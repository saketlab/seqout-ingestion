-- Single-cell collection: one row per study with any single-cell evidence
-- (declared is_single_cell, a counted pentimento matrix, single-cell
-- barcode/read-length evidence, or explicit cell/nucleus modality), plus a
-- facet rollup. Backs /singlecell/{summary,facets,projects}.
--
-- Reverse-engineered 2026-09-14 from the live production view -- no creation
-- script for singlecell_study/singlecell_facet exists in any checked-in repo
-- (pysradb-server's queries.py and main.py only SELECT them; grepped
-- pysradb-server, pysraweb, pysradb-ingestion, pysradb-analytics,
-- pysradb-private, pysradb). This is `pg_get_viewdef()` output verbatim, not
-- hand-written -- if production's definition changes, re-pull it rather than
-- hand-editing this file out of sync.
--
-- Depends on longread_study (sql/matviews/longread_pg.sql, must run first),
-- unified_metadata, pentimento_sample/run/study, ontology_samples_v5, and
-- study_download_summary (sql/matviews/country_pg.sql) already existing.
--
-- DROP plus CREATE, not IF NOT EXISTS, so a definition change is applied.
-- country_facet (sql/matviews/country_pg.sql) selects from singlecell_study,
-- so re-run country_matview after this if singlecell_study's shape changes.
--
-- Postgres only; run with `just singlecell_matview`. Not part of the SQLite
-- schema test, which is why it lives outside sql/tables.

DROP MATERIALIZED VIEW IF EXISTS singlecell_facet;
DROP MATERIALIZED VIEW IF EXISTS singlecell_study CASCADE;

CREATE MATERIALIZED VIEW singlecell_study AS
WITH alias_map AS (
    SELECT x.alias,
           min(x.canonical_accession) AS canonical_accession
    FROM (
        SELECT unified_metadata.canonical_accession,
               unified_metadata.canonical_accession AS alias
        FROM unified_metadata
        UNION
        SELECT u_1.canonical_accession,
               a.value
        FROM unified_metadata u_1,
             LATERAL jsonb_each_text(u_1.aliases) a(key, value)
        WHERE a.value <> ''
    ) x
    GROUP BY x.alias
), run_ev AS (
    SELECT COALESCE(am.canonical_accession, s.raw_accession) AS study_accession,
           max(CASE WHEN s.src = 'matrix' THEN 1 ELSE 0 END) AS ev_matrix,
           max(CASE WHEN s.src = 'barcode' THEN 1 ELSE 0 END) AS ev_barcode,
           max(CASE WHEN s.src = 'r1len' THEN 1 ELSE 0 END) AS ev_r1len,
           array_agg(DISTINCT s.chem) FILTER (
               WHERE s.chem IS NOT NULL
                 AND s.chem NOT IN ('Not single-cell', 'RNA', 'United States', 'Drop-seq')
           ) AS chemistries
    FROM (
        SELECT pentimento_sample.study_accession AS raw_accession,
               'matrix' AS src,
               NULL::text AS chem
        FROM pentimento_sample
        WHERE pentimento_sample.cells > 0
        UNION ALL
        SELECT COALESCE(s2.study_accession, r.study_accession) AS raw_accession,
               'barcode',
               r.assay
        FROM pentimento_run r
        LEFT JOIN pentimento_sample s2 ON s2.sample_accession = r.gsm_accession
        WHERE r.assay_is_single_cell
        UNION ALL
        SELECT COALESCE(s2.study_accession, r.study_accession) AS raw_accession,
               'r1len',
               NULL::text
        FROM pentimento_run r
        LEFT JOIN pentimento_sample s2 ON s2.sample_accession = r.gsm_accession
        WHERE r.bulk_vs_sc = 'single_cell'
    ) s(raw_accession, src, chem)
    LEFT JOIN alias_map am ON am.alias = s.raw_accession
    GROUP BY COALESCE(am.canonical_accession, s.raw_accession)
), base AS (
    SELECT run_ev.study_accession FROM run_ev
    UNION
    SELECT unified_metadata.canonical_accession
    FROM unified_metadata
    WHERE unified_metadata.is_single_cell
), tissue_organism AS (
    SELECT ontology_samples_v5.study_accession,
           array_agg(DISTINCT COALESCE(ontology_samples_v5.tissue_ontology_name, ontology_samples_v5.tissue))
               FILTER (WHERE COALESCE(ontology_samples_v5.tissue_ontology_name, ontology_samples_v5.tissue) IS NOT NULL) AS tissues,
           array_agg(DISTINCT ontology_samples_v5.organism)
               FILTER (WHERE ontology_samples_v5.organism IS NOT NULL) AS organisms
    FROM ontology_samples_v5
    GROUP BY ontology_samples_v5.study_accession
), cell_nucleus_ev AS (
    SELECT COALESCE(am.canonical_accession, m.raw_accession) AS study_accession,
           array_agg(DISTINCT m.modality) AS cell_or_nucleus
    FROM (
        SELECT COALESCE(s2.study_accession, r.study_accession) AS raw_accession,
               r.modality
        FROM pentimento_run r
        LEFT JOIN pentimento_sample s2 ON s2.sample_accession = r.gsm_accession
        WHERE r.modality IN ('single-cell', 'single-nucleus')
    ) m
    LEFT JOIN alias_map am ON am.alias = m.raw_accession
    GROUP BY COALESCE(am.canonical_accession, m.raw_accession)
), perturbation_ev AS (
    SELECT b_1.study_accession,
           array_remove(ARRAY[
               CASE WHEN u_1.title ~* 'perturb.?seq' OR u_1.description ~* 'perturb.?seq' THEN 'Perturb-seq' END,
               CASE WHEN u_1.title ~* 'crop.?seq' OR u_1.description ~* 'crop.?seq' THEN 'CROP-seq' END,
               CASE WHEN u_1.title ~* 'eccite.?seq' OR u_1.description ~* 'eccite.?seq' THEN 'ECCITE-seq' END,
               CASE WHEN u_1.title ~* 'mosaic.?seq' OR u_1.description ~* 'mosaic.?seq' THEN 'Mosaic-seq' END,
               CASE WHEN u_1.title ~* 'sci.?plex' OR u_1.description ~* 'sci.?plex' THEN 'sci-Plex' END,
               CASE WHEN u_1.title ~* 'crispr.?detect' OR u_1.description ~* 'crispr.?detect' THEN 'CRISPR-detect' END
           ], NULL::text) AS perturbation_method
    FROM base b_1
    LEFT JOIN unified_metadata u_1 ON u_1.canonical_accession = b_1.study_accession
)
SELECT b.study_accession,
       COALESCE(u.title, ps.title) AS title,
       COALESCE(u.dominant_scientific_name, tso.organisms[1]) AS organism,
       tso.organisms,
       tso.tissues,
       u.single_cell_modality,
       re.chemistries,
       cn.cell_or_nucleus,
       u.dominant_assay_l1 AS assay_l1,
       u.n_samples,
       COALESCE(re.ev_matrix, 0) = 1 OR COALESCE(ps.study_cells, 0::bigint) > 0 AS has_matrix,
       CASE WHEN ps.any_unfiltered THEN NULL::bigint ELSE ps.study_cells END AS n_cells,
       dl.has_fastq,
       dl.n_fastq_runs,
       dl.has_sra,
       dl.n_runs,
       lrs.study_accession IS NOT NULL AS is_long_read,
       NULLIF(pe.perturbation_method, '{}'::text[]) AS perturbation_method,
       NULLIF(array_remove(ARRAY[
           CASE WHEN pe.perturbation_method && ARRAY['Perturb-seq', 'CROP-seq', 'ECCITE-seq', 'Mosaic-seq', 'CRISPR-detect'] THEN 'genetic' END,
           CASE WHEN pe.perturbation_method && ARRAY['sci-Plex'] THEN 'chemical' END
       ], NULL::text), '{}'::text[]) AS intervention_kind,
       CASE WHEN cardinality(pe.perturbation_method) > 0 THEN true ELSE NULL::boolean END AS is_pooled,
       u.country,
       u.pmid,
       CASE WHEN u.first_published ~ '^\d{4}-\d{2}-\d{2}'
            THEN EXTRACT(year FROM left(u.first_published, 10)::date)::integer
       END AS year
FROM base b
LEFT JOIN unified_metadata u ON u.canonical_accession = b.study_accession
LEFT JOIN pentimento_study ps ON ps.study_accession = b.study_accession
LEFT JOIN run_ev re ON re.study_accession = b.study_accession
LEFT JOIN tissue_organism tso ON tso.study_accession = b.study_accession
LEFT JOIN study_download_summary dl ON dl.study_accession = b.study_accession
LEFT JOIN longread_study lrs ON lrs.study_accession = b.study_accession
LEFT JOIN cell_nucleus_ev cn ON cn.study_accession = b.study_accession
LEFT JOIN perturbation_ev pe ON pe.study_accession = b.study_accession
-- hide SRA embargoes until a rebuild after release; spatial/perturbation inherit
WHERE u.first_published IS NULL OR u.first_published <= to_char(current_date, 'YYYY-MM-DD');

CREATE UNIQUE INDEX singlecell_study_study_accession_idx ON singlecell_study(study_accession);
ANALYZE singlecell_study;

CREATE MATERIALIZED VIEW singlecell_facet AS
WITH tagged AS (
    SELECT 'modality' AS facet, single_cell_modality AS value, study_accession
    FROM singlecell_study WHERE single_cell_modality IS NOT NULL
    UNION ALL
    SELECT 'chemistry', unnest(chemistries), study_accession FROM singlecell_study
    UNION ALL
    SELECT 'cell_or_nucleus', unnest(cell_or_nucleus), study_accession FROM singlecell_study
    UNION ALL
    SELECT 'organism', unnest(organisms), study_accession FROM singlecell_study
    UNION ALL
    SELECT 'tissue', unnest(tissues), study_accession FROM singlecell_study
    UNION ALL
    SELECT 'assay_l1', assay_l1, study_accession FROM singlecell_study WHERE assay_l1 IS NOT NULL
    UNION ALL
    SELECT 'year', year::text, study_accession FROM singlecell_study WHERE year IS NOT NULL
    UNION ALL
    SELECT 'has_matrix', CASE WHEN has_matrix THEN 'true' ELSE 'false' END, study_accession FROM singlecell_study
    UNION ALL
    SELECT 'has_fastq', CASE WHEN has_fastq THEN 'true' ELSE 'false' END, study_accession
    FROM singlecell_study WHERE has_fastq IS NOT NULL
    UNION ALL
    SELECT 'is_long_read', CASE WHEN is_long_read THEN 'true' ELSE 'false' END, study_accession FROM singlecell_study
    UNION ALL
    SELECT 'perturbation_method', unnest(perturbation_method), study_accession FROM singlecell_study
    UNION ALL
    SELECT 'intervention_kind', unnest(intervention_kind), study_accession FROM singlecell_study
)
SELECT facet, value, count(DISTINCT study_accession) AS studies
FROM tagged
WHERE value <> ''
GROUP BY facet, value;

ANALYZE singlecell_facet;
