-- Single-cell matrix QC behind /stats/sc-quality. sc_quality_matrix is one row
-- per qualifying matrix; sc_quality_by_year aggregates it per year+technology.
-- DROP then CREATE: a REFRESH reuses the stored definition, and the aggregate
-- depends on the matrix view, so drop it first.
DROP MATERIALIZED VIEW IF EXISTS sc_quality_by_year;
DROP MATERIALIZED VIEW IF EXISTS sc_quality_matrix;

CREATE MATERIALIZED VIEW sc_quality_matrix AS
WITH sourced AS (
    SELECT sm.accession,
           sm.sample_accession,
           sm.file,
           -- first_published is TEXT; a date without a leading year is dropped
           substring(um.first_published from '^[0-9]{4}')::int AS year,
           sm.ncount_f_median,
           sm.nfeature_f_median,
           -- the read caller reaches only 10x and Drop-seq; the rest arrive
           -- declared. ATAC counts fragments and Smart-seq2/CEL-seq1/Quartz-seq1
           -- carry no UMI, so both stay unlabelled and drop out.
           CASE
             WHEN rd.is_read THEN ps.assay
             WHEN es.assay_category IS NULL
                  OR es.assay_category = 'Single-cell Transcriptomics'
               THEN COALESCE(es.assay, study_assay.assay)
           END AS assay_raw,
           CASE WHEN rd.is_read THEN 'read' ELSE 'declared' END AS evidence
    FROM sc_stats_matrix sm
    JOIN unified_metadata um ON um.canonical_accession = sm.accession
    -- both are one row per sample, so neither join fans out
    LEFT JOIN pentimento_sample ps ON ps.sample_accession = sm.sample_accession
    LEFT JOIN ontology_samples_v5 es ON es.study_accession = sm.accession
                                    AND es.sample = sm.sample_accession
    -- a series-scope file carries no GSM, so sample_accession is NULL and the
    -- join above finds nothing; the study's commonest chemistry stands in
    LEFT JOIN LATERAL (
        SELECT o.assay
        FROM ontology_samples_v5 o
        WHERE o.study_accession = sm.accession
          AND o.assay IS NOT NULL
          AND (o.assay_category IS NULL
               OR o.assay_category = 'Single-cell Transcriptomics')
        GROUP BY o.assay
        ORDER BY count(*) DESC, o.assay
        LIMIT 1
    ) study_assay ON TRUE
    -- one predicate; assay_raw and evidence must agree on what a read call is.
    -- the 'Not single-cell' sentinel is written with assay_is_single_cell NULL
    CROSS JOIN LATERAL (
        SELECT ps.assay IS NOT NULL
               AND ps.assay <> 'Not single-cell'
               AND ps.assay_is_single_cell IS NOT FALSE
               AND ps.calls_ambiguous IS NOT TRUE AS is_read
    ) rd
    WHERE sm.ncount_f_median IS NOT NULL
      AND sm.nfeature_f_median IS NOT NULL
      AND um.first_published ~ '^[0-9]{4}'
      -- feature-axis length separates a transcriptome from a panel or antibody
      -- matrix; the assay string does not say which half of a CITE-seq pair a
      -- file is. ps.genes stands in where n_features is absent, as a per-sample
      -- max that mis-parses some formats, so an implausible value degrades to
      -- unknown and keeps the matrix.
      AND COALESCE(
            sm.n_features,
            CASE WHEN ps.genes BETWEEN 1 AND 100000 THEN ps.genes END,
            5000
          ) >= 5000
      -- interim, until n_features covers the corpus: a per-sample max lets an
      -- antibody file inherit its pair's gene count. depth is required as well
      -- as name, since real transcriptomes carry adt/hto by coincidence.
      AND NOT (
            sm.file ~* '(^|[^a-z])(adt|hto)([^a-z]|$)|hashtag|antibody|cellsurface'
            AND sm.nfeature_f_median < 500
          )
      -- summarise_matrix sums whatever the file holds, so a normalised table
      -- yields a median in a different unit, and a study with both double-counts
      AND sm.file !~* 'tpm|fpkm|rpkm|[^a-z]cpm|normali[sz]|scaled|logcount|log2|z[-_]?score'
      -- FACS bulk deposited beside single cells; only the pair means bulk, as
      -- a plate study has few cells and a deep droplet run has many reads
      AND NOT (sm.n_cells < 100 AND sm.ncount_f_median > 1000000)
      -- a median cell under 100 features is no transcriptome, whatever the
      -- assay says; catches genome-bin counts, viral matrices and panels
      AND sm.nfeature_f_median >= 100
),
-- chemistry versions collapse to a technology; 10x 3' and 5' stay apart
labelled AS (
    SELECT accession, sample_accession, file, year, evidence,
           ncount_f_median, nfeature_f_median,
           CASE
             -- BEGIN generated from sc_evidence.rules.CHEMISTRIES
             WHEN assay_raw ~* 'atac'
               THEN NULL  -- chromatin
             WHEN assay_raw ~* 'multiome'
               THEN NULL  -- multiome
             WHEN assay_raw ~* 'visium'
                  OR assay_raw ~* 'spatial'
               THEN NULL  -- spatial
             WHEN assay_raw ~* '(?<!nano)drop.?seq'
               THEN 'Drop-seq'  -- transcriptome
             WHEN assay_raw ~* 'smart[-_. ]?seq[-_. ]?3'
               THEN 'Smart-seq3'  -- transcriptome
             WHEN assay_raw ~* 'smart.?seq'
               THEN NULL  -- reads
             WHEN assay_raw ~* 'split[-_. ]?seq(?!uenc)'
                  OR assay_raw ~* 'parse ?bio|evercode'
               THEN 'SPLiT-seq/Parse'  -- transcriptome
             WHEN assay_raw ~* 'bd[-_. ]?rhapsody'
               THEN 'BD Rhapsody'  -- transcriptome
             WHEN assay_raw ~* 'pipseq'
               THEN 'PIPseq'  -- transcriptome
             WHEN assay_raw ~* '(?<![a-z])indrop'
               THEN 'inDrop'  -- transcriptome
             WHEN assay_raw ~* 'cel(?!l)[-_. ]?seq[-_. ]?2'
               THEN 'CEL-seq2'  -- transcriptome
             WHEN assay_raw ~* 'cel(?!l)[-_. ]?seq'
               THEN NULL  -- reads
             WHEN assay_raw ~* 'seq[-_.]well'
               THEN 'Seq-Well'  -- transcriptome
             WHEN assay_raw ~* '(?<![a-z0-9])sci[-_. ]?rna'
               THEN 'sci-RNA-seq'  -- transcriptome
             WHEN assay_raw ~* 'mars[-_. ]?seq(?!uenc)'
               THEN 'MARS-seq'  -- transcriptome
             WHEN assay_raw ~* 'quartz[-_. ]?seq[-_. ]?2'
               THEN 'Quartz-seq2'  -- transcriptome
             WHEN assay_raw ~* 'quartz[-_. ]?seq'
               THEN NULL  -- reads
             WHEN assay_raw ~* 'microwell[-_. ]?seq'
               THEN 'Microwell-seq'  -- transcriptome
             -- END generated from sc_evidence.rules.CHEMISTRIES
             -- Flex is probe capture, so its depth is not comparable to a 3'
             -- or 5' library
             WHEN assay_raw ILIKE '%fixed rna%' OR assay_raw ILIKE '%flex%'
               THEN '10x Flex'
             WHEN (assay_raw ILIKE '%chromium 3''%' OR assay_raw ILIKE '10x%3''%')
                  AND assay_raw NOT ILIKE '% or %'
                  AND (assay_raw ILIKE '%v4%' OR assay_raw ILIKE '%gem-x%')
                  AND evidence = 'declared'
               THEN '10x 3'' v4'
             WHEN (assay_raw ILIKE '%chromium 3''%' OR assay_raw ILIKE '10x%3''%')
                  AND assay_raw NOT ILIKE '% or %'
                  AND (assay_raw ILIKE '%v3%' OR assay_raw ILIKE '%v4%'
                       OR assay_raw ILIKE '%gem-x%')
               THEN '10x 3'' v3'
             WHEN (assay_raw ILIKE '%chromium 3''%' OR assay_raw ILIKE '10x%3''%')
                  AND assay_raw NOT ILIKE '% or %' AND assay_raw ILIKE '%v2%'
               THEN '10x 3'' v2'
             WHEN (assay_raw ILIKE '%chromium 3''%' OR assay_raw ILIKE '10x%3''%')
                  AND assay_raw NOT ILIKE '% or %' AND assay_raw ILIKE '%v1%'
               THEN '10x 3'' v1'
             WHEN assay_raw ILIKE '%chromium 3''%' OR assay_raw ILIKE '10x%3''%'
               THEN '10x 3'''
             WHEN assay_raw ILIKE '%chromium 5''%' OR assay_raw ILIKE '10x%5''%'
               THEN '10x 5'''
             WHEN assay_raw ILIKE '%10x%' OR assay_raw ILIKE '%chromium%'
               THEN '10x (unspecified)'
           END AS technology
    FROM sourced
),
-- A study cannot use a kit released after it was published. The enrichment
-- prompt's "10x 3' v3" example makes the LLM write v3 for any unversioned 10x
-- study, and whitelist hits on plate reads produce 10x calls before Chromium
-- existed. A too-new version falls back to the unversioned bucket; a 10x label
-- before 10x shipped is dropped.
dated AS (
    SELECT accession, sample_accession, file, year, evidence,
           ncount_f_median, nfeature_f_median,
           CASE
             WHEN technology LIKE '10x%' AND year < 2016 THEN NULL
             WHEN technology = '10x 5''' AND year < 2017 THEN NULL
             WHEN technology = '10x Flex' AND year < 2022 THEN NULL
             WHEN technology = '10x 3'' v4' AND year < 2024 THEN '10x 3'''
             WHEN technology = '10x 3'' v3' AND year < 2018 THEN '10x 3'''
             ELSE technology
           END AS technology
    FROM labelled
)
SELECT *
FROM dated
WHERE technology IS NOT NULL
  AND year BETWEEN 2009 AND EXTRACT(YEAR FROM now())::int;

CREATE INDEX sc_quality_matrix_year ON sc_quality_matrix (year);

CREATE MATERIALIZED VIEW sc_quality_by_year AS
SELECT year,
       -- NULL here is the GROUPING SETS rollup, never an unlabelled matrix
       COALESCE(technology, 'All') AS technology,
       COALESCE(evidence, 'all')   AS evidence,
       count(*)                   AS n_matrices,
       count(DISTINCT accession)  AS n_studies,
       -- only the _f_ pair is comparable; the unfiltered pair counts ambient
       -- barcodes wherever is_raw_barcode_space is set
       percentile_cont(0.25) WITHIN GROUP (ORDER BY ncount_f_median)   AS p25_ncount,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY ncount_f_median)   AS median_ncount,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY ncount_f_median)   AS p75_ncount,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY nfeature_f_median) AS p25_nfeature,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY nfeature_f_median) AS median_nfeature,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY nfeature_f_median) AS p75_nfeature
FROM sc_quality_matrix
GROUP BY GROUPING SETS ((year, technology, evidence), (year))
-- a quantile over a handful of matrices swings on one study, so thin cells go
HAVING count(*) >= 10 AND count(DISTINCT accession) >= 3;
