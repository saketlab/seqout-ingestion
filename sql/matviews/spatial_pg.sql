-- Spatial transcriptomics studies: one row per singlecell_study study already
-- flagged single_cell_modality = 'Spatial Transcriptomics' (src/sc_enrich's
-- `visium|xenium|merfish|seqfish|slide.?seq|cosmx|spatial` modality rule),
-- with the named platform(s) and resolution class layered on top.
--
-- Membership is the pre-existing modality flag, trusted as-is. The new work
-- here is naming which platform a study used, by matching study text against
-- spatial_platform_lexicon (sql/tables/spatial_platform_lexicon_pg.sql):
-- hand-curated, since there are only a few dozen real spatial platforms.
-- Adding a platform is a row in that file, not an edit here -- see its
-- header comment.
--
-- Known ceilings of that reuse, not fixed here (upstream, in src/sc_enrich):
--   * the modality rule's bare `spatial` alternative has no transcriptomic
--     guard, so spatial proteomics or an unrelated "spatially variable"
--     phrase can enter this population same as a true spatial-RNA study.
--   * Stereo-seq/GeoMx/DBiT-seq/STARmap/osmFISH/HDST are not in the modality
--     rule's own keyword list; a study naming only one of those, and never
--     saying "spatial" anywhere, never reaches singlecell_study at all and
--     so is invisible to this view too.
--
-- `platforms` names a mention in the study's own text: a paper naming
-- Visium only as a comparator, or MERFISH only in a background paragraph,
-- is indistinguishable here from one that generated Visium/MERFISH data.
-- Same caveat singlecell_perturbation_study documents for title_compound.
-- `platforms` is also often NULL, for studies whose text only says generic
-- "spatial transcriptomics" language and never names a platform.
--
-- `resolution` ('single-cell'/'spot'/'roi') and `technology`
-- ('imaging'/'sequencing'/'hybrid') are each matched platform's own columns
-- in the lexicon, reduced to one value per study by that same precedence
-- when platforms from more than one group are named (e.g. a GeoMx-vs-Xenium
-- comparison becomes resolution='single-cell', technology='imaging'); NULL
-- when no platform was named. The two are independent axes that happen to
-- track each other for every current platform except GeoMx (roi/hybrid);
-- see the lexicon file's header for why they are not derived from one
-- another. `platforms` lists every name found; `resolution`/`technology`
-- each pick one via that precedence. A platform's `supersedes` column (see
-- the lexicon file) can also drop a less-specific name from `platforms`
-- itself, e.g. a study naming both Visium and Visium HD reports only
-- 'Visium HD'.
--
-- Rejected platform names and the CosMx/GeoMx false-positive tightening are
-- documented per-row in the lexicon file's `notes` column and header.
--
-- Study text and the alias map are built the same way as
-- singlecell_perturbation_pg.sql (see its header for why study_canonical is
-- not used). SRA experiment title/library_name/design_description are
-- scanned too (lib_txt), same as that view's lib_ev, because the upstream
-- modality classifier reaches platform evidence there that the study-level
-- title, summary and abstract alone would miss.
--
-- Depends on singlecell_study (sql/matviews/singlecell_pg.sql) and
-- spatial_platform_lexicon (sql/tables/spatial_platform_lexicon_pg.sql,
-- loaded by `just spatial_platform_lexicon`). Rebuilding singlecell_study
-- CASCADE-drops this view; run `just refresh_technology_matviews`,
-- which rebuilds this view too, not `just singlecell_matview` alone.
--
-- Postgres only; run with `just spatial_matview`.

DO $$
BEGIN
    IF (SELECT count(*) FROM spatial_platform_lexicon) = 0 THEN
        RAISE EXCEPTION
            'spatial_platform_lexicon is empty; load it with '
            '`just spatial_platform_lexicon` first, or every platform is silently missed.';
    END IF;
END $$;

DROP MATERIALIZED VIEW IF EXISTS spatial_study;

CREATE MATERIALIZED VIEW spatial_study AS
WITH alias_map AS (
    SELECT x.alias, min(x.canonical_accession) AS canonical_accession
    FROM (
        SELECT canonical_accession, canonical_accession AS alias FROM unified_metadata
        UNION
        SELECT u.canonical_accession, a.value
        FROM unified_metadata u, LATERAL jsonb_each_text(u.aliases) a(key, value)
        WHERE a.value <> ''
    ) x
    GROUP BY x.alias
), sc_alias AS (
    SELECT am.canonical_accession AS study_accession, am.alias
    FROM alias_map am
    JOIN singlecell_study s ON s.study_accession = am.canonical_accession
    WHERE s.single_cell_modality = 'Spatial Transcriptomics'
    UNION
    -- singlecell_study also holds studies absent from unified_metadata
    SELECT study_accession, study_accession FROM singlecell_study
    WHERE single_cell_modality = 'Spatial Transcriptomics'
), body AS (
    -- string_agg's ORDER BY makes the concatenation order deterministic
    -- across rebuilds; without it, the proximity window
    -- spatial_platform_lexicon's Molecular Cartography pattern scans could
    -- match or miss depending on unspecified aggregation order (still no
    -- guarantee that adjacent words come from the same source field: a
    -- proximity pattern can span a title/summary boundary either way)
    SELECT a.study_accession,
           concat_ws(' ',
               max(um.title), max(um.description),
               string_agg(DISTINCT gs.title, ' ' ORDER BY gs.title),
               string_agg(DISTINCT gs.summary, ' ' ORDER BY gs.summary),
               string_agg(DISTINCT gs.overall_design, ' ' ORDER BY gs.overall_design),
               string_agg(DISTINCT ss.title, ' ' ORDER BY ss.title),
               string_agg(DISTINCT ss.abstract, ' ' ORDER BY ss.abstract)
           ) AS txt
    FROM sc_alias a
    LEFT JOIN unified_metadata um
           ON um.canonical_accession = a.study_accession AND a.alias = a.study_accession
    LEFT JOIN geo_series gs ON gs.accession = a.alias
    LEFT JOIN sra_studies ss ON ss.accession = a.alias
    GROUP BY a.study_accession
), lib_txt AS (
    SELECT a.study_accession,
           string_agg(DISTINCT concat_ws(' ', e.title, e.library_name, e.design_description), ' '
               ORDER BY concat_ws(' ', e.title, e.library_name, e.design_description)) AS txt
    FROM sc_alias a
    JOIN sra_studies ss ON ss.accession = a.alias
    JOIN sra_experiments e ON e.study = ss.accession
    GROUP BY a.study_accession
), txt AS (
    SELECT b.study_accession, b.txt || ' ' || coalesce(l.txt, '') AS txt
    FROM body b LEFT JOIN lib_txt l USING (study_accession)
), platform_matches AS (
    SELECT t.study_accession, pl.platform, pl.resolution, pl.technology, pl.supersedes
    FROM txt t
    JOIN spatial_platform_lexicon pl ON t.txt ~* pl.pattern
), platform_ev AS (
    SELECT m.study_accession,
           array_agg(DISTINCT m.platform ORDER BY m.platform) AS platforms,
           CASE WHEN bool_or(m.resolution = 'single-cell') THEN 'single-cell'
                WHEN bool_or(m.resolution = 'spot') THEN 'spot'
                WHEN bool_or(m.resolution = 'roi') THEN 'roi' END AS resolution,
           -- same precedence as resolution: imaging pairs with single-cell,
           -- sequencing with spot, hybrid with roi, so a study naming
           -- platforms from more than one group collapses the same way both
           -- columns describe
           CASE WHEN bool_or(m.technology = 'imaging') THEN 'imaging'
                WHEN bool_or(m.technology = 'sequencing') THEN 'sequencing'
                WHEN bool_or(m.technology = 'hybrid') THEN 'hybrid' END AS technology
    FROM platform_matches m
    -- drop a name a more specific match on the same study supersedes, e.g.
    -- Visium when Visium HD also matched (see the lexicon's `supersedes` col).
    -- This is edge-based, not transitive: a future 3-tier hierarchy needs
    -- the top tier's `supersedes` to list every tier below it directly, or a
    -- study matching only the top and bottom tiers won't suppress the
    -- bottom one.
    WHERE NOT EXISTS (
        SELECT 1 FROM platform_matches m2
        WHERE m2.study_accession = m.study_accession
          AND m.platform = ANY(m2.supersedes)
    )
    GROUP BY m.study_accession
), sample_ev AS (
    SELECT a.study_accession,
           array_agg(DISTINCT o.assay_category ORDER BY o.assay_category)
               FILTER (WHERE o.assay_category IS NOT NULL) AS readout_assays,
           (array_agg(DISTINCT lower(btrim(o.cell_line)) ORDER BY lower(btrim(o.cell_line))) FILTER (
               WHERE o.cell_line IS NOT NULL
                 AND lower(btrim(o.cell_line)) NOT IN ('n/a', 'na', 'none', 'not applicable', 'unknown', '-')
           ))[1:10] AS cell_lines,
           (array_agg(DISTINCT lower(btrim(o.sample_type)) ORDER BY lower(btrim(o.sample_type)))
               FILTER (WHERE o.sample_type IS NOT NULL))[1:10] AS sample_types
    FROM sc_alias a
    JOIN ontology_samples_v5 o ON o.study_accession = a.alias
    GROUP BY a.study_accession
)
SELECT s.study_accession, s.title, s.organism, s.organisms, s.tissues,
       s.single_cell_modality, s.is_long_read, s.assay_l1,
       se.readout_assays, se.cell_lines, se.sample_types,
       s.n_samples, s.n_cells,
       s.has_matrix, s.has_fastq, s.has_sra, s.n_fastq_runs, s.n_runs,
       CASE WHEN s.has_matrix AND coalesce(s.has_fastq, false) THEN 'both'
            WHEN s.has_matrix THEN 'matrix_only'
            WHEN coalesce(s.has_fastq, false) THEN 'fastq_only'
            ELSE 'neither' END AS data_availability,
       NULLIF(p.platforms, '{}'::text[]) AS platforms,
       p.resolution, p.technology,
       s.country, s.pmid, s.year
FROM singlecell_study s
LEFT JOIN platform_ev p ON p.study_accession = s.study_accession
LEFT JOIN sample_ev se ON se.study_accession = s.study_accession
WHERE s.single_cell_modality = 'Spatial Transcriptomics';

CREATE UNIQUE INDEX spatial_study_pk ON spatial_study(study_accession);
CREATE INDEX spatial_study_resolution ON spatial_study(resolution);
CREATE INDEX spatial_study_technology ON spatial_study(technology);
ANALYZE spatial_study;
