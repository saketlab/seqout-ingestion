-- Single-cell perturbation studies: one row per singlecell_study study with
-- genetic, chemical or other-stimulus perturbation evidence, its confidence,
-- the readout assay and whether matrices, FASTQ or both are available.
--
-- Deterministic: regexes, a control-arm design test and the ChEBI lexicon in
-- perturbation_lexicon. No model output is trusted as a label.
-- ontology_samples_v5 is itself LLM-derived, so its treatment and
-- genetic_modification columns are only ever evidence, never the answer.
--
-- Evidence, each recorded by code in `evidence`:
--   method_text   named method (Perturb-seq, CROP-seq, sci-Plex ...) in study text
--   guide_text    sgRNA / guide RNA in study text
--   crispr_text   CRISPR screen / KO / i / a in study text
--   rnai_orf_text shRNA / siRNA / ORF / base or prime editing in study text
--   pooled_text   pooled or genome-scale screen phrasing
--   library_guide SRA experiment title/library/design names a guide library
--   library_crispr SRA experiment mentions crispr or perturb
--   sample_genetic sample genetic_modification or treatment names a CRISPR/RNAi tool
--   design_compound a lexicon compound in sample treatment values
--   control_arm   a control-like treatment value exists beside the compound
--   drug_text_named named chemical-screen method or "drug screen" phrasing
--   drug_text     drug/compound response phrasing
--   title_compound a lexicon compound in the study's own title (a compound
--                 named in a title is often what is being studied, not what
--                 was applied, so this evidence code by itself never raises
--                 c_rank above 1/low -- see the c_rank CASE, title_compounds
--                 only appears in its last WHEN. A row can still show
--                 title_compound alongside medium/high confidence when
--                 OTHER, independent evidence -- design_compound, drug_text
--                 -- already reached that tier on its own; title_compound
--                 never causes that, it only corroborates it)
--
-- Confidence:
--   genetic  high: named method, or a guide library plus guide/pooled-screen
--            text (a guide library alone is medium: knockout-model papers
--            also name an sgRNA in library descriptions). medium: a guide-RNA
--            mention plus a sample, library or screen signal, or a CRISPR/RNAi
--            mention plus a screen or library signal. low: any single signal.
--   chemical high: sci-Plex/MIX-Seq, or >=3 compound-bearing treatment values
--            (drugs, doses or times) beside a control arm.
--            medium: a compound beside a control arm or drug text.
--            low: a compound alone, drug text alone, or a compound named only
--            in the title. Text alone is never high.
--   Germline knockouts, "wild type" and transgenic models are not evidence.
--   Doxycycline/tamoxifen (Cre/Tet inducers) and cytokines are never chemical.
--   perturbation_type is 'both' whenever g_rank>0 AND c_rank>0, regardless of
--   tier: a genetic-high study whose title happens to name a compound becomes
--   'both' at chemical_confidence='low', on genetic_confidence alone unchanged.
--
-- Study text and ontology rows are reached through singlecell_study's own
-- alias map, not study_canonical (the two can disagree), so a GEO summary
-- reaches an SRP-keyed study.
--
-- Depends on singlecell_study (sql/matviews/singlecell_pg.sql) and
-- perturbation_lexicon (sql/tables/perturbation_lexicon_pg.sql, loaded by
-- `just perturbation_lexicon`). Rebuilding singlecell_study CASCADE-drops
-- this view; run `just refresh_technology_matviews`.
--
-- Postgres only; run with `just perturbation_matview`.

DO $$
BEGIN
    IF (SELECT count(*) FROM perturbation_lexicon WHERE kind = 'compound') < 5000 THEN
        RAISE EXCEPTION
            'perturbation_lexicon is empty or too small; load it with '
            '`just perturbation_lexicon` first, or compounds are silently missed.';
    END IF;
END $$;

DROP MATERIALIZED VIEW IF EXISTS singlecell_perturbation_study;

CREATE MATERIALIZED VIEW singlecell_perturbation_study AS
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
    UNION
    -- singlecell_study also holds studies absent from unified_metadata
    SELECT study_accession, study_accession FROM singlecell_study
), body AS (
    SELECT a.study_accession,
           concat_ws(' ',
               max(um.title), max(um.description),
               string_agg(DISTINCT gs.title, ' '), string_agg(DISTINCT gs.summary, ' '),
               string_agg(DISTINCT gs.overall_design, ' '),
               string_agg(DISTINCT ss.title, ' '), string_agg(DISTINCT ss.abstract, ' ')
           ) AS txt
    FROM sc_alias a
    LEFT JOIN unified_metadata um
           ON um.canonical_accession = a.study_accession AND a.alias = a.study_accession
    LEFT JOIN geo_series gs ON gs.accession = a.alias
    LEFT JOIN sra_studies ss ON ss.accession = a.alias
    GROUP BY a.study_accession
), text_ev AS (
    SELECT study_accession,
           array_remove(ARRAY[
               CASE WHEN txt ~* 'perturb.?seq' THEN 'Perturb-seq' END,
               CASE WHEN txt ~* 'crop.?seq' THEN 'CROP-seq' END,
               CASE WHEN txt ~* 'eccite.?seq' THEN 'ECCITE-seq' END,
               CASE WHEN txt ~* 'mosaic.?seq' THEN 'Mosaic-seq' END,
               CASE WHEN txt ~* '\ytap.?seq' THEN 'TAP-seq' END,
               CASE WHEN txt ~* 'perturb.?cite' THEN 'Perturb-CITE-seq' END,
               CASE WHEN txt ~* 'perturb.?atac' THEN 'Perturb-ATAC' END,
               CASE WHEN txt ~* 'spear.?atac' THEN 'Spear-ATAC' END,
               CASE WHEN txt ~* 'crisp.?seq' THEN 'CRISP-seq' END,
               CASE WHEN txt ~* 'crispr.?detect' THEN 'CRISPR-detect' END,
               CASE WHEN txt ~* 'sci.?plex' THEN 'sci-Plex' END,
               CASE WHEN txt ~* '\ymix.?seq' THEN 'MIX-Seq' END
           ], NULL) AS methods,
           txt ~* 'sgrnas?\y|\ygrnas?\y|guide.?rnas?' AS guide_text,
           txt ~* 'crispr.{0,40}(screen|knock|interfer|activat|librar|pooled|perturb|cas9)|crispri|crispra|cas9' AS crispr_text,
           txt ~* 'shrnas?\y|sirnas?\y|rnai screen|orf (library|screen)|overexpression screen|orfeome|base.editing|prime.editing' AS rnai_orf_text,
           txt ~* '(pooled|large.scale|genome.scale|genome.wide|high.throughput).{0,30}(screen|perturbation)|perturbation (screen|atlas|library)|crispr.{0,30}screens?|screens?.{0,30}crispr|crispri.{0,20}screen|crispra.{0,20}screen' AS pooled_text,
           txt ~* 'drug screen|compound screen|chemical screen|small.molecule screen|chemical perturbation|drug perturbation|compound library|drug library' AS drug_text_named,
           txt ~* '(drug|compound|small.molecule|chemical)s?.{0,25}(screen|perturb|library|response|treat)|dose.?response|drug.{0,10}(sensitiv|resist)' AS drug_text,
           array_remove(ARRAY[
               CASE WHEN txt ~* 'crispri|crispr.?interference|dcas9.?krab' THEN 'CRISPRi' END,
               CASE WHEN txt ~* 'crispra|crispr.?activation|dcas9.?vp64' THEN 'CRISPRa' END,
               CASE WHEN txt ~* 'crispr.{0,20}(knock.?out|screen|cas9)|cas9' THEN 'CRISPR-KO' END,
               CASE WHEN txt ~* 'shrnas?\y|sirnas?\y|rnai' THEN 'RNAi' END,
               CASE WHEN txt ~* 'orf (library|screen)|overexpression screen|orfeome' THEN 'ORF' END,
               CASE WHEN txt ~* 'base.editing|prime.editing' THEN 'base/prime editing' END
           ], NULL) AS subtypes
    FROM body
), lib_ev AS (
    SELECT a.study_accession,
           bool_or(concat_ws(' ', e.title, e.library_name, e.design_description)
                   ~* 'sgrna|\ygrna|guide.?rna|guide capture|crispr guide') AS library_guide,
           bool_or(concat_ws(' ', e.title, e.library_name, e.design_description)
                   ~* 'crispr|perturb') AS library_crispr
    FROM sc_alias a
    JOIN sra_studies ss ON ss.accession = a.alias
    JOIN sra_experiments e ON e.study = ss.accession
    GROUP BY a.study_accession
), tv AS (
    SELECT a.study_accession, o.sample,
           lower(btrim(o.treatment)) AS v,
           lower(o.genetic_modification) AS gm,
           o.assay_category,
           lower(btrim(o.cell_line)) AS cell_line,
           lower(btrim(o.sample_type)) AS sample_type
    FROM sc_alias a
    JOIN ontology_samples_v5 o ON o.study_accession = a.alias
), val AS (
    SELECT study_accession, v,
           v ~ '^(untreated|control|ctrl|vehicle|dmso|none|mock|pbs|unstimulated|naive|saline|placebo|sham|isotype|igg|no treatment|non-?treated)' AS is_ctrl,
           v ~ '(^|[^a-z])(lps|ifn|ifng|interferon|tnf|tnfa|tgf|tgfb|il-?[0-9]+|gm-?csf|m-?csf|g-?csf|egf|fgf|vegf|pma|poly.?i:?c|cd3|cd28|anti-cd|ionomycin|lipopolysaccharide|cytokine|oncostatin)' AS is_stim,
           v ~ 'sgrna|\ygrnas?\y|guide.?rnas?|crispr|cas9|sirna|shrna|knockdown' AS is_gen
    FROM tv WHERE v IS NOT NULL
    GROUP BY study_accession, v
), val_lex AS (
    -- coalesce: a value with no lexicon hit has only NULL kinds, and NOT NULL
    -- would silently drop its control/stimulus flags
    SELECT val.study_accession, val.v,
           coalesce(bool_or(k.kind IN ('compound', 'biologic')), false) AS is_comp,
           array_agg(DISTINCT k.label) FILTER (WHERE k.kind IN ('compound', 'biologic')) AS labels,
           coalesce(bool_or(k.kind = 'stimulus'), false) AS lex_stim
    FROM val
    CROSS JOIN LATERAL regexp_split_to_table(val.v, '[^a-z0-9-]+') t
    LEFT JOIN perturbation_lexicon k ON k.token = t
    GROUP BY val.study_accession, val.v
), design AS (
    SELECT val.study_accession,
           bool_or(val.is_ctrl AND NOT vl.is_comp AND NOT val.is_stim) AS has_ctrl,
           count(*) FILTER (WHERE vl.is_comp) AS n_compound_values,
           bool_or(val.is_gen) AS treatment_genetic,
           (array_agg(DISTINCT val.v) FILTER (WHERE (val.is_stim OR vl.lex_stim) AND NOT vl.is_comp))[1:10] AS stimuli,
           bool_or((val.is_stim OR vl.lex_stim) AND NOT vl.is_comp) AS has_stimulus
    FROM val JOIN val_lex vl USING (study_accession, v)
    GROUP BY val.study_accession
), compounds AS (
    SELECT study_accession, (array_agg(DISTINCT l))[1:25] AS compounds
    FROM val_lex, LATERAL unnest(labels) l
    GROUP BY study_accession
), title_ev AS (
    -- The study's own title only, not the full text: a compound named in a
    -- title is often what is being studied (a receptor, a resistance
    -- mechanism), not something applied to the cells. Low tier only.
    SELECT s.study_accession,
           (array_agg(DISTINCT k.label) FILTER (WHERE k.kind IN ('compound', 'biologic')))[1:10]
               AS title_compounds
    FROM singlecell_study s
    CROSS JOIN LATERAL regexp_split_to_table(lower(coalesce(s.title, '')), '[^a-z0-9-]+') tok
    LEFT JOIN perturbation_lexicon k ON k.token = tok
    GROUP BY s.study_accession
), sample_ev AS (
    SELECT study_accession,
           bool_or(gm ~ 'crispr|cas9|sgrna|\ygrnas?\y|guide.?rnas?|shrna|sirna|knockdown|base.edit|prime.edit'
                   AND gm !~ '^(wild|wt\y|none)') AS gm_genetic,
           array_agg(DISTINCT assay_category) FILTER (WHERE assay_category IS NOT NULL) AS readout_assays,
           (array_agg(DISTINCT cell_line) FILTER (
               WHERE cell_line IS NOT NULL
                 AND cell_line NOT IN ('n/a', 'na', 'none', 'not applicable', 'unknown', '-')
           ))[1:10] AS cell_lines,
           (array_agg(DISTINCT sample_type) FILTER (WHERE sample_type IS NOT NULL))[1:10] AS sample_types
    FROM tv
    GROUP BY study_accession
), scored AS (
    SELECT s.*,
           t.methods, t.subtypes, t.guide_text, t.crispr_text, t.rnai_orf_text, t.pooled_text,
           t.drug_text_named, t.drug_text,
           coalesce(l.library_guide, false) AS library_guide,
           coalesce(l.library_crispr, false) AS library_crispr,
           coalesce(se.gm_genetic, false) OR coalesce(d.treatment_genetic, false) AS sample_genetic,
           coalesce(d.has_ctrl, false) AS has_ctrl,
           coalesce(d.n_compound_values, 0) AS n_compound_values,
           coalesce(d.has_stimulus, false) AS has_stimulus,
           d.stimuli, c.compounds, se.readout_assays, se.cell_lines, se.sample_types,
           NULLIF(te.title_compounds, '{}'::text[]) AS title_compounds
    FROM singlecell_study s
    LEFT JOIN text_ev t ON t.study_accession = s.study_accession
    LEFT JOIN lib_ev l ON l.study_accession = s.study_accession
    LEFT JOIN sample_ev se ON se.study_accession = s.study_accession
    LEFT JOIN design d ON d.study_accession = s.study_accession
    LEFT JOIN compounds c ON c.study_accession = s.study_accession
    LEFT JOIN title_ev te ON te.study_accession = s.study_accession
), tiered AS (
    SELECT *,
           CASE WHEN cardinality(methods) > 0 AND methods && ARRAY['Perturb-seq','CROP-seq','ECCITE-seq','Mosaic-seq','TAP-seq','Perturb-CITE-seq','Perturb-ATAC','Spear-ATAC','CRISP-seq','CRISPR-detect']
                     OR (library_guide AND (guide_text OR pooled_text)) THEN 3
                WHEN library_guide
                     OR (guide_text AND (sample_genetic OR pooled_text OR library_crispr))
                     OR ((crispr_text OR rnai_orf_text) AND (pooled_text OR library_crispr)) THEN 2
                WHEN guide_text OR crispr_text OR rnai_orf_text OR sample_genetic THEN 1
                ELSE 0 END AS g_rank,
           CASE WHEN methods && ARRAY['sci-Plex','MIX-Seq'] THEN 3
                WHEN n_compound_values >= 3 AND has_ctrl THEN 3
                WHEN n_compound_values >= 1 AND (has_ctrl OR drug_text OR drug_text_named) THEN 2
                WHEN n_compound_values >= 1 OR drug_text OR drug_text_named
                     OR cardinality(coalesce(title_compounds, '{}'::text[])) > 0 THEN 1
                ELSE 0 END AS c_rank
    FROM scored
)
SELECT study_accession, title, organism, organisms, tissues, single_cell_modality,
       is_long_read, assay_l1, readout_assays, cell_lines, sample_types,
       n_samples, n_cells,
       has_matrix, has_fastq, has_sra, n_fastq_runs, n_runs,
       CASE WHEN has_matrix AND coalesce(has_fastq, false) THEN 'both'
            WHEN has_matrix THEN 'matrix_only'
            WHEN coalesce(has_fastq, false) THEN 'fastq_only'
            ELSE 'neither' END AS data_availability,
       CASE WHEN g_rank > 0 AND c_rank > 0 THEN 'both'
            WHEN g_rank > 0 THEN 'genetic'
            WHEN c_rank > 0 THEN 'chemical'
            ELSE 'other' END AS perturbation_type,
       (ARRAY['low', 'low', 'medium', 'high'])[greatest(g_rank, c_rank) + 1] AS confidence,
       (ARRAY['none', 'low', 'medium', 'high'])[g_rank + 1] AS genetic_confidence,
       (ARRAY['none', 'low', 'medium', 'high'])[c_rank + 1] AS chemical_confidence,
       CASE WHEN g_rank > 0 THEN subtypes END AS genetic_subtypes,
       NULLIF(methods, '{}'::text[]) AS perturbation_methods,
       CASE WHEN c_rank > 0 THEN compounds END AS compounds,
       CASE WHEN c_rank > 0 THEN title_compounds END AS title_compounds,
       stimuli,
       has_ctrl AS has_control_arm,
       n_compound_values,
       CASE WHEN cardinality(methods) > 0 OR library_guide OR pooled_text THEN true END AS is_pooled,
       array_remove(ARRAY[
           CASE WHEN cardinality(methods) > 0 THEN 'method_text' END,
           CASE WHEN guide_text THEN 'guide_text' END,
           CASE WHEN crispr_text THEN 'crispr_text' END,
           CASE WHEN rnai_orf_text THEN 'rnai_orf_text' END,
           CASE WHEN pooled_text THEN 'pooled_text' END,
           CASE WHEN library_guide THEN 'library_guide' END,
           CASE WHEN library_crispr THEN 'library_crispr' END,
           CASE WHEN sample_genetic THEN 'sample_genetic' END,
           CASE WHEN n_compound_values > 0 THEN 'design_compound' END,
           CASE WHEN n_compound_values > 0 AND has_ctrl THEN 'control_arm' END,
           CASE WHEN drug_text_named THEN 'drug_text_named' END,
           CASE WHEN drug_text THEN 'drug_text' END,
           CASE WHEN cardinality(coalesce(title_compounds, '{}'::text[])) > 0
                THEN 'title_compound' END
       ], NULL) AS evidence,
       country, pmid, year
FROM tiered
WHERE g_rank > 0 OR c_rank > 0 OR (has_stimulus AND has_ctrl);

CREATE UNIQUE INDEX singlecell_perturbation_study_pk ON singlecell_perturbation_study(study_accession);
CREATE INDEX singlecell_perturbation_study_type ON singlecell_perturbation_study(perturbation_type, confidence);
ANALYZE singlecell_perturbation_study;
