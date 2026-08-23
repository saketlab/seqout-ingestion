-- Single-cell evidence: one row per (object, rule).
--
-- Archive fields are partial on their own. library_strategy reads RNA-Seq for
-- scRNA and ATAC-seq for scATAC, and GEO carries no library_source. The
-- booleans are derived from these rows by single_cell_study.

-- Rule catalog for each evidence row. Rebuilt from the builder's RULES table
-- every run, so changed gates get a new rule_version.
CREATE TABLE IF NOT EXISTS sc_rule (
    id           INTEGER PRIMARY KEY,
    rule         TEXT NOT NULL,   -- stable slug, e.g. 'insdc_library_source'
    -- sra | ena | geo | ae | dra | gsa | gea | enrichment. 'enrichment' is
    -- model-derived archive prose and is capped at the medium tier.
    archive      TEXT NOT NULL,
    grain        TEXT NOT NULL,   -- study | sample | experiment | run
    field        TEXT NOT NULL,   -- the source column, e.g. 'library_source'
    -- high   controlled archive field or 10x pipeline output file name.
    -- medium free-text prose or a call propagated across an archive link.
    -- low    heuristics: platform, read geometry, weak keyword hits.
    --
    -- preflightx is corroboration surfaced in cohort views. Barcode geometry
    -- can match bulk short-R1 libraries; Smart-seq2 and CEL-seq plates carry no
    -- barcode read.
    tier         TEXT NOT NULL CHECK (tier IN ('high','medium','low')),
    gate         TEXT NOT NULL,   -- the literal predicate, e.g. "library_source ILIKE '%SINGLE CELL%'"
    rule_version TEXT NOT NULL,
    built_at     TEXT NOT NULL,
    -- A rule is identified by rule AND archive: insdc_library_source applies
    -- to SRA and ENA with the same gate but different source tables, and the
    -- two must stay separately countable.
    UNIQUE (rule, archive, rule_version)
);

-- One row per (object, rule), so agreement across independent fields is
-- countable. `accession` is the evidence object; `study_accession` is
-- denormalised beside it for local rollups.
--
CREATE TABLE IF NOT EXISTS single_cell_evidence (
    accession        TEXT NOT NULL,
    grain            TEXT NOT NULL,   -- study | sample | experiment | run
    study_accession  TEXT,            -- NULL only when the link is unresolvable
    rule_id          INTEGER NOT NULL REFERENCES sc_rule(id),
--
    -- The literal text that satisfied the gate ('TRANSCRIPTOMIC SINGLE CELL',
    -- 'GSM123_barcodes.tsv.gz'), kept so disputed substring hits can be audited
    -- against raw text.
    value            TEXT,
    -- Multimodal experiments submit separate libraries, so a library_source
    -- match finds the GEX library and misses CITE-seq, scATAC, multiome and
    -- guide siblings. Store the raw source/strategy pair.

    -- What was measured: analyte, verbatim from library_source:
    -- TRANSCRIPTOMIC SINGLE CELL | GENOMIC SINGLE CELL | SYNTHETIC | OTHER | ...
    library_source   TEXT,
    -- How it was measured. Verbatim library_strategy: RNA-Seq | ATAC-seq |
    -- Targeted-Capture | AMPLICON | OTHER | ...
    library_strategy TEXT,

    -- Coarse analyte class from library_source. CITE-seq and hashtag libraries
    -- use the 'synthetic' analyte while remaining single-cell signals.
--
--
    analyte          TEXT,

    -- The experimental design this library belongs to:
    -- scRNA-seq | snRNA-seq | scATAC-seq | snATAC-seq | Multiome | CITE-seq |
    -- VDJ | Spatial Transcriptomics | Perturb-seq | Other single-cell.
    -- NULL means this rule abstains on modality. A SYNTHETIC/Targeted-Capture
    -- library becomes CITE-seq only after sibling evidence in the same
    -- experiment, so the rollup unions siblings before judging modality.
    assay_modality   TEXT,
    PRIMARY KEY (accession, grain, rule_id)
) WITHOUT ROWID;
--

-- Study-level rollup for the website and cohort API.
--
-- has_single_cell is a discovery predicate ("contains single-cell data"),
-- all_single_cell a cohort predicate ("every sample is single-cell").
-- sc_sample_fraction is NULL when sample-grained evidence is absent: unknown
-- is distinct from zero.
--
-- Drop first so edited CREATE VIEW bodies are applied.
DROP VIEW IF EXISTS single_cell_study;
CREATE VIEW IF NOT EXISTS single_cell_study AS
SELECT
    e.study_accession,
    MAX(r.tier = 'high')                                        AS has_high_confidence,
    COUNT(DISTINCT CASE WHEN r.grain IN ('sample','experiment','run')
                        THEN e.accession END)                   AS sc_objects,
    COUNT(DISTINCT r.rule)                                      AS n_rules,
    -- Archive-local study keys miss cross-archive evidence until
    -- crossarchive_propagation maps mirror accessions.
    COUNT(DISTINCT r.archive)                                   AS n_archives,
    -- Highest tier reached, as text. 'high' > 'medium' > 'low' sorts wrong
    -- lexically.
    CASE WHEN MAX(r.tier = 'high')   THEN 'high'
         WHEN MAX(r.tier = 'medium') THEN 'medium'
         ELSE 'low' END                                         AS evidence_tier,
    -- The rules that fired, so any claim is auditable from the row itself.
    json_group_array(DISTINCT r.rule)                           AS rules,
    json_group_array(DISTINCT e.assay_modality)                 AS assay_modalities,
    json_group_array(DISTINCT e.analyte)                        AS analytes,
    json_group_array(DISTINCT e.library_strategy)               AS library_strategies
FROM single_cell_evidence e
JOIN sc_rule r ON r.id = e.rule_id
WHERE e.study_accession IS NOT NULL
GROUP BY e.study_accession;
--

-- Per-archive coverage, recorded on every build. A view reports matches; this
-- table stores the archive denominator for regressions and rule gaps.
CREATE TABLE IF NOT EXISTS sc_archive_coverage (
    archive              TEXT NOT NULL,
    built_at             TEXT NOT NULL,
    -- Denominator: studies the archive holds at all, from its own table.
    studies_total        INTEGER,
    -- Studies this build found single-cell evidence for, any tier.
    studies_with_evidence INTEGER,
    studies_high_tier    INTEGER,
    experiments_with_evidence INTEGER,
    -- Studies the archive's own structured field declares single-cell. When this
    -- exceeds studies_with_evidence, rules have gaps for declared data.
    studies_declared_sc  INTEGER,
    notes                TEXT,
    PRIMARY KEY (archive, built_at)
) WITHOUT ROWID;

-- ENA combines a base checkpoint and optional dated increment.
-- Pass --ena-delta-db-file to make ENA coverage complete.
-- combined/ena_metadata_combined.db is corrupt. Do not repair it.
-- enriched_assay_library_col reads one enrichment generation;
-- sc_archive_coverage.studies_total records its denominator.

-- Studies asserted single-cell and awaiting matrix/read confirmation.
--
-- External channels: ncount for matrix cells/genes, preflightx for read calls.
-- `reason` orders work: synthetic source, medium-tier prose, unconfirmed
-- high-tier declaration. Every study is listed because declaration is still
-- unmeasured; callers subtract settled rows by joining Pentimento/sample counts.
DROP VIEW IF EXISTS sc_needs_confirmation;   -- see single_cell_study above
CREATE VIEW IF NOT EXISTS sc_needs_confirmation AS
SELECT
    s.study_accession,
    s.evidence_tier,
    s.sc_objects,
    s.n_rules,
    CASE WHEN s.analytes = '["synthetic"]' THEN 'source_says_synthetic'
         WHEN s.evidence_tier <> 'high'     THEN 'medium_tier_only'
         ELSE 'unconfirmed_high' END AS reason,
    s.rules,
    s.assay_modalities,
    s.analytes
FROM single_cell_study s;

-- Cohort-facing view: evidence plus organism, counts and chemistry from
-- unified_metadata and Pentimento.
--
-- Read-derived axes: modality from intron/mito, chemistry from assay_display,
-- bulk_vs_sc from R1 geometry. 5' chemistry can read as 'bulk_or_5prime_broken'.
--
-- Left joins keep unmeasured studies visible. SQLite needs all four source
-- tables in one schema; ATTACH leaves view names bound to their original schema.
DROP VIEW IF EXISTS single_cell_cohort;      -- see single_cell_study above
CREATE VIEW IF NOT EXISTS single_cell_cohort AS
SELECT
    s.study_accession,
    s.evidence_tier,
    s.has_high_confidence,
    s.assay_modalities,          -- stated: scRNA-seq | scATAC-seq | CITE-seq | ...
    s.analytes,                  -- transcriptomic | genomic | synthetic | other
    s.library_strategies,
    s.rules,
    -- Organism side by side: submitter-registered and preflightx k-mer call.
    --
    -- The vocabularies differ in punctuation: preflightx emits 'Homo_sapiens',
    -- archives emit 'Homo sapiens', so organism_agrees normalises before
    -- comparing. run_preflightx.species_mislabel is empty; derive disagreement
    -- here.
    u.dominant_scientific_name   AS organism_stated,
    p.organism_measured,
    p.organism_confidence,
    CASE WHEN p.organism_measured IS NULL
              OR p.organism_measured IN ('unknown','')
              OR u.dominant_scientific_name IS NULL THEN NULL
         WHEN lower(replace(p.organism_measured,'_',' '))
            = lower(u.dominant_scientific_name)     THEN 1
         ELSE 0 END              AS organism_agrees,

    -- measured, from the reads
    p.modality_measured,         -- single-cell | single-nucleus | ambiguous | unknown
    p.modality_confidence,
    p.chemistry,                 -- e.g. "10x Chromium 3' v3"
    p.bulk_vs_sc,
    -- size, for "at least N cells"
    p.n_runs_measured,
    n.n_samples_measured,
    n.cells_total,
    n.any_unfiltered,        -- 1 = cells_total counts BARCODES, do not sum it
    n.genes_mean,
    -- NULL = never looked, not "no".
    CASE WHEN p.study_accession IS NULL AND n.study_accession IS NULL
         THEN 0 ELSE 1 END AS is_measured
FROM single_cell_study s
LEFT JOIN (
    -- Roll read calls and matrix sizes to study before joining; sample_master
    -- lacks modality / assay_display / bulk_vs_sc.
    SELECT study_accession,
           max(species_called)      AS organism_measured,
           max(species_confidence)  AS organism_confidence,
           max(modality)            AS modality_measured,
           max(modality_confidence) AS modality_confidence,
           max(assay_display)       AS chemistry,
           max(bulk_vs_sc)          AS bulk_vs_sc,
           count(DISTINCT sample_accession) AS n_runs_measured
    FROM run_preflightx GROUP BY study_accession
) p ON p.study_accession = s.study_accession
LEFT JOIN (
    -- cells is unsafe to sum when unfiltered: columns are 10x barcodes. Carry
    -- any_unfiltered so callers can filter the total.
    SELECT study_accession,
           count(*)          AS n_samples_measured,
           sum(cells)        AS cells_total,
           max(unfiltered)   AS any_unfiltered,
           avg(genes)        AS genes_mean
    FROM sample_ncount GROUP BY study_accession
) n ON n.study_accession = s.study_accession
LEFT JOIN unified_metadata u ON u.canonical_accession = s.study_accession
                             OR json_extract(u.aliases, '$.geo') = s.study_accession;
