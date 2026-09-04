CREATE TABLE IF NOT EXISTS pentimento_study (
    study_accession           TEXT PRIMARY KEY,
    source                    TEXT,
    title                     TEXT,
    aliases                   JSONB,
    -- Matrix COLUMN count. See any_unfiltered before summing.
    study_cells               BIGINT,
    cells_unfiltered          BOOLEAN NOT NULL DEFAULT FALSE,
    single_cell_file_format   TEXT,
    n_samples_reported        INTEGER,
    n_samples_detailed        INTEGER,
    sample_cells_total        BIGINT,
    -- Study-level total outside visible per-sample rows. UI labels this as an
    -- unassigned total.
--
    unassigned_cells          BIGINT,
    sample_breakdown_complete BOOLEAN,
    -- OR across the study and its samples: one unfiltered matrix poisons a sum,
    -- so this is a warning flag.
    any_unfiltered            BOOLEAN,
--
    -- Three coverages; conflating them understates Pentimento.
    --   n_runs_linked      runs attributable to this study
    --   n_runs_preflightx  runs with a read-derived species/sex/assay call
    --   n_runs_measurable  runs quantified against the microbial panel
    n_runs_linked             INTEGER,
    n_runs_preflightx         INTEGER,
    n_runs_measurable         INTEGER,
    has_metadata              BOOLEAN,
    has_celltype              BOOLEAN,
    has_donor                 BOOLEAN,
    has_demographics          BOOLEAN,
    -- Per-taxon rollup of the study's samples. NULL = unmeasured study.
    -- Column list mirrors pentimento.flags.FLAG_PANEL.
    flags                     JSONB
);

CREATE TABLE IF NOT EXISTS pentimento_sample (
    study_accession       TEXT NOT NULL,
    sample_accession      TEXT NOT NULL,
    cells                 BIGINT,
    genes                 BIGINT,
    nnz                   BIGINT,
    file_format           TEXT,
    unfiltered            BOOLEAN NOT NULL DEFAULT FALSE,
    n_runs                INTEGER,
    n_runs_measurable     INTEGER,
    species_called        TEXT,
    species_confidence    TEXT,
    species_hit_reads     BIGINT,
    species_hit_fraction  DOUBLE PRECISION,
    species_runner_up     TEXT,
    species_margin        DOUBLE PRECISION,
    species_ambiguous     BOOLEAN,
    species_mislabel      BOOLEAN,
    sex_verdict           TEXT,
    sex_confidence        TEXT,   -- 'low'|'medium'|'high'
    -- sex_reads_scanned gives the denominator for Y-unique and XIST read counts;
    -- raw counts are depth-dependent.
--
--
    sex_reads_scanned     BIGINT,
    y_hits                INTEGER,
    xist_hits             INTEGER,
    y_xist_ratio          DOUBLE PRECISION,
    -- 'unsupported' = panel lacks species validation; verdict stands, trust flag off.
    sex_panel_status      TEXT,
    sex_mixed_suspected   BOOLEAN,
    deep_n_reads          BIGINT,
    assay                 TEXT,
    assay_display         TEXT,   -- human-readable chemistry, e.g. "10x 3' v3"
    assay_is_single_cell  BOOLEAN,
--
    -- Cell vs nucleus, with read-depth context for intron_frac/mito_pct.
    -- Nuclei retain unspliced transcripts and lose cytoplasmic mitochondria.
    modality               TEXT,
    modality_confidence    TEXT,   -- 'low'|'medium'|'high'
    intron_frac            DOUBLE PRECISION,
    mito_pct               DOUBLE PRECISION,
    modality_reads_scanned BIGINT,
    bulk_vs_sc             TEXT,
    bulk_vs_sc_confidence  TEXT,
    read_end               TEXT,
    read_end_confidence    TEXT,
    -- Set when the sample's runs disagreed; displayed value is one member of
    -- the set.
    calls_ambiguous       BOOLEAN,
    -- Tripped flags only, as a JSON array. NULL means unmeasured; an empty
    -- array means measured with zero flags.
    flags                 JSONB,
    n_flags               INTEGER,
    hpv_top_type          TEXT,
    hpv_ambiguous         BOOLEAN,
    has_viral_reads       BOOLEAN,
    has_viral_evidence    BOOLEAN,
    has_bacterial_reads   BOOLEAN,
    viral_kmer_mass       DOUBLE PRECISION,
    bacterial_kmer_mass   DOUBLE PRECISION,
    -- (study, sample) mirrors pentimento_run, where a GSM can appear under
    -- more than one study. On this table sample_accession is one per sample,
    -- enforced by the UNIQUE index below that cohort search depends on.
    PRIMARY KEY (study_accession, sample_accession)
);

-- sample_accession is unique here because cohort search joins counts on sample
-- alone. A duplicate would attach arbitrary counts; cross-study samples require
-- changing this index and those joins together.
DROP INDEX IF EXISTS idx_pentimento_sample_sample;  -- superseded by the unique one
CREATE UNIQUE INDEX IF NOT EXISTS idx_pentimento_sample_sample_uniq
    ON pentimento_sample (sample_accession);

-- Resolves a caller's SRP/ERP/PRJ to its GSE in one indexed containment probe.
--
CREATE INDEX IF NOT EXISTS idx_pentimento_study_aliases
    ON pentimento_study USING GIN (aliases jsonb_path_ops);
--

-- Per-run detail for sample drill-down. Kept separate from the sample table.
CREATE TABLE IF NOT EXISTS pentimento_run (
    run_accession         TEXT PRIMARY KEY,
    experiment_accession  TEXT,
    sample_accession      TEXT,
    gsm_accession         TEXT,
    study_accession       TEXT,
    species_called        TEXT,
    species_confidence    TEXT,   -- 'low'|'medium'|'high'
    species_mislabel      BOOLEAN,
    sex_verdict           TEXT,
    sex_confidence        TEXT,   -- 'low'|'medium'|'high'
    assay                 TEXT,
    assay_is_single_cell  BOOLEAN,
    assay_confidence      TEXT,   -- 'low'|'medium'|'high'
    -- onlist overlap for the called chemistry
    -- NULL when no onlist was scored, never 0
    assay_onlist_pct      DOUBLE PRECISION,
    -- Cell vs nucleus, measured from the reads. The text route
    -- (unified_metadata.single_cell_modality) only knows this when the authors
    -- wrote it down; this column is the read-derived answer.
    modality              TEXT,
    modality_confidence   TEXT,   -- 'low'|'medium'|'high'
    bulk_vs_sc            TEXT,
    read_end              TEXT,   -- "3'"|"5'"|'full-length'
    preflightx_source     TEXT,
    library_layout        TEXT,
    fastq_ftp             TEXT,
    fastq_bytes           TEXT,
    -- Quantification status: NULL means this run was never attempted.
    quant_status          TEXT,
    total_reads           BIGINT,
    measurable            BOOLEAN,
    flags                 JSONB,
    n_flags               INTEGER
);

CREATE INDEX IF NOT EXISTS idx_pentimento_run_study ON pentimento_run (study_accession);
CREATE INDEX IF NOT EXISTS idx_pentimento_run_exp    ON pentimento_run (experiment_accession);
CREATE INDEX IF NOT EXISTS idx_pentimento_run_gsm
    ON pentimento_run (gsm_accession, experiment_accession);
CREATE INDEX IF NOT EXISTS idx_pentimento_run_sample
    ON pentimento_run (sample_accession, experiment_accession);

-- The cohort microbe filter drives from pentimento_microbe and reads back only
-- these two accessions per matching run. INCLUDE avoids heap fetches on hits.
CREATE INDEX IF NOT EXISTS idx_pentimento_run_acc_covering
    ON pentimento_run (run_accession) INCLUDE (gsm_accession, sample_accession);

CREATE TABLE IF NOT EXISTS pentimento_microbe (
    run_accession          TEXT NOT NULL,
    organism               TEXT NOT NULL,
    class                  TEXT,
    reads                  BIGINT,
    kmer_mass              DOUBLE PRECISION,
    n_unitigs              INTEGER,
    ref_bp                 BIGINT,
    covered_bp             BIGINT,
    breadth_frac           REAL,
    is_background          BOOLEAN,
    is_validated_viral     BOOLEAN,
    is_viral_evidence      BOOLEAN,
    is_validated_bacterial BOOLEAN,
    PRIMARY KEY (run_accession, organism)
);

CREATE INDEX IF NOT EXISTS idx_pentimento_microbe_viral
    ON pentimento_microbe (run_accession) WHERE is_validated_viral;
CREATE INDEX IF NOT EXISTS idx_pentimento_microbe_bact
    ON pentimento_microbe (run_accession) WHERE is_validated_bacterial;
CREATE INDEX IF NOT EXISTS idx_pentimento_microbe_breadth
    ON pentimento_microbe (breadth_frac DESC) WHERE NOT is_background;

-- CREATE EXTENSION is idempotent when pysradb-server's create_vocabulary.sh
-- installed it; named here so this file stands alone.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Trigram support for sample cohort organism ILIKE filters; partial to match
-- the query predicate.
CREATE INDEX IF NOT EXISTS idx_pentimento_microbe_organism_trgm
    ON pentimento_microbe USING gin (organism gin_trgm_ops) WHERE NOT is_background;

-- Read-call benchmark, long form: one row per experiment x field. correct is
-- tri-state; NULL means comparison unavailable, distinct from wrong. Sex ground
-- truth is literal 'NA', so sex predictions are retained for audit.
--
CREATE TABLE IF NOT EXISTS pentimento_benchmark (
    accession   TEXT NOT NULL,
    experiment  TEXT NOT NULL,
    field       TEXT NOT NULL,   -- organism | assay | sex
    gt_raw      TEXT,
    pred_raw    TEXT,
    gt_norm     TEXT,
    pred_norm   TEXT,
    correct     BOOLEAN,
    PRIMARY KEY (accession, experiment, field)
);

CREATE TABLE IF NOT EXISTS pentimento_flag_panel (
    flag      TEXT NOT NULL,
    organism  TEXT NOT NULL,
    gate      TEXT NOT NULL,
    PRIMARY KEY (flag, organism)
);
