-- Pentimento in Postgres: the projection of the SQLite DB the seqout API serves.

CREATE TABLE IF NOT EXISTS pentimento_study (
    study_accession           TEXT PRIMARY KEY,
    source                    TEXT,
    title                     TEXT,
    aliases                   JSONB,
    -- Matrix COLUMN count, not cells. See any_unfiltered before summing.
    study_cells               BIGINT,
    cells_unfiltered          BOOLEAN NOT NULL DEFAULT FALSE,
    single_cell_file_format   TEXT,
    n_samples_reported        INTEGER,
    n_samples_detailed        INTEGER,
    sample_cells_total        BIGINT,
    -- study_cells minus what the visible per-sample rows account for. Non-zero
    -- for studies deposited without a per-sample breakdown; a UI showing the
    -- total must not imply the rows add up to it.
    unassigned_cells          BIGINT,
    sample_breakdown_complete BOOLEAN,
    -- OR across the study and its samples: one unfiltered matrix poisons a sum,
    -- so this is a warning flag, not a measurement.
    any_unfiltered            BOOLEAN,
    -- Three DIFFERENT coverages; conflating them understates Pentimento ~7x.
    --   n_runs_linked      runs attributable to this study at all
    --   n_runs_preflightx  runs with a read-derived species/sex/assay call
    --   n_runs_measurable  runs quantified against the microbial panel
    --                      (Logan quantification is ongoing)
    n_runs_linked             INTEGER,
    n_runs_preflightx         INTEGER,
    n_runs_measurable         INTEGER,
    has_metadata              BOOLEAN,
    has_celltype              BOOLEAN,
    has_donor                 BOOLEAN,
    has_demographics          BOOLEAN,
    -- Per-taxon rollup of the study's samples, tri-state (NULL = nothing in
    -- this study was measurable). Column list mirrors pentimento.flags.FLAG_PANEL.
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
    -- Evidence behind the sex call: Y-unique vs XIST read counts, over the depth
    -- actually scanned. 21 Y-hits in 200 k reads is not 21 in 2 k, so y_hits
    -- without sex_reads_scanned is unreadable.
    sex_reads_scanned     BIGINT,
    y_hits                INTEGER,
    xist_hits             INTEGER,
    y_xist_ratio          DOUBLE PRECISION,
    -- 'unsupported' = the panel is not validated for this species. The verdict
    -- stands; this says not to trust it.
    sex_panel_status      TEXT,
    sex_mixed_suspected   BOOLEAN,
    deep_n_reads          BIGINT,
    assay                 TEXT,
    assay_display         TEXT,   -- human-readable chemistry, e.g. "10x 3' v3"
    assay_is_single_cell  BOOLEAN,
    -- Set when the sample's runs disagreed; the displayed value is one of
    -- several, not a consensus.
    calls_ambiguous       BOOLEAN,
    -- Tripped flags only, as a JSON array. NULL means nothing was measurable,
    -- which is NOT the same as an empty array (measured, nothing found).
    flags                 JSONB,
    n_flags               INTEGER,
    hpv_top_type          TEXT,
    hpv_ambiguous         BOOLEAN,
    -- (study, sample) not sample alone: 425 GSMs appear under >1 study, and a
    -- sample-only key would attach a run to the wrong project page.
    PRIMARY KEY (study_accession, sample_accession)
);

CREATE INDEX IF NOT EXISTS idx_pentimento_sample_sample
    ON pentimento_sample (sample_accession);

-- Resolves a caller's SRP/ERP/PRJ to its GSE in one containment probe. Without
-- it the resolver falls back to a seq scan on every project page.
CREATE INDEX IF NOT EXISTS idx_pentimento_study_aliases
    ON pentimento_study USING GIN (aliases jsonb_path_ops);

-- Per-run detail, for a sample drill-down. Kept separate from the sample table
-- because it is 29x larger and the project page never needs it up front.
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

CREATE TABLE IF NOT EXISTS pentimento_microbe (
    run_accession          TEXT NOT NULL,
    organism               TEXT NOT NULL,
    class                  TEXT,
    reads                  BIGINT,
    ref_bp                 BIGINT,
    covered_bp             BIGINT,
    breadth_frac           REAL,
    is_background          BOOLEAN,
    PRIMARY KEY (run_accession, organism)
);

-- How accurate the read-derived calls are, long form (one row per experiment
-- x field). Shipped so the UI can qualify a call with a measured number.
--
-- correct is tri-state: NULL means the comparison could not be made, which
-- differs from a wrong answer, and for sex that is every row. Ground truth is
-- the literal string 'NA' on every row, so sex has predictions and zero
-- evaluable ones. Never render a NULL-heavy field as 0% accurate.
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
