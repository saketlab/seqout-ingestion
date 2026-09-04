-- Staging schema for the fleet sc_stats sweep (SQLite).
--
-- One row per matrix, not per study: a GEO series routinely publishes one matrix
-- per sample and their quantiles differ, so collapsing them to a study mean would
-- describe no real object.

CREATE TABLE IF NOT EXISTS sc_stats_matrix (
    accession             TEXT NOT NULL,
    file                  TEXT NOT NULL,
    -- GSM parsed off the filename, the join key to pentimento_sample, which
    -- already holds cells and genes from the counting job. NULL for files
    -- published at series scope (a bare `matrix.mtx`), which carry no sample
    -- identity to recover.
    sample_accession      TEXT,
    -- n_cells is barcodes carrying any counts, n_barcodes the whole barcode
    -- axis. Genes MEASURED is not stored: pentimento_sample.genes has it, and a
    -- second copy would be a second source to reconcile. nfeature_* below is
    -- genes DETECTED in one cell, a different quantity.
    n_cells               INTEGER,
    n_barcodes            INTEGER,
    -- barcode axis matches a 10x whitelist size, so the unfiltered quantiles
    -- describe empty droplets. Read the _filtered columns for these.
    is_raw_barcode_space  INTEGER,
    species               TEXT,
    species_method        TEXT,
    n_features            INTEGER,
    n_mito_genes          INTEGER,

    ncount_min            REAL,
    ncount_mean           REAL,
    ncount_median         REAL,
    ncount_max            REAL,
    nfeature_min          REAL,
    nfeature_mean         REAL,
    nfeature_median       REAL,
    nfeature_max          REAL,
    -- Quad like the others, and all four are NULL, never 0, when the study
    -- publishes no features file: with no features there is no mito gene to
    -- measure, so a 0 would assert a clean result nothing established.
    pct_mito_min          REAL,
    pct_mito_mean         REAL,
    pct_mito_median       REAL,
    pct_mito_max          REAL,

    min_counts            INTEGER,
    n_cells_filtered      INTEGER,
    ncount_f_min          REAL,
    ncount_f_mean         REAL,
    ncount_f_median       REAL,
    ncount_f_max          REAL,
    nfeature_f_min        REAL,
    nfeature_f_mean       REAL,
    nfeature_f_median     REAL,
    nfeature_f_max        REAL,
    pct_mito_f_min        REAL,
    pct_mito_f_mean       REAL,
    pct_mito_f_median     REAL,
    pct_mito_f_max        REAL,

    PRIMARY KEY (accession, file)
);

CREATE INDEX IF NOT EXISTS idx_sc_stats_matrix_sample  ON sc_stats_matrix (sample_accession);
CREATE INDEX IF NOT EXISTS idx_sc_stats_matrix_species ON sc_stats_matrix (species);

-- Per-study outcome. `state` is what the sweep recorded, and keeping it apart
-- from an absent row is the whole point: no_matrix means the study publishes
-- none, fetch_failed means the download failed, and a missing row means the
-- study was never attempted. An absence must never read as a data finding.
CREATE TABLE IF NOT EXISTS sc_stats_study (
    accession          TEXT PRIMARY KEY,
    state              TEXT,
    detail             TEXT,
    n_matrices         INTEGER NOT NULL DEFAULT 0,
    n_failures         INTEGER NOT NULL DEFAULT 0,
    failures_json      TEXT,

    -- Cell and gene totals are NOT rolled up here. pentimento_study.study_cells
    -- and pentimento_sample.cells/genes already carry them from the counting
    -- job; join through sc_stats_matrix.sample_accession rather than keeping a
    -- second copy that can drift.
    --
    -- These two are sc_stats' own, with no counterpart there: species is read
    -- off the matrix's gene symbols, not off reads like
    -- pentimento_sample.species_called.
    species            TEXT,
    any_raw_barcode_space INTEGER
);

CREATE INDEX IF NOT EXISTS idx_sc_stats_study_state   ON sc_stats_study (state);
CREATE INDEX IF NOT EXISTS idx_sc_stats_study_species ON sc_stats_study (species);
