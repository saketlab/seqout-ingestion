-- Postgres schema for the fleet sc_stats sweep. Column-for-column twin of
-- sql/tables/sc_stats.sql, so the transfer's column lists stay in step with both.
--
-- Grain is one row per matrix. A GEO series publishes one matrix per sample and
-- their quantiles differ, so a study-level mean would describe no real object;
-- sc_stats_study carries the rollups instead.

CREATE TABLE IF NOT EXISTS sc_stats_matrix (
    accession             TEXT NOT NULL,
    file                  TEXT NOT NULL,

    -- Join key to pentimento_sample; see the staging twin for the rest.
    sample_accession      TEXT,

    n_cells               INTEGER,
    n_barcodes            INTEGER,

    -- TRUE when the barcode axis matches a 10x whitelist size. Those matrices
    -- are raw droplet space: most barcodes are ambient, so the unfiltered
    -- quantiles below describe empty droplets. Read the _f_ columns instead.
    is_raw_barcode_space  BOOLEAN,

    species               TEXT,
    species_method        TEXT,
    n_features            INTEGER,
    n_mito_genes          INTEGER,

    -- Per-cell quantiles over barcodes with any counts.
    ncount_min            DOUBLE PRECISION,
    ncount_mean           DOUBLE PRECISION,
    ncount_median         DOUBLE PRECISION,
    ncount_max            DOUBLE PRECISION,
    nfeature_min          DOUBLE PRECISION,
    nfeature_mean         DOUBLE PRECISION,
    nfeature_median       DOUBLE PRECISION,
    nfeature_max          DOUBLE PRECISION,
    pct_mito_min          DOUBLE PRECISION,
    pct_mito_mean         DOUBLE PRECISION,
    pct_mito_median       DOUBLE PRECISION,
    pct_mito_max          DOUBLE PRECISION,

    min_counts            INTEGER,
    n_cells_filtered      INTEGER,
    ncount_f_min          DOUBLE PRECISION,
    ncount_f_mean         DOUBLE PRECISION,
    ncount_f_median       DOUBLE PRECISION,
    ncount_f_max          DOUBLE PRECISION,
    nfeature_f_min        DOUBLE PRECISION,
    nfeature_f_mean       DOUBLE PRECISION,
    nfeature_f_median     DOUBLE PRECISION,
    nfeature_f_max        DOUBLE PRECISION,
    pct_mito_f_min        DOUBLE PRECISION,
    pct_mito_f_mean       DOUBLE PRECISION,
    pct_mito_f_median     DOUBLE PRECISION,
    pct_mito_f_max        DOUBLE PRECISION,

    PRIMARY KEY (accession, file)
);

CREATE INDEX IF NOT EXISTS idx_sc_stats_matrix_sample  ON sc_stats_matrix (sample_accession);
CREATE INDEX IF NOT EXISTS idx_sc_stats_matrix_species ON sc_stats_matrix (species);

-- Per-study outcome and rollups. `state` kept apart from an absent row on
-- purpose: no_matrix means the study publishes none, fetch_failed means the
-- download failed, and a missing row means the study was never scanned. An
-- absence must never read as a data finding.
CREATE TABLE IF NOT EXISTS sc_stats_study (
    accession             TEXT PRIMARY KEY,
    state                 TEXT,
    detail                TEXT,
    n_matrices            INTEGER NOT NULL DEFAULT 0,
    n_failures            INTEGER NOT NULL DEFAULT 0,
    failures_json         TEXT,

    -- Cell and gene totals are NOT held here. pentimento_study.study_cells and
    -- pentimento_sample.cells/genes already carry them; join through
    -- sc_stats_matrix.sample_accession rather than keeping a copy that drifts.
    --
    -- species is sc_stats' own, read off the matrix's gene symbols, and so is
    -- not the same measurement as pentimento_sample.species_called, which is
    -- read-derived. any_raw_barcode_space warns that the study's unfiltered
    -- quantiles describe ambient droplets.
    species               TEXT,
    any_raw_barcode_space BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_sc_stats_study_state   ON sc_stats_study (state);
CREATE INDEX IF NOT EXISTS idx_sc_stats_study_species ON sc_stats_study (species);
