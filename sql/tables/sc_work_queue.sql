-- Single-cell work queue, with borrowed facts for FASTQ, matrix and prior
-- measurements.
--
-- Borrowed tables are snapshots for offline queue builds.
-- Copying also lets single_cell_cohort resolve in standalone SQLite; ATTACH
-- leaves unqualified view names bound to their original schema. sc_borrowed_source
-- records provenance.

-- provenance
CREATE TABLE IF NOT EXISTS sc_borrowed_source (
    table_name     TEXT PRIMARY KEY,   -- the table in THIS file
    source_kind    TEXT NOT NULL,      -- postgres | sqlite | derived
    source_ref     TEXT NOT NULL,      -- pg table name, or the file path
--
--
    source_rows    INTEGER,
    rows_copied    INTEGER,
    -- The literal filter applied, or NULL for a whole-table copy.
    copy_filter    TEXT,
    -- Columns omitted from the copy. NULL means none were omitted.
    columns_missing TEXT,
    copied_at      TEXT NOT NULL,
    notes          TEXT
);

-- borrowed verbatim from Postgres
-- pentimento_sample and pentimento_study keep their Postgres names. run_preflightx
-- and sample_ncount use the SQLite Pentimento schema names for the same data.
--
-- BOOLEANs arrive as 0/1 INTEGER. NULL keeps its Postgres meaning throughout:
-- unmeasured, distinct from negative.
CREATE TABLE IF NOT EXISTS pentimento_sample (
    study_accession        TEXT NOT NULL,
    sample_accession       TEXT NOT NULL,
    cells                  INTEGER,
    genes                  INTEGER,
    nnz                    INTEGER,
    file_format            TEXT,
    unfiltered             INTEGER,
    n_runs                 INTEGER,
    n_runs_measurable      INTEGER,
    species_called         TEXT,
    species_confidence     TEXT,
    sex_verdict            TEXT,
    sex_confidence         TEXT,
    assay                  TEXT,
    assay_display          TEXT,
    assay_is_single_cell   INTEGER,
    modality               TEXT,
    modality_confidence    TEXT,
    intron_frac            REAL,
    mito_pct               REAL,
    bulk_vs_sc             TEXT,
    bulk_vs_sc_confidence  TEXT,
    read_end               TEXT,
    flags                  TEXT,
    n_flags                INTEGER,
    PRIMARY KEY (study_accession, sample_accession)
);

CREATE TABLE IF NOT EXISTS pentimento_study (
    study_accession        TEXT PRIMARY KEY,
    source                 TEXT,
    title                  TEXT,
    aliases                TEXT,
    study_cells            INTEGER,
    cells_unfiltered       INTEGER,
    n_samples_reported     INTEGER,
    n_samples_detailed     INTEGER,
    n_runs_measurable      INTEGER,
    n_runs_linked          INTEGER,
    n_runs_preflightx      INTEGER
);

-- Cell counts from ncount gap runs in data/ncount/. Separate storage preserves
-- source_file and survives a re-borrow from Postgres.
CREATE TABLE IF NOT EXISTS ncount_extra (
    sample_accession  TEXT NOT NULL,
    study_accession   TEXT NOT NULL,
    cells             INTEGER,
    -- -1 in the source means "not derivable from this file type", not zero.
    genes             INTEGER,
    file_format       TEXT,
    unfiltered        INTEGER,
    source_file       TEXT NOT NULL,
    PRIMARY KEY (sample_accession, study_accession, source_file)
);

-- derived: what can be downloaded
-- Per-study rollup of sra_run_download_links; the run-grained table stays
-- server-side.
--
-- n_runs_sra_only counts runs with an SRA object and no fastq_ftp. They read as
-- available, but the usual cause is a BAM deposit where ENA has only
-- submitted_ftp and no FASTQ.
--
-- fastq_bytes sums every listed FASTQ, so it is an upper bound for stage 1,
-- which streams a slice, and the true cost for mirroring.
CREATE TABLE IF NOT EXISTS sc_study_downloads (
    study_accession   TEXT PRIMARY KEY,   -- the archive's own run-bearing key (SRP/ERP/DRP/CRA/...)
    n_runs            INTEGER NOT NULL,
    n_runs_fastq      INTEGER NOT NULL,
    n_runs_sra_only   INTEGER NOT NULL,
    fastq_bytes       INTEGER,
    sra_bytes         INTEGER
);

-- derived: what can be counted
-- Countable expression matrices from TENX_FILE_TERMS. Channels cover
-- geo_sample_tenx evidence rows, geo_series_tenx series matrices, and
-- ae_processed files.
--
-- TENX_FILE_TERMS includes pipeline outputs; has_countable is 1 only for terms
-- that are matrices. .h5ad/.loom alone leave has_matrix = 0.
CREATE TABLE IF NOT EXISTS sc_study_matrix (
    study_accession   TEXT NOT NULL,
    channel           TEXT NOT NULL,     -- geo_sample_tenx | geo_series_tenx | ae_processed
    n_objects         INTEGER NOT NULL,  -- samples / series / experiments that matched
    has_countable     INTEGER NOT NULL,
    terms             TEXT,              -- json array of the terms that matched
    example_value     TEXT,
    PRIMARY KEY (study_accession, channel)
);

-- the queue
-- One row per archive study accession. group_key/group_size identify mirrors.
--
-- Stages: preflightx for FASTQ awaiting stage 1; pentimento for positive stage 1
-- awaiting quant; ncount for countable matrices; plate_route for plate protocols;
-- blocked for missing route or a negative gate; done after applicable channels.
--
-- Plate protocols keep cell identity in filenames; barcode evidence is weak and
-- stays on plate_route. Missing pentimento row means unmeasured.
CREATE TABLE IF NOT EXISTS sc_work_queue (
    study_accession      TEXT PRIMARY KEY,
    archive              TEXT,     -- archive of the highest-tier rule that fired
    archives             TEXT,     -- json array; every archive that contributed
    group_key            TEXT,     -- canonical accession this study's alias group collapses to
    group_size           INTEGER,  -- accessions in that group, this row included
    source               TEXT,     -- unified_metadata.source, NULL when absent from it
    evidence_tier        TEXT,
    has_high_confidence  INTEGER,
    n_rules              INTEGER,
    sc_objects           INTEGER,
    assay_modality       TEXT,     -- the most specific single modality, or 'mixed'
    assay_modalities     TEXT,     -- json array, verbatim from single_cell_study
    analytes             TEXT,
    n_samples            INTEGER,  -- unified_metadata.n_samples; NULL = unknown, NOT zero
    n_experiments        INTEGER,
    organism_stated      TEXT,

    -- what data exists
    n_runs               INTEGER NOT NULL DEFAULT 0,
    n_runs_fastq         INTEGER NOT NULL DEFAULT 0,
    n_runs_sra_only      INTEGER NOT NULL DEFAULT 0,
    fastq_bytes          INTEGER,
    sra_bytes            INTEGER,
    has_fastq            INTEGER NOT NULL,
    is_sra_object_only   INTEGER NOT NULL,   -- runs exist, an SRA object exists, no FASTQ
    has_matrix           INTEGER NOT NULL,
    has_countable_matrix INTEGER NOT NULL,
    matrix_channels      TEXT,
    -- no|yes x no|yes from the two columns above: ncount | preflightx | both |
    -- unavailable. The census bucket counts what exists, before anything
    -- already done is subtracted.
    data_bucket          TEXT NOT NULL,

    -- what has already been measured
    n_samples_ncounted   INTEGER NOT NULL DEFAULT 0,
    cells_total          INTEGER,
    any_unfiltered       INTEGER,
    ncount_sources       TEXT,
    ncount_status        TEXT NOT NULL,      -- none | partial | done
    -- Grain matters: run status and study status are carried as a pair.
--
--
    n_runs_preflightx    INTEGER NOT NULL DEFAULT 0,
    n_runs_sc_true       INTEGER NOT NULL DEFAULT 0,
    n_runs_sc_false      INTEGER NOT NULL DEFAULT 0,
    preflightx_status    TEXT NOT NULL,      -- none | partial | done
    preflightx_verdict   TEXT NOT NULL,      -- single_cell | not_single_cell | uncertain | absent
    n_runs_quantified    INTEGER NOT NULL DEFAULT 0,
    pentimento_status    TEXT NOT NULL,      -- none | partial | done

    -- the plate route
    is_plate_candidate   INTEGER NOT NULL,
    plate_terms          TEXT,

    -- the decision
    next_stage           TEXT NOT NULL,
    stage_reason         TEXT NOT NULL,
    needs_ncount         INTEGER NOT NULL,
    needs_preflightx     INTEGER NOT NULL,
    needs_pentimento     INTEGER NOT NULL,
--
    -- Standing rule: defer mega-studies. Flag only; the row stays in the queue.
    is_mega_study        INTEGER NOT NULL,
    defer_reason         TEXT,
    built_at             TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sc_work_queue_stage   ON sc_work_queue (next_stage);
CREATE INDEX IF NOT EXISTS idx_sc_work_queue_bucket  ON sc_work_queue (data_bucket);
CREATE INDEX IF NOT EXISTS idx_sc_work_queue_group   ON sc_work_queue (group_key);
CREATE INDEX IF NOT EXISTS idx_sc_study_matrix_study ON sc_study_matrix (study_accession);
CREATE INDEX IF NOT EXISTS idx_ncount_extra_study    ON ncount_extra (study_accession);

-- Cost per stage. Stage 2 cost applies only to studies confirmed by stage 1, so
-- it is grouped by stage.
--
-- Deduped to one representative row per (group, stage); groups split across
-- stages count once in each stage.
DROP VIEW IF EXISTS sc_work_queue_cost;
CREATE VIEW IF NOT EXISTS sc_work_queue_cost AS
WITH representative AS (
    SELECT q.*
    FROM sc_work_queue q
    WHERE q.study_accession = (
        SELECT min(q2.study_accession) FROM sc_work_queue q2
        WHERE q2.group_key = q.group_key AND q2.next_stage = q.next_stage
    )
)
SELECT
    next_stage,
    count(*)                            AS groups,
    sum(is_mega_study)                  AS mega_studies,
    sum(n_runs)                         AS runs,
    sum(n_runs_fastq)                   AS runs_with_fastq,
    -- full-download bytes; upper bound for streaming stages, where preflightx
    -- reads the head of one run
    sum(coalesce(fastq_bytes, 0))       AS fastq_bytes,
    (SELECT count(*) FROM sc_work_queue w WHERE w.next_stage = representative.next_stage)
                                        AS queue_rows
FROM representative
GROUP BY next_stage;
