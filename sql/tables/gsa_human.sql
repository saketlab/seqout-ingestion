-- GSA-Human — CNCB-NGDC Genome Sequence Archive for Human, HRA studies.
-- Only Access = 'Open' studies are fetched; controlled-access studies keep their
-- study row but no detail (gated behind a data-access request).
-- HRA workbooks ship in three layouts, all funnelling into the tables below:
--   * Individual  + Sample/Experiment/Run   (subject-based studies)
--   * Cell Line   + Sample/Experiment/Run   (cell-line studies)
--   * Individuals & samples + Files          (older combined layout)
-- Organism is implicitly Homo sapiens.

CREATE TABLE IF NOT EXISTS gsa_human_studies (
    accession           TEXT PRIMARY KEY,   -- HRA...
    title               TEXT,
    organization        TEXT,
    dac                 TEXT,
    access              TEXT,               -- Open | Controlled
    n_requests          INTEGER,
    n_effective_requests INTEGER,
    n_approved          INTEGER,
    project_accession   TEXT,               -- PRJCA...
    strategy            TEXT,
    study_type          TEXT,
    disease             TEXT,
    organism            TEXT DEFAULT 'Homo sapiens',  -- every HRA study is human
    n_individuals       INTEGER,
    n_samples           INTEGER,
    n_experiments       INTEGER,
    release_time        DATE,
    last_processed      DATE,
    pubmed_id_pysradb   TEXT,               -- filled by backfill pm-ids, not at ingest
    pubmed_last_checked DATE
);

-- Subjects and cell lines (both carry HRI... accessions) merged with an entity_type tag.
CREATE TABLE IF NOT EXISTS gsa_human_individuals (
    accession           TEXT NOT NULL,      -- HRI...
    study_accession     TEXT NOT NULL,      -- HRA...
    entity_type         TEXT,               -- individual | cell_line
    identifier          TEXT,               -- Individual Identifier / Cell Line Name
    gender              TEXT,
    tissue              TEXT,               -- cell line
    ethnicity           TEXT,
    disease_name        TEXT,
    disease_stage       TEXT,
    treatment           TEXT,
    experiment_group    TEXT,
    web_url             TEXT,               -- cell line
    PRIMARY KEY (accession, study_accession)
);

CREATE TABLE IF NOT EXISTS gsa_human_samples (
    accession            TEXT NOT NULL,     -- HRS...
    study_accession      TEXT NOT NULL,     -- HRA...
    sample_name          TEXT,
    individual_accession TEXT,              -- HRI...
    organism             TEXT DEFAULT 'Homo sapiens',
    tissue               TEXT,
    age                  TEXT,
    age_unit             TEXT,
    public_description   TEXT,
    sample_title         TEXT,
    sample_description   TEXT,              -- Individuals & samples layout
    collection_date      TEXT,
    biomaterial_provider TEXT,
    geographic_location  TEXT,
    culture_collection   TEXT,
    karyotype            TEXT,
    phenotype            TEXT,
    population           TEXT,
    PRIMARY KEY (accession, study_accession)
);

CREATE TABLE IF NOT EXISTS gsa_human_experiments (
    accession            TEXT NOT NULL,     -- HRX...
    study_accession      TEXT NOT NULL,     -- HRA...
    title                TEXT,
    biosample_name       TEXT,
    sample_accession     TEXT,              -- HRS...
    platform             TEXT,
    library_construction TEXT,
    library_name         TEXT,
    library_strategy     TEXT,
    library_source       TEXT,
    library_selection    TEXT,
    library_layout       TEXT,
    read_length_1        TEXT,
    read_length_2        TEXT,
    insert_size          TEXT,
    nominal_size         TEXT,
    nominal_sdev         TEXT,
    planned_cycles       TEXT,
    PRIMARY KEY (accession, study_accession)
);

CREATE TABLE IF NOT EXISTS gsa_human_runs (
    accession            TEXT NOT NULL,     -- HRR...
    study_accession      TEXT NOT NULL,     -- HRA...
    experiment_accession TEXT,              -- HRX...
    sample_accession     TEXT,              -- HRS... (Files layout only)
    run_title            TEXT,
    platform             TEXT,              -- Files layout only
    data_file_type       TEXT,
    files                TEXT,              -- combined filename string (Files layout)
    file1_name           TEXT,
    file1_md5            TEXT,
    file1_url            TEXT,
    file2_name           TEXT,
    file2_md5            TEXT,
    file2_url            TEXT,
    file3_name           TEXT,
    file3_md5            TEXT,
    file3_url            TEXT,
    index1_name          TEXT,
    index1_md5           TEXT,
    index2_name          TEXT,
    index2_md5           TEXT,
    index2_url           TEXT,
    reference_name       TEXT,
    reference_md5        TEXT,
    assembly_accession   TEXT,
    omics_type           TEXT,
    PRIMARY KEY (accession, study_accession)
);

-- Per-study download/parse status, for resume + reporting.
CREATE TABLE IF NOT EXISTS gsa_human_fetch_log (
    study_accession TEXT PRIMARY KEY,       -- HRA...
    status          TEXT NOT NULL,          -- ok | empty | failed
    study_id        TEXT,                   -- internal numeric id scraped from browse page
    n_individuals   INTEGER,
    n_samples       INTEGER,
    n_experiments   INTEGER,
    n_runs          INTEGER,
    fetched_at      DATE NOT NULL
);
