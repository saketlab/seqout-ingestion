-- GSA (open / non-human) — CNCB-NGDC Genome Sequence Archive, CRA studies.
-- Study-level title/organization/counts come from the browse scrape and may be NULL.

CREATE TABLE IF NOT EXISTS gsa_studies (
    accession           TEXT PRIMARY KEY,   -- CRA...
    alias               TEXT,
    title               TEXT,
    organization        TEXT,
    project_accession   TEXT,               -- PRJCA...
    organism            TEXT,               -- most-common organism across the study's samples
    n_projects          INTEGER,
    n_samples           INTEGER,
    n_experiments       INTEGER,
    n_runs              INTEGER,
    release_time        DATE,
    pubmed_id_pysradb   TEXT,               -- filled by backfill pm-ids, not at ingest
    pubmed_last_checked DATE
);

CREATE TABLE IF NOT EXISTS gsa_samples (
    accession            TEXT NOT NULL,     -- SAMC... (BioSample)
    study_accession      TEXT NOT NULL,     -- CRA...
    sample_name          TEXT,
    public_description   TEXT,
    project_accession    TEXT,
    sample_title         TEXT,
    organism             TEXT,
    cultivar             TEXT,
    biomaterial_provider TEXT,
    tissue               TEXT,
    age                  TEXT,
    age_unit             TEXT,
    dev_stage            TEXT,
    cell_line            TEXT,
    cell_type            TEXT,
    collected_by         TEXT,
    collection_date      TEXT,
    culture_collection   TEXT,
    disease              TEXT,
    disease_stage        TEXT,
    genotype             TEXT,
    growth_protocol      TEXT,
    height_length        TEXT,
    isolation_source     TEXT,
    latitude_longitude   TEXT,
    phenotype            TEXT,
    population           TEXT,
    type                 TEXT,
    sex                  TEXT,
    specimen_voucher     TEXT,
    temperature          TEXT,
    treatment            TEXT,
    PRIMARY KEY (accession, study_accession)
);

CREATE TABLE IF NOT EXISTS gsa_experiments (
    accession             TEXT NOT NULL,    -- CRX...
    study_accession       TEXT NOT NULL,    -- CRA...
    title                 TEXT,
    project_accession     TEXT,
    biosample_name        TEXT,
    sample_accession      TEXT,             -- SAMC... (BioSample accession)
    platform              TEXT,
    library_construction  TEXT,
    library_name          TEXT,
    library_strategy      TEXT,
    library_source        TEXT,
    library_selection     TEXT,
    library_layout        TEXT,
    read_length_1         TEXT,
    read_length_2         TEXT,
    insert_size           TEXT,
    nominal_size          TEXT,
    nominal_sdev          TEXT,
    planned_cycles        TEXT,
    other_db              TEXT,
    accession_in_other_db TEXT,
    other_db_url          TEXT,
    PRIMARY KEY (accession, study_accession)
);

CREATE TABLE IF NOT EXISTS gsa_runs (
    accession             TEXT NOT NULL,    -- CRR...
    study_accession       TEXT NOT NULL,    -- CRA...
    experiment_accession  TEXT,             -- CRX...
    project_accession     TEXT,
    run_title             TEXT,
    data_file_type        TEXT,
    file1_name            TEXT,
    file1_md5             TEXT,
    file1_url             TEXT,
    file2_name            TEXT,
    file2_md5             TEXT,
    file2_url             TEXT,
    index1_name           TEXT,
    index1_md5            TEXT,
    index1_url            TEXT,
    index2_name           TEXT,
    index2_md5            TEXT,
    index2_url            TEXT,
    reference_name        TEXT,
    reference_md5         TEXT,
    assembly_accession    TEXT,
    assembly_url          TEXT,
    other_db              TEXT,
    accession_in_other_db TEXT,
    other_db_url          TEXT,
    PRIMARY KEY (accession, study_accession)
);

-- Per-study download/parse status, for resume + reporting.
CREATE TABLE IF NOT EXISTS gsa_fetch_log (
    study_accession TEXT PRIMARY KEY,       -- CRA...
    status          TEXT NOT NULL,          -- ok | empty | failed
    n_samples       INTEGER,
    n_experiments   INTEGER,
    n_runs          INTEGER,
    fetched_at      DATE NOT NULL
);
