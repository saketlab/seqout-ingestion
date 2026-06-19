CREATE TABLE IF NOT EXISTS sra_submissions (
    accession    TEXT PRIMARY KEY,
    alias        TEXT,
    broker_name  TEXT,
    center_name  TEXT,
    lab_name     TEXT,
    published_at DATE,
    updated_at   DATE
);

CREATE TABLE IF NOT EXISTS sra_experiments (
    accession           TEXT PRIMARY KEY,
    design_description  TEXT,
    library_layout      TEXT,
    library_name        TEXT,
    library_selection   TEXT,
    library_source      TEXT,
    library_strategy    TEXT,
    samples             TEXT CHECK (json_valid(samples)),
    platform            TEXT,
    instrument_model    TEXT,
    study               TEXT,
    title               TEXT,
    submission          TEXT
);

CREATE TABLE IF NOT EXISTS sra_samples (
    accession       TEXT PRIMARY KEY,
    alias           TEXT,
    description     TEXT,
    title           TEXT,
    scientific_name TEXT,
    taxon_id        TEXT,
    external_id     TEXT CHECK (json_valid(external_id)),
    attributes_json TEXT CHECK (json_valid(attributes_json)),
    submission      TEXT,
    links           TEXT CHECK (json_valid(links))
);

CREATE TABLE IF NOT EXISTS sra_runs (
    accession  TEXT PRIMARY KEY,
    experiment TEXT,
    submission TEXT
);

CREATE TABLE IF NOT EXISTS sra_studies (
    accession           TEXT PRIMARY KEY,
    alias               TEXT,
    center_name         TEXT,
    abstract            TEXT,
    title               TEXT,
    external_id         TEXT CHECK (json_valid(external_id)),
    study_type          TEXT,
    attributes_json     TEXT CHECK (json_valid(attributes_json)),
    links               TEXT CHECK (json_valid(links)),
    center_project_name TEXT,
    submission          TEXT,
    published_at        DATE,
    pubmed_id_pysradb   TEXT,
    pubmed_last_checked DATE,
    updated_at          DATE
);
