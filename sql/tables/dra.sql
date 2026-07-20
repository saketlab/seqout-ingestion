-- Column-identical to sra_* (sql/tables/sra.sql): same INSDC XML, and
-- ingest/insdc_xml.py emits one tuple shape for both. Keep them in lockstep.

CREATE TABLE IF NOT EXISTS dra_submissions (
    accession    TEXT PRIMARY KEY,
    alias        TEXT,
    broker_name  TEXT,
    center_name  TEXT,
    lab_name     TEXT,
    published_at DATE,
    updated_at   DATE
);

CREATE TABLE IF NOT EXISTS dra_experiments (
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

CREATE TABLE IF NOT EXISTS dra_samples (
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

CREATE TABLE IF NOT EXISTS dra_runs (
    accession  TEXT PRIMARY KEY,
    experiment TEXT,
    submission TEXT
);

CREATE TABLE IF NOT EXISTS dra_studies (
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
    published_at        TEXT,
    pubmed_id_pysradb   TEXT,
    pubmed_last_checked DATE,
    updated_at          TEXT,
    -- 1 = no SRA/ENA mirror. NULL = undetermined (ingest had no sra/ena db or
    -- --pg-url to check against); kept distinct from 0 rather than defaulted.
    is_only_ddbj        INTEGER CHECK (is_only_ddbj IN (0, 1))
);

-- Pins which SRA/ENA snapshot the novelty check ran against.
CREATE TABLE IF NOT EXISTS dra_ingest_provenance (
    ingested_at      TEXT PRIMARY KEY,
    start_date       TEXT,
    end_date         TEXT,
    livelist_mtime   TEXT,
    sra_db_file      TEXT,
    sra_db_mtime     TEXT,
    ena_db_file      TEXT,
    ena_db_mtime     TEXT,
    novelty_filtered INTEGER NOT NULL,
    n_candidates     INTEGER NOT NULL,
    n_kept           INTEGER NOT NULL,
    n_skipped        INTEGER NOT NULL
);
