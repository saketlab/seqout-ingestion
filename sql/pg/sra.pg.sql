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
    samples             TEXT[],
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
    external_id     JSONB,
    attributes_json JSONB,
    submission      TEXT,
    links           JSONB
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
    external_id         JSONB,
    study_type          TEXT,
    attributes_json     JSONB,
    links               JSONB,
    center_project_name TEXT,
    submission          TEXT,
    published_at        DATE,
    pubmed_id_pysradb   TEXT,
    pubmed_last_checked DATE,
    updated_at          DATE
);

ALTER TABLE sra_studies ADD COLUMN IF NOT EXISTS tsv tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(title,'')),    'A') ||
    setweight(to_tsvector('english', coalesce(abstract,'')), 'B') ||
    setweight(to_tsvector('english', coalesce(center_project_name,'')), 'C') ||
    setweight(to_tsvector('english', coalesce(study_type,'')),'D')
) STORED;

CREATE INDEX IF NOT EXISTS sra_studies_fts_idx ON sra_studies USING GIN(tsv);
CREATE INDEX IF NOT EXISTS sra_studies_accession_upper_idx ON sra_studies (UPPER(accession));
CREATE INDEX IF NOT EXISTS sra_experiments_accession_upper_idx ON sra_experiments (UPPER(accession));
CREATE INDEX IF NOT EXISTS sra_experiments_study_idx ON sra_experiments (study);
CREATE INDEX IF NOT EXISTS idx_sra_exp_library_strategy ON sra_experiments (library_strategy);
CREATE INDEX IF NOT EXISTS idx_sra_exp_platform ON sra_experiments (platform);
CREATE INDEX IF NOT EXISTS idx_sra_studies_organisms ON sra_studies USING GIN (organisms);
CREATE INDEX IF NOT EXISTS idx_sra_studies_published_at ON sra_studies (published_at);
CREATE INDEX IF NOT EXISTS idx_sra_submissions_center ON sra_submissions (center_name);
