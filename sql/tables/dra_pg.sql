
CREATE TABLE IF NOT EXISTS dra_submissions (
    accession    TEXT PRIMARY KEY,
    alias        TEXT,
    broker_name  TEXT,
    center_name  TEXT,
    lab_name     TEXT,
    published_at DATE,
    updated_at   DATE
);

CREATE TABLE IF NOT EXISTS dra_studies (
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
    bioproject_id       TEXT,
    submission          TEXT,
    published_at        DATE,
    updated_at          DATE,
    pubmed_id_pysradb   TEXT,
    pubmed_last_checked DATE,
    -- 1 = DDBJ holds the only copy; 0 = mirrored into SRA/ENA; NULL = undetermined.
    -- The growth charts split on this. The three states must stay distinct.
    is_only_ddbj        SMALLINT
);

CREATE TABLE IF NOT EXISTS dra_experiments (
    accession          TEXT PRIMARY KEY,
    design_description TEXT,
    library_layout     TEXT,
    library_name       TEXT,
    library_selection  TEXT,
    library_source     TEXT,
    library_strategy   TEXT,
    samples            TEXT[],
    platform           TEXT,
    instrument_model   TEXT,
    study              TEXT,
    title              TEXT,
    submission         TEXT
);

CREATE TABLE IF NOT EXISTS dra_samples (
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

CREATE TABLE IF NOT EXISTS dra_runs (
    accession  TEXT PRIMARY KEY,
    experiment TEXT,
    submission TEXT
);

-- Full-text twin, mirroring gsa_studies_search. is_only_ddbj = 1 only: a
-- mirrored DRP is the same study as its SRA/ENA twin and would duplicate hits.
CREATE TABLE IF NOT EXISTS dra_studies_search (
    accession         TEXT PRIMARY KEY,
    study_title       TEXT,
    study_description TEXT,
    study_name        TEXT,
    center_name       TEXT,
    project_name      TEXT,
    last_updated      DATE,
    first_public      DATE,
    tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(study_title, '')),       'A') ||
        setweight(to_tsvector('english', coalesce(study_description, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(study_name, '')),        'C') ||
        setweight(to_tsvector('english', coalesce(project_name, '')),      'C')
    ) STORED
);
CREATE INDEX IF NOT EXISTS dra_studies_search_fts_idx
    ON dra_studies_search USING GIN (tsv);
CREATE INDEX IF NOT EXISTS dra_studies_search_accession_upper_idx
    ON dra_studies_search (UPPER(accession));

CREATE INDEX IF NOT EXISTS idx_dra_studies_is_only_ddbj ON dra_studies (is_only_ddbj);
CREATE INDEX IF NOT EXISTS idx_dra_studies_published_at ON dra_studies (published_at);
CREATE INDEX IF NOT EXISTS idx_dra_studies_bioproject ON dra_studies (bioproject_id);
CREATE INDEX IF NOT EXISTS idx_dra_studies_submission ON dra_studies (submission);
CREATE INDEX IF NOT EXISTS idx_dra_experiments_study ON dra_experiments (study);
CREATE INDEX IF NOT EXISTS idx_dra_experiments_submission ON dra_experiments (submission);
-- samples @> ARRAY[...] full-scans 627k rows (280ms) without this.
CREATE INDEX IF NOT EXISTS idx_dra_experiments_samples ON dra_experiments USING GIN (samples);
CREATE INDEX IF NOT EXISTS idx_dra_samples_submission ON dra_samples (submission);
CREATE INDEX IF NOT EXISTS idx_dra_runs_experiment ON dra_runs (experiment);
CREATE INDEX IF NOT EXISTS idx_dra_runs_submission ON dra_runs (submission);
