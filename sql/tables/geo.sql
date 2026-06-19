CREATE TABLE IF NOT EXISTS geo_contributors (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    accession           TEXT NOT NULL,
    person              TEXT,
    department          TEXT,
    organization        TEXT,
    address_line        TEXT,
    address_city        TEXT,
    address_state       TEXT,
    address_zipcode     TEXT,
    address_country     TEXT
);

CREATE TABLE IF NOT EXISTS geo_series (
    accession            TEXT PRIMARY KEY,
    overall_design       TEXT,
    pubmed_id            TEXT CHECK (pubmed_id IS NULL OR json_valid(pubmed_id)),
    relation             TEXT CHECK (relation IS NULL OR json_valid(relation)),
    samples_ref          TEXT CHECK (samples_ref IS NULL OR json_valid(samples_ref)),
    published_at         DATE,
    updated_at           DATE,
    summary              TEXT,
    supplementary_data   TEXT CHECK (supplementary_data IS NULL OR json_valid(supplementary_data)),
    title                TEXT,
    series_type          TEXT CHECK (series_type IS NULL OR json_valid(series_type)),
    pubmed_id_pysradb    TEXT,
    pubmed_last_checked  DATE
);

CREATE TABLE IF NOT EXISTS geo_samples (
    accession               TEXT PRIMARY KEY,
    channel_count           INTEGER,
    channels                TEXT,
    description             TEXT,
    platform_ref            TEXT,
    published_at            DATE,
    updated_at              DATE,
    supplementary_data      TEXT,
    title                   TEXT,
    sample_type             TEXT,
    hybridization_protocol  TEXT,
    scan_protocol           TEXT
);

CREATE TABLE IF NOT EXISTS geo_platforms (
    accession       TEXT PRIMARY KEY,
    description     TEXT,
    title           TEXT,
    technology      TEXT,
    published_at    DATE,
    updated_at      DATE,
    organism        TEXT,
    manufacturer    TEXT,
    relation        TEXT
);
