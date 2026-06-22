CREATE TABLE IF NOT EXISTS geo_series (
    accession            TEXT PRIMARY KEY,
    overall_design       TEXT,
    summary              TEXT,
    title                TEXT,
    pubmed_id            TEXT CHECK json_valid(pubmed_id),
    samples_ref          TEXT CHECK json_valid(samples_ref),
    series_type          TEXT CHECK json_valid(series_type),
    relation             TEXT CHECK json_valid(relation),
    supplementary_data   TEXT CHECK json_valid(supplementary_data),
    published_at         DATE,
    updated_at           DATE,
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
