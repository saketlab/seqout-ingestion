CREATE TABLE IF NOT EXISTS geo_contributors (
    id                  SERIAL PRIMARY KEY,
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
    pubmed_id            TEXT[],
    relation             JSONB[],
    samples_ref          TEXT[],
    published_at         DATE,
    updated_at           DATE,
    summary              TEXT,
    supplementary_data   JSONB[],
    title                TEXT,
    series_type          TEXT[],
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

CREATE INDEX IF NOT EXISTS idx_geo_contributors_country ON geo_contributors (address_country);
CREATE INDEX IF NOT EXISTS idx_geo_series_published_at ON geo_series (published_at);
CREATE INDEX IF NOT EXISTS idx_geo_samples_published_at ON geo_samples (published_at);
