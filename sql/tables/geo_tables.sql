PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS geo_contributors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession TEXT NOT NULL,
    person TEXT,
    department TEXT,
    organization TEXT,
    address_line TEXT,
    address_city TEXT,
    address_state TEXT,
    address_zipcode TEXT,
    address_country TEXT
);

CREATE TABLE IF NOT EXISTS geo_series (
    accession TEXT PRIMARY KEY,
    overall_design TEXT,
    pubmed_id TEXT,
    relation TEXT,
    samples_ref TEXT,
    published_at DATE,
    updated_at DATE,
    summary TEXT,
    supplementary_data TEXT,
    title TEXT,
    series_type TEXT
);

CREATE TABLE IF NOT EXISTS geo_samples (
    accession TEXT PRIMARY KEY,
    channel_count INTEGER,
    channels TEXT,
    description TEXT,
    platform_ref TEXT,
    published_at DATE,
    updated_at DATE,
    supplementary_data TEXT,
    title TEXT,
    sample_type TEXT,
    hybridization_protocol TEXT,
    scan_protocol TEXT
);

CREATE TABLE IF NOT EXISTS geo_platforms (
    accession TEXT PRIMARY KEY,
    description TEXT,
    title TEXT,
    technology TEXT,
    published_at DATE,
    updated_at DATE,
    organism TEXT,
    manufacturer TEXT,
    relation TEXT
);
