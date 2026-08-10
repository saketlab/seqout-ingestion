
CREATE TABLE IF NOT EXISTS gea_experiments (
    accession                 TEXT PRIMARY KEY,
    title                     TEXT,
    organism                  TEXT,
    description               TEXT,
    study_type                TEXT,
    bioproject                TEXT,
    release_date              TEXT,
    submission_date           TEXT,
    updated_at                TEXT,
    experimental_factors      TEXT CHECK (experimental_factors IS NULL OR json_valid(experimental_factors)),
    experimental_factor_types TEXT CHECK (experimental_factor_types IS NULL OR json_valid(experimental_factor_types)),
    contact_name              TEXT,
    contact_email             TEXT,
    contact_affiliation       TEXT,
    pubmed_id                 TEXT,
    pubmed_id_pysradb         TEXT,
    pubmed_last_checked       DATE,
    protocol_names            TEXT CHECK (protocol_names IS NULL OR json_valid(protocol_names)),
    protocol_types            TEXT CHECK (protocol_types IS NULL OR json_valid(protocol_types)),
    protocols_description     TEXT CHECK (protocols_description IS NULL OR json_valid(protocols_description)),
    attributes_json           TEXT CHECK (attributes_json IS NULL OR json_valid(attributes_json)),
    data_files                TEXT CHECK (data_files IS NULL OR json_valid(data_files))
);

-- dra_experiment / dra_run come from Comment[SRA_EXPERIMENT] / Comment[SRA_RUN]:
-- DDBJ keeps the INSDC column names though the accessions are DRX/DRR. NULL for
-- microarray.
CREATE TABLE IF NOT EXISTS gea_samples (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_accession TEXT NOT NULL,
    source_name          TEXT,
    organism             TEXT,
    taxon_id             TEXT,
    organism_part        TEXT,
    tissue               TEXT,
    cell_type            TEXT,
    genotype             TEXT,
    strain               TEXT,
    sex                  TEXT,
    age                  TEXT,
    biosample            TEXT,
    sample_title         TEXT,
    sample_description   TEXT,
    library_strategy     TEXT,
    library_source       TEXT,
    library_selection    TEXT,
    library_layout       TEXT,
    instrument_model     TEXT,
    dra_experiment       TEXT,
    dra_run              TEXT,
    attributes_json      TEXT CHECK (attributes_json IS NULL OR json_valid(attributes_json))
);
