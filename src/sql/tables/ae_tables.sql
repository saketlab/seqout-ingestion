CREATE TABLE IF NOT EXISTS arrayexpress_experiments (
    accession                   TEXT PRIMARY KEY,
    title                       TEXT,
    organism                    TEXT,
    description                 TEXT,
    study_type                  TEXT,
    release_date                TEXT,
    submission_date             TEXT,
    experimental_factors        TEXT CHECK (experimental_factors IS NULL OR json_valid(experimental_factors)),
    experimental_factor_types   TEXT CHECK (experimental_factor_types IS NULL OR json_valid(experimental_factor_types)),
    contact_name                TEXT,
    contact_email               TEXT,
    contact_affiliation         TEXT,
    pubmed_id                   TEXT,
    protocol_names              TEXT CHECK (protocol_names IS NULL OR json_valid(protocol_names)),
    protocol_types              TEXT CHECK (protocol_types IS NULL OR json_valid(protocol_types)),
    attributes_json             TEXT CHECK (attributes_json IS NULL OR json_valid(attributes_json)),
    publication_title           TEXT,
    publication_authors         TEXT,
    publication_doi             TEXT,
    external_links              TEXT CHECK (external_links IS NULL OR json_valid(external_links)),
    processed_data_files        TEXT CHECK (processed_data_files IS NULL OR json_valid(processed_data_files)),
    experimental_factors_detail TEXT CHECK (experimental_factors_detail IS NULL OR json_valid(experimental_factors_detail)),
    authors                     TEXT CHECK (authors IS NULL OR json_valid(authors)),
    organizations               TEXT CHECK (organizations IS NULL OR json_valid(organizations)),
    protocols_description       TEXT,
    assay_count                 INTEGER,
    technology                  TEXT,
    assay_by_molecule           TEXT,
    minseqe_score               TEXT CHECK (minseqe_score IS NULL OR json_valid(minseqe_score))
);

CREATE TABLE IF NOT EXISTS arrayexpress_samples (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_accession    TEXT NOT NULL,
    source_name             TEXT,
    organism                TEXT,
    organism_part           TEXT,
    cell_type               TEXT,
    genotype                TEXT,
    sample_source_name      TEXT,
    sample_title            TEXT,
    sample_description      TEXT,
    library_strategy        TEXT,
    library_source          TEXT,
    library_selection       TEXT,
    ena_experiment          TEXT,
    ena_run                 TEXT,
    attributes_json         TEXT CHECK (attributes_json IS NULL OR json_valid(attributes_json))
);

CREATE VIRTUAL TABLE IF NOT EXISTS arrayexpress_experiments_fts USING fts5(title, description, attributes_json, content=arrayexpress_experiments, content_rowid=rowid);

CREATE VIRTUAL TABLE IF NOT EXISTS arrayexpress_samples_fts USING fts5(attributes_json, content=arrayexpress_samples, content_rowid=id);
