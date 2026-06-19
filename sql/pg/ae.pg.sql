CREATE TABLE IF NOT EXISTS arrayexpress_experiments (
    accession                   TEXT PRIMARY KEY,
    title                       TEXT,
    organisms                   TEXT[],
    organism                    TEXT,
    description                 TEXT,
    study_type                  TEXT,
    release_date                DATE,
    submission_date             DATE,
    experimental_factors        TEXT[],
    experimental_factor_types   TEXT[],
    contact_name                TEXT,
    contact_email               TEXT,
    contact_affiliation         TEXT,
    pubmed_id                   TEXT,
    pubmed_id_pysradb           TEXT,
    pubmed_last_checked         DATE,
    protocol_names              TEXT[],
    protocol_types              TEXT[],
    attributes_json             JSONB,
    publication_title           TEXT,
    publication_authors         TEXT,
    publication_doi             TEXT,
    external_links              JSONB[],
    processed_data_files        JSONB[],
    experimental_factors_detail JSONB[],
    authors                     TEXT[],
    organizations               JSONB[],
    protocols_description       TEXT,
    assay_count                 INTEGER,
    technology                  TEXT,
    assay_by_molecule           TEXT,
    minseqe_score               JSONB
);

CREATE TABLE IF NOT EXISTS arrayexpress_samples (
    id                      SERIAL PRIMARY KEY,
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
    attributes_json         JSONB
);

ALTER TABLE arrayexpress_experiments ADD COLUMN IF NOT EXISTS tsv tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(title,'')), 'A') ||
    setweight(to_tsvector('english', coalesce(description,'')), 'B') ||
    setweight(to_tsvector('english', coalesce(protocols_description,'')), 'C') ||
    setweight(to_tsvector('english', coalesce(publication_title,'')), 'D')
) STORED;

CREATE INDEX IF NOT EXISTS ae_experiments_fts_idx ON arrayexpress_experiments USING GIN(tsv);
CREATE INDEX IF NOT EXISTS ae_experiments_accession_upper_idx ON arrayexpress_experiments (UPPER(accession));
CREATE INDEX IF NOT EXISTS ae_experiments_organisms_idx ON arrayexpress_experiments USING GIN(organisms);
CREATE INDEX IF NOT EXISTS ae_samples_experiment_idx ON arrayexpress_samples (experiment_accession);
CREATE INDEX IF NOT EXISTS ae_samples_ena_experiment_idx ON arrayexpress_samples (UPPER(ena_experiment));
CREATE INDEX IF NOT EXISTS ae_samples_ena_run_idx ON arrayexpress_samples (UPPER(ena_run));
CREATE INDEX IF NOT EXISTS ae_experiments_pubmed_idx ON arrayexpress_experiments (pubmed_id);
CREATE INDEX IF NOT EXISTS ae_experiments_external_links_idx ON arrayexpress_experiments USING GIN (external_links);
CREATE INDEX IF NOT EXISTS idx_ae_experiments_release_date ON arrayexpress_experiments (release_date);
