-- MAGE-TAB like ArrayExpress: mirrors arrayexpress_*, tsvector on the experiment table.
-- No is_only_ddbj -- nothing in GEA is mirrored anywhere.

CREATE TABLE IF NOT EXISTS gea_experiments (
    accession                 TEXT PRIMARY KEY,
    title                     TEXT,
    organism                  TEXT,
    description               TEXT,
    study_type                TEXT,
    bioproject                TEXT,
    release_date              DATE,
    submission_date           DATE,
    updated_at                DATE,
    experimental_factors      TEXT[],
    experimental_factor_types TEXT[],
    contact_name              TEXT,
    contact_email             TEXT,
    contact_affiliation       TEXT,
    pubmed_id                 TEXT,
    pubmed_id_pysradb         TEXT,
    pubmed_last_checked       DATE,
    protocol_names            TEXT[],
    protocol_types            TEXT[],
    protocols_description     JSONB,
    attributes_json           JSONB,
    data_files                JSONB,
    tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')),       'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(organism, '')),    'C') ||
        setweight(to_tsvector('english', coalesce(study_type, '')),  'C')
    ) STORED
);

CREATE TABLE IF NOT EXISTS gea_samples (
    id                   BIGINT PRIMARY KEY,
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
    attributes_json      JSONB
);

CREATE INDEX IF NOT EXISTS gea_experiments_fts_idx ON gea_experiments USING GIN (tsv);
CREATE INDEX IF NOT EXISTS gea_experiments_accession_upper_idx
    ON gea_experiments (UPPER(accession));
CREATE INDEX IF NOT EXISTS idx_gea_experiments_bioproject
    ON gea_experiments (bioproject);
CREATE INDEX IF NOT EXISTS idx_gea_experiments_release_date
    ON gea_experiments (release_date);
CREATE INDEX IF NOT EXISTS idx_gea_samples_experiment
    ON gea_samples (experiment_accession);
CREATE INDEX IF NOT EXISTS idx_gea_samples_organism ON gea_samples (organism);
