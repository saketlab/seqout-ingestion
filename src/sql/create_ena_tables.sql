PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS ena_studies (
    study_accession             TEXT PRIMARY KEY,
    secondary_study_accession   TEXT,
    secondary_study_alias       TEXT,
    secondary_study_center_name TEXT,
    study_alias                 TEXT,
    study_title                 TEXT,
    study_description           TEXT,
    study_name                  TEXT,
    center_name                 TEXT,
    broker_name                 TEXT,
    project_name                TEXT,
    tax_id                      INTEGER,
    tax_lineage                 TEXT,
    tax_division                TEXT,
    scientific_name             TEXT,
    strain                      TEXT,
    breed                       TEXT,
    cultivar                    TEXT,
    isolate                     TEXT,
    first_public                TEXT,
    last_updated                TEXT,
    geo_accession               TEXT,
    keywords                    TEXT,
    tag                         TEXT,
    submission_tool             TEXT,
    parent_study_accession      TEXT,
    description                 TEXT,
    status                      TEXT
);

CREATE INDEX IF NOT EXISTS idx_ena_studies_scientific_name ON ena_studies(scientific_name);
CREATE INDEX IF NOT EXISTS idx_ena_studies_first_public ON ena_studies(first_public);
CREATE INDEX IF NOT EXISTS idx_ena_studies_geo_accession ON ena_studies(geo_accession);
CREATE INDEX IF NOT EXISTS idx_ena_studies_tax_id ON ena_studies(tax_id);

CREATE TABLE IF NOT EXISTS ena_experiments (
    run_accession               TEXT PRIMARY KEY,
    run_alias                   TEXT,
    experiment_accession        TEXT NOT NULL,
    sample_accession            TEXT,
    study_accession             TEXT NOT NULL,
    experiment_title            TEXT,
    library_strategy            TEXT,
    library_source              TEXT,
    library_selection           TEXT,
    library_layout              TEXT,
    library_name                TEXT,
    instrument_platform         TEXT,
    instrument_model            TEXT,
    scientific_name             TEXT,
    tax_id                      INTEGER,
    center_name                 TEXT,
    first_public                TEXT,
    last_updated                TEXT,
    sample_title                TEXT,
    sample_alias                TEXT,
    sample_description          TEXT,
    country                     TEXT,
    host                        TEXT,
    tissue_type                 TEXT,
    cell_type                   TEXT,
    disease                     TEXT,
    dev_stage                   TEXT,
    base_count                  INTEGER,
    read_count                  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_ena_exp_experiment ON ena_experiments(experiment_accession);
CREATE INDEX IF NOT EXISTS idx_ena_exp_study ON ena_experiments(study_accession);
CREATE INDEX IF NOT EXISTS idx_ena_exp_sample ON ena_experiments(sample_accession);
CREATE INDEX IF NOT EXISTS idx_ena_exp_platform ON ena_experiments(instrument_platform);
CREATE INDEX IF NOT EXISTS idx_ena_exp_strategy ON ena_experiments(library_strategy);
CREATE INDEX IF NOT EXISTS idx_ena_exp_scientific_name ON ena_experiments(scientific_name);
CREATE INDEX IF NOT EXISTS idx_ena_exp_first_public ON ena_experiments(first_public);

CREATE TABLE IF NOT EXISTS ena_samples (
    sample_accession            TEXT PRIMARY KEY,
    secondary_sample_accession  TEXT,
    sample_alias                TEXT,
    sample_title                TEXT,
    sample_description          TEXT,
    tax_id                      INTEGER,
    scientific_name             TEXT,
    study_accession             TEXT NOT NULL,
    center_name                 TEXT,
    broker_name                 TEXT,
    first_public                TEXT,
    last_updated                TEXT,
    submission_accession        TEXT,
    submission_tool             TEXT,
    status                      TEXT,
    project_name                TEXT,
    description                 TEXT,
    checklist                   TEXT,
    ncbi_reporting_standard     TEXT,
    keywords                    TEXT,
    tag                         TEXT,
    datahub                     TEXT,
    -- organism details
    strain                      TEXT,
    cultivar                    TEXT,
    ecotype                     TEXT,
    isolate                     TEXT,
    isolation_source            TEXT,
    bio_material                TEXT,
    culture_collection          TEXT,
    specimen_voucher            TEXT,
    sub_species                 TEXT,
    sub_strain                  TEXT,
    variety                     TEXT,
    mating_type                 TEXT,
    sex                         TEXT,
    germline                    TEXT,
    -- sampling / collection
    collection_date             TEXT,
    collection_date_start       TEXT,
    collection_date_end         TEXT,
    collected_by                TEXT,
    identified_by               TEXT,
    country                     TEXT,
    lat                         TEXT,
    lon                         TEXT,
    location                    TEXT,
    location_start              TEXT,
    location_end                TEXT,
    altitude                    TEXT,
    depth                       TEXT,
    elevation                   TEXT,
    marine_region               TEXT,
    sampling_campaign           TEXT,
    sampling_platform           TEXT,
    sampling_site               TEXT,
    sample_collection           TEXT,
    sample_material             TEXT,
    sample_capture_status       TEXT,
    protocol_label              TEXT,
    -- environmental
    environmental_sample        TEXT,
    environment_biome           TEXT,
    environment_feature         TEXT,
    environment_material        TEXT,
    broad_scale_environmental_context TEXT,
    local_environmental_context TEXT,
    environmental_medium        TEXT,
    investigation_type          TEXT,
    ph                          TEXT,
    salinity                    TEXT,
    temperature                 TEXT,
    -- host
    host                        TEXT,
    host_body_site              TEXT,
    host_genotype               TEXT,
    host_gravidity              TEXT,
    host_growth_conditions      TEXT,
    host_phenotype              TEXT,
    host_scientific_name        TEXT,
    host_sex                    TEXT,
    host_status                 TEXT,
    host_tax_id                 TEXT,
    submitted_host_sex          TEXT,
    -- biological features
    cell_line                   TEXT,
    cell_type                   TEXT,
    dev_stage                   TEXT,
    disease                     TEXT,
    tissue_lib                  TEXT,
    tissue_type                 TEXT,
    age                         TEXT,
    experimental_factor         TEXT,
    -- surveillance / clinical
    serotype                    TEXT,
    serovar                     TEXT,
    surveillance_target         TEXT,
    target_gene                 TEXT,
    -- assembly / mag
    assembly_quality            TEXT,
    assembly_software           TEXT,
    binning_software            TEXT,
    completeness_score          TEXT,
    contamination_score         TEXT,
    sequencing_method           TEXT,
    -- taxonomy
    tax_lineage                 TEXT,
    taxonomic_classification    TEXT,
    taxonomic_identity_marker   TEXT,
    related_sample_accession    TEXT
);

CREATE INDEX IF NOT EXISTS idx_ena_samples_study ON ena_samples(study_accession);
CREATE INDEX IF NOT EXISTS idx_ena_samples_scientific_name ON ena_samples(scientific_name);
CREATE INDEX IF NOT EXISTS idx_ena_samples_country ON ena_samples(country);
CREATE INDEX IF NOT EXISTS idx_ena_samples_disease ON ena_samples(disease);
CREATE INDEX IF NOT EXISTS idx_ena_samples_tissue_type ON ena_samples(tissue_type);
CREATE INDEX IF NOT EXISTS idx_ena_samples_cell_type ON ena_samples(cell_type);
CREATE INDEX IF NOT EXISTS idx_ena_samples_host ON ena_samples(host);
CREATE INDEX IF NOT EXISTS idx_ena_samples_first_public ON ena_samples(first_public);
CREATE INDEX IF NOT EXISTS idx_ena_samples_tax_id ON ena_samples(tax_id);

CREATE VIRTUAL TABLE IF NOT EXISTS ena_studies_fts
    USING fts5(study_title, study_description, content=ena_studies, content_rowid=rowid);

CREATE VIRTUAL TABLE IF NOT EXISTS ena_experiments_fts
    USING fts5(experiment_title, sample_description, content=ena_experiments, content_rowid=rowid);

CREATE VIRTUAL TABLE IF NOT EXISTS ena_samples_fts
    USING fts5(sample_title, sample_description, content=ena_samples, content_rowid=rowid);
