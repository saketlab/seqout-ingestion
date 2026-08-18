CREATE INDEX IF NOT EXISTS idx_gea_samples_experiment ON gea_samples(experiment_accession);
CREATE INDEX IF NOT EXISTS idx_gea_samples_organism ON gea_samples(organism);
CREATE INDEX IF NOT EXISTS idx_gea_samples_dra_run ON gea_samples(dra_run);

CREATE INDEX IF NOT EXISTS idx_gea_experiments_bioproject ON gea_experiments(bioproject);
CREATE INDEX IF NOT EXISTS idx_gea_experiments_study_type ON gea_experiments(study_type);
