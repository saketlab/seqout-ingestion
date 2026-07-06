CREATE INDEX IF NOT EXISTS idx_ae_samples_experiment ON arrayexpress_samples(experiment_accession);
CREATE INDEX IF NOT EXISTS idx_ae_experiments_organism ON arrayexpress_experiments(organism);
CREATE INDEX IF NOT EXISTS idx_ae_experiments_release_date ON arrayexpress_experiments(release_date);
