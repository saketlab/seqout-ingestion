CREATE INDEX IF NOT EXISTS idx_gsa_studies_project ON gsa_studies(project_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_studies_release_time ON gsa_studies(release_time);

CREATE INDEX IF NOT EXISTS idx_gsa_samples_study ON gsa_samples(study_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_samples_organism ON gsa_samples(organism);
CREATE INDEX IF NOT EXISTS idx_gsa_samples_project ON gsa_samples(project_accession);

CREATE INDEX IF NOT EXISTS idx_gsa_exp_study ON gsa_experiments(study_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_exp_sample ON gsa_experiments(sample_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_exp_strategy ON gsa_experiments(library_strategy);
CREATE INDEX IF NOT EXISTS idx_gsa_exp_platform ON gsa_experiments(platform);

CREATE INDEX IF NOT EXISTS idx_gsa_runs_study ON gsa_runs(study_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_runs_experiment ON gsa_runs(experiment_accession);
