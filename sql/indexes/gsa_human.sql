CREATE INDEX IF NOT EXISTS idx_gsa_human_studies_project ON gsa_human_studies(project_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_human_studies_access ON gsa_human_studies(access);
CREATE INDEX IF NOT EXISTS idx_gsa_human_studies_study_type ON gsa_human_studies(study_type);
CREATE INDEX IF NOT EXISTS idx_gsa_human_studies_strategy ON gsa_human_studies(strategy);
CREATE INDEX IF NOT EXISTS idx_gsa_human_studies_release_time ON gsa_human_studies(release_time);

CREATE INDEX IF NOT EXISTS idx_gsa_human_indiv_study ON gsa_human_individuals(study_accession);

CREATE INDEX IF NOT EXISTS idx_gsa_human_samples_study ON gsa_human_samples(study_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_human_samples_individual ON gsa_human_samples(individual_accession);

CREATE INDEX IF NOT EXISTS idx_gsa_human_experiments_study ON gsa_human_experiments(study_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_human_experiments_sample ON gsa_human_experiments(sample_accession);

CREATE INDEX IF NOT EXISTS idx_gsa_human_runs_study ON gsa_human_runs(study_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_human_runs_experiment ON gsa_human_runs(experiment_accession);
CREATE INDEX IF NOT EXISTS idx_gsa_human_runs_sample ON gsa_human_runs(sample_accession);

-- run_accession is already covered by the (run_accession, file_ord) primary key.
CREATE INDEX IF NOT EXISTS idx_gsa_human_run_files_study ON gsa_human_run_files(study_accession);
