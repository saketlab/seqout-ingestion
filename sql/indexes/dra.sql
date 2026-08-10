CREATE INDEX IF NOT EXISTS idx_dra_samples_submission ON dra_samples(submission);
CREATE INDEX IF NOT EXISTS idx_dra_samples_scientific_name ON dra_samples(scientific_name);

CREATE INDEX IF NOT EXISTS idx_dra_submissions_published_at ON dra_submissions(published_at);

CREATE INDEX IF NOT EXISTS idx_dra_studies_study_type ON dra_studies(study_type);
CREATE INDEX IF NOT EXISTS idx_dra_studies_submission_type ON dra_studies(submission, study_type);

CREATE INDEX IF NOT EXISTS idx_dra_experiments_library_strategy ON dra_experiments(library_strategy);
CREATE INDEX IF NOT EXISTS idx_dra_experiments_submission_strategy ON dra_experiments(submission, library_strategy);

CREATE INDEX IF NOT EXISTS idx_dra_experiments_study ON dra_experiments(study);
CREATE INDEX IF NOT EXISTS idx_dra_runs_experiment ON dra_runs(experiment);

CREATE INDEX IF NOT EXISTS idx_dra_studies_is_only_ddbj ON dra_studies(is_only_ddbj);
