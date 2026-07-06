CREATE INDEX IF NOT EXISTS idx_samples_submission ON sra_samples(submission);
CREATE INDEX IF NOT EXISTS idx_samples_scientific_name ON sra_samples(scientific_name);

CREATE INDEX IF NOT EXISTS idx_submissions_published_at ON sra_submissions(published_at);

CREATE INDEX IF NOT EXISTS idx_studies_accession ON sra_studies(accession);
CREATE INDEX IF NOT EXISTS idx_studies_study_type ON sra_studies(study_type);
CREATE INDEX IF NOT EXISTS idx_studies_submission_type ON sra_studies(submission, study_type);

CREATE INDEX IF NOT EXISTS idx_experiments_library_strategy ON sra_experiments(library_strategy);
CREATE INDEX IF NOT EXISTS idx_experiments_submission_strategy ON sra_experiments(submission, library_strategy);
CREATE INDEX IF NOT EXISTS idx_experiments_samples ON sra_experiments(samples);
