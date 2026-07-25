CREATE INDEX IF NOT EXISTS idx_ena_studies_scientific_name ON ena_studies(scientific_name);
CREATE INDEX IF NOT EXISTS idx_ena_studies_first_public ON ena_studies(first_public);
CREATE INDEX IF NOT EXISTS idx_ena_studies_geo_accession ON ena_studies(geo_accession);
CREATE INDEX IF NOT EXISTS idx_ena_studies_tax_id ON ena_studies(tax_id);
CREATE INDEX IF NOT EXISTS idx_ena_studies_organisms_gin ON ena_studies USING gin(organisms);

CREATE INDEX IF NOT EXISTS idx_ena_exp_experiment ON ena_experiments(experiment_accession);
CREATE INDEX IF NOT EXISTS idx_ena_exp_study ON ena_experiments(study_accession);
CREATE INDEX IF NOT EXISTS idx_ena_exp_sample ON ena_experiments(sample_accession);
CREATE INDEX IF NOT EXISTS idx_ena_exp_platform ON ena_experiments(instrument_platform);
CREATE INDEX IF NOT EXISTS idx_ena_exp_strategy ON ena_experiments(library_strategy);
CREATE INDEX IF NOT EXISTS idx_ena_exp_scientific_name ON ena_experiments(scientific_name);
CREATE INDEX IF NOT EXISTS idx_ena_exp_first_public ON ena_experiments(first_public);

CREATE INDEX IF NOT EXISTS idx_ena_samples_study ON ena_samples(study_accession);
CREATE INDEX IF NOT EXISTS idx_ena_samples_scientific_name ON ena_samples(scientific_name);
CREATE INDEX IF NOT EXISTS idx_ena_samples_country ON ena_samples(country);
CREATE INDEX IF NOT EXISTS idx_ena_samples_disease ON ena_samples(disease);
CREATE INDEX IF NOT EXISTS idx_ena_samples_tissue_type ON ena_samples(tissue_type);
CREATE INDEX IF NOT EXISTS idx_ena_samples_cell_type ON ena_samples(cell_type);
CREATE INDEX IF NOT EXISTS idx_ena_samples_host ON ena_samples(host);
CREATE INDEX IF NOT EXISTS idx_ena_samples_first_public ON ena_samples(first_public);
CREATE INDEX IF NOT EXISTS idx_ena_samples_tax_id ON ena_samples(tax_id);
