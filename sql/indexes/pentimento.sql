CREATE INDEX IF NOT EXISTS idx_sample_ncount_study   ON sample_ncount (study_accession);
CREATE INDEX IF NOT EXISTS idx_run_preflightx_study   ON run_preflightx (study_accession);
CREATE INDEX IF NOT EXISTS idx_run_preflightx_gsm     ON run_preflightx (gsm_accession);
CREATE INDEX IF NOT EXISTS idx_run_preflightx_sample  ON run_preflightx (sample_accession);
CREATE INDEX IF NOT EXISTS idx_run_preflightx_exp     ON run_preflightx (experiment_accession);
CREATE INDEX IF NOT EXISTS idx_microbe_quant_class    ON microbe_quant (class);
CREATE INDEX IF NOT EXISTS idx_microbe_quant_organism ON microbe_quant (organism);
CREATE INDEX IF NOT EXISTS idx_microbe_quant_viral    ON microbe_quant (is_validated_viral) WHERE is_validated_viral = 1;
CREATE INDEX IF NOT EXISTS idx_microbe_quant_bact     ON microbe_quant (is_bacterial_evidence) WHERE is_bacterial_evidence = 1;
CREATE INDEX IF NOT EXISTS idx_microbe_quant_bactval  ON microbe_quant (is_validated_bacterial) WHERE is_validated_bacterial = 1;
-- idx_sfp_field intentionally omitted: field_id is low-cardinality and the
-- predicate is broad.
--
CREATE INDEX IF NOT EXISTS idx_pfxprov_study  ON preflightx_provenance (study_accession);
CREATE INDEX IF NOT EXISTS idx_quantprov_stat ON quant_provenance (status);
-- sample is the leading PK column of a WITHOUT ROWID table. field_id is
-- low-cardinality.
CREATE INDEX IF NOT EXISTS idx_sfp_study ON sample_field_provenance (study_accession);
