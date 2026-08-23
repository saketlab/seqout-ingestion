-- The rollup requires a study_accession index.
CREATE INDEX IF NOT EXISTS idx_sce_study ON single_cell_evidence(study_accession);
CREATE INDEX IF NOT EXISTS idx_sce_rule ON single_cell_evidence(rule_id);
CREATE INDEX IF NOT EXISTS idx_sce_grain ON single_cell_evidence(grain);
--
-- Rule-pair coverage uses (rule_id, study_accession).
CREATE INDEX IF NOT EXISTS idx_sce_rule_study ON single_cell_evidence(rule_id, study_accession);
CREATE INDEX IF NOT EXISTS idx_sce_assay_modality ON single_cell_evidence(assay_modality);
CREATE INDEX IF NOT EXISTS idx_sce_analyte ON single_cell_evidence(analyte);
CREATE INDEX IF NOT EXISTS idx_sce_libstrategy ON single_cell_evidence(library_strategy);
CREATE INDEX IF NOT EXISTS idx_sc_rule_tier ON sc_rule(tier);
