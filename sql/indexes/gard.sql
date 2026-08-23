--
-- mondo_id is not unique; multiple GARD diseases can resolve to one MONDO term.
CREATE INDEX IF NOT EXISTS idx_gard_disease_mondo ON gard_disease (mondo_id);
