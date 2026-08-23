--
--
-- mondo_id is not unique; NORD collections can map several rows to one MONDO term.
CREATE INDEX IF NOT EXISTS idx_nord_disease_mondo ON nord_disease (mondo_id);
