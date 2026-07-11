-- Beacon age-range filters need a btree on age_days. CREATE CONCURRENTLY so the
-- 13.6M-row ontology_samples_v4 stays writable during the build; must run on an
-- autocommit connection (transfer age runs this outside a txn after the load,
-- dropping any leftover invalid index from an interrupted prior build first).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ontology_v4_age_days
    ON ontology_samples_v4 (age_days);
