-- CONCURRENTLY keeps the 13.6M-row ontology_samples_v4 writable during the
-- build, and requires an autocommit connection.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ontology_v4_age_days
    ON ontology_samples_v4 (age_days);
