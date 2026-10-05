-- Ontology closure, term labels and Beacon filtering-terms cache, Postgres.
-- All three are rebuilt whole by `transfer ontology-closure` (TRUNCATE+COPY,
-- not DROP+CREATE), so this file is applied idempotently on every run and its
-- indexes are never at risk of being dropped along with the data.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS ontology_closure (
    ontology TEXT NOT NULL,
    ancestor_id TEXT NOT NULL,
    descendant_id TEXT NOT NULL,
    PRIMARY KEY (ontology, ancestor_id, descendant_id)
);
CREATE INDEX IF NOT EXISTS idx_closure_desc ON ontology_closure (ontology, descendant_id);

-- resolve_ontology_term (pysradb-server/queries.py) normalizes a label the same
-- way for an exact match (idx_ontology_terms_label_norm) and a substring
-- fallback (idx_ontology_terms_label_trgm); keep the expression identical to
-- that function or the indexes stop being usable by it.
CREATE TABLE IF NOT EXISTS ontology_terms (
    ontology TEXT NOT NULL,
    id TEXT NOT NULL,
    label TEXT,
    PRIMARY KEY (ontology, id)
);
CREATE INDEX IF NOT EXISTS idx_ontology_terms_label_norm ON ontology_terms
    (ontology, (regexp_replace(lower(label), '[^a-z0-9]+', '', 'g')));
CREATE INDEX IF NOT EXISTS idx_ontology_terms_label_trgm ON ontology_terms
    USING gin ((regexp_replace(lower(label), '[^a-z0-9]+', '', 'g')) gin_trgm_ops);

CREATE TABLE IF NOT EXISTS beacon_filtering_terms (
    term_type TEXT NOT NULL,
    id TEXT PRIMARY KEY,
    label TEXT,
    scopes TEXT[] NOT NULL,
    vals TEXT[]
);
