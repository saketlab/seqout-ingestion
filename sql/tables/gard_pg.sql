-- GARD dimension table for Postgres.
-- Tags become text[] for GIN facets; mondo_rare becomes boolean. Corpus joins
-- use ontology_samples_v4.disease_ontology_id plus MONDO closure in server
-- queries.
CREATE TABLE IF NOT EXISTS gard_disease (
    gard_id       TEXT PRIMARY KEY,
    gard_name     TEXT NOT NULL,
    mondo_id      TEXT,
    mondo_label   TEXT,
    mondo_rare    BOOLEAN,
    categories    TEXT[] NOT NULL DEFAULT '{}',
    specialties   TEXT[] NOT NULL DEFAULT '{}',
    groups        TEXT[] NOT NULL DEFAULT '{}',
    synonyms      TEXT[] NOT NULL DEFAULT '{}',
    inheritance   TEXT[] NOT NULL DEFAULT '{}',
    inheritance_fetched_at TIMESTAMPTZ,
    fetched_at    TIMESTAMPTZ
);

-- Additive migration for existing installs; IF NOT EXISTS keeps it idempotent.
--
ALTER TABLE gard_disease ADD COLUMN IF NOT EXISTS inheritance TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE gard_disease ADD COLUMN IF NOT EXISTS inheritance_fetched_at TIMESTAMPTZ;
--

-- mondo_id is not unique; multiple GARD diseases can resolve to one MONDO term.
CREATE INDEX IF NOT EXISTS idx_gard_disease_mondo ON gard_disease (mondo_id);
CREATE INDEX IF NOT EXISTS idx_gard_disease_rare ON gard_disease (mondo_rare);
CREATE INDEX IF NOT EXISTS idx_gard_disease_categories ON gard_disease USING gin (categories);
CREATE INDEX IF NOT EXISTS idx_gard_disease_specialties ON gard_disease USING gin (specialties);
CREATE INDEX IF NOT EXISTS idx_gard_disease_name_trgm ON gard_disease (lower(gard_name));
CREATE INDEX IF NOT EXISTS idx_gard_disease_inheritance
    ON gard_disease USING gin (inheritance);
