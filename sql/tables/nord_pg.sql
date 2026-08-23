-- NORD dimension table for Postgres.
-- synonyms become text[]; mondo_rare becomes boolean. Corpus joins use
-- ontology_samples_v4.disease_ontology_id plus MONDO closure in server queries.
CREATE TABLE IF NOT EXISTS nord_disease (
    nord_type     TEXT NOT NULL,
    nord_slug     TEXT NOT NULL,
    nord_name     TEXT NOT NULL,
    nord_url      TEXT,
    mondo_id      TEXT,
    mondo_label   TEXT,
    match_kind    TEXT,
    mondo_rare    BOOLEAN,
    synonyms      TEXT[] NOT NULL DEFAULT '{}',
    fetched_at    TIMESTAMPTZ,
    PRIMARY KEY (nord_type, nord_slug)
);

CREATE INDEX IF NOT EXISTS idx_nord_disease_mondo ON nord_disease (mondo_id);
CREATE INDEX IF NOT EXISTS idx_nord_disease_type ON nord_disease (nord_type);
CREATE INDEX IF NOT EXISTS idx_nord_disease_rare ON nord_disease (mondo_rare);
CREATE INDEX IF NOT EXISTS idx_nord_disease_name ON nord_disease (lower(nord_name));
