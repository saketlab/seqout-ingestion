-- Disease inheritance table for Postgres. ';'-joined columns become text[] for
-- GIN facets.
CREATE TABLE IF NOT EXISTS disease_inheritance (
    mondo_id    TEXT PRIMARY KEY,
    modes       TEXT[] NOT NULL DEFAULT '{}',
    hpo_ids     TEXT[] NOT NULL DEFAULT '{}',
    omim_ids    TEXT[] NOT NULL DEFAULT '{}',
    orpha_ids   TEXT[] NOT NULL DEFAULT '{}',
    sources     TEXT[] NOT NULL DEFAULT '{}',
    hpoa_version TEXT,
    hpo_version  TEXT,
    orphanet_version TEXT,
    fetched_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_disease_inheritance_modes
    ON disease_inheritance USING gin (modes);

-- Additive migration for installs created before Orphanet source. IF NOT EXISTS
-- keeps this idempotent; existing rows stay intact.
ALTER TABLE disease_inheritance ADD COLUMN IF NOT EXISTS orpha_ids TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE disease_inheritance ADD COLUMN IF NOT EXISTS sources TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE disease_inheritance ADD COLUMN IF NOT EXISTS orphanet_version TEXT;
