-- Keyed by (study_accession, sample) to match ontology_samples_v4's real PK: the
-- same sample id can appear under multiple studies with different ages, so
-- collapsing to sample alone would cross-contaminate them.
CREATE TABLE IF NOT EXISTS age_norm (
    study_accession TEXT NOT NULL,
    sample          TEXT NOT NULL,
    age_days        REAL,
    age_group       TEXT,
    prenatal        INTEGER,   -- 0/1 (SQLite has no boolean)
    age_raw         TEXT,
    PRIMARY KEY (study_accession, sample)
);
