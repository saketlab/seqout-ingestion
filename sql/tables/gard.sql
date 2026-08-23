-- GARD rare-disease dimension table, SQLite staging.
-- One row per GARD disease with its MONDO xref key; corpus joins happen in
-- Postgres. GARD ids resolve to MONDO by exact xref.
CREATE TABLE IF NOT EXISTS gard_disease (
    gard_id       TEXT PRIMARY KEY,   -- 'GARD:5658', zero-stripped from MONDO's padding
    gard_name     TEXT NOT NULL,
    mondo_id      TEXT,               -- NULL for 9 GARD terms lacking MONDO xref
    mondo_label   TEXT,
    mondo_rare    INTEGER,            -- 1 if in MONDO's #rare subset; NULL if unmapped
    -- GARD's facets, ';'-joined; Postgres splits these into text[] (gard_pg.sql).
    -- tagsCause is empty in the catalogue, so inheritance is scraped separately.
    categories    TEXT,               -- tagsDiseaseCategory
    specialties   TEXT,               -- tagsSpecialist
    groups        TEXT,               -- tagsAccount
    synonyms      TEXT,               -- GARD's own synonyms, ';'-joined, verbatim
    -- Scraped from per-disease pages by gard/inheritance.py. ';'-joined because
    -- a disease can cite multiple modes, e.g. ARVC is dominant and recessive.
    inheritance   TEXT,
    inheritance_fetched_at TEXT,
    fetched_at    TEXT NOT NULL
);
