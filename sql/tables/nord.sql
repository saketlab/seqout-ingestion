-- NORD rare-disease dimension table, SQLite staging.
-- One row per NORD entry with a MONDO name-match key. Keep separate from GARD:
-- membership and match quality differ.
CREATE TABLE IF NOT EXISTS nord_disease (
    -- NORD collection. 'rare-diseases' is curated NORD content; the other two
    -- mirror MONDO and GARD, so this stays facetable.
    nord_type     TEXT NOT NULL,
    -- slug alone is not unique. A disease can appear in two collections at
    -- once: trisomy-x is both a gard-rare-disease and a mondo-disease post.
    nord_slug     TEXT NOT NULL,
    nord_name     TEXT NOT NULL,
    nord_url      TEXT,
    mondo_id      TEXT,               -- NULL for the 983 entries no rule resolved
    mondo_label   TEXT,
    match_kind    TEXT,               -- 'mondo' or 'unmatched'; no finer grade is published
    mondo_rare    INTEGER,            -- 1 if in MONDO's #rare subset; NULL if unmapped
    synonyms      TEXT,               -- NORD's search terms, ';'-joined, verbatim
    fetched_at    TEXT NOT NULL,
    PRIMARY KEY (nord_type, nord_slug)
);
