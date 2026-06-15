PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS unified_studies (
    canonical_accession TEXT PRIMARY KEY,
    source              TEXT NOT NULL,
    title               TEXT,
    description         TEXT,
    aliases             TEXT CHECK (aliases IS NULL OR json_valid(aliases))
)
