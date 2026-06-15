PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE INDEX IF NOT EXISTS idx_unified_source ON unified_studies(source)
