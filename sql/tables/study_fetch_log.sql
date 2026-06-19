CREATE TABLE IF NOT EXISTS study_fetch_log (
    study_accession TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    n_runs          INTEGER,
    fetched_at      DATE NOT NULL,
    ncbi_status     TEXT,
    ncbi_n_runs     INTEGER,
    ncbi_fetched_at TEXT
);
