-- Staging schema for the fleet PMID retry sweep (SQLite).

CREATE TABLE IF NOT EXISTS pmid_retry_results (
    accession        TEXT PRIMARY KEY,
    source           TEXT NOT NULL,
    resolver         TEXT,
    pmids_json       TEXT NOT NULL,   
    pmid             TEXT,            
    n_pmids          INTEGER NOT NULL,
    error            TEXT,            
    pysradb_version  TEXT,
    elapsed_s        REAL,
    checked_at       TEXT,
    host             TEXT
);

CREATE INDEX IF NOT EXISTS idx_pmid_retry_source ON pmid_retry_results (source);
CREATE INDEX IF NOT EXISTS idx_pmid_retry_pmid   ON pmid_retry_results (pmid);

CREATE TABLE IF NOT EXISTS pmid_retry_requested (
    accession TEXT PRIMARY KEY,
    source    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pmid_retry_verification (
    accession       TEXT PRIMARY KEY,
    claimed_pmid    TEXT NOT NULL,
    -- archive_link and epmc_cites are the only promotable verdicts; pysradb
    -- keeps the smallest pmid a search returns, which is the oldest paper.
    --   archive_link  the archive returns the claimed pmid
    --   archive_other the archive returns pmids and the claim is absent
    --   epmc_cites    the archive is silent, the paper prints the accession
    --   unconfirmed   nothing vouches for the claim
    --   lookup_failed transport failure, safe to re-run
    verdict         TEXT NOT NULL,
    archive_pmids   TEXT,   -- json list the archive returned
    archive_route   TEXT,   -- which lookup answered
    epmc_pmids      TEXT,   -- json list of papers printing the accession
    verified_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pmid_retry_verdict
    ON pmid_retry_verification (verdict);
