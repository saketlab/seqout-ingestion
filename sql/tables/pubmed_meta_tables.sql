PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS pubmed_metadata (
    pmid                       TEXT PRIMARY KEY,
    title                      TEXT,
    journal                    TEXT,
    doi                        TEXT,
    pub_date                   TEXT,
    authors                    TEXT,
    issn                       TEXT,
    citation_count             INTEGER,
    journal_h_index            INTEGER,
    journal_i10_index          INTEGER,
    journal_2yr_mean_citedness REAL,
    journal_cited_by_count     INTEGER,
    journal_works_count        INTEGER
)
