-- Shaped like geo_series; publications/funding/pubmed_id are JSON text.
CREATE TABLE IF NOT EXISTS gsa_projects (
    accession            TEXT PRIMARY KEY,   -- PRJCA...
    title                TEXT,
    summary              TEXT,               -- Description
    relevance            TEXT,               -- e.g. Medical
    data_type            TEXT,               -- e.g. Single cell sequencing
    organism             TEXT,
    sample_scope         TEXT,               -- e.g. Single cell
    pubmed_id            TEXT,               -- JSON list of PMIDs
    publications         TEXT,               -- JSON list {pmid,title,journal,doi,year}
    funding              TEXT,               -- JSON list {agency,program,grant_id,grant_title}
    submitter            TEXT,
    submitter_email      TEXT,
    submitting_org       TEXT,
    published_at         DATE,               -- Release date
    submitted_at         DATE,               -- Submission date
    pubmed_id_pysradb    TEXT,               -- reserved for backfill; NULL in v1
    pubmed_last_checked  DATE                -- reserved for backfill; NULL in v1
);

CREATE TABLE IF NOT EXISTS gsa_bioproject_fetch_log (
    accession   TEXT PRIMARY KEY,            -- PRJCA...
    status      TEXT NOT NULL,               -- ok | empty | failed | no-english
    fetched_at  DATE NOT NULL
);
