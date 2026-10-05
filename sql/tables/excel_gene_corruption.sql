-- Staging schema for the fleet excel-gene-corruption scan (SQLite).
--
-- One row per supplementary file, not per study: a study can publish several
-- files (e.g. E-GEAD-332's separate .processed.zip and .raw.zip), and their
-- hit counts differ. `source` spans every archive the scan covers (GEO,
-- ArrayExpress, DDBJ GEA, GSA) under one table rather than a divergent JSON
-- column bolted onto each of geo_series/arrayexpress_experiments/gsa_studies/
-- gea_studies -- most of those don't even have a comparable column (GSA has
-- none at all), and a study with zero flagged files should read as "no
-- corrupted file found among what was scanned", not "not scanned"; that
-- distinction belongs in a separate scan-log table if it's ever needed, same
-- tri-state approach as gsa_fetch_log.

CREATE TABLE IF NOT EXISTS supplementary_file_gene_corruption (
    accession       TEXT NOT NULL,   -- study accession: GSE.../E-MTAB-.../E-GEAD-.../CRA...
    source          TEXT NOT NULL,   -- geo | arrayexpress | gea | gsa
    url             TEXT NOT NULL,
    n_hits          INTEGER NOT NULL,
    fmt             TEXT,
    rows_scanned    INTEGER,
    truncated       INTEGER,
    -- Corrupted-name counts by kind, e.g. {"date": 10, "sci": 2}.
    kinds           TEXT CHECK (kinds IS NULL OR json_valid(kinds)),
    -- Up to the top 10 distinct corrupted strings with their counts,
    -- e.g. [["1-Mar", 2], ["2-Sep", 1]] -- see recipes/excel_genes/merge_results.py.
    examples        TEXT CHECK (examples IS NULL OR json_valid(examples)),
    checked_at      DATE NOT NULL,
    PRIMARY KEY (accession, url)
);

CREATE INDEX IF NOT EXISTS idx_supp_file_gene_corruption_accession ON supplementary_file_gene_corruption (accession);
CREATE INDEX IF NOT EXISTS idx_supp_file_gene_corruption_source    ON supplementary_file_gene_corruption (source);
