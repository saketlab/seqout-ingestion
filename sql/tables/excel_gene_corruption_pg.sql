-- Postgres schema for the fleet excel-gene-corruption scan. Column-for-column
-- twin of sql/tables/excel_gene_corruption.sql, so the transfer's column
-- lists stay in step with both.
--
-- Grain is one row per supplementary file (see the staging twin for why).

CREATE TABLE IF NOT EXISTS supplementary_file_gene_corruption (
    accession       TEXT NOT NULL,
    source          TEXT NOT NULL,
    url             TEXT NOT NULL,
    n_hits          INTEGER NOT NULL,
    fmt             TEXT,
    rows_scanned    INTEGER,
    truncated       BOOLEAN,
    kinds           JSONB,
    examples        JSONB,
    checked_at      DATE NOT NULL,
    PRIMARY KEY (accession, url)
);

CREATE INDEX IF NOT EXISTS idx_supp_file_gene_corruption_accession ON supplementary_file_gene_corruption (accession);
CREATE INDEX IF NOT EXISTS idx_supp_file_gene_corruption_source    ON supplementary_file_gene_corruption (source);
