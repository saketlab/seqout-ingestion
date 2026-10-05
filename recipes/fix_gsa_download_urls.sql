-- One-shot repair for GSA (CRA + HRA) run download links already in Postgres.
--
-- CNCB emits ftp://download.big.ac.cn/... , which no browser can open; only
-- https://download.cncb.ac.cn works (docs/ddbj-gsa-operations.md).
-- transfer/gsa.py::_https_url rewrites new rows; this fixes older ones. Idempotent.
--
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f recipes/fix_gsa_download_urls.sql

BEGIN;

UPDATE sra_run_download_links
SET fastq_ftp = replace(
        fastq_ftp,
        'ftp://download.big.ac.cn/',
        'https://download.cncb.ac.cn/'
    )
WHERE fastq_ftp LIKE '%ftp://download.big.ac.cn/%';

COMMIT;

-- Expect legacy_left = 0.
SELECT count(*) FILTER (WHERE fastq_ftp LIKE '%big.ac.cn%')               AS legacy_left,
       count(*) FILTER (WHERE fastq_ftp LIKE 'https://download.cncb.ac.cn/%') AS rewritten
FROM sra_run_download_links
WHERE run_accession LIKE 'CRR%' OR run_accession LIKE 'HRR%';
