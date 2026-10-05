#!/bin/bash
set -e

# Per-study download availability: does this study have FASTQ, and does it have
# .sra / SRA-Lite?
#
# Kept as its own matview because sra_run_download_links is too large to
# aggregate per request, and disease views may be keyed by a linked alias.
#
# Tri-state: a study absent from the links table gets no row at all, which
# means "not known". Present-with-zero means "we looked and there are none".

DATABASE_URL="${DATABASE_URL:-$(grep '^DATABASE_URL=' .env | cut -d= -f2-)}"

echo "Creating study_download_summary ..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
DROP MATERIALIZED VIEW IF EXISTS study_download_summary;

CREATE MATERIALIZED VIEW study_download_summary AS
WITH per_source_study AS (
    SELECT study_accession,
           count(*)                                                  AS n_runs,
           count(*) FILTER (WHERE nullif(fastq_ftp, '') IS NOT NULL) AS n_fastq,
           -- any of the three routes NCBI serves .sra by. sra_ftp is ENA's
           -- mirror, ncbi_sra_url the original, ncbi_sra_lite_url the reduced
           -- form; a study with only the Lite copy still has .sra available.
           count(*) FILTER (
               WHERE nullif(sra_ftp, '') IS NOT NULL
                  OR nullif(ncbi_sra_url, '') IS NOT NULL
                  OR nullif(ncbi_sra_lite_url, '') IS NOT NULL
           )                                                         AS n_sra,
           count(*) FILTER (WHERE nullif(ncbi_sra_lite_url, '') IS NOT NULL)
                                                                     AS n_sra_lite
    FROM sra_run_download_links
    GROUP BY 1
),
alias_map AS (
    -- every accession a study is known by, pointing at its canonical form.
    -- unified_metadata.aliases is jsonb like {"ena":"PRJNA1181917","sra":"SRP543156"}
    SELECT canonical_accession, canonical_accession AS alias FROM unified_metadata
    UNION
    SELECT u.canonical_accession, a.value
    FROM unified_metadata u, jsonb_each_text(u.aliases) AS a
    WHERE a.value <> ''
)
SELECT m.canonical_accession                AS study_accession,
       sum(l.n_runs)::bigint                AS n_runs,
       sum(l.n_fastq)::bigint               AS n_fastq_runs,
       sum(l.n_sra)::bigint                 AS n_sra_runs,
       sum(l.n_sra_lite)::bigint            AS n_sra_lite_runs,
       sum(l.n_fastq) > 0                   AS has_fastq,
       sum(l.n_sra) > 0                     AS has_sra
FROM alias_map m
JOIN per_source_study l ON l.study_accession = m.alias
GROUP BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_study_download_pk
    ON study_download_summary (study_accession);
CREATE INDEX IF NOT EXISTS idx_study_download_fastq
    ON study_download_summary (has_fastq) WHERE has_fastq;
CREATE INDEX IF NOT EXISTS idx_study_download_sra
    ON study_download_summary (has_sra) WHERE has_sra;
SQL
echo "Done."
