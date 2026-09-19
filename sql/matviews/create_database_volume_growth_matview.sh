#!/usr/bin/env bash

set -euo pipefail

DB_URL=$(grep -oP 'DATABASE_URL=\K.*' "$(dirname "$0")/../.env")

if [[ "$(psql "$DB_URL" -Atc "SELECT to_regclass('public.sra_run_download_links') IS NOT NULL;")" != "t" ]]; then
  echo "sra_run_download_links table is missing; cannot build database_volume_growth_monthly." >&2
  exit 1
fi

read -r -d '' VIEW_SQL <<'SQL' || true
CREATE MATERIALIZED VIEW database_volume_growth_monthly_next AS
  SELECT 'ena'::text AS db,
         DATE_TRUNC('month', first_public)::date AS month,
         SUM(base_count::bigint)::bigint AS count
  FROM ena_experiments
  WHERE first_public IS NOT NULL
    AND base_count IS NOT NULL
  GROUP BY DATE_TRUNC('month', first_public)::date

  UNION ALL

  SELECT 'sra_fastq_bytes'::text,
         DATE_TRUNC('month', s.published_at)::date,
         SUM(
           split_part(dl.fastq_bytes, ';', 1)::bigint +
           COALESCE(NULLIF(split_part(dl.fastq_bytes, ';', 2), '')::bigint, 0) +
           COALESCE(NULLIF(split_part(dl.fastq_bytes, ';', 3), '')::bigint, 0)
         )::bigint
  FROM sra_run_download_links dl
  JOIN sra_studies s ON s.accession = dl.study_accession
  WHERE dl.fastq_bytes IS NOT NULL
    AND dl.fastq_bytes <> ''
    AND s.published_at IS NOT NULL
  GROUP BY DATE_TRUNC('month', s.published_at)::date

  UNION ALL

  SELECT 'sra_sra_bytes'::text,
         DATE_TRUNC('month', s.published_at)::date,
         SUM(
           CASE
             WHEN dl.sra_bytes NOT LIKE '%;%' THEN dl.sra_bytes::bigint
             ELSE (SELECT SUM(x::bigint) FROM unnest(string_to_array(dl.sra_bytes, ';')) x)
           END
         )::bigint
  FROM sra_run_download_links dl
  JOIN sra_studies s ON s.accession = dl.study_accession
  WHERE dl.sra_bytes IS NOT NULL
    AND dl.sra_bytes <> ''
    AND s.published_at IS NOT NULL
  GROUP BY DATE_TRUNC('month', s.published_at)::date
WITH DATA;
SQL

echo "Building database_volume_growth_monthly_next..."
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "DROP MATERIALIZED VIEW IF EXISTS database_volume_growth_monthly_next;"
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "$VIEW_SQL"
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "CREATE UNIQUE INDEX idx_dvgm_next_db_month ON database_volume_growth_monthly_next(db, month);"
psql "$DB_URL" -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
SET LOCAL lock_timeout = '10s';
DROP MATERIALIZED VIEW IF EXISTS database_volume_growth_monthly;
ALTER MATERIALIZED VIEW database_volume_growth_monthly_next RENAME TO database_volume_growth_monthly;
ALTER INDEX idx_dvgm_next_db_month RENAME TO idx_dvgm_db_month;
COMMIT;
SQL

echo "database_volume_growth_monthly is ready."
