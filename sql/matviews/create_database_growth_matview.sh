#!/usr/bin/env bash
# Refresh database_growth_monthly for fast /stats/growth projects and experiments.
# The API reads this pre-aggregated relation when present. Safe to re-run.

set -euo pipefail

DB_URL=$(grep -oP 'DATABASE_URL=\K.*' "$(dirname "$0")/../.env")

psql_exec() {
  psql "$DB_URL" -v ON_ERROR_STOP=1 "$@"
}

run_insert() {
  local label="$1"
  local sql="$2"
  echo "$label"
  psql_exec -c "$sql"
}

echo "Preparing database_growth_monthly_stage..."
psql_exec <<'SQL'
DROP TABLE IF EXISTS database_growth_monthly_stage;
CREATE TABLE database_growth_monthly_stage (
  metric text NOT NULL,
  db text NOT NULL,
  month date NOT NULL,
  count bigint NOT NULL,
  PRIMARY KEY (metric, db, month)
);
SQL

run_insert "1/8 projects.geo" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'projects', 'geo', DATE_TRUNC('month', published_at)::date, COUNT(*)::bigint
FROM geo_series
WHERE published_at IS NOT NULL
GROUP BY DATE_TRUNC('month', published_at)::date;
"

run_insert "2/8 projects.sra" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'projects', 'sra', DATE_TRUNC('month', published_at)::date, COUNT(*)::bigint
FROM sra_studies
WHERE published_at IS NOT NULL
GROUP BY DATE_TRUNC('month', published_at)::date;
"

run_insert "3/8 projects.arrayexpress" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'projects', 'arrayexpress', DATE_TRUNC('month', release_date)::date, COUNT(*)::bigint
FROM arrayexpress_experiments
WHERE release_date IS NOT NULL
GROUP BY DATE_TRUNC('month', release_date)::date;
"

run_insert "4/8 projects.ena" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'projects', 'ena', DATE_TRUNC('month', first_public)::date, COUNT(*)::bigint
FROM ena_studies
WHERE first_public IS NOT NULL
GROUP BY DATE_TRUNC('month', first_public)::date;
"

run_insert "projects.gsa" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'projects', 'gsa', DATE_TRUNC('month', first_public)::date, COUNT(*)::bigint
FROM gsa_studies
WHERE first_public IS NOT NULL
GROUP BY DATE_TRUNC('month', first_public)::date;
"

run_insert "5/8 experiments.geo" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'experiments', 'geo', DATE_TRUNC('month', published_at)::date, COUNT(*)::bigint
FROM geo_samples
WHERE published_at IS NOT NULL
GROUP BY DATE_TRUNC('month', published_at)::date;
"

run_insert "6/8 experiments.sra" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'experiments', 'sra', DATE_TRUNC('month', s.published_at)::date, COUNT(*)::bigint
FROM sra_experiments e
JOIN sra_studies s ON s.accession = e.study
WHERE s.published_at IS NOT NULL
GROUP BY DATE_TRUNC('month', s.published_at)::date;
"

run_insert "7/8 experiments.arrayexpress" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'experiments', 'arrayexpress', DATE_TRUNC('month', e.release_date)::date, COUNT(*)::bigint
FROM arrayexpress_samples s
JOIN arrayexpress_experiments e ON e.accession = s.experiment_accession
WHERE e.release_date IS NOT NULL
GROUP BY DATE_TRUNC('month', e.release_date)::date;
"

run_insert "8/8 experiments.ena" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'experiments', 'ena', DATE_TRUNC('month', first_public)::date, COUNT(*)::bigint
FROM ena_experiments
WHERE first_public IS NOT NULL
GROUP BY DATE_TRUNC('month', first_public)::date;
"

run_insert "experiments.gsa" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'experiments', 'gsa', DATE_TRUNC('month', first_public)::date, COUNT(*)::bigint
FROM gsa_experiments
WHERE first_public IS NOT NULL
GROUP BY DATE_TRUNC('month', first_public)::date;
"

run_insert "projects.dra" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'projects', 'dra', DATE_TRUNC('month', published_at)::date, COUNT(*)::bigint
FROM dra_studies
WHERE published_at IS NOT NULL
GROUP BY DATE_TRUNC('month', published_at)::date;
"

run_insert "projects.gea" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'projects', 'gea', DATE_TRUNC('month', release_date)::date, COUNT(*)::bigint
FROM gea_experiments
WHERE release_date IS NOT NULL
GROUP BY DATE_TRUNC('month', release_date)::date;
"

run_insert "experiments.dra" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'experiments', 'dra', DATE_TRUNC('month', s.published_at)::date, COUNT(*)::bigint
FROM dra_experiments e
JOIN dra_studies s ON s.accession = e.study
WHERE s.published_at IS NOT NULL
GROUP BY DATE_TRUNC('month', s.published_at)::date;
"

run_insert "experiments.gea" "
INSERT INTO database_growth_monthly_stage(metric, db, month, count)
SELECT 'experiments', 'gea', DATE_TRUNC('month', e.release_date)::date, COUNT(*)::bigint
FROM gea_samples g
JOIN gea_experiments e ON e.accession = g.experiment_accession
WHERE e.release_date IS NOT NULL
GROUP BY DATE_TRUNC('month', e.release_date)::date;
"

echo "Publishing database_growth_monthly..."
psql_exec <<'SQL'
BEGIN;
DO $$
DECLARE
  existing_relkind "char";
BEGIN
  SELECT c.relkind INTO existing_relkind
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'public'
    AND c.relname = 'database_growth_monthly';

  IF existing_relkind = 'm' THEN
    DROP MATERIALIZED VIEW database_growth_monthly;
  ELSIF existing_relkind IS NOT NULL THEN
    DROP TABLE database_growth_monthly;
  END IF;
END $$;
ALTER TABLE database_growth_monthly_stage RENAME TO database_growth_monthly;
ALTER INDEX database_growth_monthly_stage_pkey RENAME TO database_growth_monthly_pkey;
COMMIT;
SQL

echo "database_growth_monthly is ready."
