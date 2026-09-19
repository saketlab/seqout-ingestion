#!/bin/bash
set -e

DATABASE_URL=$(grep '^DATABASE_URL=' .env | cut -d= -f2-)

echo "Creating organism_growth_monthly materialized view..."
# DROP + CREATE so re-running applies definition changes.
psql "$DATABASE_URL" <<'EOF'
DROP MATERIALIZED VIEW IF EXISTS organism_growth_monthly;
CREATE MATERIALIZED VIEW organism_growth_monthly AS
  SELECT 'geo'::text AS db,
         UNNEST(g.organisms) AS organism,
         DATE_TRUNC('month', s.published_at) AS month,
         COUNT(*) AS count
  FROM geo_samples s
  JOIN geo_series g ON s.accession = ANY(g.samples_ref)
  WHERE s.published_at IS NOT NULL AND g.organisms IS NOT NULL
  GROUP BY UNNEST(g.organisms), DATE_TRUNC('month', s.published_at)

  UNION ALL

  SELECT 'sra'::text, UNNEST(st.organisms),
         DATE_TRUNC('month', st.published_at), COUNT(*)
  FROM sra_experiments e
  JOIN sra_studies st ON st.accession = e.study
  WHERE st.published_at IS NOT NULL AND st.organisms IS NOT NULL
  GROUP BY UNNEST(st.organisms), DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'arrayexpress'::text, s.organism,
         DATE_TRUNC('month', ex.release_date), COUNT(*)
  FROM arrayexpress_samples s
  JOIN arrayexpress_experiments ex ON ex.accession = s.experiment_accession
  WHERE ex.release_date IS NOT NULL AND s.organism IS NOT NULL AND s.organism != ''
  GROUP BY s.organism, DATE_TRUNC('month', ex.release_date)

  UNION ALL

  SELECT 'ena'::text, es.scientific_name,
         DATE_TRUNC('month', e.first_public), COUNT(*)
  FROM ena_experiments e
  JOIN ena_studies es ON es.study_accession = e.study_accession
  WHERE e.first_public IS NOT NULL AND es.scientific_name IS NOT NULL
  GROUP BY es.scientific_name, DATE_TRUNC('month', e.first_public)

  UNION ALL

  SELECT 'gsa'::text, es.scientific_name,
         DATE_TRUNC('month', e.first_public), COUNT(*)
  FROM gsa_experiments e
  JOIN gsa_studies es ON es.study_accession = e.study_accession
  WHERE e.first_public IS NOT NULL AND es.scientific_name IS NOT NULL
  GROUP BY es.scientific_name, DATE_TRUNC('month', e.first_public)

  UNION ALL

  SELECT 'dra'::text, s.scientific_name,
         DATE_TRUNC('month', st.published_at), COUNT(*)
  FROM dra_experiments e
  JOIN dra_studies st ON st.accession = e.study
  JOIN dra_samples s ON s.accession = ANY(e.samples)
  WHERE st.published_at IS NOT NULL AND s.scientific_name IS NOT NULL
  GROUP BY s.scientific_name, DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'gea'::text, g.organism,
         DATE_TRUNC('month', ex.release_date), COUNT(*)
  FROM gea_samples g
  JOIN gea_experiments ex ON ex.accession = g.experiment_accession
  WHERE ex.release_date IS NOT NULL AND g.organism IS NOT NULL AND g.organism != ''
  GROUP BY g.organism, DATE_TRUNC('month', ex.release_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ogm_organism_db_month
  ON organism_growth_monthly(organism, db, month);
CREATE INDEX IF NOT EXISTS idx_ogm_organism
  ON organism_growth_monthly(organism);
EOF

echo "Refreshing materialized view..."
psql "$DATABASE_URL" -c "REFRESH MATERIALIZED VIEW CONCURRENTLY organism_growth_monthly;"

echo "Done."
