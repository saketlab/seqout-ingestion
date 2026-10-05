#!/bin/bash
set -e

# DROP plus CREATE applies source-arm definition changes; CREATE IF NOT EXISTS
# would leave stale views.
# GEO and GEA platforms come from the backing SRA and DRA archives, so they are
# excluded from their own arms.
# A leading '<' is never a platform name: some sra_experiments rows hold
# '<cyfunction Comment at 0x...>', a Python repr written when the SRA XML parse
# hit a comment node where PLATFORM was expected. Excluded here as unknown (same
# as NULL/''); the parse bug itself lives in seqout-ingestion.
# GSA's instrument_platform holds MODEL names ("Illumina NovaSeq 6000",
# "MGISEQ-2000") where every other archive holds the platform vocabulary, so the
# mapping below translates them the way ENA classifies the same models
# (MGISEQ -> BGISEQ). Models with no canonical equivalent, such as
# BioNano optical mapping, stay under their own name.

DATABASE_URL=$(grep '^DATABASE_URL=' .env | cut -d= -f2-)

echo "Recreating platform growth materialized views..."
psql "$DATABASE_URL" <<'EOF'
DROP MATERIALIZED VIEW IF EXISTS platform_growth_monthly;
CREATE MATERIALIZED VIEW platform_growth_monthly AS
  SELECT 'geo'::text AS db,
         e.platform,
         e.instrument_model,
         DATE_TRUNC('month', st.published_at) AS month,
         COUNT(*) AS count
  FROM sra_experiments e
  JOIN sra_studies st ON st.accession = e.study
  WHERE st.alias LIKE 'GSE%'
    AND st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
  GROUP BY e.platform, e.instrument_model, DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'sra'::text, e.platform, e.instrument_model,
         DATE_TRUNC('month', st.published_at), COUNT(*)
  FROM sra_experiments e
  JOIN sra_studies st ON st.accession = e.study
  WHERE (st.alias IS NULL OR st.alias NOT LIKE 'GSE%')
    AND st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
  GROUP BY e.platform, e.instrument_model, DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'ena'::text, e.instrument_platform, e.instrument_model,
         DATE_TRUNC('month', e.first_public), COUNT(*)
  FROM ena_experiments e
  WHERE e.first_public IS NOT NULL
    AND e.instrument_platform IS NOT NULL AND e.instrument_platform != ''
    AND e.instrument_platform NOT LIKE '<%'
  GROUP BY e.instrument_platform, e.instrument_model, DATE_TRUNC('month', e.first_public)

  UNION ALL

  SELECT 'gsa'::text, CASE
           WHEN e.instrument_platform ILIKE 'Illumina %'
             OR e.instrument_platform ILIKE 'NextSeq %'          THEN 'ILLUMINA'
           WHEN e.instrument_platform ILIKE 'DNBSEQ%'
             OR e.instrument_platform ILIKE 'DNBSE-%'            THEN 'DNBSEQ'
           WHEN e.instrument_platform ILIKE 'MGISEQ%'
             OR e.instrument_platform ILIKE 'BGISEQ%'            THEN 'BGISEQ'
           WHEN e.instrument_platform ILIKE 'PacBio %'           THEN 'PACBIO_SMRT'
           WHEN e.instrument_platform ILIKE 'OXFORD_NANOPORE%'   THEN 'OXFORD_NANOPORE'
           WHEN e.instrument_platform ILIKE 'Ion Torrent%'       THEN 'ION_TORRENT'
           WHEN e.instrument_platform ILIKE '454 GS%'            THEN 'LS454'
           WHEN e.instrument_platform ILIKE 'AB SOLiD%'
             OR e.instrument_platform ILIKE 'AB 5500%'           THEN 'ABI_SOLID'
           WHEN e.instrument_platform ILIKE 'AB %Genetic Analyzer' THEN 'CAPILLARY'
           WHEN e.instrument_platform ILIKE 'Complete Genomics%' THEN 'COMPLETE_GENOMICS'
           WHEN e.instrument_platform ILIKE 'Helicos%'           THEN 'HELICOS'
           WHEN e.instrument_platform ILIKE 'Element AVITI%'     THEN 'ELEMENT'
           WHEN e.instrument_platform ILIKE 'GenoLab%'
             OR e.instrument_platform ILIKE 'FASTASeq%'          THEN 'GENEMIND'
           ELSE e.instrument_platform
         END, e.instrument_model,
         DATE_TRUNC('month', e.first_public), COUNT(*)
  FROM gsa_experiments e
  WHERE e.first_public IS NOT NULL
    AND e.instrument_platform IS NOT NULL AND e.instrument_platform != ''
    AND e.instrument_platform NOT LIKE '<%'
  GROUP BY 2, e.instrument_model, DATE_TRUNC('month', e.first_public)

  UNION ALL

  SELECT 'dra'::text, e.platform, e.instrument_model,
         DATE_TRUNC('month', st.published_at), COUNT(*)
  FROM dra_experiments e
  JOIN dra_studies st ON st.accession = e.study
  WHERE st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
    AND NOT EXISTS (SELECT 1 FROM gea_experiments g WHERE g.bioproject = st.bioproject_id)
  GROUP BY e.platform, e.instrument_model, DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'gea'::text, e.platform, e.instrument_model,
         DATE_TRUNC('month', st.published_at), COUNT(*)
  FROM dra_experiments e
  JOIN dra_studies st ON st.accession = e.study
  WHERE st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
    AND EXISTS (SELECT 1 FROM gea_experiments g WHERE g.bioproject = st.bioproject_id)
  GROUP BY e.platform, e.instrument_model, DATE_TRUNC('month', st.published_at)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pgm_platform_db_month_model
  ON platform_growth_monthly(platform, db, month, instrument_model);
CREATE INDEX IF NOT EXISTS idx_pgm_platform
  ON platform_growth_monthly(platform);

DROP MATERIALIZED VIEW IF EXISTS platform_growth_projects_monthly;
CREATE MATERIALIZED VIEW platform_growth_projects_monthly AS
  SELECT 'geo'::text AS db,
         e.platform,
         DATE_TRUNC('month', st.published_at) AS month,
         COUNT(DISTINCT st.accession) AS count
  FROM sra_experiments e
  JOIN sra_studies st ON st.accession = e.study
  WHERE st.alias LIKE 'GSE%'
    AND st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
  GROUP BY e.platform, DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'sra'::text, e.platform,
         DATE_TRUNC('month', st.published_at), COUNT(DISTINCT st.accession)
  FROM sra_experiments e
  JOIN sra_studies st ON st.accession = e.study
  WHERE (st.alias IS NULL OR st.alias NOT LIKE 'GSE%')
    AND st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
  GROUP BY e.platform, DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'ena'::text, e.instrument_platform,
         DATE_TRUNC('month', e.first_public), COUNT(DISTINCT e.study_accession)
  FROM ena_experiments e
  WHERE e.first_public IS NOT NULL
    AND e.instrument_platform IS NOT NULL AND e.instrument_platform != ''
    AND e.instrument_platform NOT LIKE '<%'
  GROUP BY e.instrument_platform, DATE_TRUNC('month', e.first_public)

  UNION ALL

  SELECT 'gsa'::text, CASE
           WHEN e.instrument_platform ILIKE 'Illumina %'
             OR e.instrument_platform ILIKE 'NextSeq %'          THEN 'ILLUMINA'
           WHEN e.instrument_platform ILIKE 'DNBSEQ%'
             OR e.instrument_platform ILIKE 'DNBSE-%'            THEN 'DNBSEQ'
           WHEN e.instrument_platform ILIKE 'MGISEQ%'
             OR e.instrument_platform ILIKE 'BGISEQ%'            THEN 'BGISEQ'
           WHEN e.instrument_platform ILIKE 'PacBio %'           THEN 'PACBIO_SMRT'
           WHEN e.instrument_platform ILIKE 'OXFORD_NANOPORE%'   THEN 'OXFORD_NANOPORE'
           WHEN e.instrument_platform ILIKE 'Ion Torrent%'       THEN 'ION_TORRENT'
           WHEN e.instrument_platform ILIKE '454 GS%'            THEN 'LS454'
           WHEN e.instrument_platform ILIKE 'AB SOLiD%'
             OR e.instrument_platform ILIKE 'AB 5500%'           THEN 'ABI_SOLID'
           WHEN e.instrument_platform ILIKE 'AB %Genetic Analyzer' THEN 'CAPILLARY'
           WHEN e.instrument_platform ILIKE 'Complete Genomics%' THEN 'COMPLETE_GENOMICS'
           WHEN e.instrument_platform ILIKE 'Helicos%'           THEN 'HELICOS'
           WHEN e.instrument_platform ILIKE 'Element AVITI%'     THEN 'ELEMENT'
           WHEN e.instrument_platform ILIKE 'GenoLab%'
             OR e.instrument_platform ILIKE 'FASTASeq%'          THEN 'GENEMIND'
           ELSE e.instrument_platform
         END,
         DATE_TRUNC('month', e.first_public), COUNT(DISTINCT e.study_accession)
  FROM gsa_experiments e
  WHERE e.first_public IS NOT NULL
    AND e.instrument_platform IS NOT NULL AND e.instrument_platform != ''
    AND e.instrument_platform NOT LIKE '<%'
  GROUP BY 2, DATE_TRUNC('month', e.first_public)

  UNION ALL

  SELECT 'dra'::text, e.platform,
         DATE_TRUNC('month', st.published_at), COUNT(DISTINCT st.accession)
  FROM dra_experiments e
  JOIN dra_studies st ON st.accession = e.study
  WHERE st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
    AND NOT EXISTS (SELECT 1 FROM gea_experiments g WHERE g.bioproject = st.bioproject_id)
  GROUP BY e.platform, DATE_TRUNC('month', st.published_at)

  UNION ALL

  SELECT 'gea'::text, e.platform,
         DATE_TRUNC('month', st.published_at), COUNT(DISTINCT st.accession)
  FROM dra_experiments e
  JOIN dra_studies st ON st.accession = e.study
  WHERE st.published_at IS NOT NULL
    AND e.platform IS NOT NULL AND e.platform != '' AND e.platform NOT LIKE '<%'
    AND EXISTS (SELECT 1 FROM gea_experiments g WHERE g.bioproject = st.bioproject_id)
  GROUP BY e.platform, DATE_TRUNC('month', st.published_at)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pgpm_platform_db_month
  ON platform_growth_projects_monthly(platform, db, month);
EOF

echo "Done."
