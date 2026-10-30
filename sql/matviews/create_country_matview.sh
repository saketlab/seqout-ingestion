#!/bin/bash
set -e

# One row per study, any archive or technology, scoped by submitter country.
#
# Kept as its own matview because unified_metadata (~1.8M rows) has poor
# cache locality for a per-country scan: a plain indexed
# `WHERE country_code_iso2 = ...` took 4-12s for a 16k-study country and 142s
# for the US (514k studies) in production. Ordering the defining query by
# country clusters matching rows together on disk so a country page loads
# from a handful of pages instead of scattered random I/O.

DATABASE_URL="${DATABASE_URL:-$(grep '^DATABASE_URL=' .env | cut -d= -f2-)}"

echo "Creating country_study ..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
DROP MATERIALIZED VIEW IF EXISTS country_study;

CREATE MATERIALIZED VIEW country_study AS
SELECT u.canonical_accession                                   AS study_accession,
       u.title,
       u.dominant_scientific_name                              AS organism,
       u.dominant_assay_l1                                     AS assay_l1,
       u.dominant_assay_l2                                     AS assay_l2,
       CASE u.source
           WHEN 'ae' THEN 'ArrayExpress'
           WHEN 'geo' THEN 'GEO'
           WHEN 'sra' THEN 'SRA'
           WHEN 'ena' THEN 'ENA'
           WHEN 'gsa' THEN 'GSA'
           WHEN 'dra' THEN 'DRA'
           WHEN 'gea' THEN 'GEA'
           ELSE upper(u.source)
       END                                                     AS source,
       u.country,
       u.country_code_iso2,
       u.n_samples,
       u.n_experiments,
       u.is_single_cell,
       u.single_cell_modality,
       u.center_name,
       u.pmid,
       NULLIF(substring(u.first_published from 1 for 4), '')::int AS year,
       dl.has_fastq, dl.has_sra, dl.n_runs, dl.n_fastq_runs, dl.n_sra_runs
FROM unified_metadata u
LEFT JOIN study_download_summary dl ON dl.study_accession = u.canonical_accession
WHERE u.country_code_iso2 IS NOT NULL
  -- hide SRA embargoes (future release date) until a rebuild after release
  AND (u.first_published IS NULL OR u.first_published <= to_char(current_date, 'YYYY-MM-DD'))
ORDER BY u.country_code_iso2;

CREATE UNIQUE INDEX idx_country_study_accession ON country_study(study_accession);
CREATE INDEX idx_country_study_country ON country_study(country_code_iso2);
ANALYZE country_study;

-- Precomputed so /country/{code}/facets doesn't re-aggregate organism/assay
-- values (long-tailed, thousands of distinct values for a big country) on
-- every request; capped to the top 40 values per facet per country.
DROP MATERIALIZED VIEW IF EXISTS country_facet;

CREATE MATERIALIZED VIEW country_facet AS
WITH raw AS (
    SELECT country_code_iso2, 'organism' AS facet, organism AS value, count(*) AS studies
    FROM country_study WHERE organism IS NOT NULL GROUP BY 1, 3
    UNION ALL
    SELECT country_code_iso2, 'assay_l1', assay_l1, count(*)
    FROM country_study WHERE assay_l1 IS NOT NULL GROUP BY 1, 3
    UNION ALL
    SELECT country_code_iso2, 'source', source, count(*)
    FROM country_study GROUP BY 1, 3
    UNION ALL
    SELECT country_code_iso2, 'year', year::text, count(*)
    FROM country_study WHERE year IS NOT NULL GROUP BY 1, 3
    UNION ALL
    SELECT country_code_iso2, 'has_fastq', has_fastq::text, count(*)
    FROM country_study WHERE has_fastq IS NOT NULL GROUP BY 1, 3
    UNION ALL
    SELECT country_code_iso2, 'has_sra', has_sra::text, count(*)
    FROM country_study WHERE has_sra IS NOT NULL GROUP BY 1, 3
    UNION ALL
    SELECT country_code_iso2, 'is_single_cell', is_single_cell::text, count(*)
    FROM country_study WHERE is_single_cell IS NOT NULL GROUP BY 1, 3
    UNION ALL
    -- has_matrix only exists for the single-cell subset, via singlecell_study
    SELECT cs.country_code_iso2, 'has_matrix', sc.has_matrix::text, count(*)
    FROM country_study cs
    JOIN singlecell_study sc ON sc.study_accession = cs.study_accession
    WHERE sc.has_matrix IS NOT NULL
    GROUP BY 1, 3
    UNION ALL
    -- presence in longread_study is itself the long-read signal
    SELECT cs.country_code_iso2, 'has_long_read',
           (lr.study_accession IS NOT NULL)::text, count(*)
    FROM country_study cs
    LEFT JOIN longread_study lr ON lr.study_accession = cs.study_accession
    GROUP BY 1, 3
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY country_code_iso2, facet ORDER BY studies DESC
    ) AS rn
    FROM raw
)
SELECT country_code_iso2, facet, value, studies
FROM ranked
WHERE rn <= 40
ORDER BY country_code_iso2, facet, studies DESC;

CREATE INDEX idx_country_facet_country ON country_facet(country_code_iso2);
ANALYZE country_facet;

-- Precomputed for the same reason as country_facet: a live aggregate over
-- country_study costs as much as the largest country (up to ~514k rows for
-- the US) on every /country/{code}/summary request.
DROP MATERIALIZED VIEW IF EXISTS country_summary;

CREATE MATERIALIZED VIEW country_summary AS
SELECT cs.country_code_iso2                                   AS country_code_iso2,
       count(*)                                                AS studies,
       sum(cs.n_samples)                                       AS samples,
       sum(cs.n_experiments)                                   AS experiments,
       count(*) FILTER (WHERE cs.has_fastq)                    AS studies_with_fastq,
       count(*) FILTER (WHERE cs.has_sra)                      AS studies_with_sra,
       count(*) FILTER (WHERE cs.organism = 'Homo sapiens')    AS studies_human,
       count(*) FILTER (WHERE cs.is_single_cell)               AS studies_single_cell,
       count(*) FILTER (WHERE lr.study_accession IS NOT NULL)  AS studies_long_read,
       count(DISTINCT cs.organism)                             AS n_organisms,
       min(cs.year)                                            AS first_year,
       max(cs.year)                                            AS last_year
FROM country_study cs
LEFT JOIN longread_study lr ON lr.study_accession = cs.study_accession
GROUP BY cs.country_code_iso2;

CREATE UNIQUE INDEX idx_country_summary_country
  ON country_summary(country_code_iso2);
ANALYZE country_summary;

-- Geocoded institution clusters per country, for map/OG rendering. Reading
-- straight off unified_metadata timed out (30s statement_timeout) for large
-- countries, same locality problem country_study exists to solve.
DROP MATERIALIZED VIEW IF EXISTS country_points;

CREATE MATERIALIZED VIEW country_points AS
WITH raw AS (
    SELECT country_code_iso2,
           ROUND(latitude::numeric, 1) AS lat,
           ROUND(longitude::numeric, 1) AS lng,
           count(*) AS n
    FROM unified_metadata
    WHERE country_code_iso2 IS NOT NULL
      AND latitude IS NOT NULL AND longitude IS NOT NULL
    GROUP BY 1, 2, 3
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY country_code_iso2 ORDER BY n DESC
    ) AS rn
    FROM raw
)
SELECT country_code_iso2, lat, lng, n
FROM ranked
WHERE rn <= 300
ORDER BY country_code_iso2, n DESC;

CREATE INDEX idx_country_points_country ON country_points(country_code_iso2);
ANALYZE country_points;
SQL

echo "Done."
