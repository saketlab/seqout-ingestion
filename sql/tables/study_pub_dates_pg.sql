-- Parsed publication days, one row per (study, source, day). Postgres only.
-- study_publications.publications needs a LATERAL expansion per row; no index
-- reaches it. Only a full pub_date parses, so a year-only paper has no row.
CREATE MATERIALIZED VIEW IF NOT EXISTS study_pub_dates AS
SELECT DISTINCT
    sp.accession,
    sp.source,
    to_date(pub ->> 'pub_date', 'YYYY Mon DD') AS pub_date
FROM study_publications sp
CROSS JOIN LATERAL jsonb_array_elements(sp.publications) pub
WHERE pub ->> 'pub_date' ~ '^[0-9]{4} [A-Z][a-z]{2} [0-9]{2}$';

-- REFRESH CONCURRENTLY needs a unique index, which the DISTINCT above allows.
CREATE UNIQUE INDEX IF NOT EXISTS idx_study_pub_dates_key
    ON study_pub_dates (accession, source, pub_date);

-- drives the date side of the semi-join
CREATE INDEX IF NOT EXISTS idx_study_pub_dates_date
    ON study_pub_dates (pub_date, accession);
