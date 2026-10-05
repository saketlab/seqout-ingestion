#!/bin/bash
set -e

# Every mode of inheritance known for a MONDO term, from every source, in one
# MONDO-keyed table. Inheritance is a property of the disease, so the key is
# the MONDO id and not the catalogue that lists it.

DATABASE_URL="${DATABASE_URL:-$(grep '^DATABASE_URL=' .env | cut -d= -f2-)}"

echo "Creating disease_inheritance_all ..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
DROP MATERIALIZED VIEW IF EXISTS disease_inheritance_all;

CREATE MATERIALIZED VIEW disease_inheritance_all AS
WITH sourced AS (
    -- HPO (via OMIM) and Orphanet (via OrphaCode), already merged upstream
    SELECT mondo_id, unnest(modes) AS mode, unnest(sources) AS source
    FROM disease_inheritance
    UNION
    -- GARD's own per-disease pages. Its labels are normalized into the same
    -- vocabulary by gard/inheritance.py, so 'Autosomal dominant' appears once.
    SELECT mondo_id, unnest(inheritance), 'gard'
    FROM gard_disease
    WHERE mondo_id IS NOT NULL AND cardinality(inheritance) > 0
)
SELECT mondo_id,
       array_agg(DISTINCT mode)   AS modes,
       array_agg(DISTINCT source) AS sources
FROM sourced
GROUP BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_disease_inheritance_all_pk
    ON disease_inheritance_all (mondo_id);
CREATE INDEX IF NOT EXISTS idx_disease_inheritance_all_modes
    ON disease_inheritance_all USING gin (modes);
SQL
echo "Done."
