#!/bin/bash
set -e

# Every name a rare disease is known by, mapped to its MONDO id.
#
# Catalogue names, MONDO labels and published synonyms are flattened into one
# alias -> mondo_id table, keyed by normalized text so submitter spellings converge.
# queries.py _ALIAS_NORM must stay identical to that expression.

DATABASE_URL="${DATABASE_URL:-$(grep '^DATABASE_URL=' .env | cut -d= -f2-)}"

echo "Creating disease_alias ..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
DROP MATERIALIZED VIEW IF EXISTS disease_alias;

CREATE MATERIALIZED VIEW disease_alias AS
WITH named AS (
    SELECT gard_name AS name, mondo_id, 'gard' AS source FROM gard_disease
     WHERE mondo_id IS NOT NULL
    UNION ALL
    SELECT mondo_label, mondo_id, 'mondo' FROM gard_disease WHERE mondo_id IS NOT NULL
    UNION ALL
    SELECT unnest(synonyms), mondo_id, 'gard_synonym' FROM gard_disease
     WHERE mondo_id IS NOT NULL
    UNION ALL
    SELECT nord_name, mondo_id, 'nord' FROM nord_disease WHERE mondo_id IS NOT NULL
    UNION ALL
    SELECT mondo_label, mondo_id, 'mondo' FROM nord_disease WHERE mondo_id IS NOT NULL
    UNION ALL
    SELECT unnest(synonyms), mondo_id, 'nord_synonym' FROM nord_disease
     WHERE mondo_id IS NOT NULL
)
SELECT btrim(regexp_replace(lower(name), '[^a-z0-9]+', ' ', 'g')) AS alias,
       array_agg(DISTINCT mondo_id)                               AS mondo_ids,
       array_agg(DISTINCT source)                                 AS sources,
       min(name)                                                  AS display_name
FROM named
WHERE btrim(regexp_replace(lower(name), '[^a-z0-9]+', ' ', 'g')) <> ''
GROUP BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_disease_alias_pk ON disease_alias (alias);
-- prefix and substring lookup for the typeahead; pg_trgm is already installed
CREATE INDEX IF NOT EXISTS idx_disease_alias_trgm
    ON disease_alias USING gin (alias gin_trgm_ops);
SQL
echo "Done."
