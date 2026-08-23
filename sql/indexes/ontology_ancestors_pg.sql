-- Index support for arm 2 of the rare-disease predicate. Postgres only.
-- See src/gard/join.py for query context.
--
-- GIN needs ancestor ids as text[]; expression indexes need IMMUTABLE. STRICT
-- handles NULL; CASE maps '' to '{}' before jsonb cast.
CREATE OR REPLACE FUNCTION mondo_ancestor_ids(t text) RETURNS text[]
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS
$$ SELECT CASE
       WHEN t = '' THEN '{}'::text[]
       ELSE ARRAY(SELECT jsonb_array_elements(t::jsonb) ->> 'id')
   END $$;

-- Partial on disease_ontology_id IS NULL, the selective slice.
-- Wider ancestor indexes build larger and return rows the query discards.
--
-- Use CONCURRENTLY on an autocommit connection.
--
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_os_v4_mondo_ancestor_ids
    ON ontology_samples_v4 USING gin (mondo_ancestor_ids(disease_ancestors))
    WHERE disease_ancestors IS NOT NULL
      AND disease_ancestors <> ''
      AND disease_ontology_id IS NULL;
