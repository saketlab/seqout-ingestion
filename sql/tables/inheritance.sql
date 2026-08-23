-- Disease inheritance by MONDO id, SQLite staging.
--
-- HPO phenotype.hpoa covers OMIM, Orphanet en_product9_ages.xml covers
-- OrphaCodes, and MONDO cross-references both. Several OMIM entries can
-- collapse onto one MONDO term and disagree, so modes stays a provenance-backed
-- list.
CREATE TABLE IF NOT EXISTS disease_inheritance (
    mondo_id    TEXT PRIMARY KEY,
    modes       TEXT NOT NULL,  -- ';'-joined labels; Postgres splits to text[]
    hpo_ids     TEXT NOT NULL,  -- ';'-joined HP terms the labels came from
    omim_ids    TEXT,           -- ';'-joined OMIM entries that carried them
    orpha_ids   TEXT,           -- ';'-joined OrphaCodes that carried them
    -- Source bridge: 'hpoa', 'orphanet', or both. Kept for disagreements.
    sources     TEXT NOT NULL,
    -- Download URL says "latest"; version fields pin exact inputs. Pass
    -- --hpoa-url for a fixed release.
    hpoa_version TEXT,
    hpo_version  TEXT,
    orphanet_version TEXT,
    fetched_at  TEXT NOT NULL
);
