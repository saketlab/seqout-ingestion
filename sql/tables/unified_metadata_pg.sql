-- Postgres schema for unified_metadata, the cross-source study table for search
-- and clients. JSONB/GIN keep it out of SQLite schema tests. Re-runnable: adds
-- missing objects.

CREATE TABLE IF NOT EXISTS unified_metadata (
    canonical_accession      TEXT PRIMARY KEY,
    source                   TEXT,
    title                    TEXT,
    description              TEXT,
    -- Every accession this study is also known by, across archives. GIN-indexed
    -- below; a project page resolves SRP/ERP/PRJ to its GSE through it.
    aliases                  JSONB,
    -- Per-organism sample-count map.
    -- Study organism filters must test membership here, not equality against
    -- dominant_scientific_name; mixed-organism studies can be dominated by a
    -- different species.
    organism_counts          JSONB,
    common_name_counts       JSONB,
    dominant_scientific_name TEXT,
    dominant_common_name     TEXT,
    library_strategy_counts  JSONB,
    assay_l1_counts          JSONB,
    assay_l2_counts          JSONB,
    dominant_assay_l1        TEXT,
    dominant_assay_l2        TEXT,
    n_experiments            INTEGER,
    n_samples                INTEGER,
    center_name              TEXT,
    pmid                     TEXT,
    journal                  TEXT,
    citation_count           INTEGER,
    first_published          TEXT,
    place_name               TEXT,
    short_label              TEXT,
    longitude                DOUBLE PRECISION,
    latitude                 DOUBLE PRECISION,
    city                     TEXT,
    district                 TEXT,
    subdistrict              TEXT,
    county                   TEXT,
    suburb                   TEXT,
    neighbourhood            TEXT,
    state                    TEXT,
    state_district           TEXT,
    territory                TEXT,
    metro_area               TEXT,
    postcode                 TEXT,
    country_code             TEXT,
    country                  TEXT,
    country_mismatch         TEXT,
    geocode_score            TEXT,
    country_code_iso2        TEXT,
    center_names             JSONB,
    place_type               TEXT,
    address_type             TEXT,

    -- Single-cell tri-state; no default. A default false would mark unexamined
    -- studies as measured negatives.
    --
    --   TRUE  = evidence says single-cell (read-derived OR study text)
    --   FALSE = screened by preflightx/pentimento, the call was negative
    --   NULL  = unmeasured
--
    is_single_cell           BOOLEAN,

    -- Spelled-out state for consumers; `WHERE is_single_cell = FALSE` hides
    -- unscreened rows.
    --
    --   measured_single_cell                     matrix count or preflightx call
    --   text_indicates_single_cell               study says so; unmeasured
    --   measured_not_single_cell                 screened bulk
    --   measured_not_single_cell_text_disagrees  text says single-cell, reads bulk
    --   not_screened                             unmeasured
    single_cell_status       TEXT,

    -- scRNA-seq | snRNA-seq | Spatial Transcriptomics | Multiome | scATAC-seq |
    -- snATAC-seq | Other single-cell
    single_cell_modality     TEXT,
    -- single_cell_modality_evidence distinguishes wording-derived modalities
    -- from measured ones.
    single_cell_modality_evidence TEXT
);

-- Additive columns for existing databases; CREATE TABLE IF NOT EXISTS leaves
-- existing tables unchanged.
ALTER TABLE unified_metadata ADD COLUMN IF NOT EXISTS single_cell_status TEXT;
ALTER TABLE unified_metadata
    ADD COLUMN IF NOT EXISTS single_cell_modality_evidence TEXT;

-- Drop DEFAULT 0 / false so new rows stay unmeasured until screened.
--
ALTER TABLE unified_metadata ALTER COLUMN is_single_cell DROP DEFAULT;
