-- Postgres shape for single-cell evidence.
-- JSONB and materialized views keep this in _pg; SQLite tests load the twin.
-- Re-runnable: adds missing tables/indexes and leaves unified_metadata.is_single_cell
-- untouched for comparison.

CREATE TABLE IF NOT EXISTS sc_rule (
    id            integer PRIMARY KEY,
    rule          text NOT NULL,
    -- sra | ena | geo | ae | dra | gsa | gea | enrichment. 'enrichment' is
    -- model-derived archive prose and is capped at the medium tier.
    archive       text NOT NULL,
    grain         text NOT NULL,
    field         text NOT NULL,
    tier          text NOT NULL CHECK (tier IN ('high','medium','low')),
    gate          text NOT NULL,
    rule_version  text NOT NULL,
    built_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (rule, archive, rule_version)
);

CREATE TABLE IF NOT EXISTS single_cell_evidence (
    accession        text NOT NULL,
    grain            text NOT NULL,
    study_accession  text,
    rule_id          integer NOT NULL REFERENCES sc_rule(id),
    value            text,
    -- Archive source/strategy verbatim. Multimodal runs submit separate
    -- libraries; CITE-seq antibody panels are SYNTHETIC / Targeted-Capture
    -- beside GEX.
    library_source   text,
    library_strategy text,
    analyte          text,   -- transcriptomic | genomic | synthetic | other
    assay_modality   text,   -- scRNA-seq | scATAC-seq | CITE-seq | Multiome | VDJ | ...
    PRIMARY KEY (accession, grain, rule_id)
);

CREATE INDEX IF NOT EXISTS idx_sce_study ON single_cell_evidence(study_accession);
CREATE INDEX IF NOT EXISTS idx_sce_rule_study ON single_cell_evidence(rule_id, study_accession);
CREATE INDEX IF NOT EXISTS idx_sce_assay_modality ON single_cell_evidence(assay_modality);
CREATE INDEX IF NOT EXISTS idx_sce_analyte ON single_cell_evidence(analyte);

-- Materialized for study-page reads over the evidence table.
-- REFRESH MATERIALIZED VIEW CONCURRENTLY needs the unique index below.
CREATE MATERIALIZED VIEW IF NOT EXISTS single_cell_study AS
SELECT
    e.study_accession,
    bool_or(r.tier = 'high')                                    AS has_high_confidence,
    count(DISTINCT e.accession) FILTER (
        WHERE r.grain IN ('sample','experiment','run'))         AS sc_objects,
    count(DISTINCT r.rule)                                      AS n_rules,
    -- Archive-local until crossarchive_propagation maps mirror accessions.
    count(DISTINCT r.archive)                                   AS n_archives,
    CASE WHEN bool_or(r.tier = 'high')   THEN 'high'
         WHEN bool_or(r.tier = 'medium') THEN 'medium'
         ELSE 'low' END                                         AS evidence_tier,
    jsonb_agg(DISTINCT r.rule)                                  AS rules,
    jsonb_agg(DISTINCT e.assay_modality) FILTER (
        WHERE e.assay_modality IS NOT NULL)                     AS assay_modalities,
    jsonb_agg(DISTINCT e.analyte) FILTER (
        WHERE e.analyte IS NOT NULL)                            AS analytes,
    jsonb_agg(DISTINCT e.library_strategy) FILTER (
        WHERE e.library_strategy IS NOT NULL)                   AS library_strategies
FROM single_cell_evidence e
JOIN sc_rule r ON r.id = e.rule_id
WHERE e.study_accession IS NOT NULL
GROUP BY e.study_accession;

CREATE UNIQUE INDEX IF NOT EXISTS idx_scs_study ON single_cell_study(study_accession);
CREATE INDEX IF NOT EXISTS idx_scs_tier ON single_cell_study(evidence_tier);
