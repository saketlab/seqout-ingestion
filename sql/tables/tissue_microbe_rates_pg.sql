-- Per (tissue, organism) detection rate over quantified samples. Postgres only.
-- Backs /stats/tissue-microbes in pysradb-server.
--
-- quant_status='ok' is the only honest denominator: 2.0M of the 2.5M rows in
-- pentimento_run were never quantified and would deflate every rate 5x.
-- tissue_ontology_id also carries CL/GO/NCBITaxon terms, so UBERON is filtered
-- for explicitly; CL:0000236 otherwise ranks as a top-20 "tissue".
--
-- DROP then CREATE, not IF NOT EXISTS: a REFRESH reuses the stored definition,
-- so an edited file would never reach the database. Run under a single
-- transaction and readers wait out the ~15s rebuild instead of seeing no view.
DROP MATERIALIZED VIEW IF EXISTS tissue_microbe_rates;

CREATE MATERIALIZED VIEW tissue_microbe_rates AS
WITH scr AS (
    SELECT DISTINCT r.run_accession,
           coalesce(r.gsm_accession, r.sample_accession) AS samp,
           r.sample_accession, r.gsm_accession, r.study_accession
    FROM pentimento_run r
    WHERE r.quant_status = 'ok'
),
-- GEO and SRA each index the same submission, so one sample has a row under
-- its GSE and another under its SRP, and for 8.5% of screened samples the two
-- carry different tissue text (GSM1080212 is "Embryo" in GSE44183 and "Blood"
-- in SRP018525). Both map at score 1.0, so no match-quality field separates
-- them. Prefer the GEO row: it carries a tissue more often on exactly this
-- disagreeing set, and it is where submitters fill per-sample characteristics.
samp_tissue AS (
    SELECT DISTINCT ON (s.samp)
           s.samp,
           o.tissue_ontology_id   AS uberon,
           o.tissue_ontology_name AS tissue
    FROM (SELECT DISTINCT samp, sample_accession, gsm_accession FROM scr) s
    JOIN ontology_samples_v5 o
      ON o.sample = s.gsm_accession OR o.sample = s.sample_accession
    WHERE o.tissue_ontology_id LIKE 'UBERON:%'
    ORDER BY s.samp, (o.sample = s.gsm_accession) DESC, o.tissue_ontology_id
),
denom AS (
    SELECT uberon, min(tissue) AS tissue, count(*) AS screened
    FROM samp_tissue GROUP BY uberon
),
hits AS (
    SELECT st.uberon,
           -- influenza ships one reference per genome segment; uncollapsed it
           -- takes 8 heatmap columns and splits its own sample count
           CASE WHEN m.organism LIKE 'InfluenzaA\_%' THEN 'InfluenzaA'
                WHEN m.organism LIKE 'InfluenzaB\_%' THEN 'InfluenzaB'
                ELSE m.organism END AS organism,
           min(m.class) AS class,
           bool_or(m.is_background) AS is_background,
           bool_or(m.organism ~ '^(Murine_leukemia_virus|MMTV|HIV|HTLV)')
               AS is_endogenous,
           count(DISTINCT st.samp) AS positives,
           count(DISTINCT st.samp) FILTER (
               WHERE m.is_validated_viral OR m.is_validated_bacterial
                  OR (m.class = 'virus' AND m.breadth_frac >= 0.50)
           ) AS positives_confirmed,
           count(DISTINCT s.study_accession) AS studies
    FROM scr s
    JOIN samp_tissue st ON st.samp = s.samp
    JOIN pentimento_microbe m ON m.run_accession = s.run_accession
    WHERE m.is_validated_viral OR m.is_viral_evidence OR m.is_validated_bacterial
       OR (m.class IN ('pathogen', 'culture') AND m.breadth_frac >= 0.01)
       OR (m.class = 'virus' AND m.breadth_frac >= 0.50)
    GROUP BY 1, 2
)
SELECT d.tissue, d.uberon, h.organism, h.class,
       h.is_background, h.is_endogenous,
       d.screened, h.positives, h.positives_confirmed, h.studies,
       round(h.positives::numeric * 100 / d.screened, 4) AS rate_pct
FROM hits h
JOIN denom d ON d.uberon = h.uberon
-- studies >= 2 drops cells a single submitter created on their own; screened
-- >= 200 drops tissues where one positive reads as a double-digit rate.
WHERE h.studies >= 2 AND d.screened >= 200;

CREATE UNIQUE INDEX idx_tissue_microbe_rates_key
    ON tissue_microbe_rates (uberon, organism);
CREATE INDEX idx_tissue_microbe_rates_organism
    ON tissue_microbe_rates (organism);
