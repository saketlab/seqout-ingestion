CREATE TABLE IF NOT EXISTS study_ncount (
    study_accession          TEXT PRIMARY KEY,
    source                   TEXT,
    aliases                  TEXT CHECK (aliases IS NULL OR json_valid(aliases)),
    title                    TEXT,
    cells                    INTEGER,
    -- Reported "cells" is the matrix COLUMN count. When the count is an exact
    -- multiple of a 10x barcode whitelist (737280 v2, 6794880 v3) the matrix is
    -- unfiltered and those columns are barcodes, not cells. Filter on this
    -- flag before summing cells.
    cells_unfiltered         INTEGER NOT NULL DEFAULT 0,
    single_cell_file_format  TEXT,
    n_samples                INTEGER,
    n_gsm_samples            INTEGER,
    has_metadata             INTEGER,
    has_celltype             INTEGER,
    has_donor                INTEGER,
    has_demographics         INTEGER,
    metadata_fields          TEXT CHECK (metadata_fields IS NULL OR json_valid(metadata_fields))
);

CREATE TABLE IF NOT EXISTS sample_ncount (
    sample_accession   TEXT PRIMARY KEY,
    study_accession    TEXT NOT NULL,
    scope              TEXT,
    cells              INTEGER,
    genes              INTEGER,
    nnz                INTEGER,
    file_format        TEXT,
    unfiltered         INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS run_preflightx (
    run_accession         TEXT PRIMARY KEY,
    experiment_accession  TEXT,
    sample_accession      TEXT,   -- SRS/ERS/DRS
    gsm_accession         TEXT,   -- GEO sample when resolvable, else NULL
    study_accession       TEXT,
    -- read-derived calls (preflightx). The run-grained CSV route leaves
    -- confidence/evidence NULL until an experiment-grained route overlays it.
    species               TEXT,
    species_called        TEXT,
    species_confidence    TEXT,   -- 'low'|'medium'|'high', NOT numeric
    species_hit_reads     INTEGER,
    species_hit_fraction  REAL,
    species_runner_up     TEXT,
    species_margin        REAL,
    species_ambiguous     INTEGER,
    species_mislabel      INTEGER,
    sex                   TEXT,
    sex_verdict           TEXT,
    sex_confidence        TEXT,   -- 'low'|'medium'|'high', NOT numeric
    -- sex_reads_scanned gives the denominator for y_hits/xist_hits; raw counts are
    -- depth-dependent.
--
    sex_reads_scanned     INTEGER,
    y_hits                INTEGER,
    xist_hits             INTEGER,
    y_xist_ratio          REAL,
    -- 'experimental' | 'unsupported'. Flags a verdict produced with a panel
    -- unvalidated for that species.
    sex_panel_status      TEXT,
    sex_mixed_suspected   INTEGER,
--
--
    -- Deep sex-pass read depth; 0/NULL means cheap pass only.
    deep_n_reads          INTEGER,
    assay                 TEXT,
    assay_display         TEXT,   -- human-readable chemistry, e.g. "10x 3' v3"
    assay_is_single_cell  INTEGER,
    -- Cell vs nucleus from intron fraction and mitochondrial %. sc_enrich reads
    -- author wording; this field is read-derived. Nuclei retain unspliced
    -- transcripts and lose cytoplasmic mitochondria. Use modality_reads_scanned
    -- with intron_frac/mito_pct.
    modality               TEXT,   -- 'single-cell'|'single-nucleus'|'ambiguous'|'unknown'
    modality_confidence    TEXT,   -- 'low'|'medium'|'high', NOT numeric
    intron_frac            REAL,
    mito_pct               REAL,
    modality_reads_scanned INTEGER,
    -- Read geometry. bulk_vs_sc is called off R1 length structure, so it is
    -- independent of assay_is_single_cell (which comes from barcode whitelist
    -- hits); disagreement is informative.
    bulk_vs_sc             TEXT,   -- 'single_cell'|'bulk'|'bulk_or_5prime_broken'|'uncertain'
    bulk_vs_sc_confidence  TEXT,
    -- `end` is a SQL keyword, hence read_end.
    read_end               TEXT,   -- "3'"|"5'"|'full-length'|'ambiguous'|'unknown'
    read_end_confidence    TEXT,
    preflightx_source     TEXT,   -- 'db' | 'csv'; routes can disagree
    library_layout        TEXT,
    fastq_ftp             TEXT,
    fastq_bytes           TEXT,
    fastq_md5             TEXT,
    sra_ftp               TEXT,
    ncbi_sra_url          TEXT
);

CREATE TABLE IF NOT EXISTS microbe_quant (
    run_accession   TEXT NOT NULL,
    class           TEXT NOT NULL,   -- virus|pathogen|kitome|flora|parasite|spike|animalPV|culture|vector
    organism        TEXT NOT NULL,
    ref_accession   TEXT,
    reads           INTEGER,
    -- weighted k-mer count (ka:f mean abundance)
    kmer_mass       REAL,
    n_unitigs       INTEGER,
    ref_bp          INTEGER,
    covered_bp      INTEGER,
    breadth_frac    REAL,
    total_reads     INTEGER,
    -- The calibrated viral gate requires a small genome and majority tiling;
    -- bacterial refs exceed the viral-size gate.
--
    is_validated_viral    INTEGER NOT NULL DEFAULT 0,
--
    -- Non-endogenous breadth screen for expression-data HPV follow-up evidence.
    is_viral_evidence     INTEGER NOT NULL DEFAULT 0,
    -- Loose bacterial screen for follow-up evidence.
    is_bacterial_evidence INTEGER NOT NULL DEFAULT 0,
    is_validated_bacterial INTEGER NOT NULL DEFAULT 0,
    -- Set when the organism is near-universal across unrelated studies
    -- (reagent kitome / skin flora), e.g. E. coli across unrelated runs.
    is_background         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_accession, organism)
);

-- Flag panel: organism set and gate behind each is_* column. Rebuilt from
-- pentimento.flags.FLAG_PANEL every build.
CREATE TABLE IF NOT EXISTS microbe_flag_panel (
    flag      TEXT NOT NULL,
    organism  TEXT NOT NULL,
    gate      TEXT NOT NULL,
    PRIMARY KEY (flag, organism)
) WITHOUT ROWID;

-- run_microbe_flags is created by pentimento.flags from FLAG_PANEL.

--
CREATE TABLE IF NOT EXISTS background_taxa (
    organism    TEXT PRIMARY KEY,
    runs        INTEGER,
    prevalence  REAL
);
--

-- Per-run microbe rollup driven from quant_provenance, preserving measured-empty
-- depth before background filtering.
CREATE VIEW IF NOT EXISTS run_microbe_summary AS
SELECT
    qp.run_accession,
    qp.status,
    qp.total_reads,
    COALESCE(SUM(CASE WHEN mq.is_background = 0
                      THEN mq.is_validated_viral END), 0)              AS viral_taxa,
    COALESCE(SUM(CASE WHEN mq.is_background = 0 AND mq.is_validated_viral = 1
                      THEN mq.reads END), 0)                            AS viral_reads,
    COALESCE(SUM(CASE WHEN mq.is_background = 0
                      THEN mq.is_bacterial_evidence END), 0)            AS bact_taxa,
    COALESCE(SUM(CASE WHEN mq.is_background = 0 AND mq.is_bacterial_evidence = 1
                      THEN mq.reads END), 0)                            AS bact_reads
FROM quant_provenance qp
LEFT JOIN microbe_quant mq ON mq.run_accession = qp.run_accession
GROUP BY qp.run_accession, qp.status, qp.total_reads;

CREATE VIEW IF NOT EXISTS sample_master AS
SELECT
    p.study_accession,
    st.title                AS study_title,
    st.source,
    st.cells                AS study_cells,
    st.cells_unfiltered,
    st.has_donor,
    st.has_celltype,
    st.has_demographics,
    COALESCE(p.gsm_accession, p.sample_accession) AS sample_accession,
    sn.cells                AS sample_cells,
    sn.genes                AS sample_genes,
    sn.unfiltered           AS sample_unfiltered,
    p.run_accession,
    p.experiment_accession,
    p.species_called,
    p.species_mislabel,
    p.sex_verdict,
    p.sex_confidence,
    p.assay,
    p.assay_is_single_cell,
    p.preflightx_source,
    p.fastq_ftp,
    p.fastq_bytes,
    ms.status AS quant_status,
    ms.total_reads,
    COALESCE(ms.viral_taxa, 0)  AS viral_taxa,
    COALESCE(ms.viral_reads, 0) AS viral_reads,
    COALESCE(ms.bact_taxa, 0)   AS bact_taxa,
    COALESCE(ms.bact_reads, 0)  AS bact_reads
FROM run_preflightx p
LEFT JOIN study_ncount        st ON st.study_accession  = p.study_accession
LEFT JOIN sample_ncount       sn ON sn.sample_accession = COALESCE(p.gsm_accession, p.sample_accession)
LEFT JOIN run_microbe_summary ms ON ms.run_accession    = p.run_accession;


-- How each viral/bacterial call was produced. Depth and read length change what
-- breadth means.
--
CREATE TABLE IF NOT EXISTS quant_provenance (
    run_accession    TEXT PRIMARY KEY,
    status           TEXT,     -- ok | download_failed <stage> | absent_from_logan
    total_reads      INTEGER,  -- __total_reads__
    read_length      INTEGER,  -- __read_length__
    source_url       TEXT,     -- Logan unitig object actually fetched
    kmer_size        INTEGER,  -- k-mer size for unitig construction (ka:f mean abundance)
    min_nmatch       INTEGER,
    min_identity     REAL,
    aligner          TEXT,
    panel_ref        TEXT,
    viral_gate       TEXT,     -- the exact rule behind is_validated_viral
    bacterial_gate   TEXT,     -- the exact rule behind is_bacterial_evidence
    quantified_at    TEXT
);

CREATE TABLE IF NOT EXISTS preflightx_provenance (
    experiment_accession TEXT PRIMARY KEY,
    study_accession      TEXT,
    preflightx_version   TEXT,
    source_type          TEXT,   -- 'detailed' | ...
    source_file          TEXT,
    deep_n_reads         INTEGER,
    params_json          TEXT CHECK (params_json IS NULL OR json_valid(params_json)),
    report_json          TEXT CHECK (report_json IS NULL OR json_valid(report_json)),
    updated_at           TEXT
);

-- preflightx cross-run vote per experiment, with scan depth from provenance JSON.
--
--
--
--
--
--
CREATE TABLE IF NOT EXISTS preflightx_vote (
    experiment_accession TEXT PRIMARY KEY,
    study_accession      TEXT,
--
    -- '' in the source means abstain and is stored as NULL.
    vote_species         TEXT,
    vote_sex             TEXT,
    vote_assay           TEXT,
    n_runs_scanned       INTEGER,
    n_deep_runs          INTEGER,   -- 0 = the deep sex pass never ran
    n_reads              INTEGER,   -- reads per run in the cheap pass
    deep_n_reads         INTEGER    -- reads per run in the deep pass
) WITHOUT ROWID;

-- Read-call benchmark, long form: one row per experiment x field. correct is
-- tri-state; NULL means comparison unavailable, distinct from wrong. Sex ground
-- truth is literal 'NA', so sex predictions are retained for audit.
--
CREATE TABLE IF NOT EXISTS preflightx_benchmark (
    accession   TEXT NOT NULL,   -- study
    experiment  TEXT NOT NULL,
    field       TEXT NOT NULL,   -- organism | assay | sex
--
    -- Raw and normalised values stay side by side for audit; assay disagreements
    -- are often vocabulary-grain differences.
    gt_raw      TEXT,
    pred_raw    TEXT,
    gt_norm     TEXT,
    pred_norm   TEXT,
    correct     INTEGER,
    PRIMARY KEY (accession, experiment, field)
) WITHOUT ROWID;
--

-- Per-field provenance for author or LLM-derived metadata, one GSM row per
-- field across the enrichment DB. "Author-provided" is <field>_source =
-- 'llm_primary' plus non-empty <field>_column; _original_* keeps the
-- pre-normalisation source.
--
-- Stored normalised: repeated field/source/column text is interned into
-- prov_dict. Query sample_field_provenance_v for text.
CREATE TABLE IF NOT EXISTS prov_dict (
    id     INTEGER PRIMARY KEY,
    kind   TEXT NOT NULL,   -- 'field' | 'source' | 'column'
    value  TEXT NOT NULL,
    UNIQUE (kind, value)
);

CREATE TABLE IF NOT EXISTS sample_field_provenance (
    sample              TEXT NOT NULL,
    field_id            INTEGER NOT NULL REFERENCES prov_dict(id),
    study_accession     TEXT,
    source_id           INTEGER REFERENCES prov_dict(id),
    score               REAL,
    column_id           INTEGER REFERENCES prov_dict(id),
    original_source_id  INTEGER REFERENCES prov_dict(id),
    original_column_id  INTEGER REFERENCES prov_dict(id),
    original_score      REAL,
    PRIMARY KEY (sample, field_id)
) WITHOUT ROWID;

-- Text-valued view over the normalised table for callers.
CREATE VIEW IF NOT EXISTS sample_field_provenance_v AS
SELECT p.study_accession,
       p.sample,
       f.value  AS field,
       s.value  AS source,
       p.score,
       c.value  AS column_name,
       os.value AS original_source,
       oc.value AS original_column,
       p.original_score
FROM sample_field_provenance p
JOIN prov_dict f       ON f.id  = p.field_id
LEFT JOIN prov_dict s  ON s.id  = p.source_id
LEFT JOIN prov_dict c  ON c.id  = p.column_id
LEFT JOIN prov_dict os ON os.id = p.original_source_id
LEFT JOIN prov_dict oc ON oc.id = p.original_column_id;

