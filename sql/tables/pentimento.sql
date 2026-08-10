-- Pentimento: one SQLite joining cell counts, preflightx calls, links and quant.

CREATE TABLE IF NOT EXISTS study_ncount (
    study_accession          TEXT PRIMARY KEY,
    source                   TEXT,
    aliases                  TEXT CHECK (aliases IS NULL OR json_valid(aliases)),
    title                    TEXT,
    cells                    INTEGER,
    -- Reported "cells" is the matrix COLUMN count. When the count is an exact
    -- multiple of a 10x barcode whitelist (737280 v2, 6794880 v3) the matrix is
    -- unfiltered and those columns are barcodes, not cells: 896 studies carried
    -- 4.27 B of a 7.59 B nominal total this way. Never sum cells without
    -- filtering on this flag.
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
    -- read-derived calls (preflightx). Confidence columns exist only in the
    -- enrichment DB route; the annotated-CSV route leaves them NULL.
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
    -- Evidence behind the sex call. y_hits/xist_hits are meaningless without
    -- sex_reads_scanned: 21 Y-unique reads out of 200 k is not 21 out of 2 k.
    sex_reads_scanned     INTEGER,
    y_hits                INTEGER,
    xist_hits             INTEGER,
    y_xist_ratio          REAL,
    -- 'experimental' | 'unsupported'. Some graded experiments were called with
    -- a panel not validated for their species; the verdict still stands, and
    -- this is the column saying not to trust it.
    sex_panel_status      TEXT,
    sex_mixed_suspected   INTEGER,
    -- Reads used by the deep sex pass, 0/NULL when only the cheap pass ran.
    -- Sex abstains ~80% of the time at the shallow depth, so this separates
    -- "looked hard and found nothing" from "barely looked".
    deep_n_reads          INTEGER,
    assay                 TEXT,
    assay_display         TEXT,   -- human-readable chemistry, e.g. "10x 3' v3"
    assay_is_single_cell  INTEGER,
    preflightx_source     TEXT,   -- 'db' | 'csv' -- the two routes can disagree
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
    ref_bp          INTEGER,
    covered_bp      INTEGER,
    breadth_frac    REAL,
    total_reads     INTEGER,
    -- The calibrated viral gate: small genome AND majority tiled. Applying it to
    -- bacteria is meaningless -- only 0.1% of bacterial references are <= 50 kb,
    -- so it returns ~0 BY CONSTRUCTION and must never be read as "no bacteria".
    is_validated_viral    INTEGER NOT NULL DEFAULT 0,
    -- Bacteria get an absolute-coverage screen instead. Deliberately NOT called
    -- "validated" -- it is a screening heuristic pending a size-aware gate.
    is_bacterial_evidence INTEGER NOT NULL DEFAULT 0,
    -- Set when the organism is near-universal across unrelated studies
    -- (reagent kitome / skin flora), e.g. E. coli in 69% of all runs.
    is_background         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_accession, organism)
);

-- Which organisms each is_* flag covers, and the exact gate behind it, for the
-- same reason background_taxa exists: a boolean that cannot be traced back to a
-- rule is not evidence. Rebuilt from pentimento.flags.FLAG_PANEL every build.
CREATE TABLE IF NOT EXISTS microbe_flag_panel (
    flag      TEXT NOT NULL,
    organism  TEXT NOT NULL,
    gate      TEXT NOT NULL,
    PRIMARY KEY (flag, organism)
) WITHOUT ROWID;

-- run_microbe_flags is created by pentimento.flags: its columns come from
-- FLAG_PANEL, so declaring them here too would let the two drift apart.

CREATE TABLE IF NOT EXISTS background_taxa (
    organism    TEXT PRIMARY KEY,
    runs        INTEGER,
    prevalence  REAL
);

-- Per-run rollup of the microbe table, one row per run. Driven from
-- quant_provenance rather than microbe_quant, so a run measured and found empty
-- still appears with its depth. Filtering background before the grouping
-- dropped a large share of the runs carrying any microbe row, which then read
-- as "no data" in sample_master, destroying the not-measured against
-- measured-nothing distinction quant_provenance exists to preserve.
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


-- How each viral/bacterial call was produced. Kept per run because depth and
-- read length vary per run and both change what a breadth number means: 0.5 off
-- 700 k reads is far weaker evidence than 0.5 off 700 M.
CREATE TABLE IF NOT EXISTS quant_provenance (
    run_accession    TEXT PRIMARY KEY,
    status           TEXT,     -- ok | download_failed <stage> | absent_from_logan
    total_reads      INTEGER,  -- __total_reads__
    read_length      INTEGER,  -- __read_length__
    source_url       TEXT,     -- Logan unitig object actually fetched
    kmer_size        INTEGER,  -- k used to build the unitigs (ka:f: is mean k-mer abundance)
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

-- preflightx's own cross-run vote per experiment, plus how much it looked at to
-- reach it. Read from the .provenance.json files beside the annotated CSVs.
--
-- The counts carry the weight: 'unknown' off 2 runs at 20 k reads and the same
-- verdict off 2 deep runs at 400 k are different findings, and sex abstains
-- ~80% of the time at the shallow depth. preflightx_provenance was supposed to
-- record this and does not, with params_json NULL throughout.
--
-- The raw JSON is not stored: it projects to ~10.2 GB, larger than all of
-- Pentimento. Only its structure is kept.
CREATE TABLE IF NOT EXISTS preflightx_vote (
    experiment_accession TEXT PRIMARY KEY,
    study_accession      TEXT,
    -- '' in the source means "did not call" and is stored as NULL. 72.9% of
    -- experiments carry a non-empty vote; the rest genuinely abstained.
    vote_species         TEXT,
    vote_sex             TEXT,
    vote_assay           TEXT,
    n_runs_scanned       INTEGER,
    n_deep_runs          INTEGER,   -- 0 = the deep sex pass never ran
    n_reads              INTEGER,   -- reads per run in the cheap pass
    deep_n_reads         INTEGER    -- reads per run in the deep pass
) WITHOUT ROWID;

-- How accurate the read-derived calls are, benchmarked against archive metadata
-- against archive metadata. Long form, one row per experiment x field, so the
-- summary is a GROUP BY and a fourth benchmarked field needs no migration.
--
-- correct is tri-state, and NULL carries meaning: the comparison could not be
-- made because one side abstained or there is no ground truth, which differs
-- from a wrong answer. Sex is why this matters: ground truth is the literal
-- string 'NA' on every row, so sex has predictions and zero evaluable ones,
-- making its accuracy unmeasured rather than low.
CREATE TABLE IF NOT EXISTS preflightx_benchmark (
    accession   TEXT NOT NULL,   -- study
    experiment  TEXT NOT NULL,
    field       TEXT NOT NULL,   -- organism | assay | sex
    -- Raw beside normalised: a disagreement can be re-judged without trusting
    -- the normaliser, and assay needs that -- most of its "errors" are the two
    -- vocabularies differing in GRAIN (library strategy vs chemistry), not the
    -- call being wrong.
    gt_raw      TEXT,
    pred_raw    TEXT,
    gt_norm     TEXT,
    pred_norm   TEXT,
    correct     INTEGER,
    PRIMARY KEY (accession, experiment, field)
) WITHOUT ROWID;

-- Per-field provenance for the stated author or LLM-derived metadata,
-- GSM-grained, covering every sample in the enrichment DB rather than only
-- those linked into Pentimento. "Author-provided" is not a column of its own:
-- it is <field>_source = 'llm_primary' with a non-empty <field>_column. The
-- _original_* variants record the pre-normalisation value, so a disagreement
-- between stated and read-derived traces to the exact source column.
--
-- STORED NORMALISED. The natural long form repeats a handful of strings tens of
-- millions of times: field has 15 distinct values, source a similar handful,
-- and column names are drawn from a small vocabulary. Interned into prov_dict
-- they become 1-2 byte integers. Use the sample_field_provenance_v view for
-- text-valued queries.
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

-- Text-valued view over the normalised table; query this, not the raw table.
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

