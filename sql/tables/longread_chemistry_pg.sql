-- Long-read chemistry, one row per run: bucket tier from populate.py
-- (instrument_model alone, never first_public), declared tier from ENA's
-- author-written library_construction_protocol/experimental_protocol text
-- (ONT only; the BAM-header/FASTQ-comment routes measured 0% yield there),
-- exact tier from a fleet BAM-header scan. Covers every long-read run,
-- independent of single-cell status.
CREATE TABLE IF NOT EXISTS longread_chemistry (
    run_accession               TEXT PRIMARY KEY,
    study_accession              TEXT,
    instrument_platform           TEXT,
    instrument_model             TEXT,
    chemistry                    TEXT,
    -- 'bucket' | 'declared' | 'exact' | 'unknown'. 'bucket-low' is a
    -- retired value some rows may still carry from before instrument_model
    -- became the only bucket-tier signal.
    chemistry_confidence          TEXT,
    -- 'instrument_model' | 'protocol_text' | 'bam_header'
    chemistry_source             TEXT,
    raw_evidence                  TEXT,
    bam_checked_at                TIMESTAMPTZ,
    declared_checked_at           TIMESTAMPTZ,
    basecaller_software           TEXT,
    basecaller_software_version   TEXT,
    flow_cell_id                  TEXT,
    ont_pore                      TEXT,
    ont_kit                       TEXT,
    ont_speed_bps                 TEXT,
    ont_model_tier                TEXT,
    ont_basecall_model_version    TEXT,
    pacbio_platform_model         TEXT,
    pacbio_chemistry_code         TEXT,
    pacbio_binding_kit            TEXT,
    pacbio_sequencing_kit         TEXT,
    pacbio_smrtcell_kit           TEXT,
    pacbio_basecaller_version     TEXT
);

CREATE INDEX IF NOT EXISTS idx_longread_chemistry_study
    ON longread_chemistry (study_accession);
