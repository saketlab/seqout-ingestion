-- Postgres-only indexes for unified_metadata. The SQLite twin is
-- sql/indexes/unified_metadata.sql.

CREATE INDEX IF NOT EXISTS idx_um_is_single_cell
    ON unified_metadata (is_single_cell);

-- Cohort clients filter on spelled-out state to avoid NULL boolean semantics.
CREATE INDEX IF NOT EXISTS idx_um_single_cell_status
    ON unified_metadata (single_cell_status);

CREATE INDEX IF NOT EXISTS idx_um_modality
    ON unified_metadata (single_cell_modality);

-- Study organism membership index. `organism_counts ? 'Homo sapiens'` is the
-- membership filter; otherwise this seq-scans the table. jsonb_path_ops matches
-- key-existence use and stays smaller.
CREATE INDEX IF NOT EXISTS idx_um_organism_counts
    ON unified_metadata USING gin (organism_counts jsonb_path_ops);
