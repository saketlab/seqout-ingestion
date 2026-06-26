PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE INDEX IF NOT EXISTS idx_um_source ON unified_metadata(source);
CREATE INDEX IF NOT EXISTS idx_um_dominant_sci ON unified_metadata(dominant_scientific_name);
CREATE INDEX IF NOT EXISTS idx_um_dominant_common ON unified_metadata(dominant_common_name);
CREATE INDEX IF NOT EXISTS idx_um_dominant_l1 ON unified_metadata(dominant_assay_l1);
CREATE INDEX IF NOT EXISTS idx_um_dominant_l2 ON unified_metadata(dominant_assay_l2);
CREATE INDEX IF NOT EXISTS idx_um_pmid ON unified_metadata(pmid);
CREATE INDEX IF NOT EXISTS idx_um_country ON unified_metadata(country);
CREATE INDEX IF NOT EXISTS idx_um_country_code ON unified_metadata(country_code);
CREATE INDEX IF NOT EXISTS idx_um_is_single_cell ON unified_metadata(is_single_cell);
CREATE INDEX IF NOT EXISTS idx_um_modality ON unified_metadata(single_cell_modality);
