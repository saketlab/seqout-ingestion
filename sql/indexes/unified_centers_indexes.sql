PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE INDEX IF NOT EXISTS idx_umc_center ON unified_centers(center_name);
CREATE INDEX IF NOT EXISTS idx_umc_latlon ON unified_centers(latitude, longitude) WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_umc_country ON unified_centers(country) WHERE country IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_umc_country_code ON unified_centers(country_code) WHERE country_code IS NOT NULL;
