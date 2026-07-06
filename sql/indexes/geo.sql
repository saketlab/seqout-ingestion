CREATE INDEX IF NOT EXISTS idx_geo_series_published_at ON geo_series(published_at);
CREATE INDEX IF NOT EXISTS idx_geo_series_updated_at ON geo_series(updated_at);
CREATE INDEX IF NOT EXISTS idx_series_samples_ref ON geo_series(samples_ref);
CREATE INDEX IF NOT EXISTS idx_series_accession_published ON geo_series(accession, published_at);

CREATE INDEX IF NOT EXISTS idx_geo_samples_platform_ref ON geo_samples(platform_ref);
CREATE INDEX IF NOT EXISTS idx_samples_accession ON geo_samples(accession);
CREATE INDEX IF NOT EXISTS idx_samples_accession_platform ON geo_samples(accession, platform_ref);

CREATE INDEX IF NOT EXISTS idx_geo_platforms_technology ON geo_platforms(technology);
CREATE INDEX IF NOT EXISTS idx_platforms_accession_organism ON geo_platforms(accession, organism);

CREATE INDEX IF NOT EXISTS idx_contributors_accession ON geo_contributors(accession);
CREATE INDEX IF NOT EXISTS idx_contributors_country ON geo_contributors(address_country);
CREATE INDEX IF NOT EXISTS idx_contributors_accession_country ON geo_contributors(accession, address_country);
