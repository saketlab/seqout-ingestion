from geopy import ArcGIS
from geopy.extra.rate_limiter import RateLimiter

osm_geocoder = RateLimiter(
    ArcGIS(
        user_agent="seqout-ingestion-v2",
        timeout=30,
    ).geocode,
    min_delay_seconds=3,
    max_retries=20,
)
