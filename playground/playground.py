import json

from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from geopy.location import Location

arcgis = Nominatim(user_agent="seqout-ingestion-v2")
geocoder = RateLimiter(func=arcgis.geocode)
location: Location = geocoder(
    "PLA general hospital",
    addressdetails=True,
    extratags=True,
    namedetails=True,
    language="en",
)


print(json.dumps(location.raw, indent=4))
