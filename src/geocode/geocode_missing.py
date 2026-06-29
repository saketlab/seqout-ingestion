import json
import logging
import random
import re
import sqlite3
import time
from enum import StrEnum
from pathlib import Path
from typing import Any

import pycountry
from geopy import ArcGIS
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from geopy.location import Location

logger = logging.getLogger(__name__)


class GeocodingProviders(StrEnum):
    Nominatim = "nominatim"
    Arcgis = "arcgis"


FIELD_KEYS = [
    "geocode_query",
    "official_address",
    "place_name",
    "place_type",
    "short_label",
    "loc_name",
    "address_type",
    "osm_id",
    "osm_type",
    "place_id",
    "match_id",
    "longitude",
    "latitude",
    "city",
    "district",
    "subdistrict",
    "county",
    "suburb",
    "neighbourhood",
    "state",
    "state_district",
    "territory",
    "metro_area",
    "postcode",
    "country_code",
    "country",
    "country_mismatch",
    "geocode_score",
    "source",
    "error",
    "multiple_institutes",
]

UPDATE_COLS = [
    "latitude",
    "longitude",
    "place_name",
    "place_type",
    "short_label",
    "address_type",
    "osm_id",
    "osm_type",
    "place_id",
    "city",
    "district",
    "subdistrict",
    "county",
    "suburb",
    "neighbourhood",
    "state",
    "state_district",
    "territory",
    "metro_area",
    "postcode",
    "country_code",
    "country",
    "country_mismatch",
    "geocode_score",
]


def geocode_missing(
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    cache_file: Path | None,
    save_every: int,
    rate_limit: float,
    retries: int,
    limit: int | None,
    is_dry_run: bool,
) -> None:
    conn = sqlite3.connect(str(unified_metadata_db_file))
    missing = _find_missing(conn, limit)
    logger.info("found %d records with missing geocoded information", len(missing))

    if is_dry_run or not missing:
        conn.close()
        return

    geo_accessions = [acc for (_, acc) in missing if acc.startswith("GSE")]

    geo_country_mapping: dict[str, str] = {}
    if geo_accessions:
        with sqlite3.Connection(geo_db_file) as geo_conn:
            placeholders = ",".join("?" for _ in geo_accessions)
            query = (
                "SELECT accession, address_country FROM geo_contributors "  # noqa: S608
                f"WHERE accession IN ({placeholders}) "
                "AND address_country IS NOT NULL GROUP BY accession"
            )
            rows = geo_conn.execute(query, geo_accessions).fetchall()
            geo_country_mapping = dict(rows)

    cache = _load_cache(cache_file)

    nominatim = RateLimiter(
        Nominatim(user_agent="seqout-ingestion-v2", timeout=30).geocode,
        min_delay_seconds=rate_limit,
        max_retries=retries,
    )
    arcgis = RateLimiter(
        ArcGIS(timeout=30).geocode,
        min_delay_seconds=rate_limit,
        max_retries=retries,
    )

    updated = 0
    total = len(missing)

    for idx, (center_name, accession) in enumerate(missing):
        result: dict[str, Any] | None = None

        if center_name in cache:
            result = cache[center_name]
        else:
            cleaned = clean_text(center_name)
            if not cleaned:
                continue

            result = _geocode_with_retry(
                cleaned,
                nominatim,
                GeocodingProviders.Nominatim,
                max_retries=retries,
            )

            if result is None:
                result = _geocode_with_retry(
                    cleaned,
                    arcgis,
                    GeocodingProviders.Arcgis,
                    max_retries=retries,
                )

            if result is None:
                result = _empty_result(cleaned, "No result found from any provider")

            if (
                result.get("error") is None
                and accession.startswith("GSE")
                and accession in geo_country_mapping
            ):
                geo_country = geo_country_mapping[accession]
                if result.get("country") != geo_country:
                    result = {
                        "geocode_query": result.get("geocode_query", center_name),
                        "country": geo_country,
                    }

        cache[center_name] = result
        _update_by_name(conn, "unified_centers", center_name, result)
        _update_by_name(conn, "unified_metadata", center_name, result)
        updated += 1

        if (idx + 1) % save_every == 0:
            conn.commit()
            _save_cache(cache_file, cache)
            logger.info(
                "progress: %d/%d geocoded, %d updated",
                idx + 1,
                total,
                updated,
            )

    conn.commit()
    _save_cache(cache_file, cache)
    conn.close()
    logger.info("done: %d/%d records geocoded successfully", updated, total)


def _first(*values: Any) -> Any:
    for v in values:
        if v is not None and v != "":
            return v
    return None


def clean_text(text: str | None) -> str | None:
    if text is None or text in {"", "NA"}:
        return None

    text = str(text)
    text = re.sub(r'\\+"', "", text)
    text = re.sub(r'"{2,}', "", text)
    text = re.sub(r'["\u201C\u201D\u2018\u2019`]', "", text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\\[nrt]", " ", text)
    text = re.sub(r"[\[\]|<>]", "", text)
    text = re.sub(r"[{}]", "", text)
    text = re.sub(r"^\s*\(|\)\s*$", "", text)
    text = re.sub(r"^[^a-zA-Z0-9]+", "", text)
    text = re.sub(r",\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    text = text.split(",")[-1].strip()

    return text or None


def paste_parts(*parts: str | None) -> str | None:
    cleaned = [clean_text(p) for p in parts]
    cleaned = [c for c in cleaned if c is not None]
    return ", ".join(cleaned) if cleaned else None


def normalize_country(country: str | None) -> str | None:
    if not country:
        return None
    x = country.strip()

    try:
        c = pycountry.countries.lookup(x)
        return c.name.lower()
    except LookupError:
        return x.lower()


def countries_mismatch(
    geocoded_country: str | None,
    input_country: str | None,
) -> bool | None:
    gc = normalize_country(geocoded_country)
    ic = normalize_country(input_country)
    if gc is None or ic is None:
        return None
    return gc != ic


def _empty_result(query: str, error_msg: str = "Not attempted") -> dict[str, Any]:
    base = {k: None for k in FIELD_KEYS if k != "geocode_query"}
    base["geocode_query"] = query
    base["error"] = error_msg
    return base


def _parse_osm(query: str, location: Location | None) -> dict[str, Any]:
    if location is None:
        return _empty_result(query, error_msg="No result found")

    raw = location.raw
    addr = raw.get("address", {}) or {}
    extra = raw.get("extratags", {}) or {}
    names = raw.get("namedetails", {}) or {}

    place_name = _first(names.get("name"), raw.get("name"))

    city = _first(
        addr.get("city"),
        addr.get("town"),
        addr.get("village"),
        addr.get("municipality"),
    )
    state = addr.get("state")

    return {
        "geocode_query": query,
        "official_address": raw.get("display_name"),
        "place_name": place_name,
        "place_type": raw.get("type"),
        "short_label": _first(
            f"{place_name}, {state}" if place_name and state else None,
            f"{place_name}, {city}" if place_name and city else None,
            place_name,
        ),
        "loc_name": _first(extra.get("loc_name"), names.get("name:en")),
        "address_type": _first(raw.get("addresstype"), raw.get("class")),
        "osm_id": raw.get("osm_id"),
        "osm_type": raw.get("osm_type"),
        "place_id": raw.get("place_id"),
        "match_id": None,
        "longitude": location.longitude,
        "latitude": location.latitude,
        "city": city,
        "district": _first(addr.get("district"), addr.get("city_district")),
        "subdistrict": _first(addr.get("subdistrict"), addr.get("quarter")),
        "county": addr.get("county"),
        "suburb": addr.get("suburb"),
        "neighbourhood": _first(addr.get("neighbourhood"), addr.get("hamlet")),
        "state": state,
        "state_district": addr.get("state_district"),
        "territory": addr.get("territory"),
        "metro_area": _first(addr.get("municipality"), extra.get("is_in:city")),
        "postcode": addr.get("postcode"),
        "country_code": addr.get("country_code"),
        "country": addr.get("country"),
        "country_mismatch": None,
        "geocode_score": raw.get("importance"),
        "source": "osm",
        "error": None,
        "multiple_institutes": None,
    }


def _parse_arcgis(query: str, location: Location | None) -> dict[str, Any]:
    if location is None:
        return _empty_result(query, error_msg="No result found")

    raw = location.raw

    return {
        "geocode_query": query,
        "official_address": raw.get("LongLabel")
        or raw.get("Match_addr")
        or raw.get("address"),
        "place_name": raw.get("PlaceName")
        or raw.get("Place_addr")
        or raw.get("Address"),
        "place_type": raw.get("Type") or raw.get("Addr_type"),
        "short_label": raw.get("ShortLabel") or raw.get("LongLabel"),
        "loc_name": raw.get("Loc_name"),
        "address_type": raw.get("Addr_type"),
        "osm_id": None,
        "osm_type": None,
        "place_id": None,
        "match_id": raw.get("Loc_name"),
        "longitude": location.longitude,
        "latitude": location.latitude,
        "city": _first(raw.get("City"), raw.get("Municipality")),
        "district": raw.get("District"),
        "subdistrict": None,
        "county": raw.get("County"),
        "suburb": None,
        "neighbourhood": None,
        "state": _first(
            raw.get("Region"),
            raw.get("AdminAreaName"),
            raw.get("SubAdminAreaName"),
        ),
        "state_district": None,
        "territory": None,
        "metro_area": None,
        "postcode": raw.get("Postal") or raw.get("PostalExt"),
        "country_code": raw.get("CountryCode"),
        "country": raw.get("Country"),
        "country_mismatch": None,
        "geocode_score": raw.get("Score"),
        "source": "arcgis",
        "error": None,
        "multiple_institutes": None,
    }


def _geocode(
    query: str,
    geocoder: RateLimiter,
    provider: GeocodingProviders,
) -> dict[str, Any] | None:
    try:
        location = geocoder(
            query,
            addressdetails=True,
            extratags=True,
            namedetails=True,
            language="en",
        )
    except Exception as e:
        logger.debug("geocoding failed for '%s': %s", query, e)
        raise

    if location is None:
        return None

    match provider:
        case GeocodingProviders.Nominatim:
            return _parse_osm(query, location)
        case GeocodingProviders.Arcgis:
            return _parse_arcgis(query, location)

    return None


def _geocode_with_retry(
    query: str,
    geocoder: RateLimiter,
    provider: GeocodingProviders,
    max_retries: int = 5,
    base_delay: float = 2.0,
) -> dict[str, Any] | None:
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        try:
            return _geocode(query, geocoder, provider)
        except Exception as e:
            last_exc = e
            if attempt < max_retries - 1:
                delay = base_delay * (2**attempt) + random.uniform(0, 1)  # noqa: S311
                logger.warning(
                    "retry %d/%d for '%s' in %.1fs: %s",
                    attempt + 1,
                    max_retries,
                    query,
                    delay,
                    e,
                )
                time.sleep(delay)

    logger.error("all %d retries exhausted for '%s': %s", max_retries, query, last_exc)
    return _empty_result(
        query,
        error_msg=f"Geocoding failed after {max_retries} retries: {last_exc}",
    )


def _load_cache(cache_file: Path | None) -> dict[str, dict[str, Any]]:
    if cache_file and cache_file.exists():
        with cache_file.open() as f:
            return json.load(f)
    return {}


def _save_cache(cache_file: Path | None, cache: dict[str, dict[str, Any]]) -> None:
    if not cache_file:
        return
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_file.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(cache, f, indent=2, default=str)
    tmp.replace(cache_file)


def _find_missing(
    conn: sqlite3.Connection,
    limit: int | None = None,
) -> list[tuple[str, str]]:
    query = (
        "SELECT DISTINCT center_name, canonical_accession FROM unified_centers "
        "WHERE (center_name IS NOT NULL AND center_name != '') "
        "AND (latitude IS NULL OR longitude IS NULL)"
    )
    if limit is not None:
        query += f" LIMIT {limit}"

    return conn.execute(query).fetchall()


def _update_by_name(
    conn: sqlite3.Connection,
    table: str,
    center_name: str,
    result: dict[str, Any],
) -> None:
    set_clauses = ", ".join(f"{c} = ?" for c in UPDATE_COLS)
    values = [result.get(c) for c in UPDATE_COLS] + [center_name]
    conn.execute(
        f"UPDATE {table} SET {set_clauses} "  # noqa: S608
        "WHERE center_name = ? "
        "AND (latitude IS NULL OR longitude IS NULL)",
        values,
    )
