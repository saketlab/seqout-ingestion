import json
import logging
import re
import sqlite3
from enum import StrEnum
from pathlib import Path
from typing import Any, Optional

import pycountry
from geopy import ArcGIS
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from geopy.location import Location


class GeocodingProviders(StrEnum):
    Nominatim = "nominatim"
    Arcgis = "arcgis"


_CENTERS_TABLE = "unified_centers"
_METADATA_TABLE = "unified_metadata"
_NAME_COL = "center_name"
_ACCESSION_COL = "canonical_accession"

_FIELD_KEYS = [
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

_UPDATE_COLS = [
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
    cache_file: Optional[Path],
    save_every: int,
    rate_limit: float,
    limit: Optional[int],
    is_dry_run: bool,
):
    conn = sqlite3.connect(str(unified_metadata_db_file))
    missing = _find_missing(conn, limit)
    logging.info("found %d records with missing geocoded information", len(missing))

    if is_dry_run or not missing:
        conn.close()
        return

    geo_accessions = [acc for (_, acc) in missing if acc.startswith("GSE")]

    with sqlite3.Connection(geo_db_file) as geo_conn:
        accessions = ", ".join(f"'{acc}'" for acc in geo_accessions)
        query = f"SELECT accession, address_country from geo_contributors WHERE accession IN ({accessions}) AND address_country IS NOT NULL GROUP BY accession;"
        rows = geo_conn.execute(query).fetchall()

        geo_country_mapping = {
            accession: address_country for accession, address_country in rows
        }

    cache = _load_cache(cache_file)
    nominatim = RateLimiter(
        Nominatim(user_agent="seqout-ingestion-v2").geocode,
        min_delay_seconds=rate_limit,
    )
    arcgis = RateLimiter(ArcGIS().geocode, min_delay_seconds=rate_limit)

    updated = 0
    total = len(missing)

    for idx, (center_name, accession) in enumerate(missing):
        result = None

        if center_name in cache:
            result = cache[center_name]
        else:
            cleaned = clean_text(center_name)
            if not cleaned:
                continue

            result = _geocode(cleaned, nominatim, GeocodingProviders.Nominatim)

            if result is None:
                result = _geocode(cleaned, arcgis, GeocodingProviders.Arcgis)

            # check if the country present on GEO and country returned by geocoder are the same or not
            # if no, then remove all the data returned by geocoder and just pass forward country field
            if result and accession.startswith("GSE"):
                if result["country"] != geo_country_mapping[accession]:
                    result = {
                        "geocode_query": query,
                        "country": geo_country_mapping[accession],
                    }

        if result:
            cache[center_name] = result
            _update_by_name(conn, _CENTERS_TABLE, center_name, result)
            _update_by_name(conn, _METADATA_TABLE, center_name, result)
            updated += 1

        if (idx + 1) % save_every == 0:
            conn.commit()
            _save_cache(cache_file, cache)
            logging.info(
                "progress: %d/%d geocoded, %d updated",
                idx + 1,
                total,
                updated,
            )

    conn.commit()
    _save_cache(cache_file, cache)
    conn.close()
    logging.info("done: %d/%d records geocoded successfully", updated, total)


def _first(*values):
    for v in values:
        if v is not None and v != "":
            return v
    return None


def clean_text(text):
    if text is None or text == "" or text == "NA":
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


def paste_parts(*parts):
    cleaned = [clean_text(p) for p in parts]
    cleaned = [c for c in cleaned if c is not None]
    return ", ".join(cleaned) if cleaned else None


def normalize_country(country):
    if not country:
        return None
    x = country.strip()

    try:
        c = pycountry.countries.lookup(x)
        return c.name.lower()
    except LookupError:
        return x.lower()


def countries_mismatch(geocoded_country, input_country):
    gc = normalize_country(geocoded_country)
    ic = normalize_country(input_country)
    if gc is None or ic is None:
        return None
    return gc != ic


def _empty_result(query, error_msg="Not attempted"):
    base = {k: None for k in _FIELD_KEYS if k != "geocode_query"}
    base["geocode_query"] = query
    base["error"] = error_msg
    return base


def _parse_osm(query: str, location: Location | None) -> dict:
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


def _geocode(
    query: str,
    geocoder: RateLimiter,
    provider: GeocodingProviders,
) -> Optional[dict]:
    try:
        location = geocoder(
            query,
            addressdetails=True,
            extratags=True,
            namedetails=True,
            language="en",
        )

        if location is not None:
            match provider:
                case GeocodingProviders.Nominatim:
                    result = _parse_osm(query, location)
                case GeocodingProviders.Arcgis:
                    pass

            # if input_country and result.get("country"):
            #     result["country_mismatch"] = countries_mismatch(
            #         result["country"], input_country
            #     )

            return result
    except Exception as e:
        logging.debug("geocoding failed for '%s': %s", query, e)

    return _empty_result(query, error_msg="Geocoding failed")


def _load_cache(cache_file: Optional[Path]) -> dict[str, dict[str, Any]]:
    if cache_file and cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)
    return {}


def _save_cache(cache_file: Optional[Path], cache: dict[str, dict[str, Any]]):
    if cache_file:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump(cache, f, indent=2, default=str)


def _find_missing(
    conn: sqlite3.Connection,
    limit: Optional[int] = None,
) -> list[tuple[str, str]]:
    query = f"SELECT DISTINCT {_NAME_COL}, {_ACCESSION_COL} FROM {_CENTERS_TABLE} WHERE ({_NAME_COL} IS NOT NULL AND {_NAME_COL} != '') AND (latitude IS NULL OR longitude IS NULL)"
    if limit:
        query += f"LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    return rows


def _update_by_name(
    conn: sqlite3.Connection,
    table: str,
    center_name: str,
    result: dict[str, Any],
):
    set_clauses = ", ".join(f"{c} = ?" for c in _UPDATE_COLS)
    values = [result.get(c) for c in _UPDATE_COLS] + [center_name]
    conn.execute(
        f"UPDATE {table} SET {set_clauses} "
        f"WHERE {_NAME_COL} = ? "
        f"AND (latitude IS NULL OR longitude IS NULL)",
        values,
    )
