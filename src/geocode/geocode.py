import json
import logging
import queue
import re
import sqlite3
import threading
from collections.abc import Callable
from enum import StrEnum
from pathlib import Path
from typing import Any

import pycountry
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import ArcGIS, Nominatim
from geopy.location import Location
from rapidfuzz import fuzz
from tqdm import tqdm

from geocode.geocode_constants import HARDCODED_GEOCODE_DATA
from helpers import batch_query_in, sqlite_connect

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
]
_UPDATE_EXCLUDE = {
    "geocode_query",
    "official_address",
    "loc_name",
    "match_id",
    "source",
    "error",
}

UPDATE_COLS = [k for k in FIELD_KEYS if k not in _UPDATE_EXCLUDE]
_VALID_TABLES = {"unified_centers", "unified_metadata"}


def geocode_missing(
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    cache_file: Path | None,
    save_every: int,
    rate_limit: float,
    retries: int,
    timeout: int,
    is_dry_run: bool,
    ror_data_file: Path | None = None,
    mode: str = "missing",
) -> None:
    missing = _find_missing(unified_metadata_db_file, mode)
    logger.info("found %d records with missing geocoded information", len(missing))

    if is_dry_run or not missing:
        return

    center_to_accessions: dict[str, list[str]] = {}
    for cn, acc in missing:
        center_to_accessions.setdefault(cn, []).append(acc)
    unique_centers = list(center_to_accessions)
    logger.info("unique centers to geocode: %d", len(unique_centers))

    hardcoded_map: dict[str, dict[str, Any]] = {}
    for key, name, country, cc, city, state, lat, lng in HARDCODED_GEOCODE_DATA:
        r = _base_result(name)
        r.update(
            {
                "place_name": name,
                "country": country,
                "country_code": cc,
                "city": city,
                "state": state,
                "latitude": lat,
                "longitude": lng,
                "short_label": f"{name}, {city}",
                "geocode_score": 1.0,
                "source": "hardcoded",
            }
        )
        hardcoded_map[key] = r

    hardcoded_centers: dict[str, dict[str, Any]] = {}
    remaining_centers: list[str] = []
    for cn in unique_centers:
        key = cn.strip().lower()
        if key in hardcoded_map:
            hardcoded_centers[cn] = hardcoded_map[key]
        else:
            remaining_centers.append(cn)
    unique_centers = remaining_centers
    if hardcoded_centers:
        logger.info("hardcoded centers: %d", len(hardcoded_centers))

    ror_index = _load_ror_index(ror_data_file)
    geo_accessions = [
        acc
        for acc_list in center_to_accessions.values()
        for acc in acc_list
        if acc.startswith("GSE")
    ]

    geo_country_mapping: dict[str, str] = {}
    if geo_accessions:
        with sqlite_connect(str(geo_db_file)) as geo_conn:
            rows = batch_query_in(
                geo_conn,
                (
                    "SELECT accession, address_country "
                    "FROM geo_contributors WHERE accession"
                ),
                geo_accessions,
                500,
                "AND address_country IS NOT NULL GROUP BY accession",
            )

            for acc, country in rows:
                if not country:
                    continue
                try:
                    code = pycountry.countries.lookup(country).alpha_2.lower()
                except LookupError:
                    code = country.lower()
                geo_country_mapping[acc] = code

    cache = _load_cache(cache_file)
    cache_lock = threading.Lock()

    cached_centers = [cn for cn in unique_centers if cn in cache]
    uncached_centers = [cn for cn in unique_centers if cn not in cache]

    in_queue: queue.Queue = queue.Queue(maxsize=save_every * 2)
    stop_event = threading.Event()
    writer = threading.Thread(
        target=_writer_worker,
        args=(
            unified_metadata_db_file,
            in_queue,
            stop_event,
            save_every,
            cache,
            cache_file,
            cache_lock,
        ),
    )
    writer.start()

    updated = 0
    total = len(missing)

    try:
        with tqdm(total=total, desc="geocoding", unit="rec") as pbar:
            for center_name, result in hardcoded_centers.items():
                n = len(center_to_accessions[center_name])
                in_queue.put((center_name, result))
                updated += n
                pbar.update(n)

            for center_name in cached_centers:
                n = len(center_to_accessions[center_name])
                in_queue.put((center_name, cache[center_name]))
                updated += n
                pbar.update(n)

            if uncached_centers:
                api_centers: list[str] = []

                for center_name in uncached_centers:
                    accessions = center_to_accessions[center_name]
                    n = len(accessions)
                    result = _geocode_from_ror(
                        center_name,
                        ror_index,
                        accessions,
                        geo_country_mapping,
                    )
                    if result is not None:
                        with cache_lock:
                            cache[center_name] = result
                        in_queue.put((center_name, result))
                        updated += n
                        pbar.update(n)
                    else:
                        api_centers.append(center_name)

                if api_centers:
                    logger.info("geocoding %d centers via ArcGIS API", len(api_centers))
                    arcgis_geocoder = RateLimiter(
                        ArcGIS(timeout=timeout).geocode,
                        min_delay_seconds=max(rate_limit // 10, 1),
                        max_retries=retries,
                    )
                    osm_geocoder = RateLimiter(
                        Nominatim(
                            user_agent="seqout-ingestion-v2",
                            timeout=timeout,
                        ).geocode,
                        min_delay_seconds=max(rate_limit, 1),
                        max_retries=retries,
                    )

                    for center_name in api_centers:
                        accessions = center_to_accessions[center_name]
                        n = len(accessions)
                        result = _geocode_item(
                            center_name,
                            accessions,
                            geo_country_mapping,
                            osm_geocoder,
                            arcgis_geocoder,
                        )
                        if result is None:
                            pbar.update(n)
                            continue

                        with cache_lock:
                            cache[center_name] = result
                        in_queue.put((center_name, result))
                        updated += n
                        pbar.update(n)

    finally:
        in_queue.put(None)
        writer.join()

    logger.info("done: %d/%d records geocoded successfully", updated, total)


def _load_ror_index(ror_data_file: Path | None) -> dict[str, list[dict[str, Any]]]:
    if ror_data_file is None or not ror_data_file.exists():
        return {}

    logger.info("loading ror index from %s", ror_data_file)
    with ror_data_file.open() as f:
        records = json.load(f)

    index: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        locs = record.get("locations", [])
        if not locs:
            continue
        details = locs[0].get("geonames_details")
        if not details:
            continue

        names = record.get("names", [])
        ror_name = _first(
            *(n["value"] for n in names if "ror_display" in n.get("types", [])),
            *(n["value"] for n in names if "label" in n.get("types", [])),
        )

        for name_entry in names:
            value = name_entry.get("value")
            if not value:
                continue
            key = value.strip().lower()
            if not key:
                continue

            index.setdefault(key, []).append(
                {
                    "ror_name": ror_name,
                    "ror_id": record.get("id"),
                    "geonames": details,
                    "name_value": value,
                    "name_types": name_entry.get("types", []),
                },
            )

    logger.info("ror index built with %d unique keys", len(index))
    return index


def _writer_worker(
    db_file: Path,
    in_queue: queue.Queue,
    stop_event: threading.Event,
    save_every: int,
    cache: dict[str, dict[str, Any]],
    cache_file: Path | None,
    cache_lock: threading.Lock,
) -> None:
    conn = sqlite_connect(str(db_file))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -8000")
    conn.execute("PRAGMA temp_store = MEMORY")

    batch: list[tuple[str, dict[str, Any]]] = []

    def flush() -> None:
        if not batch:
            return
        _update_many(conn, "unified_centers", batch)
        _update_many(conn, "unified_metadata", batch)
        conn.commit()
        batch.clear()

    while True:
        try:
            item = in_queue.get(timeout=1)
            if item is None:
                break
            batch.append(item)
            if len(batch) >= save_every:
                flush()
        except queue.Empty:
            if stop_event.is_set():
                break

    flush()
    with cache_lock:
        _save_cache(cache_file, cache)
    conn.close()


def _geocode_item(
    center_name: str,
    accessions: list[str],
    geo_country_mapping: dict[str, str],
    osm_geocoder: RateLimiter,
    arcgis_geocoder: RateLimiter | None = None,
) -> dict[str, Any] | None:
    cleaned = _clean_text(center_name)
    if not cleaned:
        return None

    result = _geocode(
        cleaned, arcgis_geocoder, _parse_arcgis, GeocodingProviders.Arcgis
    )
    if result is None or result.get("error"):
        result = _geocode(
            cleaned, osm_geocoder, _parse_osm, GeocodingProviders.Nominatim
        )
    if result is None:
        result = _empty_result(cleaned, "no result found")

    geo_accessions = [a for a in accessions if a.startswith("GSE")]
    geo_countries = {
        geo_country_mapping[a] for a in geo_accessions if a in geo_country_mapping
    }
    if result.get("error") is None and geo_countries:
        geo_country = next(iter(geo_countries))
        result_country_code = (result.get("country_code") or "").lower()
        if result_country_code == geo_country:
            result["country"] = geo_country
        else:
            return None

    return result


def _geocode(
    query: str,
    geocoder: RateLimiter | None,
    parse_fn: Callable[[str, Location | None], dict[str, Any]],
    source: GeocodingProviders,
) -> dict[str, Any] | None:
    if geocoder is None:
        return None

    kwargs: dict[str, Any] = {}
    if source == GeocodingProviders.Nominatim:
        kwargs = {
            "addressdetails": True,
            "extratags": True,
            "namedetails": True,
            "language": "en",
        }

    try:
        location = geocoder(query, **kwargs)
    except Exception as e:
        logger.debug("%s geocoding failed for '%s': %s", source.value, query, e)
        return None

    if location is None:
        return None

    return parse_fn(query, location)


def _score_ror_match(
    cname: str,
    entry: dict[str, Any],
    country_set: set[str] | None = None,
) -> int:
    ror_name = (entry.get("ror_name") or "").lower()
    name_value = (entry.get("name_value") or "").lower()
    name_types = entry.get("name_types", [])
    country_code = (entry.get("geonames", {}).get("country_code") or "").lower()

    score = int(0.5 * fuzz.WRatio(cname, ror_name, processor=None))

    if name_value == cname:
        score += 30
        if "acronym" in name_types:
            score += 20

    if country_set and country_code in country_set:
        score += 20

    return score


def _geocode_from_ror(
    center_name: str,
    ror_index: dict[str, list[dict[str, Any]]] | None,
    accessions: list[str] | None = None,
    geo_country_mapping: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    if not ror_index:
        return None

    cleaned = _clean_text(center_name)
    if not cleaned:
        return None

    key = cleaned.lower()
    matches = ror_index.get(key)

    if not matches:
        return None

    geo_accessions = [a for a in (accessions or []) if a.startswith("GSE")]
    country_set: set[str] = set()
    if geo_accessions and geo_country_mapping:
        country_set = {
            geo_country_mapping[a] for a in geo_accessions if a in geo_country_mapping
        }

    if len(country_set) == 1:
        preferred = next(iter(country_set))
        country_matches = [
            e
            for e in matches
            if (e.get("geonames", {}).get("country_code") or "").lower() == preferred
        ]
        if country_matches:
            matches = country_matches

    if len(matches) == 1:
        result = _parse_ror(center_name, matches[0])
    else:
        scored = [(e, _score_ror_match(key, e, country_set)) for e in matches]
        scored.sort(key=lambda x: -x[1])
        top_score = scored[0][1]
        if not country_set and all(s == top_score for _, s in scored):
            return None
        result = _parse_ror(center_name, scored[0][0])

    if country_set:
        result_country = (result.get("country_code") or "").lower()
        if result_country not in country_set:
            return None

    return result


def _parse_ror(query: str, entry: dict[str, Any]) -> dict[str, Any]:
    details = entry["geonames"]
    ror_name = entry.get("ror_name")

    country_code = (details.get("country_code") or "").lower()

    try:
        country_obj = pycountry.countries.get(alpha_2=country_code.upper())
        country_name = country_obj.name if country_obj else details.get("country_name")
    except LookupError:
        country_name = details.get("country_name")

    location_name = details.get("name")
    state_name = details.get("country_subdivision_name")

    return _base_result(query) | {
        "official_address": ror_name,
        "place_name": ror_name,
        "short_label": _first(
            f"{ror_name}, {location_name}" if ror_name and location_name else None,
            f"{ror_name}, {state_name}" if ror_name and state_name else None,
            ror_name,
        ),
        "loc_name": ror_name,
        "longitude": details.get("lng"),
        "latitude": details.get("lat"),
        "city": location_name,
        "state": state_name,
        "country_code": country_code,
        "country": country_name,
        "geocode_score": 1.0,
        "source": "ror",
    }


def _parse_osm(query: str, location: Location | None) -> dict[str, Any]:
    if location is None:
        return _empty_result(query, error_msg="No result found")

    raw = location.raw
    addr = raw.get("address", {}) or {}
    extra = raw.get("extratags", {}) or {}
    names = raw.get("namedetails", {}) or {}

    place_name = _first(names.get("name:en"), names.get("name"), raw.get("name"))

    city = _first(
        addr.get("city"),
        addr.get("town"),
        addr.get("village"),
        addr.get("municipality"),
    )
    state = addr.get("state")

    place_type = raw.get("type")
    if place_type == "yes":
        place_type = None

    return _base_result(query) | {
        "official_address": raw.get("display_name"),
        "place_name": place_name,
        "place_type": place_type,
        "short_label": _first(
            f"{place_name}, {state}" if place_name and state else None,
            f"{place_name}, {city}" if place_name and city else None,
            place_name,
        ),
        "loc_name": _first(extra.get("loc_name"), names.get("name")),
        "address_type": _first(raw.get("addresstype"), raw.get("class")),
        "osm_id": raw.get("osm_id"),
        "osm_type": raw.get("osm_type"),
        "place_id": raw.get("place_id"),
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
        "geocode_score": raw.get("importance"),
        "source": "osm",
    }


def _parse_arcgis(query: str, location: Location | None) -> dict[str, Any]:
    if location is None:
        return _empty_result(query, error_msg="No result found")

    raw = location.raw
    attrs = raw.get("attributes", {}) or {}
    addr = raw.get("address", "")
    score = raw.get("score") or attrs.get("Score")
    loc = raw.get("location", {}) or {}

    place_name = attrs.get("PlaceName") or addr

    city = attrs.get("City") or attrs.get("Subregion")
    state = attrs.get("Region")
    country = attrs.get("CntryName")
    country_code = attrs.get("Country")

    return _base_result(query) | {
        "official_address": attrs.get("LongLabel") or attrs.get("Match_addr") or addr,
        "place_name": place_name,
        "place_type": attrs.get("Type") or attrs.get("Addr_type"),
        "short_label": _first(
            attrs.get("ShortLabel"),
            attrs.get("Match_addr"),
            attrs.get("LongLabel"),
            addr,
            place_name,
        ),
        "loc_name": place_name,
        "address_type": attrs.get("Addr_type"),
        "longitude": loc.get("x") or attrs.get("X"),
        "latitude": loc.get("y") or attrs.get("Y"),
        "city": city,
        "district": attrs.get("District"),
        "subdistrict": attrs.get("SubAddr"),
        "county": None,
        "suburb": attrs.get("Nbrhd"),
        "state": state,
        "state_district": None,
        "territory": attrs.get("Territory"),
        "metro_area": attrs.get("MetroArea"),
        "postcode": attrs.get("Postal"),
        "country_code": country_code.lower() if country_code else None,
        "country": country,
        "geocode_score": score / 100 if score else None,
        "source": "arcgis",
    }


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
    db_file: Path,
    mode: str = "missing",
) -> list[tuple[str, str]]:
    conn = sqlite_connect(str(db_file))
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_um_center ON unified_metadata(center_name)",
    )

    try:
        if mode == "all":
            query = (
                "SELECT DISTINCT center_name, canonical_accession FROM unified_centers "
                "WHERE (center_name IS NOT NULL AND center_name != '')"
            )
        else:
            query = (
                "SELECT DISTINCT center_name, canonical_accession FROM unified_centers "
                "WHERE (center_name IS NOT NULL AND center_name != '') "
                "AND (latitude IS NULL OR longitude IS NULL)"
            )

        return conn.execute(query).fetchall()
    finally:
        conn.close()


def _update_many(
    conn: sqlite3.Connection,
    table: str,
    items: list[tuple[str, dict[str, Any]]],
) -> None:
    if table not in _VALID_TABLES:
        msg = f"Invalid table name: {table}"
        raise ValueError(msg)

    set_clauses = ", ".join(f"{c} = ?" for c in UPDATE_COLS)
    sql = (
        f"UPDATE {table} SET {set_clauses} "  # noqa: S608
        "WHERE center_name = ?"
    )
    params = [
        [result.get(c) for c in UPDATE_COLS] + [center_name]
        for center_name, result in items
    ]

    conn.executemany(sql, params)


def _clean_text(text: str | None) -> str | None:
    if text is None or text in {"", "NA"}:
        return None

    text = re.sub(r'\\+"|"{2,}', "", text)
    text = re.sub(r'["\u201C\u201D\u2018\u2019`]', "", text)
    text = re.sub(r"[\r\n\t]|\\[nrt]", " ", text)
    text = re.sub(r"[\[\]|<>{}]", "", text)
    text = re.sub(r"^\s*\(|\)\s*$", "", text)
    text = re.sub(r"^[^a-zA-Z0-9]+", "", text)
    text = re.sub(r",\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    text = text.split(",")[-1].strip()

    return text or None


def _first(*values: Any) -> Any:
    for v in values:
        if v is not None and v != "":
            return v
    return None


def _base_result(query: str) -> dict[str, Any]:
    return dict.fromkeys(FIELD_KEYS, None) | {"geocode_query": query}


def _empty_result(query: str, error_msg: str = "Not attempted") -> dict[str, Any]:
    result = _base_result(query)
    result["error"] = error_msg
    return result
