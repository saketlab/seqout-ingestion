import json
import logging
import queue
import re
import sqlite3
import threading
import unicodedata
from collections.abc import Callable
from enum import StrEnum
from pathlib import Path
from typing import Any

import psycopg
import pycountry
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import ArcGIS, Nominatim
from geopy.location import Location
from psycopg import sql as psycopg_sql
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

# Postgres columns that mirror unified_centers' own geocode fields. Postgres
# lacks short_label/osm_id/osm_type/place_id/county/suburb/neighbourhood/
# territory/metro_area, so a copy-back leaves those blank; harmless, since
# _find_missing only gates on latitude/longitude.
_PG_COPYBACK_COLS = [
    "place_name",
    "longitude",
    "latitude",
    "city",
    "district",
    "subdistrict",
    "state",
    "state_district",
    "postcode",
    "country_code",
    "country",
    "country_mismatch",
    "geocode_score",
]


def _pg_connect_or_none(pg_url: str | None) -> "psycopg.Connection | None":
    if not pg_url:
        return None
    try:
        return psycopg.connect(pg_url)
    except Exception:
        logger.warning(
            "could not reach postgres at pg_url; geocoding without a postgres"
            " cross-check (no copy-back, no new-ids restriction)",
        )
        return None


def _copy_back_from_pg(db_file: Path, pg_conn: "psycopg.Connection") -> int:
    """
    Fill locally-missing unified_centers rows from Postgres before calling ArcGIS.

    unified_metadata.db is a staging snapshot that never receives back what a
    prior geocode + transfer already wrote to Postgres, so a center geocoded
    months ago still reads as "missing" here.
    """
    conn = sqlite_connect(str(db_file))
    accs = [
        acc
        for (acc,) in conn.execute(
            "SELECT DISTINCT canonical_accession FROM unified_centers "
            "WHERE center_name IS NOT NULL AND center_name != '' "
            "AND (latitude IS NULL OR longitude IS NULL)",
        ).fetchall()
    ]
    if not accs:
        conn.close()
        return 0

    query = psycopg_sql.SQL(
        "SELECT canonical_accession, center_name, {cols} FROM unified_metadata"
        " WHERE canonical_accession = ANY(%s) AND latitude IS NOT NULL"
        " AND longitude IS NOT NULL AND center_name IS NOT NULL",
    ).format(
        cols=psycopg_sql.SQL(", ").join(
            psycopg_sql.Identifier(c) for c in _PG_COPYBACK_COLS
        ),
    )
    with pg_conn.cursor() as cur:
        cur.execute(query, (accs,))
        pg_rows = cur.fetchall()

    set_clause = ", ".join(f"{c} = ?" for c in _PG_COPYBACK_COLS)
    sql = (
        f"UPDATE unified_centers SET {set_clause} "  # noqa: S608
        "WHERE canonical_accession = ? AND center_name = ? "
        "AND (latitude IS NULL OR longitude IS NULL)"
    )
    params = [(*vals, acc, center_name) for acc, center_name, *vals in pg_rows]
    updated = conn.executemany(sql, params).rowcount
    conn.commit()
    conn.close()
    logger.info(
        "copied geocode data back from postgres for %d unified_centers rows",
        updated,
    )
    return updated


def _restrict_to_new_accessions(
    missing: list[tuple[str, str]],
    pg_conn: "psycopg.Connection",
) -> list[tuple[str, str]]:
    """
    After the copy-back, keep only centers tied to accessions Postgres has never seen.

    An accession already in Postgres without coordinates is pre-existing
    backlog, not new data from this ingest -- leave it for a dedicated
    backfill rather than blocking every nightly geocode on it.
    """
    accs = sorted({acc for _, acc in missing})
    if not accs:
        return missing
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT canonical_accession FROM unified_metadata"
            " WHERE canonical_accession = ANY(%s)",
            (accs,),
        )
        known = {r[0] for r in cur.fetchall()}
    return [(cn, acc) for cn, acc in missing if acc not in known]


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
    pg_url: str | None = None,
) -> None:
    pg_conn = _pg_connect_or_none(pg_url)
    if pg_conn is not None:
        _copy_back_from_pg(unified_metadata_db_file, pg_conn)

    missing = _find_missing(unified_metadata_db_file, mode)

    if pg_conn is not None and mode != "all":
        before = len(missing)
        missing = _restrict_to_new_accessions(missing, pg_conn)
        logger.info(
            "restricted to centers for accessions new to postgres: %d -> %d",
            before,
            len(missing),
        )

    if pg_conn is not None:
        pg_conn.close()

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
                    geo_result: dict[str, Any] | None = _geocode_from_ror(
                        center_name,
                        ror_index,
                        accessions,
                        geo_country_mapping,
                    )
                    if geo_result is None:
                        api_centers.append(center_name)
                        continue

                    with cache_lock:
                        cache[center_name] = geo_result
                    in_queue.put((center_name, geo_result))
                    updated += n
                    pbar.update(n)

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
                        geo_result = _geocode_item(
                            center_name,
                            accessions,
                            geo_country_mapping,
                            osm_geocoder,
                            arcgis_geocoder,
                        )
                        if geo_result is None:
                            pbar.update(n)
                            continue

                        with cache_lock:
                            cache[center_name] = geo_result
                        in_queue.put((center_name, geo_result))
                        updated += n
                        pbar.update(n)

    finally:
        in_queue.put(None)
        writer.join()

    logger.info("done: %d/%d records geocoded successfully", updated, total)


_ROR_ZENODO_API = (
    "https://zenodo.org/api/records?communities=ror-data&sort=newest&size=1"
)


def _fetch_ror_dump(dest: Path) -> bool:
    """
    Download the newest ROR dump to `dest`. Returns whether it is now present.
    """
    import io
    import urllib.request
    import zipfile

    try:
        with urllib.request.urlopen(_ROR_ZENODO_API, timeout=60) as r:  # noqa: S310
            hits = json.load(r)["hits"]["hits"]
        files = hits[0]["files"]
        zip_url = next(f["links"]["self"] for f in files if f["key"].endswith(".zip"))
        logger.info("fetching ROR dump %s", hits[0]["metadata"]["title"])
        with urllib.request.urlopen(zip_url, timeout=600) as r:  # noqa: S310
            blob = r.read()
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            # v2 schema only: _load_ror_index reads locations[].geonames_details
            # and names[].types, neither of which exists in the v1 layout
            name = next(
                n
                for n in zf.namelist()
                if n.startswith("v2") and n.endswith(".json") and "schema" not in n
            )
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(dest.suffix + ".partial")
            tmp.write_bytes(zf.read(name))
            tmp.replace(dest)
        logger.info("ROR dump written to %s", dest)
    except Exception as exc:
        logger.warning("could not fetch ROR dump: %s", exc)
        return False
    return dest.exists()


def _load_ror_index(ror_data_file: Path | None) -> dict[str, list[dict[str, Any]]]:
    if ror_data_file is None:
        logger.warning("no --ror-data-file: geocoding without institution matching")
        return {}

    if not ror_data_file.exists() and not _fetch_ror_dump(ror_data_file):
        logger.warning(
            "ROR data absent at %s and could not be fetched; every uncached "
            "centre will fall through to the address geocoder",
            ror_data_file,
        )
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

    # both providers answer anything; with no distinctive token nothing can
    # be checked against the reply
    if not _distinctive_tokens(cleaned):
        return _empty_result(cleaned, "no distinctive token to verify against")

    result = _geocode(
        cleaned, arcgis_geocoder, _parse_arcgis, GeocodingProviders.Arcgis
    )
    if result is None or result.get("error") or not _is_plausible(cleaned, result):
        result = _geocode(
            cleaned, osm_geocoder, _parse_osm, GeocodingProviders.Nominatim
        )
        # the fallback needs the same gate; it answers what ArcGIS rejected
        if (
            result is not None
            and not result.get("error")
            and not _is_plausible(cleaned, result)
        ):
            result = _empty_result(cleaned, "no plausible match")
    if result is None:
        result = _empty_result(cleaned, "no result found")

    geo_accessions = [a for a in accessions if a.startswith("GSE")]
    geo_countries = {
        geo_country_mapping[a] for a in geo_accessions if a in geo_country_mapping
    }
    if result.get("error") is None and geo_countries:
        if len(geo_countries) > 1:
            # a center used by studies in several countries cannot be resolved
            # from the GEO mapping; an arbitrary pick would be a coin toss
            return None
        geo_country = next(iter(geo_countries))
        result_country_code = (result.get("country_code") or "").lower()
        if result_country_code != geo_country:
            return None
        # geo_country is alpha-2; assigning it to `country` put "cn" in a column
        # that holds country NAMES. Confirm the code, keep the geocoder's name.
        result["country_code"] = geo_country

    return result


#: ArcGIS score floor.
_ARCGIS_MIN_SCORE = 0.85

#: Words shared by thousands of institutions. A candidate matching only these
#: has matched nothing: "Sanger Institute" and "Singer Institute" agree on
#: every token but the one that names the place.
_GENERIC_TOKENS = frozenset(
    {
        "the",
        "of",
        "for",
        "and",
        "de",
        "di",
        "du",
        "der",
        "la",
        "el",
        "university",
        "universite",
        "universitat",
        "universidad",
        "univ",
        "institute",
        "institut",
        "instituto",
        "institution",
        "inst",
        "college",
        "school",
        "faculty",
        "department",
        "dept",
        "division",
        "laboratory",
        "laboratories",
        "lab",
        "labs",
        "center",
        "centre",
        "hospital",
        "clinic",
        "medical",
        "research",
        "science",
        "sciences",
        "technology",
        "national",
        "international",
        "state",
        "academy",
        "foundation",
        "society",
        "group",
        "unit",
        "program",
        "programme",
        "ltd",
        "inc",
        "llc",
        "gmbh",
        "corp",
        # role words that read as distinctive but name no place
        "people",
        "peoples",
        "general",
        "project",
        "central",
        # a field of study identifies a subject, not a place
        # rarity cannot separate these: "acupuncture" is as rare as "wyoming"
        "agricultural",
        "agriculture",
        "animal",
        "biological",
        "biomedical",
        "cancer",
        "environmental",
        "fisheries",
        "food",
        "forestry",
        "health",
        "life",
        "marine",
        "molecular",
        "plant",
        "acupuncture",
        "anatomy",
        "biochemistry",
        "bioinformatics",
        "biology",
        "biotechnology",
        "cardiology",
        "chemistry",
        "dentistry",
        "dermatology",
        "ecology",
        "endocrinology",
        "engineering",
        "epidemiology",
        "genetics",
        "genomics",
        "gynecology",
        "haematology",
        "hematology",
        "immunology",
        "medicine",
        "microbiology",
        "neurology",
        "neuroscience",
        "nursing",
        "nutrition",
        "obstetrics",
        "oncology",
        "ophthalmology",
        "orthopedics",
        "parasitology",
        "pathology",
        "pediatrics",
        "pharmacology",
        "pharmacy",
        "physiology",
        "psychiatry",
        "radiology",
        "surgery",
        "toxicology",
        "urology",
        "veterinary",
        "virology",
        "zoology",
        "co",
        "company",
    }
)


#: below this a token is an initial or a stray fragment, not a name
_MIN_TOKEN_LEN = 3


#: Letters NFKD cannot decompose: a stroke or ligature is part of the glyph,
#: not a combining mark, so "Wroclaw" never matched "Wroclaw" without this.
_STROKED = str.maketrans(
    {"ł": "l", "ø": "o", "đ": "d", "ð": "d", "þ": "th", "ß": "ss", "æ": "ae", "œ": "oe"}
)


def _distinctive_tokens(text: str) -> set[str]:
    # \w not [a-z0-9]: the ASCII class yields an empty set for CJK names
    # the length floor applies only to ASCII; a short CJK name is a whole word
    # fold diacritics first; archives and geocoders disagree on accents
    folded = "".join(
        ch
        for ch in unicodedata.normalize("NFKD", text.lower().translate(_STROKED))
        if not unicodedata.combining(ch)
    )
    tokens = re.findall(r"\w+", folded, re.UNICODE)
    return {
        t
        for t in tokens
        if t not in _GENERIC_TOKENS and (len(t) >= _MIN_TOKEN_LEN or not t.isascii())
    }


#: Shortest real country name: Chad, Cuba, Iran, Mali, Oman, Togo.
_MIN_COUNTRY_NAME_LEN = 4

#: country names that are also a well-known place elsewhere, so naming one
#: claims nothing: Georgia is usually the US state, Jersey is New Jersey
_AMBIGUOUS_COUNTRY_WORDS = frozenset({"georgia", "jersey"})

#: nationality adjectives that name a country as reliably as the country does
#: "british" is absent: it collides with British Columbia
_NATIONALITY = {
    "chinese": "cn",
    "german": "de",
    "indian": "in",
    "spanish": "es",
    "french": "fr",
    "italian": "it",
    "japanese": "jp",
    "korean": "kr",
    "norwegian": "no",
    "swedish": "se",
    "danish": "dk",
    "finnish": "fi",
    "dutch": "nl",
    "belgian": "be",
    "swiss": "ch",
    "austrian": "at",
    "polish": "pl",
    "russian": "ru",
    "brazilian": "br",
    "mexican": "mx",
    "canadian": "ca",
    "australian": "au",
    "portuguese": "pt",
    "greek": "gr",
    "turkish": "tr",
    "israeli": "il",
    "egyptian": "eg",
    "thai": "th",
    "vietnamese": "vn",
    "indonesian": "id",
    "malaysian": "my",
    "czech": "cz",
    "hungarian": "hu",
}

#: Territories a claim of "cn" must not be judged against: each has its own
#: code, and "the Chinese University of Hong Kong" is genuinely in hk.
_COMPATIBLE = {"cn": {"cn", "hk", "mo", "tw"}}


#: cities whose name alone fixes the country, used only to veto a
#: contradicting answer, never to assign one
#: entries must be unambiguous worldwide; Cambridge, Birmingham, Springfield,
#: Victoria, San Jose, Santiago, Valencia, Alexandria, Hamilton and Richmond
#: are absent because each names cities in more than one country
_CITY_COUNTRY: dict[str, str] = {
    # China
    "beijing": "cn",
    "shanghai": "cn",
    "guangzhou": "cn",
    "shenzhen": "cn",
    "kunming": "cn",
    "chengdu": "cn",
    "wuhan": "cn",
    "hangzhou": "cn",
    "nanjing": "cn",
    "tianjin": "cn",
    "chongqing": "cn",
    "xiamen": "cn",
    "qingdao": "cn",
    "changchun": "cn",
    "harbin": "cn",
    "shenyang": "cn",
    "zhengzhou": "cn",
    "lanzhou": "cn",
    "urumqi": "cn",
    "hefei": "cn",
    "jinan": "cn",
    "suzhou": "cn",
    "ningbo": "cn",
    "wenzhou": "cn",
    "guiyang": "cn",
    "nanchang": "cn",
    "fuzhou": "cn",
    "taiyuan": "cn",
    # Japan
    "tokyo": "jp",
    "osaka": "jp",
    "kyoto": "jp",
    "nagoya": "jp",
    "sapporo": "jp",
    "fukuoka": "jp",
    "sendai": "jp",
    "tsukuba": "jp",
    "hiroshima": "jp",
    "okayama": "jp",
    "kanazawa": "jp",
    "yokohama": "jp",
    # Korea
    "seoul": "kr",
    "busan": "kr",
    "daejeon": "kr",
    "gwangju": "kr",
    "incheon": "kr",
    "daegu": "kr",
    # India
    "mumbai": "in",
    "bengaluru": "in",
    "kolkata": "in",
    "chennai": "in",
    "hyderabad": "in",
    "ahmedabad": "in",
    "lucknow": "in",
    "guwahati": "in",
    "bhubaneswar": "in",
    "thiruvananthapuram": "in",
    "kanpur": "in",
    "kharagpur": "in",
    "varanasi": "in",
    "chandigarh": "in",
    "pune": "in",
    # elsewhere, only where the name is globally unique
    "singapore": "sg",
    "helsinki": "fi",
    "stockholm": "se",
    "copenhagen": "dk",
    "oslo": "no",
    "reykjavik": "is",
    "warsaw": "pl",
    "krakow": "pl",
    "budapest": "hu",
    "bucharest": "ro",
    "istanbul": "tr",
    "ankara": "tr",
    "tehran": "ir",
    "riyadh": "sa",
    "nairobi": "ke",
    "kampala": "ug",
    "addis": "et",
    "johannesburg": "za",
    "pretoria": "za",
    "montevideo": "uy",
    "bogota": "co",
    "lima": "pe",
    "quito": "ec",
    "caracas": "ve",
    "reykjavík": "is",
    "ljubljana": "si",
    "zagreb": "hr",
    "bratislava": "sk",
    "vilnius": "lt",
    "tallinn": "ee",
    "riga": "lv",
    "minsk": "by",
    "tashkent": "uz",
    "almaty": "kz",
    "bangkok": "th",
    "hanoi": "vn",
    "jakarta": "id",
    "manila": "ph",
    "colombo": "lk",
    "kathmandu": "np",
    "dhaka": "bd",
    "islamabad": "pk",
    "karachi": "pk",
    "lahore": "pk",
    "edinburgh": "gb",
    "glasgow": "gb",
    "manchester": "gb",
    "liverpool": "gb",
    "amsterdam": "nl",
    "rotterdam": "nl",
    "utrecht": "nl",
    "leuven": "be",
    "heidelberg": "de",
    "leipzig": "de",
    "dresden": "de",
    "freiburg": "de",
    "munich": "de",
    "hamburg": "de",
    "stuttgart": "de",
    "gottingen": "de",
    "marseille": "fr",
    "toulouse": "fr",
    "montpellier": "fr",
    "grenoble": "fr",
    "barcelona": "es",
    "sevilla": "es",
    "zaragoza": "es",
    "milano": "it",
    "torino": "it",
    "bologna": "it",
    "napoli": "it",
    "lisboa": "pt",
    "porto": "pt",
    "vienna": "at",
    "salzburg": "at",
    "zurich": "ch",
    "geneva": "ch",
    "lausanne": "ch",
    "basel": "ch",
}


def _claimed_cities(query: str) -> set[str]:
    """Return country codes implied by a city the centre name states outright."""
    return {
        _CITY_COUNTRY[t]
        for t in re.findall(r"[a-z]+", query.lower())
        if t in _CITY_COUNTRY
    }


def _claimed_countries(query: str) -> set[str]:
    """Country codes the centre name states outright."""
    claimed = set()
    tokens = re.findall(r"[a-z]+", query.lower())
    for i, token in enumerate(tokens):
        # "New Mexico" is not Mexico and "New Jersey" is not Jersey; the
        # preceding word makes it a different place entirely.
        if i and tokens[i - 1] == "new":
            continue
        if token in _AMBIGUOUS_COUNTRY_WORDS:
            continue
        if token in _NATIONALITY:
            claimed.add(_NATIONALITY[token])
            continue
        # whole names only; pycountry resolves "am" to Armenia, "in" to India
        if len(token) < _MIN_COUNTRY_NAME_LEN:
            continue
        try:
            claimed.add(pycountry.countries.lookup(token).alpha_2.lower())
        except LookupError:
            continue
    return claimed


def _contradicts_named_country(query: str, result: dict[str, Any]) -> bool:
    """Whether the answer's country conflicts with one the centre name states."""
    # a stated city is evidence of the same kind as a stated country
    # unioned, so a name mentioning both need only satisfy the set
    claimed = _claimed_countries(query) | _claimed_cities(query)
    code = (result.get("country_code") or "").lower()
    if not claimed or not code:
        return False
    allowed = set()
    for c in claimed:
        allowed |= _COMPATIBLE.get(c, {c})
    return code not in allowed


def _ror_key(name: str) -> str:
    """Comparison form of an institution name: no leading article, no punctuation."""
    key = re.sub(r"^the\s+", "", (name or "").strip().lower())
    key = re.sub(r"[^a-z0-9 ]+", " ", key)
    return re.sub(r"\s+", " ", key).strip()


def ror_label_index(index: dict | None) -> dict[str, str]:
    """
    Build {normalised label name: country code} for the veto below.

    Label names only; ROR's acronyms map JGI to Tanzania and NIH to Thailand.
    A name resolving to more than one country is dropped.
    """
    codes: dict[str, set[str]] = {}
    for entries in (index or {}).values():
        for entry in entries:
            types = entry.get("name_types") or []
            if "acronym" in types or not ({"label", "ror_display"} & set(types)):
                continue
            key = _ror_key(entry.get("name_value") or "")
            code = ((entry.get("geonames") or {}).get("country_code") or "").lower()
            if key and code:
                codes.setdefault(key, set()).add(code)
    return {k: next(iter(v)) for k, v in codes.items() if len(v) == 1}


def _ror_label_country(query: str, label_index: dict | None) -> str | None:
    """Look a centre's full name up in the label index, or None."""
    if not label_index or not query:
        return None
    cleaned = query.strip()
    # An acronym cannot be verified this way, whichever side it appears on.
    if " " not in cleaned or cleaned.isupper():
        return None
    return label_index.get(_ror_key(cleaned))


def _contradicts_ror_country(
    query: str, result: dict[str, Any], index: dict | None
) -> bool:
    """Whether an independent registry places this centre in another country."""
    expected = _ror_label_country(query, index)
    if not expected:
        return False
    actual = (result.get("country_code") or "").lower()
    if not actual:
        return False
    return actual not in _COMPATIBLE.get(expected, {expected})


def _is_plausible(
    query: str, result: dict[str, Any], ror_index: dict | None = None
) -> bool:
    """Whether a geocoder candidate answers the question that was asked."""
    score = result.get("geocode_score")
    if score is not None and score < _ARCGIS_MIN_SCORE:
        return False

    candidate = _first(
        result.get("place_name"),
        result.get("loc_name"),
        result.get("short_label"),
        result.get("official_address"),
    )
    if not candidate:
        return False

    wanted = _distinctive_tokens(query)
    if not wanted:
        # nothing but generic words to match on; no provider can be verified
        return False

    if wanted & _distinctive_tokens(str(candidate)):
        if _contradicts_named_country(query, result):
            return False
        return not _contradicts_ror_country(query, result, ror_index)

    logger.debug(
        "arcgis rejected for '%s': got '%s' (score %s)", query, candidate, score
    )
    return False


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
    elif source == GeocodingProviders.Arcgis:
        kwargs = {"out_fields": "*"}

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


def _geocode_from_ror(  # noqa: PLR0911
    # separate guard clauses, so the rejecting check stays visible
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

    # ROR answers bypassed the plausibility gate entirely, because
    # ROR runs before _geocode_item and _score_ror_match rewards acronym
    # matches, so hold it to the same plausibility gate as the APIs
    if not _is_plausible(cleaned, result):
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
        "country_code": _canonical_country(country_code, country_name)[0],
        "country": _canonical_country(country_code, country_name)[1],
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
        "country_code": _canonical_country(
            addr.get("country_code"), addr.get("country")
        )[0],
        # Nominatim answers in the local language ("Suisse", "Brasil"), so the
        # canonical English name replaces it here rather than downstream.
        "country": _canonical_country(addr.get("country_code"), addr.get("country"))[1],
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
        "country_code": _canonical_country(country_code, country)[0],
        "country": _canonical_country(country_code, country)[1],
        "geocode_score": score / 100 if score else None,
        "source": "arcgis",
    }


def _canonical_country(
    code: str | None, name: str | None
) -> tuple[str | None, str | None]:
    """Reduce a provider's country to one canonical (alpha-2 lower, English name)."""
    country = None
    if code:
        code = code.strip()
        country = (
            pycountry.countries.get(alpha_2=code.upper())
            if len(code) == 2  # noqa: PLR2004
            else pycountry.countries.get(alpha_3=code.upper())
        )
    if country is None and name:
        try:
            country = pycountry.countries.lookup(name.strip())
        except LookupError:
            country = None
    if country is None:
        # Nothing resolvable: keep the provider's own strings rather than
        # discard evidence, but do not invent a code.
        return (code.lower() if code else None), name
    return country.alpha_2.lower(), country.name


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
