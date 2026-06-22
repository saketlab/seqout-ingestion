import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, LiteralString, Optional, cast

import psycopg
import pycountry
from psycopg.types.json import Jsonb
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

_GEO_SERIES_SELECT_COLS = [
    "accession",
    "overall_design",
    "summary",
    "title",
    "pubmed_id",
    "samples_ref",
    "series_type",
    "relation",
    "supplementary_data",
    "published_at",
    "updated_at",
    "pubmed_id_pysradb",
    "pubmed_last_checked",
]
_GEO_SERIES_INSERT_COLS = [
    *_GEO_SERIES_SELECT_COLS,
    "bioproject_id",
    "organisms",
    "alias",
    "countries",
    "contributors",
    "departments",
    "organizations",
    "countries_iso",
    "library_strategies",
    "instrument_models",
    "alias_all",
    "platforms",
]
_GEO_GEOCODE_COLS = [
    "organization",
    "department",
    "address_line",
    "address_city",
    "address_state",
    "address_zipcode",
    "address_country",
    "n_series",
    "full_address",
    "address_key",
    "geo_query",
    "geo_formatted_address",
    "geo_place_name",
    "geo_place_type",
    "geo_address_type",
    "geo_longitude",
    "geo_latitude",
    "geo_city",
    "geo_district",
    "geo_subdistrict",
    "geo_county",
    "geo_suburb",
    "geo_neighbourhood",
    "geo_state",
    "geo_postcode",
    "geo_country",
    "geo_country_code",
    "geo_source",
    "geo_country_mismatch",
    "geo_error",
]


def pg_transfer(
    pg_url: str,
    sra_db_file: Path,
    geo_db_file: Path,
    unified_db_file: Path,
    num_workers: int,
    batch_size: int,
):
    table_info = {
        "geo_series": (
            geo_db_file,
            _geo_series_batch_modifier(sra_db_file, geo_db_file, unified_db_file),
            _GEO_SERIES_SELECT_COLS,
            _GEO_SERIES_INSERT_COLS,
        ),
        "geo_samples": (
            geo_db_file,
            _geo_sample_batch_modifier,
            None,
            None,
        ),
        "geo_contributors": (geo_db_file, _no_op_batch_modifier, None, None),
        "geo_platforms": (geo_db_file, _no_op_batch_modifier, None, None),
    }

    all_tasks = [
        (table_name, sqlite_db_file, modifier, select_cols, insert_cols)
        for table_name, (
            sqlite_db_file,
            modifier,
            select_cols,
            insert_cols,
        ) in table_info.items()
        if sqlite_db_file is not None and sqlite_db_file.exists()
    ]

    with (
        ThreadPoolExecutor(max_workers=num_workers) as pool,
        tqdm(
            total=len(all_tasks), desc="transferring tables", unit="table"
        ) as outer_pbar,
        logging_redirect_tqdm(),
    ):
        futures = {
            pool.submit(
                _transfer_helper,
                pg_url,
                sqlite_db_file,
                modifier,
                table_name,
                batch_size,
                select_cols,
                insert_cols,
            ): table_name
            for table_name, sqlite_db_file, modifier, select_cols, insert_cols in all_tasks
        }

        for f in as_completed(futures):
            f.result()
            outer_pbar.update(1)

    _transfer_geo_geocode(pg_url, geo_db_file, unified_db_file, batch_size)


def _get_table_info(conn: sqlite3.Connection, table_name: str) -> tuple[list[str], str]:
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()

    col_list = [row[1] for row in rows]
    pk = next(row[1] for row in rows if row[5] == 1)

    return col_list, pk


def _transfer_helper(
    pg_url: str,
    sqlite_db_file: Path,
    modifier: Callable[[list[dict]], list[dict]],
    table_name: str,
    batch_size: int,
    select_cols: Optional[list[str]] = None,
    insert_cols: Optional[list[str]] = None,
):
    with (
        sqlite3.connect(sqlite_db_file) as sqlite_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        sqlite_conn.row_factory = sqlite3.Row

        if select_cols:
            col_list = select_cols
        else:
            col_list, pk = _get_table_info(sqlite_conn, table_name)

        columns = ", ".join(col_list)
        placeholders = ", ".join(["%s"] * len(col_list))
        select_query = (
            f"SELECT {columns} FROM {table_name} LIMIT {batch_size} OFFSET {{offset}}"
        )

        if insert_cols:
            col_list = insert_cols
            columns = ", ".join(col_list)
            placeholders = ", ".join(["%s"] * len(col_list))

        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        total = int(
            sqlite_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        offset = 0

        with tqdm(
            desc=f"transferring {table_name}", total=total, unit="rows", leave=False
        ) as pbar:
            while True:
                batch = sqlite_conn.execute(
                    select_query.format(offset=offset)
                ).fetchall()
                if not batch:
                    break

                modified_batch = modifier([dict(row) for row in batch])
                modified_batch = [tuple(m[c] for c in col_list) for m in modified_batch]

                with pg_conn.cursor() as cur:
                    cur.executemany(cast(LiteralString, insert_query), modified_batch)

                pg_conn.commit()
                offset += batch_size
                pbar.update(len(batch))


def _no_op_batch_modifier(batch: list[dict]) -> list[dict]:
    return batch


def _geo_sample_batch_modifier(batch: list[dict]) -> list[dict]:
    for value in batch:
        channels = _ensure_list(_convert_to_json(value["channels"]))
        supp = _ensure_list(_convert_to_json(value["supplementary_data"]))
        value["channels"] = [Jsonb(x) for x in channels]
        value["supplementary_data"] = [Jsonb(x) for x in supp]
    return batch


def _geo_series_batch_modifier(
    sra_db_file: Path,
    geo_db_file: Path,
    unified_db_file: Path,
) -> Callable[[list[dict]], list[dict]]:
    def modifier(batch: list[dict]) -> list[dict]:
        _conn_cache: dict[str, sqlite3.Connection] = {}

        def _get_conn(path: Path) -> sqlite3.Connection:
            key = str(path)
            if key not in _conn_cache:
                _conn_cache[key] = sqlite3.connect(path)
            return _conn_cache[key]

        row_meta: list[dict] = []
        all_samples: list[str] = []
        alias_set: set[str] = set()
        all_accessions: list[str] = []

        for value in batch:
            accession = value["accession"]
            relations = _ensure_list(_convert_to_json(value["relation"]))
            samples_ref = _ensure_list(_convert_to_json(value["samples_ref"]))
            supp = _ensure_list(_convert_to_json(value["supplementary_data"]))

            samples = [v["@ref"] for v in samples_ref]
            alias = next(
                (
                    str(x["@target"]).split("?term=", 1)[-1]
                    for x in relations
                    if x["@type"] == "SRA"
                ),
                None,
            )

            row_meta.append(
                {
                    "accession": accession,
                    "alias": alias,
                    "samples": samples,
                    "relations": relations,
                    "supp": supp,
                }
            )

            all_samples.extend(samples)
            if alias:
                alias_set.add(alias)
            all_accessions.append(accession)

        sample_organisms: dict[str, set[str]] = {}
        sample_platforms: dict[str, list[str]] = {}

        if all_samples:
            unique_samples = list(set(all_samples))
            placeholders = ", ".join(["?"] * len(unique_samples))
            query = f"SELECT gs.accession, gp.organism, gs.platform_ref FROM geo_samples AS gs JOIN geo_platforms gp ON gs.platform_ref = gp.accession WHERE gs.accession IN ({placeholders})"
            conn = _get_conn(geo_db_file)

            for sample_acc, org_json, platform in conn.execute(
                query, unique_samples
            ).fetchall():
                org_parsed = json.loads(org_json)
                for item in org_parsed:
                    sample_organisms.setdefault(sample_acc, set[str]()).add(
                        item["#text"]
                    )
                sample_platforms.setdefault(sample_acc, []).append(platform)

        alias_strategies: dict[str, list[str]] = {}
        alias_models: dict[str, list[str]] = {}

        if alias_set:
            unique_aliases = list(alias_set)
            placeholders = ", ".join(["?"] * len(unique_aliases))
            query = f"SELECT study, library_strategy, instrument_model FROM sra_experiments WHERE study IN ({placeholders})"
            conn = _get_conn(sra_db_file)

            for study, strategy, model in conn.execute(
                query, unique_aliases
            ).fetchall():
                alias_strategies.setdefault(study, []).append(strategy)
                alias_models.setdefault(study, []).append(model)

        accession_alias_all: dict[str, list[str]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            placeholders = ", ".join(["?"] * len(unique_accessions))
            query = f"SELECT canonical_accession, aliases FROM unified_metadata WHERE canonical_accession IN ({placeholders})"
            conn = _get_conn(unified_db_file)

            for acc, aliases_json in conn.execute(query, unique_accessions).fetchall():
                aliases_data = json.loads(aliases_json)
                if aliases_data:
                    accession_alias_all[acc] = list(dict(aliases_data).values())

        accession_contributors: dict[str, dict[str, list[str]]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            placeholders = ", ".join(["?"] * len(unique_accessions))
            query = f"SELECT accession, person, department, organization, address_country FROM geo_contributors WHERE accession IN ({placeholders})"
            conn = _get_conn(geo_db_file)

            for acc, person, department, organization, country in conn.execute(
                query, unique_accessions
            ).fetchall():
                entry = accession_contributors.setdefault(
                    acc,
                    {
                        "contributors": [],
                        "departments": [],
                        "organizations": [],
                        "countries": [],
                        "countries_iso": [],
                    },
                )
                if person:
                    entry["contributors"].append(person)
                if department:
                    entry["departments"].append(department)
                if organization:
                    entry["organizations"].append(organization)
                if country:
                    entry["countries"].append(country)
                    iso = _normalize_country(country)
                    if iso:
                        entry["countries_iso"].append(iso)

        results: list[dict] = []
        for value, meta in zip(batch, row_meta):
            organisms = sorted(
                set[str]().union(
                    *(sample_organisms.get(s, set[str]()) for s in meta["samples"])
                )
            )
            platforms = sorted(
                {p for s in meta["samples"] for p in sample_platforms.get(s, [])}
            )

            library_strategies = (
                alias_strategies.get(meta["alias"], []) if meta["alias"] else []
            )
            instrument_models = (
                alias_models.get(meta["alias"], []) if meta["alias"] else []
            )

            alias_all = accession_alias_all.get(meta["accession"], [])
            contrib = accession_contributors.get(meta["accession"], {})

            bioproject_id = next(
                (
                    str(v["@target"]).split("/")[-1]
                    for v in meta["relations"]
                    if v["@type"] == "BioProject"
                ),
                None,
            )

            modified = dict(value)
            modified["pubmed_id"] = _convert_to_json(modified["pubmed_id"])
            modified["samples_ref"] = meta["samples"]
            modified["series_type"] = _convert_to_json(modified["series_type"])
            modified["relation"] = [Jsonb(x) for x in meta["relations"]]
            modified["supplementary_data"] = [Jsonb(x) for x in meta["supp"]]
            modified["bioproject_id"] = bioproject_id
            modified["alias"] = meta["alias"]
            modified["organisms"] = organisms
            if meta["alias"] and meta["alias"] not in alias_all:
                alias_all = [*alias_all, meta["alias"]]
            modified["alias_all"] = alias_all
            modified["library_strategies"] = library_strategies
            modified["instrument_models"] = instrument_models
            modified["platforms"] = platforms
            modified["countries"] = sorted(set(contrib.get("countries", [])))
            modified["countries_iso"] = sorted(set(contrib.get("countries_iso", [])))
            modified["contributors"] = contrib.get("contributors", [])
            modified["departments"] = contrib.get("departments", [])
            modified["organizations"] = contrib.get("organizations", [])

            results.append(modified)

        return results

    return modifier


def _transfer_geo_geocode(
    pg_url: str, geo_db_file: Path, unified_db_file: Path | None, batch_size: int
):
    with (
        sqlite3.connect(geo_db_file) as src_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_geo_geocode_address_key ON geo_geocode (address_key)"
            )

        center_lookup: dict[tuple[str, str], dict] = {}
        if unified_db_file and unified_db_file.exists():
            with sqlite3.connect(unified_db_file) as uc_conn:
                for row in uc_conn.execute(
                    "SELECT canonical_accession, center_name, place_name, place_type, address_type, longitude, latitude, city, district, subdistrict, county, suburb, neighbourhood, state, postcode, country_code, country, country_mismatch FROM unified_centers"
                ).fetchall():
                    (
                        acc,
                        name,
                        place_name,
                        place_type,
                        address_type,
                        lon,
                        lat,
                        city,
                        district,
                        subdistrict,
                        county,
                        suburb,
                        neighbourhood,
                        state,
                        postcode,
                        country_code,
                        country,
                        country_mismatch,
                    ) = row
                    center_lookup[(acc, name.strip().lower())] = {
                        "geo_place_name": place_name,
                        "geo_place_type": place_type,
                        "geo_address_type": address_type,
                        "geo_longitude": float(lon) if lon is not None else None,
                        "geo_latitude": float(lat) if lat is not None else None,
                        "geo_city": city,
                        "geo_district": district,
                        "geo_subdistrict": subdistrict,
                        "geo_county": county,
                        "geo_suburb": suburb,
                        "geo_neighbourhood": neighbourhood,
                        "geo_state": state,
                        "geo_postcode": postcode,
                        "geo_country_code": country_code,
                        "geo_country": country,
                        "geo_country_mismatch": country_mismatch.strip().lower()
                        in ("true", "1", "yes")
                        if country_mismatch
                        else None,
                    }

        rows = src_conn.execute(
            "SELECT DISTINCT organization, department, address_line, address_city, address_state, address_zipcode, address_country, accession FROM geo_contributors WHERE organization IS NOT NULL AND organization != ''"
        ).fetchall()

        groups: dict[str, dict] = {}
        for org, dept, addr, city, state, zipcode, country, acc in rows:
            org_s = org or ""
            dept_s = dept or ""
            addr_s = addr or ""
            city_s = city or ""
            state_s = state or ""
            country_s = country or ""

            key = f"{org_s}|{dept_s}|{addr_s}|{city_s}|{state_s}|{country_s}"

            if key not in groups:
                parts = [
                    p for p in [org_s, dept_s, addr_s, city_s, state_s, country_s] if p
                ]
                groups[key] = {
                    "organization": org_s,
                    "department": dept_s,
                    "address_line": addr_s,
                    "address_city": city_s,
                    "address_state": state_s,
                    "address_zipcode": zipcode or "",
                    "address_country": country_s,
                    "full_address": ", ".join(parts),
                    "address_key": key,
                    "geo_query": ", ".join(parts),
                    "geo_formatted_address": None,
                    "geo_place_name": None,
                    "geo_place_type": None,
                    "geo_address_type": None,
                    "geo_longitude": None,
                    "geo_latitude": None,
                    "geo_city": None,
                    "geo_district": None,
                    "geo_subdistrict": None,
                    "geo_county": None,
                    "geo_suburb": None,
                    "geo_neighbourhood": None,
                    "geo_state": None,
                    "geo_postcode": None,
                    "geo_country": None,
                    "geo_country_code": None,
                    "geo_source": None,
                    "geo_country_mismatch": None,
                    "geo_error": None,
                    "accessions": set(),
                }
            groups[key]["accessions"].add(acc)

            center = center_lookup.get((acc, org_s.strip().lower()))
            if center:
                g = groups[key]
                for field in (
                    "geo_place_name",
                    "geo_place_type",
                    "geo_address_type",
                    "geo_longitude",
                    "geo_latitude",
                    "geo_city",
                    "geo_district",
                    "geo_subdistrict",
                    "geo_county",
                    "geo_suburb",
                    "geo_neighbourhood",
                    "geo_state",
                    "geo_postcode",
                    "geo_country_code",
                    "geo_country",
                    "geo_country_mismatch",
                ):
                    if center.get(field) is not None and g.get(field) is None:
                        g[field] = center[field]

                if g["geo_formatted_address"] is None and center.get("geo_place_name"):
                    g["geo_formatted_address"] = center["geo_place_name"]

                if g["geo_source"] is None:
                    g["geo_source"] = "unified"

        cols = ", ".join(_GEO_GEOCODE_COLS)
        placeholders = ", ".join(["%s"] * len(_GEO_GEOCODE_COLS))
        insert_query = f"INSERT INTO geo_geocode ({cols}) VALUES ({placeholders}) ON CONFLICT (address_key) DO NOTHING"

        batch: list[tuple] = []
        total = len(groups)

        with tqdm(desc="transferring geo_geocode", total=total, unit="rows") as pbar:
            for key, data in groups.items():
                batch.append(
                    tuple(
                        data[c] if c != "n_series" else len(data["accessions"])
                        for c in _GEO_GEOCODE_COLS
                    )
                )
                pbar.update(1)

                if len(batch) >= batch_size:
                    with pg_conn.cursor() as cur:
                        cur.executemany(cast(LiteralString, insert_query), batch)
                    pg_conn.commit()
                    batch = []

            if batch:
                with pg_conn.cursor() as cur:
                    cur.executemany(cast(LiteralString, insert_query), batch)
                pg_conn.commit()


def _normalize_country(name: str) -> str | None:
    if not name:
        return None

    s = name.strip()

    try:
        return pycountry.countries.lookup(s).alpha_3
    except LookupError:
        pass

    c = pycountry.countries.get(alpha_3=s.upper())
    if c:
        return c.alpha_3

    c = pycountry.countries.get(alpha_2=s.upper())
    if c:
        return c.alpha_3

    return s


def _ensure_list(value):
    if isinstance(value, dict):
        return [value]
    return value or []


def _convert_to_json(value: str):
    return json.loads(value) if value else None
