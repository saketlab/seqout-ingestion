import json
import sqlite3
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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
_GEO_GEOCODE_NONE_FIELDS = [
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
    *_GEO_GEOCODE_NONE_FIELDS,
]


_SRA_STUDIES_SELECT_COLS = [
    "accession",
    "alias",
    "title",
    "abstract",
    "submission",
    "study_type",
    "published_at",
    "updated_at",
    "external_id",
    "attributes_json",
    "links",
    "center_name",
    "center_project_name",
    "pubmed_id_pysradb",
    "pubmed_last_checked",
]
_SRA_STUDIES_INSERT_COLS = [
    *_SRA_STUDIES_SELECT_COLS,
    "organisms",
    "bioproject_id",
    "countries",
    "library_strategies",
    "instrument_models",
    "platforms",
]
_SRA_GEOCODE_SOURCE_FIELDS = [
    "place_name",
    "place_type",
    "short_label",
    "address_type",
    "osm_id",
    "osm_type",
    "place_id",
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
]
_SRA_GEOCODE_COLS = [
    "center_name",
    "original_country",
    "formatted_address",
    "official_address",
    *_SRA_GEOCODE_SOURCE_FIELDS,
    "source",
    "error",
    "multiple_institutes",
]

_AE_EXPERIMENTS_SELECT_COLS = [
    "accession",
    "title",
    "organism",
    "description",
    "study_type",
    "release_date",
    "submission_date",
    "experimental_factors",
    "experimental_factor_types",
    "contact_name",
    "contact_email",
    "contact_affiliation",
    "pubmed_id",
    "pubmed_id_pysradb",
    "pubmed_last_checked",
    "protocol_names",
    "protocol_types",
    "attributes_json",
    "publication_title",
    "publication_authors",
    "publication_doi",
    "external_links",
    "processed_data_files",
    "experimental_factors_detail",
    "authors",
    "organizations",
    "protocols_description",
    "assay_count",
    "technology",
    "assay_by_molecule",
    "minseqe_score",
]
_AE_EXPERIMENTS_INSERT_COLS = [
    *_AE_EXPERIMENTS_SELECT_COLS,
    "organisms",
    "organizations_text",
    "authors_text",
    "alias",
    "countries",
]


type _Modifier = Callable[[list[dict]], list[dict]]


@dataclass
class _TableInfo:
    sqlite_db_file: Path
    modifier: _Modifier
    select_cols: list[str] | None = None
    insert_cols: list[str] | None = None
    alternative_table_name: str | None = None


def pg_transfer(
    pg_url: str,
    sra_db_file: Path,
    geo_db_file: Path,
    unified_metadata_db_file: Path,
    download_links_db_file: Path,
    ae_db_file: Path,
    num_workers: int,
    batch_size: int,
):
    table_info = {
        "geo_series": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_geo_series_batch_modifier(
                sra_db_file, geo_db_file, unified_metadata_db_file
            ),
            select_cols=_GEO_SERIES_SELECT_COLS,
            insert_cols=_GEO_SERIES_INSERT_COLS,
        ),
        "geo_samples": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_geo_sample_batch_modifier,
        ),
        "geo_platforms": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_no_op_batch_modifier,
        ),
        "geo_contributors": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_no_op_batch_modifier,
        ),
        "sra_studies": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_sra_studies_batch_modifier(sra_db_file, unified_metadata_db_file),
            select_cols=_SRA_STUDIES_SELECT_COLS,
            insert_cols=_SRA_STUDIES_INSERT_COLS,
        ),
        "sra_submissions": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_no_op_batch_modifier,
        ),
        "sra_experiments": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_sra_experiments_batch_modifier,
        ),
        "sra_samples": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_sra_samples_batch_modifier,
        ),
        "sra_runs": _TableInfo(
            sqlite_db_file=sra_db_file, modifier=_no_op_batch_modifier
        ),
        "run_download_links": _TableInfo(
            sqlite_db_file=download_links_db_file,
            modifier=_sra_run_download_links_modifier,
            alternative_table_name="sra_run_download_links",
        ),
        "arrayexpress_experiments": _TableInfo(
            sqlite_db_file=ae_db_file,
            modifier=_arrayexpress_experiments_batch_modifier(unified_metadata_db_file),
            select_cols=_AE_EXPERIMENTS_SELECT_COLS,
            insert_cols=_AE_EXPERIMENTS_INSERT_COLS,
        ),
        "arrayexpress_samples": _TableInfo(
            sqlite_db_file=ae_db_file,
            modifier=_arrayexpress_samples_batch_modifier,
        ),
    }

    all_tasks: list[tuple[str, Callable, tuple]] = []
    for table_name, info in table_info.items():
        if info.sqlite_db_file is not None and info.sqlite_db_file.exists():
            all_tasks.append(
                (
                    table_name,
                    _transfer_helper,
                    (
                        pg_url,
                        info.sqlite_db_file,
                        info.modifier,
                        table_name,
                        batch_size,
                        info.select_cols,
                        info.insert_cols,
                        info.alternative_table_name,
                    ),
                )
            )

    all_tasks.append(
        (
            "geo_geocode",
            _transfer_geo_geocode,
            (pg_url, geo_db_file, unified_metadata_db_file, batch_size),
        )
    )
    all_tasks.append(
        (
            "sra_geocode",
            _transfer_sra_geocode,
            (pg_url, sra_db_file, unified_metadata_db_file, batch_size),
        )
    )

    with (
        ThreadPoolExecutor(max_workers=num_workers) as pool,
        tqdm(
            total=len(all_tasks), desc="transferring tables", unit="table"
        ) as outer_pbar,
        logging_redirect_tqdm(),
    ):
        futures = {}
        for label, func, args in all_tasks:
            futures[pool.submit(func, *args)] = label

        for f in as_completed(futures):
            f.result()
            outer_pbar.update(1)


def _get_table_info(conn: sqlite3.Connection, table_name: str) -> tuple[list[str], str]:
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()

    col_list = [row[1] for row in rows]
    pk = next(row[1] for row in rows if row[5] == 1)

    return col_list, pk


def _execute_batch_inserts(
    pg_conn: psycopg.Connection,
    insert_query: str,
    rows,
    batch_size: int,
    desc: str,
    total: int | None = None,
    **tqdm_kwargs,
):
    batch: list[tuple] = []

    with tqdm(desc=desc, total=total, unit="rows", leave=False, **tqdm_kwargs) as pbar:
        for row in rows:
            batch.append(row)

            if len(batch) >= batch_size:
                with pg_conn.cursor() as cur:
                    cur.executemany(cast(LiteralString, insert_query), batch)
                pg_conn.commit()
                pbar.update(len(batch))
                batch = []

        if batch:
            with pg_conn.cursor() as cur:
                cur.executemany(cast(LiteralString, insert_query), batch)
            pg_conn.commit()
            pbar.update(len(batch))


def _transfer_helper(
    pg_url: str,
    sqlite_db_file: Path,
    modifier: _Modifier,
    table_name: str,
    batch_size: int,
    select_cols: Optional[list[str]] = None,
    insert_cols: Optional[list[str]] = None,
    alternative_table_name: str | None = None,
):
    pg_table_name = alternative_table_name or table_name

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

        insert_query = f"INSERT INTO {pg_table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        total = int(
            sqlite_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )

        def _gen_rows():
            offset = 0

            while True:
                batch = sqlite_conn.execute(
                    select_query.format(offset=offset)
                ).fetchall()

                if not batch:
                    break

                for m in modifier([dict(row) for row in batch]):
                    yield tuple(m[c] for c in col_list)

                offset += batch_size

        _execute_batch_inserts(
            pg_conn,
            insert_query,
            _gen_rows(),
            batch_size,
            f"transferring {table_name}",
            total=total,
        )


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
    unified_metadata_db_file: Path,
) -> _Modifier:
    def modifier(batch: list[dict]) -> list[dict]:
        _get_conn = _make_conn_cache()

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

        sample_organisms: dict[str, set[str]] = defaultdict(set[str])
        sample_platforms: dict[str, set[str]] = defaultdict(set[str])

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
                    sample_organisms[sample_acc].add(item["#text"])
                sample_platforms[sample_acc].add(platform)

        alias_strategies: dict[str, set[str]] = defaultdict(set[str])
        alias_models: dict[str, set[str]] = defaultdict(set[str])

        if alias_set:
            unique_aliases = list(alias_set)
            placeholders = ", ".join(["?"] * len(unique_aliases))
            query = f"SELECT study, library_strategy, instrument_model FROM sra_experiments WHERE study IN ({placeholders})"
            conn = _get_conn(sra_db_file)

            for study, strategy, model in conn.execute(
                query, unique_aliases
            ).fetchall():
                if strategy:
                    alias_strategies[study].add(strategy)
                if model:
                    alias_models[study].add(model)

        accession_alias_all: dict[str, list[str]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            placeholders = ", ".join(["?"] * len(unique_accessions))
            query = f"SELECT canonical_accession, aliases FROM unified_metadata WHERE canonical_accession IN ({placeholders})"
            conn = _get_conn(unified_metadata_db_file)

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
                {
                    p
                    for s in meta["samples"]
                    for p in sample_platforms.get(s, set[str]())
                }
            )

            library_strategies = sorted(
                alias_strategies.get(meta["alias"], set[str]()) if meta["alias"] else []
            )
            instrument_models = sorted(
                alias_models.get(meta["alias"], set[str]()) if meta["alias"] else []
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


def _sra_experiments_batch_modifier(batch: list[dict]) -> list[dict]:
    for value in batch:
        value["samples"] = _ensure_list(_convert_to_json(value["samples"]))
    return batch


def _sra_samples_batch_modifier(batch: list[dict]) -> list[dict]:
    for value in batch:
        value["external_id"] = Jsonb(_convert_to_json(value["external_id"]))
        value["attributes_json"] = Jsonb(_convert_to_json(value["attributes_json"]))
        value["links"] = Jsonb(_convert_to_json(value["links"]))
    return batch


def _arrayexpress_samples_batch_modifier(batch: list[dict]) -> list[dict]:
    for value in batch:
        value["attributes_json"] = Jsonb(_convert_to_json(value["attributes_json"]))
    return batch


def _sra_run_download_links_modifier(batch: list[dict]) -> list[dict]:
    for value in batch:
        value["ncbi_files"] = Jsonb(_convert_to_json(value["ncbi_files"]))
    return batch


def _sra_studies_batch_modifier(
    sra_db_file: Path,
    unified_metadata_db_file: Path,
) -> _Modifier:
    def modifier(batch: list[dict]) -> list[dict]:
        _get_conn = _make_conn_cache()

        row_meta: list[dict] = []
        all_submissions: set[str] = set()
        all_accessions: list[str] = []

        for value in batch:
            accession = value["accession"]
            submission = value["submission"]
            external_id = _convert_to_json(value["external_id"])

            row_meta.append(
                {
                    "accession": accession,
                    "submission": submission,
                    "external_id": external_id,
                }
            )

            if submission:
                all_submissions.add(submission)
            all_accessions.append(accession)

        submission_organisms: dict[str, set[str]] = {}
        if all_submissions:
            unique_submissions = list(all_submissions)
            placeholders = ", ".join(["?"] * len(unique_submissions))
            query = f"SELECT submission, scientific_name FROM sra_samples WHERE submission IN ({placeholders}) AND scientific_name IS NOT NULL"
            conn = _get_conn(sra_db_file)

            for sub, org in conn.execute(query, unique_submissions).fetchall():
                submission_organisms.setdefault(sub, set[str]()).add(org)

        submission_strategies: dict[str, set[str]] = defaultdict(set[str])
        submission_models: dict[str, set[str]] = defaultdict(set[str])
        submission_platforms: dict[str, set[str]] = defaultdict(set[str])

        if all_submissions:
            unique_submissions = list(all_submissions)
            placeholders = ", ".join(["?"] * len(unique_submissions))
            query = f"SELECT submission, library_strategy, instrument_model, platform FROM sra_experiments WHERE submission IN ({placeholders})"
            conn = _get_conn(sra_db_file)

            for sub, strategy, model, platform in conn.execute(
                query, unique_submissions
            ).fetchall():
                if strategy:
                    submission_strategies[sub].add(strategy)
                if model:
                    submission_models[sub].add(model)
                if platform:
                    submission_platforms[sub].add(platform)

        accession_countries: dict[str, set[str]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            placeholders = ", ".join(["?"] * len(unique_accessions))
            query = f"SELECT canonical_accession, country FROM unified_centers WHERE canonical_accession IN ({placeholders}) AND country IS NOT NULL"
            conn = _get_conn(unified_metadata_db_file)

            for acc, country in conn.execute(query, unique_accessions).fetchall():
                accession_countries.setdefault(acc, set[str]()).add(country)

        results: list[dict] = []

        for value, meta in zip(batch, row_meta):
            organisms = sorted(submission_organisms.get(meta["submission"], set[str]()))
            library_strategies = sorted(
                submission_strategies.get(meta["submission"], set[str]())
            )
            instrument_models = sorted(
                submission_models.get(meta["submission"], set[str]())
            )
            platforms = sorted(submission_platforms.get(meta["submission"], set[str]()))
            countries = sorted(accession_countries.get(meta["accession"], set[str]()))

            bioproject_id = None
            if meta["external_id"] and isinstance(meta["external_id"], dict):
                bioproject_id = meta["external_id"].get("BioProject")

            modified = dict(value)
            modified["external_id"] = Jsonb(meta["external_id"])
            modified["attributes_json"] = Jsonb(
                _convert_to_json(value["attributes_json"])
            )
            modified["links"] = Jsonb(_convert_to_json(value["links"]))
            modified["bioproject_id"] = bioproject_id
            modified["organisms"] = organisms
            modified["countries"] = countries
            modified["library_strategies"] = library_strategies
            modified["instrument_models"] = instrument_models
            modified["platforms"] = platforms

            results.append(modified)

        return results

    return modifier


def _arrayexpress_experiments_batch_modifier(
    unified_metadata_db_file: Path,
) -> _Modifier:
    def modifier(batch: list[dict]) -> list[dict]:
        _get_conn = _make_conn_cache()

        all_accessions = [v["accession"] for v in batch]
        accession_countries: dict[str, set[str]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            placeholders = ", ".join(["?"] * len(unique_accessions))
            query = f"SELECT canonical_accession, country FROM unified_centers WHERE canonical_accession IN ({placeholders}) AND country IS NOT NULL"
            conn = _get_conn(unified_metadata_db_file)
            for acc, country in conn.execute(query, unique_accessions).fetchall():
                accession_countries.setdefault(acc, set[str]()).add(country)

        results: list[dict] = []
        for value in batch:
            modified = dict(value)

            modified["organisms"] = (
                [modified["organism"]] if modified.get("organism") else []
            )

            orgs = _convert_to_json(value.get("organizations"))
            if orgs and isinstance(orgs, list):
                modified["organizations_text"] = " ".join(
                    o.get("name", "") for o in orgs if o.get("name")
                )
            else:
                modified["organizations_text"] = None

            authors_raw = _convert_to_json(value.get("authors"))
            if authors_raw and isinstance(authors_raw, list):
                modified["authors_text"] = " ".join(str(a) for a in authors_raw if a)
            else:
                modified["authors_text"] = None

            modified["countries"] = sorted(
                accession_countries.get(value["accession"], set[str]())
            )

            attrs = _convert_to_json(value["attributes_json"])
            modified["alias"] = (
                attrs.get("Comment[SecondaryAccession]")
                if isinstance(attrs, dict)
                else None
            )

            modified["experimental_factors"] = _ensure_list(
                _convert_to_json(value["experimental_factors"])
            )
            modified["experimental_factor_types"] = _ensure_list(
                _convert_to_json(value["experimental_factor_types"])
            )
            modified["protocol_names"] = _ensure_list(
                _convert_to_json(value["protocol_names"])
            )
            modified["protocol_types"] = _ensure_list(
                _convert_to_json(value["protocol_types"])
            )
            modified["attributes_json"] = Jsonb(
                _convert_to_json(value["attributes_json"])
            )
            modified["external_links"] = Jsonb(
                _convert_to_json(value["external_links"])
            )
            modified["processed_data_files"] = Jsonb(
                _convert_to_json(value["processed_data_files"])
            )
            modified["experimental_factors_detail"] = Jsonb(
                _convert_to_json(value["experimental_factors_detail"])
            )
            modified["minseqe_score"] = Jsonb(_convert_to_json(value["minseqe_score"]))
            modified["authors"] = _ensure_list(_convert_to_json(value["authors"]))
            modified["organizations"] = Jsonb(_convert_to_json(value["organizations"]))

            results.append(modified)

        return results

    return modifier


_unified_centers_cache: tuple | None = None
_unified_centers_lock = threading.Lock()


def _load_unified_centers(
    unified_metadata_db_file: Path,
) -> tuple[dict[tuple[str, str], dict], dict[str, dict]]:
    global _unified_centers_cache
    if _unified_centers_cache is not None:
        return _unified_centers_cache

    with _unified_centers_lock:
        if _unified_centers_cache is not None:
            return _unified_centers_cache

        lookup: dict[tuple[str, str], dict] = {}
        name_lookup: dict[str, dict] = {}
        with sqlite3.connect(unified_metadata_db_file) as uc_conn:
            for row in uc_conn.execute(
                "SELECT canonical_accession, center_name, place_name, place_type, short_label, address_type, osm_id, osm_type, place_id, longitude, latitude, city, district, subdistrict, county, suburb, neighbourhood, state, state_district, territory, metro_area, postcode, country_code, country, country_mismatch FROM unified_centers"
            ):
                (
                    acc,
                    name,
                    place_name,
                    place_type,
                    short_label,
                    address_type,
                    osm_id,
                    osm_type,
                    place_id,
                    lon,
                    lat,
                    city,
                    district,
                    subdistrict,
                    county,
                    suburb,
                    neighbourhood,
                    state,
                    state_district,
                    territory,
                    metro_area,
                    postcode,
                    country_code,
                    country,
                    country_mismatch,
                ) = row

                cname = name.strip().lower()
                key = (acc, cname)

                geo = {
                    "place_name": place_name,
                    "place_type": place_type,
                    "short_label": short_label,
                    "address_type": address_type,
                    "osm_id": osm_id,
                    "osm_type": osm_type,
                    "place_id": place_id,
                    "longitude": float(lon) if lon is not None else None,
                    "latitude": float(lat) if lat is not None else None,
                    "city": city,
                    "district": district,
                    "subdistrict": subdistrict,
                    "county": county,
                    "suburb": suburb,
                    "neighbourhood": neighbourhood,
                    "state": state,
                    "state_district": state_district,
                    "territory": territory,
                    "metro_area": metro_area,
                    "postcode": postcode,
                    "country_code": country_code,
                    "country": country,
                    "country_mismatch": country_mismatch.strip().lower()
                    in ("true", "1", "yes")
                    if country_mismatch
                    else None,
                }

                lookup[key] = geo
                if cname not in name_lookup:
                    name_lookup[cname] = geo

        _unified_centers_cache = (lookup, name_lookup)
        return _unified_centers_cache


def _transfer_geo_geocode(
    pg_url: str,
    geo_db_file: Path,
    unified_metadata_db_file: Path,
    batch_size: int,
):
    with (
        sqlite3.connect(geo_db_file) as src_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_geo_geocode_address_key ON geo_geocode (address_key)"
            )

        center_lookup, _ = _load_unified_centers(unified_metadata_db_file)

        groups: dict[str, dict] = {}
        for org, dept, addr, city, state, zipcode, country, acc in src_conn.execute(
            "SELECT DISTINCT organization, department, address_line, address_city, address_state, address_zipcode, address_country, accession FROM geo_contributors WHERE organization IS NOT NULL AND organization != ''"
        ):
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
                groups[key] = {f: None for f in _GEO_GEOCODE_NONE_FIELDS}
                groups[key].update(
                    {
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
                        "accessions": set(),
                    }
                )
            groups[key]["accessions"].add(acc)

            center = center_lookup.get((acc, org_s.strip().lower()))
            if center:
                g = groups[key]
                for src_field, dst_field in (
                    ("place_name", "geo_place_name"),
                    ("place_type", "geo_place_type"),
                    ("address_type", "geo_address_type"),
                    ("longitude", "geo_longitude"),
                    ("latitude", "geo_latitude"),
                    ("city", "geo_city"),
                    ("district", "geo_district"),
                    ("subdistrict", "geo_subdistrict"),
                    ("county", "geo_county"),
                    ("suburb", "geo_suburb"),
                    ("neighbourhood", "geo_neighbourhood"),
                    ("state", "geo_state"),
                    ("postcode", "geo_postcode"),
                    ("country_code", "geo_country_code"),
                    ("country", "geo_country"),
                    ("country_mismatch", "geo_country_mismatch"),
                ):
                    if center.get(src_field) is not None and g.get(dst_field) is None:
                        g[dst_field] = center[src_field]

                if g["geo_formatted_address"] is None and center.get("place_name"):
                    g["geo_formatted_address"] = center["place_name"]

                if g["geo_source"] is None:
                    g["geo_source"] = "unified"

        cols = ", ".join(_GEO_GEOCODE_COLS)
        placeholders = ", ".join(["%s"] * len(_GEO_GEOCODE_COLS))
        insert_query = f"INSERT INTO geo_geocode ({cols}) VALUES ({placeholders}) ON CONFLICT (address_key) DO NOTHING"

        _execute_batch_inserts(
            pg_conn,
            insert_query,
            (
                tuple(
                    d[c] if c != "n_series" else len(d["accessions"])
                    for c in _GEO_GEOCODE_COLS
                )
                for d in groups.values()
            ),
            batch_size,
            "  geo_geocode",
            total=len(groups),
            position=1,
        )


def _transfer_sra_geocode(
    pg_url: str,
    sra_db_file: Path,
    unified_metadata_db_file: Path,
    batch_size: int,
):
    with (
        sqlite3.connect(sra_db_file) as src_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_sra_geocode_center_name ON sra_geocode (center_name)"
            )

        center_lookup, center_name_lookup = _load_unified_centers(
            unified_metadata_db_file
        )

        sub_to_study: dict[str, str] = {}
        for submission, accession in src_conn.execute(
            "SELECT DISTINCT submission, accession FROM sra_studies"
        ):
            sub_to_study[submission] = accession

        groups: dict[str, dict] = {}
        for center_name, submission_acc in src_conn.execute(
            "SELECT DISTINCT center_name, accession FROM sra_submissions WHERE center_name IS NOT NULL AND center_name != ''"
        ):
            cn = center_name.strip()
            if not cn:
                continue
            key = cn.lower()

            study_acc = sub_to_study.get(submission_acc)
            geo = None
            if study_acc:
                geo = center_lookup.get((study_acc, key))
            if not geo:
                geo = center_name_lookup.get(key)

            if key not in groups:
                groups[key] = {
                    f: geo.get(f) if geo else None for f in _SRA_GEOCODE_SOURCE_FIELDS
                }
                groups[key].update(
                    {
                        "center_name": cn,
                        "original_country": geo.get("country") if geo else None,
                        "formatted_address": geo.get("place_name") if geo else None,
                        "official_address": ", ".join(
                            p
                            for p in [
                                geo.get("place_name"),
                                geo.get("city"),
                                geo.get("state"),
                                geo.get("postcode"),
                                geo.get("country"),
                            ]
                            if p
                        )
                        if geo
                        else None,
                        "loc_name": None,
                        "match_id": None,
                        "source": "unified" if geo else None,
                        "error": None,
                        "multiple_institutes": False,
                        "accessions": set(),
                    }
                )

            groups[key]["accessions"].add(submission_acc)

        for data in groups.values():
            data["multiple_institutes"] = len(data["accessions"]) > 1

        cols = ", ".join(_SRA_GEOCODE_COLS)
        placeholders = ", ".join(["%s"] * len(_SRA_GEOCODE_COLS))
        insert_query = f"INSERT INTO sra_geocode ({cols}) VALUES ({placeholders}) ON CONFLICT (center_name) DO NOTHING"

        _execute_batch_inserts(
            pg_conn,
            insert_query,
            (tuple(d[c] for c in _SRA_GEOCODE_COLS) for d in groups.values()),
            batch_size,
            "  sra_geocode",
            total=len(groups),
            position=1,
        )


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


def _make_conn_cache() -> Callable[[Path], sqlite3.Connection]:
    _conn_cache: dict[str, sqlite3.Connection] = {}

    def _get_conn(path: Path) -> sqlite3.Connection:
        key = str(path)
        if key not in _conn_cache:
            _conn_cache[key] = sqlite3.connect(path)
        return _conn_cache[key]

    return _get_conn


def _ensure_list(value):
    if isinstance(value, dict):
        return [value]
    return value or []


def _convert_to_json(value: str | None):
    return json.loads(value) if value else None
