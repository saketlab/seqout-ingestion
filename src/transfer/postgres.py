import contextlib
import datetime
import functools
import json
import logging
import sqlite3
import time
import traceback
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
import pycountry
from psycopg.types.json import Jsonb
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from helpers import batch_query_in, sqlite_connect
from transfer.gsa import gsa_transfer_tasks
from transfer.pg_fields import (
    _AE_CENTER_GEO_FIELDS,
    _AE_EXPERIMENTS_INSERT_COLS,
    _AE_EXPERIMENTS_SELECT_COLS,
    _AE_GEOCODE_COLS,
    _AE_GEOCODE_NONE_FIELDS,
    _AE_SAMPLES_COLS,
    _ENA_GEOCODE_COLS,
    _ENA_GEOCODE_SOURCE_FIELDS,
    _ENA_STUDIES_SEARCH_INSERT_COLS,
    _ENA_STUDIES_SEARCH_SELECT_COLS,
    _GEO_CENTER_GEO_FIELDS,
    _GEO_CONTRIBUTORS_COLS,
    _GEO_GEOCODE_COLS,
    _GEO_GEOCODE_NONE_FIELDS,
    _GEO_SERIES_INSERT_COLS,
    _GEO_SERIES_SELECT_COLS,
    _SRA_GEOCODE_COLS,
    _SRA_GEOCODE_SOURCE_FIELDS,
    _SRA_STUDIES_INSERT_COLS,
    _SRA_STUDIES_SELECT_COLS,
    _UNIFIED_METADATA_CENTERS_INSERT_COLS,
    _UNIFIED_METADATA_CENTERS_SELECT_COLS,
    _UNIFIED_METADATA_INSERT_COLS,
    _UNIFIED_METADATA_SELECT_COLS,
    non_nulling_update,
)

type _Modifier = Callable[[list[dict]], list[dict]]

logger = logging.getLogger(__name__)


@dataclass
class _TableInfo:
    sqlite_db_file: Path
    modifier: _Modifier
    select_cols: list[str] | None = None
    insert_cols: list[str] | None = None
    select_table_name: str | None = None
    conflict_cols: list[str] | None = None


def postgres_transfer(
    pg_url: str,
    sra_db_file: Path,
    geo_db_file: Path,
    ena_db_file: Path,
    ae_db_file: Path,
    pubmed_metadata_db_file: Path,
    unified_metadata_db_file: Path,
    download_links_db_file: Path,
    read_batch_size: int,
    num_retries: int,
    num_workers: int,
    gsa_db_file: Path | None = None,
    gsa_human_db_file: Path | None = None,
    gsa_bioproject_db_file: Path | None = None,
    *,
    prune_publications: bool = False,
) -> None:
    table_info = {
        "geo_series": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_geo_series_batch_modifier(
                sra_db_file,
                geo_db_file,
                unified_metadata_db_file,
                read_batch_size,
            ),
            select_cols=_GEO_SERIES_SELECT_COLS,
            insert_cols=_GEO_SERIES_INSERT_COLS,
            conflict_cols=["accession"],
        ),
        "geo_samples": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_geo_sample_batch_modifier,
            conflict_cols=["accession"],
        ),
        "geo_platforms": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_geo_platforms_batch_modifier,
            conflict_cols=["accession"],
        ),
        "geo_contributors": _TableInfo(
            sqlite_db_file=geo_db_file,
            modifier=_no_op_batch_modifier,
            select_cols=_GEO_CONTRIBUTORS_COLS,
            insert_cols=_GEO_CONTRIBUTORS_COLS,
        ),
        "sra_studies": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_sra_studies_batch_modifier(
                sra_db_file,
                unified_metadata_db_file,
                read_batch_size,
            ),
            select_cols=_SRA_STUDIES_SELECT_COLS,
            insert_cols=_SRA_STUDIES_INSERT_COLS,
            conflict_cols=["accession"],
        ),
        "sra_submissions": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_sra_submissions_batch_modifier,
            conflict_cols=["accession"],
        ),
        "sra_experiments": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_sra_experiments_batch_modifier,
            conflict_cols=["accession"],
        ),
        "sra_samples": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_sra_samples_batch_modifier,
            conflict_cols=["accession"],
        ),
        "sra_runs": _TableInfo(
            sqlite_db_file=sra_db_file,
            modifier=_no_op_batch_modifier,
            conflict_cols=["accession"],
        ),
        "sra_run_download_links": _TableInfo(
            sqlite_db_file=download_links_db_file,
            modifier=_sra_run_download_links_modifier,
            select_table_name="run_download_links",
            conflict_cols=["run_accession"],
        ),
        "ena_experiments": _TableInfo(
            sqlite_db_file=ena_db_file,
            modifier=_ena_experiments_batch_modifier,
            conflict_cols=["run_accession"],
        ),
        "ena_studies": _TableInfo(
            sqlite_db_file=ena_db_file,
            modifier=_ena_studies_batch_modifier,
            conflict_cols=["study_accession"],
        ),
        "ena_samples": _TableInfo(
            sqlite_db_file=ena_db_file,
            modifier=_ena_samples_batch_modifier,
            conflict_cols=["sample_accession"],
        ),
        "ena_studies_search": _TableInfo(
            sqlite_db_file=ena_db_file,
            modifier=_ena_studies_search_batch_modifier,
            select_cols=_ENA_STUDIES_SEARCH_SELECT_COLS,
            insert_cols=_ENA_STUDIES_SEARCH_INSERT_COLS,
            select_table_name="ena_studies",
        ),
        "arrayexpress_experiments": _TableInfo(
            sqlite_db_file=ae_db_file,
            modifier=_arrayexpress_experiments_batch_modifier(
                unified_metadata_db_file,
                read_batch_size,
            ),
            select_cols=_AE_EXPERIMENTS_SELECT_COLS,
            insert_cols=_AE_EXPERIMENTS_INSERT_COLS,
            conflict_cols=["accession"],
        ),
        "arrayexpress_samples": _TableInfo(
            sqlite_db_file=ae_db_file,
            modifier=_arrayexpress_samples_batch_modifier,
            select_cols=_AE_SAMPLES_COLS,
            insert_cols=_AE_SAMPLES_COLS,
        ),
        "pubmed_metadata": _TableInfo(
            sqlite_db_file=pubmed_metadata_db_file,
            modifier=_pubmed_metadata_batch_modifier,
            conflict_cols=["pmid"],
        ),
        "unified_metadata": _TableInfo(
            sqlite_db_file=unified_metadata_db_file,
            modifier=_unified_metadata_batch_modifier,
            select_cols=_UNIFIED_METADATA_SELECT_COLS,
            insert_cols=_UNIFIED_METADATA_INSERT_COLS,
            conflict_cols=["canonical_accession"],
        ),
        "unified_metadata_centers": _TableInfo(
            sqlite_db_file=unified_metadata_db_file,
            modifier=_no_op_batch_modifier,
            select_cols=_UNIFIED_METADATA_CENTERS_SELECT_COLS,
            insert_cols=_UNIFIED_METADATA_CENTERS_INSERT_COLS,
            select_table_name="unified_centers",
            conflict_cols=["canonical_accession", "position"],
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
                        read_batch_size,
                        num_retries,
                        info.select_cols,
                        info.insert_cols,
                        info.select_table_name,
                        info.conflict_cols,
                    ),
                ),
            )

    all_tasks.append(
        (
            "geo_geocode",
            _transfer_geo_geocode,
            (pg_url, geo_db_file, unified_metadata_db_file, read_batch_size),
        ),
    )
    all_tasks.append(
        (
            "sra_geocode",
            _transfer_sra_geocode,
            (pg_url, sra_db_file, unified_metadata_db_file, read_batch_size),
        ),
    )
    all_tasks.append(
        (
            "ena_geocode",
            _transfer_ena_geocode,
            (pg_url, ena_db_file, unified_metadata_db_file, read_batch_size),
        ),
    )
    all_tasks.append(
        (
            "ae_geocode",
            _transfer_ae_geocode,
            (pg_url, ae_db_file, unified_metadata_db_file, read_batch_size),
        ),
    )
    all_tasks.append(
        (
            "study_publications",
            _transfer_study_publications,
            (
                pg_url,
                unified_metadata_db_file,
                pubmed_metadata_db_file,
                read_batch_size,
                prune_publications,
            ),
        ),
    )
    all_tasks.append(
        (
            "cross_refs",
            _transfer_cross_refs,
            (pg_url, unified_metadata_db_file, read_batch_size),
        ),
    )
    all_tasks.extend(
        gsa_transfer_tasks(
            pg_url,
            gsa_db_file,
            gsa_human_db_file,
            read_batch_size,
            gsa_bioproject_db_file,
        ),
    )

    with (
        ThreadPoolExecutor(max_workers=num_workers) as pool,
        tqdm(
            total=len(all_tasks),
            desc="transferring tables",
            unit="table",
        ) as outer_pbar,
        logging_redirect_tqdm(),
    ):
        futures = {}
        for label, func, args in all_tasks:
            futures[pool.submit(func, *args)] = label

        for f in as_completed(futures):
            try:
                f.result()
            except BaseException:
                table_name = futures[f]
                pool.shutdown(wait=False, cancel_futures=True)
                tqdm.write(f"{table_name} failed\n\t{traceback.format_exc()}")
                raise
            outer_pbar.update(1)


def _transfer_helper(
    pg_url: str,
    sqlite_db_file: Path,
    modifier: _Modifier,
    table_name: str,
    read_batch_size: int,
    num_retries: int,
    select_cols: list[str] | None = None,
    insert_cols: list[str] | None = None,
    select_table_name: str | None = None,
    conflict_cols: list[str] | None = None,
) -> None:
    select_pg_table_name = select_table_name or table_name

    with sqlite_connect(sqlite_db_file) as sqlite_conn:
        sqlite_conn.execute("PRAGMA cache_size = -200000")
        sqlite_conn.execute("PRAGMA mmap_size = 30000000000")
        sqlite_conn.execute("PRAGMA temp_store = MEMORY")
        sqlite_conn.execute("PRAGMA synchronous = OFF")
        sqlite_conn.execute("PRAGMA journal_mode = WAL")

        sqlite_conn.row_factory = sqlite3.Row
        select_col_list = select_cols or _get_table_info(
            sqlite_conn, select_pg_table_name
        )
        insert_col_list = insert_cols or select_col_list

        select_columns = ", ".join(select_col_list)
        insert_columns = ", ".join(insert_col_list)

        total = int(
            sqlite_conn.execute(
                f"SELECT COUNT(*) FROM {select_pg_table_name}",  # noqa: S608
            ).fetchone()[0],
        )

        conflict_cols_str = ", ".join(conflict_cols) if conflict_cols else None
        update_sql = (
            non_nulling_update(
                table_name,
                [c for c in insert_col_list if c not in conflict_cols],
            )
            if conflict_cols
            else ""
        )
        on_conflict_sql = (
            f"ON CONFLICT ({conflict_cols_str}) DO UPDATE {update_sql}"
            if update_sql
            else "ON CONFLICT DO NOTHING"
        )

        checkpoint_path = Path(f"{sqlite_db_file}.checkpoint")
        start_offset = 0
        if checkpoint_path.exists():
            try:
                cp = json.loads(checkpoint_path.read_text())
                if cp.get("table_name") == table_name:
                    start_offset = cp["offset"]
            except (json.JSONDecodeError, KeyError):
                pass

        pg_conn = psycopg.connect(
            pg_url,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

        with tqdm(
            desc=f"copying {table_name}",
            total=total,
            unit="rows",
            leave=False,
            initial=start_offset,
        ) as pbar:
            offset = start_offset
            while offset < total:
                batch_rows = sqlite_conn.execute(
                    f"SELECT {select_columns} FROM {select_pg_table_name}"  # noqa: S608
                    f" LIMIT {read_batch_size} OFFSET {offset}",
                ).fetchall()
                if not batch_rows:
                    break

                batch = [
                    tuple(m[c] for c in insert_col_list)
                    for m in modifier([dict(r) for r in batch_rows])
                ]

                for attempt in range(num_retries):
                    try:
                        with pg_conn.cursor() as cur:
                            if conflict_cols:
                                temp_table = f"staging_{table_name}"
                                cur.execute(
                                    cast(
                                        "LiteralString",
                                        f"CREATE TEMP TABLE {temp_table} AS TABLE {table_name} WITH NO DATA",  # noqa: E501
                                    ),
                                )

                                with cur.copy(
                                    cast(
                                        "LiteralString",
                                        f"COPY {temp_table} ({insert_columns}) FROM STDIN",  # noqa: E501
                                    ),
                                ) as copy:
                                    for row in batch:
                                        copy.write_row(row)

                                cur.execute(
                                    cast(
                                        "LiteralString",
                                        f"""INSERT INTO {table_name} ({insert_columns})
                                        SELECT DISTINCT ON ({conflict_cols_str})
                                        {insert_columns} FROM {temp_table}
                                        ORDER BY {conflict_cols_str}
                                        {on_conflict_sql}""",  # noqa: S608
                                    ),
                                )

                                cur.execute(
                                    cast(
                                        "LiteralString",
                                        f"DROP TABLE IF EXISTS {temp_table}",
                                    ),
                                )
                            else:
                                with cur.copy(
                                    cast(
                                        "LiteralString",
                                        f"COPY {table_name} ({insert_columns}) FROM STDIN",  # noqa: E501
                                    ),
                                ) as copy:
                                    for row in batch:
                                        copy.write_row(row)

                            pg_conn.commit()
                        break
                    except (
                        psycopg.OperationalError,
                        psycopg.DatabaseError,
                    ) as exc:
                        if attempt == num_retries - 1:
                            raise
                        wait = min(2**attempt, 60)
                        tqdm.write(
                            f"{table_name}: batch at offset {offset} failed "
                            f"(attempt {attempt + 1}/{num_retries}, retrying in {wait:.0f}s) - {exc}",  # noqa: E501
                        )
                        time.sleep(wait)
                        pg_conn.close()
                        pg_conn = psycopg.connect(
                            pg_url,
                            keepalives=1,
                            keepalives_idle=30,
                            keepalives_interval=10,
                            keepalives_count=5,
                        )

                offset += len(batch_rows)
                checkpoint_path.write_text(
                    json.dumps(
                        {
                            "table_name": table_name,
                            "offset": offset,
                            "total": total,
                        },
                    ),
                )
                pbar.update(len(batch_rows))

        pg_conn.close()

        if checkpoint_path.exists():
            checkpoint_path.unlink()


def _get_table_info(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()
    return [row[1] for row in rows]


def _execute_batch_inserts(
    pg_conn: psycopg.Connection,
    insert_query: str,
    rows: Iterable[tuple[Any, ...]],
    batch_size: int,
    desc: str,
    total: int | None = None,
    **tqdm_kwargs: Any,
) -> None:
    batch: list[tuple] = []

    with tqdm(desc=desc, total=total, unit="rows", leave=False, **tqdm_kwargs) as pbar:
        for row in rows:
            batch.append(row)

            if len(batch) >= batch_size:
                with pg_conn.cursor() as cur:
                    cur.executemany(cast("LiteralString", insert_query), batch)
                pg_conn.commit()
                pbar.update(len(batch))
                batch = []

        if batch:
            with pg_conn.cursor() as cur:
                cur.executemany(cast("LiteralString", insert_query), batch)
            pg_conn.commit()
            pbar.update(len(batch))


def _no_op_batch_modifier(batch: list[dict]) -> list[dict]:
    return batch


def _parse_date(v: object) -> str | None:
    if not v:
        return None
    v = str(v).strip()
    if not v or v == "-":
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return (
                datetime.datetime.strptime(v, fmt)
                .astimezone(datetime.UTC)
                .strftime("%Y-%m-%d")
            )
        except ValueError:
            continue
    return None


def _fix_dates(batch: list[dict], cols: tuple[str, ...]) -> None:
    for value in batch:
        for col in cols:
            value[col] = _parse_date(value.get(col))


def _sra_submissions_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("published_at", "updated_at"))
    return batch


def _geo_platforms_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("published_at", "updated_at"))
    return batch


def _ena_studies_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("first_public", "last_updated", "pubmed_last_checked"))
    return batch


def _ena_experiments_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("first_public", "last_updated"))
    return batch


def _ena_samples_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("first_public", "last_updated"))
    return batch


def _pubmed_metadata_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("pub_date",))
    return batch


def _geo_sample_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("published_at", "updated_at"))
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
    batch_size: int,
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

            samples = [v if isinstance(v, str) else v["@ref"] for v in samples_ref]
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
                },
            )

            all_samples.extend(samples)
            if alias:
                alias_set.add(alias)
            all_accessions.append(accession)

        sample_organisms: dict[str, set[str]] = defaultdict(set[str])
        sample_platforms: dict[str, set[str]] = defaultdict(set[str])

        if all_samples:
            unique_samples = list(set(all_samples))
            conn = _get_conn(geo_db_file)

            for sample_acc, org_json, channels_json, platform in batch_query_in(
                conn,
                """SELECT
                    gs.accession, gp.organism, gs.channels,
                    gs.platform_ref
                FROM geo_samples
                    AS gs LEFT JOIN geo_platforms gp ON gs.platform_ref = gp.accession
                WHERE gs.accession""",
                unique_samples,
                batch_size=batch_size,
            ):
                if org_json:
                    org_parsed = json.loads(org_json)
                    for item in org_parsed:
                        text = item.get("#text")
                        if text:
                            sample_organisms[sample_acc].add(text)
                else:
                    channels = _ensure_list(json.loads(channels_json))
                    for ch in channels:
                        org = ch.get("Organism", {})
                        if isinstance(org, dict):
                            text = org.get("#text")
                            if text:
                                sample_organisms[sample_acc].add(text)

                if platform:
                    sample_platforms[sample_acc].add(platform)

        alias_strategies: dict[str, set[str]] = defaultdict(set[str])
        alias_models: dict[str, set[str]] = defaultdict(set[str])

        if alias_set:
            unique_aliases = list(alias_set)
            conn = _get_conn(sra_db_file)

            for study, strategy, model in batch_query_in(
                conn,
                """SELECT study, library_strategy, instrument_model
                FROM sra_experiments WHERE study""",
                unique_aliases,
                batch_size,
            ):
                if strategy:
                    alias_strategies[study].add(strategy)
                if model:
                    alias_models[study].add(model)

        accession_alias_all: dict[str, list[str]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            conn = _get_conn(unified_metadata_db_file)

            for acc, aliases_json in batch_query_in(
                conn,
                "SELECT canonical_accession, aliases "
                "FROM unified_metadata WHERE canonical_accession",
                unique_accessions,
                batch_size,
            ):
                aliases_data = json.loads(aliases_json)
                if aliases_data:
                    accession_alias_all[acc] = list(dict(aliases_data).values())

        accession_contributors: dict[str, dict[str, list[str]]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            conn = _get_conn(geo_db_file)

            for acc, person, department, organization, country in batch_query_in(
                conn,
                """SELECT
                    accession, person, department,
                    organization, address_country
                FROM geo_contributors WHERE accession""",
                unique_accessions,
                batch_size,
            ):
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

        for value, meta in zip(batch, row_meta, strict=False):
            organisms = sorted(
                set[str]().union(
                    *(sample_organisms.get(s, set[str]()) for s in meta["samples"]),
                ),
            )
            platforms = sorted(
                {
                    p
                    for s in meta["samples"]
                    for p in sample_platforms.get(s, set[str]())
                },
            )

            library_strategies = sorted(
                alias_strategies.get(meta["alias"], set[str]())
                if meta["alias"]
                else [],
            )
            instrument_models = sorted(
                alias_models.get(meta["alias"], set[str]()) if meta["alias"] else [],
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
            modified["citation"] = _convert_to_json(modified["citation"])
            modified["pubmed_id_citation"] = _convert_to_json(
                modified["pubmed_id_citation"]
            )
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

        _fix_dates(results, ("published_at", "updated_at", "pubmed_last_checked"))
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
        raw = value["attributes_json"]
        if raw:
            raw = raw.replace("\\u0000", "")
        value["attributes_json"] = Jsonb(_convert_to_json(raw))
    return batch


def _sra_run_download_links_modifier(batch: list[dict]) -> list[dict]:
    for value in batch:
        value["ncbi_files"] = Jsonb(_convert_to_json(value["ncbi_files"]))
    return batch


def _sra_studies_batch_modifier(
    sra_db_file: Path,
    unified_metadata_db_file: Path,
    batch_size: int,
) -> _Modifier:
    def modifier(batch: list[dict]) -> list[dict]:
        _get_conn = _make_conn_cache()

        row_meta: list[dict] = []
        all_accessions: list[str] = []
        all_submissions: set[str] = set()

        for value in batch:
            accession = value["accession"]
            submission = value["submission"]
            external_id = _convert_to_json(value["external_id"])

            row_meta.append(
                {
                    "accession": accession,
                    "submission": submission,
                    "external_id": external_id,
                },
            )

            if submission:
                all_submissions.add(submission)

            all_accessions.append(accession)

        # These facets are per STUDY, never per submission. A submission is not a
        # study: 68,775 studies share one with another study (one submission
        # covers 1,548), so submission-keyed values leak a study's siblings into
        # it; and a study's samples can span many submissions (SRP584992 spans
        # six), so its own submission misses most of them. Both directions were
        # measured against prod -- ~12.6% of SRA studies had a wrong organism
        # list, which search filters on directly.
        study_strategies: dict[str, set[str]] = defaultdict(set[str])
        study_models: dict[str, set[str]] = defaultdict(set[str])
        study_platforms: dict[str, set[str]] = defaultdict(set[str])
        study_sources: dict[str, set[str]] = defaultdict(set[str])
        study_sample_accs: dict[str, set[str]] = defaultdict(set[str])
        all_sample_accs: set[str] = set()

        if all_accessions:
            unique_studies = list(set(all_accessions))
            conn = _get_conn(sra_db_file)

            for study, strategy, model, platform, source, samples in batch_query_in(
                conn,
                """SELECT
                    study, library_strategy, instrument_model,
                    platform, library_source, samples
                FROM sra_experiments WHERE study""",
                unique_studies,
                batch_size,
            ):
                if strategy:
                    study_strategies[study].add(strategy)
                if model:
                    study_models[study].add(model)
                if platform:
                    study_platforms[study].add(platform)
                if source and (source := source.strip()):  # values arrive dirty
                    study_sources[study].add(source)
                # sra_samples has no study column, so organisms take the same
                # two hops the project page does: study -> experiment.samples
                # -> sample.scientific_name.
                for sample_acc in _ensure_list(_convert_to_json(samples)):
                    if sample_acc:
                        study_sample_accs[study].add(sample_acc)
                        all_sample_accs.add(sample_acc)

        sample_organism: dict[str, str] = {}
        if all_sample_accs:
            conn = _get_conn(sra_db_file)

            for sample_acc, org in batch_query_in(
                conn,
                "SELECT accession, scientific_name FROM sra_samples WHERE accession",
                list(all_sample_accs),
                query_suffix="AND scientific_name IS NOT NULL",
                batch_size=batch_size,
            ):
                sample_organism[sample_acc] = org

        # Fallback only. ~4% of studies have samples but no experiments linking
        # them (DRP001120 has organisms and zero rows in every experiments
        # table), so the study-level join yields nothing for them. Empty means
        # "no evidence", not "no organisms" — falling back to the submission
        # keeps a coarse answer instead of blanking the field.
        submission_organisms: dict[str, set[str]] = defaultdict(set[str])
        if all_submissions:
            conn = _get_conn(sra_db_file)

            for sub, org in batch_query_in(
                conn,
                "SELECT submission, scientific_name FROM sra_samples WHERE submission",
                list(all_submissions),
                query_suffix="AND scientific_name IS NOT NULL",
                batch_size=batch_size,
            ):
                submission_organisms[sub].add(org)

        accession_countries: dict[str, set[str]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            conn = _get_conn(unified_metadata_db_file)

            for acc, country in batch_query_in(
                conn,
                """SELECT canonical_accession, country
                FROM unified_centers WHERE canonical_accession""",
                unique_accessions,
                query_suffix="AND country IS NOT NULL",
                batch_size=batch_size,
            ):
                accession_countries.setdefault(acc, set[str]()).add(country)

        results: list[dict] = []

        for value, meta in zip(batch, row_meta, strict=False):
            study_acc = meta["accession"]
            organisms = sorted(
                {
                    org
                    for sample_acc in study_sample_accs.get(study_acc, set[str]())
                    if (org := sample_organism.get(sample_acc))
                },
            ) or sorted(submission_organisms.get(meta["submission"], set[str]()))
            library_strategies = sorted(study_strategies.get(study_acc, set[str]()))
            instrument_models = sorted(study_models.get(study_acc, set[str]()))
            platforms = sorted(study_platforms.get(study_acc, set[str]()))
            library_sources = sorted(study_sources.get(study_acc, set[str]()))
            countries = sorted(accession_countries.get(meta["accession"], set[str]()))

            bioproject_id = None
            if meta["external_id"] and isinstance(meta["external_id"], dict):
                bioproject_id = meta["external_id"].get("BioProject")

            modified = dict(value)
            modified["external_id"] = Jsonb(meta["external_id"])
            modified["attributes_json"] = Jsonb(
                _convert_to_json(value["attributes_json"]),
            )
            modified["links"] = Jsonb(_convert_to_json(value["links"]))
            modified["bioproject_id"] = bioproject_id
            modified["organisms"] = organisms
            modified["countries"] = countries
            modified["library_strategies"] = library_strategies
            modified["instrument_models"] = instrument_models
            modified["platforms"] = platforms
            modified["library_sources"] = library_sources

            results.append(modified)

        _fix_dates(results, ("published_at", "updated_at", "pubmed_last_checked"))
        return results

    return modifier


def _ena_studies_search_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("first_public", "last_updated"))
    for value in batch:
        value["accession"] = value.pop("study_accession")
    return batch


def _arrayexpress_experiments_batch_modifier(
    unified_metadata_db_file: Path,
    batch_size: int,
) -> _Modifier:
    def modifier(batch: list[dict]) -> list[dict]:
        _get_conn = _make_conn_cache()

        all_accessions = [v["accession"] for v in batch]
        accession_countries: dict[str, set[str]] = {}

        if all_accessions:
            unique_accessions = list(set(all_accessions))
            conn = _get_conn(unified_metadata_db_file)
            for acc, country in batch_query_in(
                conn,
                """SELECT canonical_accession, country
                FROM unified_centers WHERE canonical_accession""",
                unique_accessions,
                query_suffix="AND country IS NOT NULL",
                batch_size=batch_size,
            ):
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
                accession_countries.get(value["accession"], set[str]()),
            )

            attrs = _convert_to_json(value["attributes_json"])
            modified["alias"] = (
                attrs.get("Comment[SecondaryAccession]")
                if isinstance(attrs, dict)
                else None
            )

            modified["experimental_factors"] = _ensure_list(
                _convert_to_json(value["experimental_factors"]),
            )
            modified["experimental_factor_types"] = _ensure_list(
                _convert_to_json(value["experimental_factor_types"]),
            )
            modified["protocol_names"] = _ensure_list(
                _convert_to_json(value["protocol_names"]),
            )
            modified["protocol_types"] = _ensure_list(
                _convert_to_json(value["protocol_types"]),
            )
            modified["attributes_json"] = Jsonb(
                _convert_to_json(value["attributes_json"]),
            )
            modified["external_links"] = Jsonb(
                _convert_to_json(value["external_links"]),
            )
            modified["processed_data_files"] = Jsonb(
                _convert_to_json(value["processed_data_files"]),
            )
            modified["experimental_factors_detail"] = Jsonb(
                _convert_to_json(value["experimental_factors_detail"]),
            )
            modified["minseqe_score"] = Jsonb(_convert_to_json(value["minseqe_score"]))
            modified["authors"] = _ensure_list(_convert_to_json(value["authors"]))
            modified["organizations"] = Jsonb(_convert_to_json(value["organizations"]))

            results.append(modified)

        _fix_dates(results, ("release_date", "submission_date", "pubmed_last_checked"))
        return results

    return modifier


def _unified_metadata_batch_modifier(batch: list[dict]) -> list[dict]:
    _fix_dates(batch, ("first_published",))
    for value in batch:
        for col in (
            "aliases",
            "organism_counts",
            "common_name_counts",
            "library_strategy_counts",
            "assay_l1_counts",
            "assay_l2_counts",
            "center_names",
        ):
            value[col] = Jsonb(_convert_to_json(value[col]))

        value["is_single_cell"] = bool(value["is_single_cell"])
        cc = value.get("country_code")
        if cc:
            try:
                value["country_code_iso2"] = pycountry.countries.lookup(cc).alpha_2
            except LookupError:
                value["country_code_iso2"] = None
        else:
            value["country_code_iso2"] = None

    return batch


@functools.cache
def _load_unified_centers(
    unified_metadata_db_file: Path,
) -> tuple[dict[tuple[str, str], dict], dict[str, dict]]:
    lookup: dict[tuple[str, str], dict] = {}
    name_lookup: dict[str, dict] = {}
    with sqlite_connect(unified_metadata_db_file) as uc_conn:
        for row in uc_conn.execute(
            """SELECT
                canonical_accession, center_name, place_name,
                place_type, short_label, address_type,
                osm_id, osm_type, place_id,
                longitude, latitude, city,
                district, subdistrict, county,
                suburb, neighbourhood, state,
                state_district, territory, metro_area,
                postcode, country_code, country,
                country_mismatch
            FROM unified_centers""",
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

    return lookup, name_lookup


def _transfer_geo_geocode(
    pg_url: str,
    geo_db_file: Path,
    unified_metadata_db_file: Path,
    batch_size: int,
) -> None:
    with (
        sqlite_connect(geo_db_file) as src_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_geo_geocode_address_key
                ON geo_geocode (address_key)""",
            )

        center_lookup, _ = _load_unified_centers(unified_metadata_db_file)

        groups: dict[str, dict] = {}
        for org, dept, addr, city, state, zipcode, country, acc in src_conn.execute(
            """SELECT
                DISTINCT organization, department, address_line,
                address_city, address_state, address_zipcode,
                address_country, accession
            FROM geo_contributors
            WHERE organization IS NOT NULL AND organization != ''""",
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
                groups[key] = dict.fromkeys(_GEO_GEOCODE_NONE_FIELDS)
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
                    },
                )
            groups[key]["accessions"].add(acc)

            center = center_lookup.get((acc, org_s.strip().lower()))
            if center:
                _apply_center_geo(groups[key], center, _GEO_CENTER_GEO_FIELDS)

        _insert_geocode_batches(
            pg_conn,
            "geo_geocode",
            _GEO_GEOCODE_COLS,
            groups,
            batch_size,
            count_col="n_series",
        )


def _transfer_sra_geocode(
    pg_url: str,
    sra_db_file: Path,
    unified_metadata_db_file: Path,
    batch_size: int,
) -> None:
    with (
        sqlite_connect(sra_db_file) as src_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_sra_geocode_center_name
                ON sra_geocode (center_name)""",
            )

        center_lookup, center_name_lookup = _load_unified_centers(
            unified_metadata_db_file,
        )

        sub_to_study = dict(
            src_conn.execute("SELECT DISTINCT submission, accession FROM sra_studies"),
        )

        groups: dict[str, dict] = {}
        for center_name, submission_acc in src_conn.execute(
            """SELECT DISTINCT center_name, accession
            FROM sra_submissions
            WHERE center_name IS NOT NULL AND center_name != ''""",
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
                    },
                )

            groups[key]["accessions"].add(submission_acc)

        for data in groups.values():
            data["multiple_institutes"] = len(data["accessions"]) > 1

        _insert_geocode_batches(
            pg_conn,
            "sra_geocode",
            _SRA_GEOCODE_COLS,
            groups,
            batch_size,
        )


def _transfer_ena_geocode(
    pg_url: str,
    ena_db_file: Path,
    unified_metadata_db_file: Path,
    batch_size: int,
) -> None:
    with (
        sqlite_connect(ena_db_file) as src_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_ena_geocode_center_name
                ON ena_geocode (center_name)""",
            )

        center_lookup, center_name_lookup = _load_unified_centers(
            unified_metadata_db_file,
        )

        groups: dict[str, dict] = {}
        for center_name, study_accession in src_conn.execute(
            """SELECT DISTINCT center_name, study_accession
            FROM ena_studies
            WHERE center_name IS NOT NULL AND center_name != ''""",
        ):
            cn = center_name.strip()
            if not cn:
                continue
            key = cn.lower()

            geo = center_lookup.get((study_accession, key))
            if not geo:
                geo = center_name_lookup.get(key)

            if key not in groups:
                groups[key] = {
                    f: geo.get(f) if geo else None for f in _ENA_GEOCODE_SOURCE_FIELDS
                }
                groups[key].update(
                    {
                        "center_name": cn,
                        "formatted_address": geo.get("place_name") if geo else None,
                        "source": "unified" if geo else None,
                        "error": None,
                        "accessions": set(),
                    },
                )
            groups[key]["accessions"].add(study_accession)

        exp_counts: dict[str, int] = {}
        for center_name, cnt in src_conn.execute(
            """SELECT center_name, COUNT(*) FROM ena_experiments
            WHERE center_name IS NOT NULL AND center_name != '' GROUP BY center_name""",
        ):
            exp_counts[center_name.strip().lower()] = cnt

        for key, data in groups.items():
            data["n_studies"] = len(data["accessions"])
            data["n_experiments"] = exp_counts.get(key, 0)

        _insert_geocode_batches(
            pg_conn,
            "ena_geocode",
            _ENA_GEOCODE_COLS,
            groups,
            batch_size,
        )


def _transfer_ae_geocode(
    pg_url: str,
    ae_db_file: Path,
    unified_metadata_db_file: Path,
    batch_size: int,
) -> None:
    with (
        sqlite_connect(ae_db_file) as src_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_geocode_address_key
                ON arrayexpress_geocode (address_key)""",
            )

        center_lookup, _ = _load_unified_centers(unified_metadata_db_file)

        groups: dict[str, dict] = {}
        for accession, org_json in src_conn.execute(
            """SELECT accession, organizations FROM arrayexpress_experiments
            WHERE organizations IS NOT NULL""",
        ):
            orgs = json.loads(org_json)
            if not isinstance(orgs, list):
                continue
            for org_data in orgs:
                if not isinstance(org_data, dict):
                    continue
                org_name = (org_data.get("name") or "").strip()
                if not org_name:
                    continue
                address = (org_data.get("address") or "").strip()

                key = f"{org_name}|{address}"

                if key not in groups:
                    parts = [p for p in [org_name, address] if p]
                    groups[key] = dict.fromkeys(_AE_GEOCODE_NONE_FIELDS)
                    groups[key].update(
                        {
                            "organization": org_name,
                            "address_line": address or None,
                            "address_city": None,
                            "address_state": None,
                            "address_zipcode": None,
                            "address_country": None,
                            "address_key": key,
                            "geo_query": ", ".join(parts),
                            "accessions": set(),
                        },
                    )
                groups[key]["accessions"].add(accession)

                center = center_lookup.get((accession, org_name.lower()))
                if center:
                    _apply_center_geo(groups[key], center, _AE_CENTER_GEO_FIELDS)

        _insert_geocode_batches(
            pg_conn,
            "arrayexpress_geocode",
            _AE_GEOCODE_COLS,
            groups,
            batch_size,
            count_col="n_experiments",
        )


def _transfer_study_publications(
    pg_url: str,
    unified_metadata_db_file: Path,
    pubmed_metadata_db_file: Path,
    batch_size: int,
    prune: bool = False,  # noqa: FBT002
) -> None:
    with (
        sqlite_connect(unified_metadata_db_file) as um_conn,
        sqlite_connect(pubmed_metadata_db_file) as pm_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            cur.execute(
                """CREATE UNIQUE INDEX
                IF NOT EXISTS idx_study_publications_accession_source
                ON study_publications (accession, source)""",
            )

        um_conn.row_factory = sqlite3.Row
        rows = um_conn.execute(
            """SELECT canonical_accession, source, pmid FROM unified_metadata
            WHERE pmid IS NOT NULL""",
        ).fetchall()

        all_pmids: set[str] = set()
        accession_pmids: list[tuple[str, str, str]] = []

        for row in rows:
            pmid_str = str(row["pmid"]).strip()
            pmid_data = json.loads(pmid_str) if pmid_str.startswith("[") else [pmid_str]
            for p_val in pmid_data:
                p = p_val.strip() if isinstance(p_val, str) else str(p_val)
                if p:
                    all_pmids.add(p)
                    accession_pmids.append(
                        (row["canonical_accession"], row["source"], p),
                    )

        pmid_to_pub: dict[str, dict] = {}
        if all_pmids:
            unique_pmids = list(all_pmids)
            pm_conn.row_factory = sqlite3.Row
            for row in batch_query_in(
                pm_conn,
                "SELECT * FROM pubmed_metadata WHERE pmid",
                unique_pmids,
                batch_size=batch_size,
            ):
                d = dict(row)
                pmid_to_pub[d["pmid"]] = d

        groups: dict[tuple[str, str], list[dict]] = {}
        for acc, source, pmid in accession_pmids:
            pub = pmid_to_pub.get(pmid)
            if pub:
                key = (acc, source)
                if key not in groups:
                    groups[key] = []
                groups[key].append(pub)

        cols = ["accession", "source", "publications"]
        cols_str = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_query = f"""INSERT INTO study_publications ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (accession, source) DO UPDATE
        SET publications = EXCLUDED.publications"""  # noqa: S608

        def _gen_rows() -> Iterator[tuple]:
            for (acc, source), pubs in groups.items():
                yield (acc, source, Jsonb(pubs))

        _execute_batch_inserts(
            pg_conn,
            insert_query,
            _gen_rows(),
            batch_size,
            "study_publications",
            total=len(groups),
        )

        # orphan cleanup: upserts leave rows for studies whose pmid was later
        # removed/corrected at source. groups is every current (accession,
        # source), so anything else in the table is stale.
        #
        # Off by default, and it must stay that way: "stale" is only meaningful
        # if the sqlite buffers hold the WHOLE history. Run this against a fresh
        # or partial DATA_DIR -- a new machine, a cleared cache, a failed
        # backfill_pm_meta -- and every publication row postgres holds for a
        # study outside this run gets deleted. Enable it deliberately, against
        # buffers you know are complete, with a dump in hand.
        if not prune:
            logger.info(
                "skipping study_publications orphan cleanup "
                "(pass --prune-publications to enable)",
            )
            return

        with pg_conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE _valid_pub_keys (accession text, source text) "
                "ON COMMIT DROP",
            )
            with cur.copy(
                "COPY _valid_pub_keys (accession, source) FROM STDIN",
            ) as cp:
                for acc, source in groups:
                    cp.write_row((acc, source))
            cur.execute(
                "DELETE FROM study_publications sp WHERE NOT EXISTS ("
                "SELECT 1 FROM _valid_pub_keys v "
                "WHERE v.accession = sp.accession AND v.source = sp.source)",
            )
        pg_conn.commit()


# source -> its cross_ref table. doubles as the allow-list: a source absent here
# contributes no cross-refs.
_CROSS_REF_TABLES = {
    "ae": "cross_ref_ae",
    "ena": "cross_ref_ena",
    "geo": "cross_ref_geo",
    "sra": "cross_ref_sra",
    "dra": "cross_ref_dra",
    "gea": "cross_ref_gea",
}


def _transfer_cross_refs(
    pg_url: str,
    unified_metadata_db_file: Path,
    batch_size: int,
) -> None:
    with (
        sqlite_connect(unified_metadata_db_file) as um_conn,
        psycopg.connect(pg_url) as pg_conn,
    ):
        with pg_conn.cursor() as cur:
            # provision every table: the loop below writes whatever source
            # unified_metadata carries, including sra/dra/gea
            tables = [
                (name, f"{source}_accession, target_accession, link_type")
                for source, name in sorted(_CROSS_REF_TABLES.items())
            ]
            for table, cols in tables:
                cur.execute(
                    cast(
                        "LiteralString",
                        f"""CREATE TABLE IF NOT EXISTS {table} (
                            {cols.split(",")[0].strip()} TEXT,
                            target_accession TEXT,
                            link_type TEXT
                        )""",
                    ),
                )

            for table, cols in [
                *tables,
                ("cross_ref_pubmed", "accession, pubmed_id, source"),
            ]:
                cur.execute(
                    cast(
                        "LiteralString",
                        f"""CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_pk
                        ON {table} ({cols})""",
                    ),
                )

        rows = um_conn.execute(
            "SELECT canonical_accession, source, aliases, pmid FROM unified_metadata",
        ).fetchall()

        ref_groups: defaultdict[str, list[tuple]] = defaultdict(list)
        pmid_groups: list[tuple[str, str, str]] = []

        for acc, source, aliases_json, pmid_str_val in rows:
            if not aliases_json:
                continue
            with contextlib.suppress(json.JSONDecodeError):
                aliases = json.loads(aliases_json)
                if source in _CROSS_REF_TABLES:
                    for target_source, target_acc in aliases.items():
                        ref_groups[source].append(
                            (str(acc), str(target_acc), str(target_source)),
                        )

            if pmid_str_val:
                pmid_str = str(pmid_str_val).strip()
                if pmid_str.startswith("["):
                    pmid_data = json.loads(pmid_str)
                else:
                    pmid_data = [pmid_str]

                for p_val in pmid_data:
                    p = str(p_val).strip()
                    if p:
                        pmid_groups.append((str(acc), p, str(source)))

        if pmid_groups:
            pmid_cols = ["accession", "pubmed_id", "source"]
            cols_str = ", ".join(pmid_cols)
            ph = ", ".join(["%s"] * len(pmid_cols))
            insert_q = f"""INSERT INTO cross_ref_pubmed ({cols_str})
                VALUES ({ph})
                ON CONFLICT (accession, pubmed_id, source) DO NOTHING"""  # noqa: S608
            _execute_batch_inserts(
                pg_conn,
                insert_q,
                pmid_groups,
                batch_size,
                "cross_ref_pubmed",
                total=len(pmid_groups),
            )

        for source, groups in ref_groups.items():
            table_name = _CROSS_REF_TABLES[source]
            ref_cols = [f"{source}_accession", "target_accession", "link_type"]
            cols_str = ", ".join(ref_cols)
            ph = ", ".join(["%s"] * len(ref_cols))
            insert_q = f"""INSERT INTO {table_name} ({cols_str})
                VALUES ({ph})
                ON CONFLICT ({cols_str}) DO NOTHING"""  # noqa: S608
            _execute_batch_inserts(
                pg_conn,
                insert_q,
                groups,
                batch_size,
                table_name,
                total=len(groups),
            )


def _insert_geocode_batches(
    pg_conn: psycopg.Connection,
    table_name: str,
    cols_list: list[str],
    groups: dict,
    batch_size: int,
    count_col: str | None = None,
) -> None:
    cols = ", ".join(cols_list)
    placeholders = ", ".join(["%s"] * len(cols_list))
    conflict_col = (
        "center_name" if table_name in ("sra_geocode", "ena_geocode") else "address_key"
    )
    insert_query = (
        f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) "  # noqa: S608
        f"ON CONFLICT ({conflict_col}) DO NOTHING"
    )

    def _gen_rows() -> Iterator[tuple]:
        for d in groups.values():
            yield tuple(
                d[c] if c != count_col else len(d["accessions"]) for c in cols_list
            )

    _execute_batch_inserts(
        pg_conn,
        insert_query,
        _gen_rows(),
        batch_size,
        f"  {table_name}",
        total=len(groups),
        position=1,
    )


def _apply_center_geo(
    g: dict,
    center: dict,
    fields: list[str],
) -> None:
    for field in fields:
        val = center.get(field)
        if val is not None and g.get("geo_" + field) is None:
            g["geo_" + field] = val

    if g["geo_formatted_address"] is None and center.get("place_name"):
        g["geo_formatted_address"] = center["place_name"]
    if g["geo_source"] is None:
        g["geo_source"] = "unified"


def _make_conn_cache() -> Callable[[Path], sqlite3.Connection]:
    _conn_cache: dict[str, sqlite3.Connection] = {}

    def _get_conn(path: Path) -> sqlite3.Connection:
        key = str(path)
        if key not in _conn_cache:
            _conn_cache[key] = sqlite_connect(path)
        return _conn_cache[key]

    return _get_conn


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


def _ensure_list(value: Any) -> list:
    if isinstance(value, dict):
        return [value]
    return value or []


def _convert_to_json(value: str | None) -> Any:
    return json.loads(value) if value else None
