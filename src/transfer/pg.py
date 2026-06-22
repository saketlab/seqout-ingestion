import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, LiteralString, Optional, cast

import psycopg
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
]


def pg_transfer(
    pg_url: str, geo_db_file: Optional[Path], num_workers: int, batch_size: int
):
    table_info = {
        "geo_series": (
            geo_db_file,
            _geo_series_modifier,
            _GEO_SERIES_SELECT_COLS,
            _GEO_SERIES_INSERT_COLS,
        ),
        "geo_samples": (
            geo_db_file,
            _geo_sample_modifier,
            None,
            None,
        ),
        "geo_contributors": (geo_db_file, _no_op_modifier, None, None),
        "geo_platforms": (geo_db_file, _no_op_modifier, None, None),
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


def _get_table_info(conn: sqlite3.Connection, table_name: str) -> tuple[list[str], str]:
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    rows = cursor.fetchall()

    col_list = [row[1] for row in rows]
    pk = next(row[1] for row in rows if row[5] == 1)

    return col_list, pk


def _transfer_helper(
    pg_url: str,
    sqlite_db_file: Path,
    modifier: Callable[[dict], dict],
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

                modified_batch = []
                for row in batch:
                    modified = modifier(dict(row))
                    modified_batch.append(tuple(modified[c] for c in col_list))

                with pg_conn.cursor() as cur:
                    cur.executemany(cast(LiteralString, insert_query), modified_batch)

                pg_conn.commit()
                offset += batch_size
                pbar.update(len(batch))


def _no_op_modifier(value: dict) -> dict:
    return value


def _geo_sample_modifier(value: dict) -> dict:
    channels = _ensure_list(_convert_to_json(value["channels"]))
    supp = _ensure_list(_convert_to_json(value["supplementary_data"]))

    value["channels"] = [Jsonb(x) for x in channels]
    value["supplementary_data"] = [Jsonb(x) for x in supp]

    return value


def _geo_series_modifier(value: dict) -> dict:
    relations = _ensure_list(_convert_to_json(value["relation"]))
    samples_ref = _ensure_list(_convert_to_json(value["samples_ref"]))
    supp = _ensure_list(_convert_to_json(value["supplementary_data"]))

    bioproject_id = next(
        (
            str(v["@target"]).split("/")[-1]
            for v in relations
            if v["@type"] == "BioProject"
        ),
        None,
    )

    value["pubmed_id"] = _convert_to_json(value["pubmed_id"])
    value["samples_ref"] = [v["@ref"] for v in samples_ref]
    value["series_type"] = _convert_to_json(value["series_type"])
    value["relation"] = [Jsonb(x) for x in relations]
    value["supplementary_data"] = [Jsonb(x) for x in supp]

    value["bioproject_id"] = bioproject_id

    return value


def _ensure_list(value):
    if isinstance(value, dict):
        return [value]
    return value or []


def _convert_to_json(value: str):
    return json.loads(value) if value else None
