import json
import logging
import re
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import LiteralString, Optional, cast

import psycopg
import results
import testing.postgresql
import tqdm
from psycopg.types.json import Jsonb
from requests import JSONDecodeError
from results import db
from tqdm.contrib.concurrent import thread_map
from tqdm.contrib.logging import logging_redirect_tqdm

from helpers import exec_query_from_file

JSON_VALID_PAT = re.compile(r"json_valid\((\w+)\)")


def pg_transfer(
    sra_db_file: Optional[Path],
    geo_db_file: Optional[Path],
    ena_db_file: Optional[Path],
    ae_db_file: Optional[Path],
    pg_url: str,
    num_workers: int,
    batch_size: int,
):
    db_mapping: dict[str, Path | None] = {
        "sra": sra_db_file,
        "geo": geo_db_file,
        "ena": ena_db_file,
        "ae": ae_db_file,
    }
    dbs_to_process = {
        k: v for k, v in db_mapping.items() if v is not None and Path(v).exists()
    }

    _perform_db_migration(pg_url, dbs_to_process)

    # {db_tag: [(table_name, [json_col_name, ...]), ...]}
    db_info: dict[str, list[tuple[str, list[str]]]] = defaultdict(
        list[tuple[str, list[str]]]
    )

    for db_tag, db_file in dbs_to_process.items():
        with sqlite3.Connection(db_file) as conn:
            rows = conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()

            for table_name, sql in rows:
                match: list[str] = re.findall(JSON_VALID_PAT, sql)
                db_info[db_tag].append((table_name, match))

    all_tasks = [
        (db_tag, table_info)
        for db_tag, table_infos in db_info.items()
        for table_info in table_infos
    ]

    def run_worker(task):
        db_tag, table_info = task
        _worker(pg_url, dbs_to_process[db_tag], table_info, batch_size)

    with logging_redirect_tqdm():
        thread_map(
            run_worker,
            all_tasks,
            max_workers=num_workers,
            desc="transferring tables",
            unit="table",
        )

    with ThreadPoolExecutor(num_workers) as pool, logging_redirect_tqdm():
        futures = [
            pool.submit(_worker, pg_url, dbs_to_process[db_tag], table_info, batch_size)
            for pos, (db_tag, table_info) in enumerate(all_tasks)
        ]

        for f in as_completed(futures):
            f.result()


def _perform_db_migration(pg_url: str, dbs_to_process: dict[str, Path]):
    with (
        testing.postgresql.Postgresql() as tmp_pg,
    ):
        dev_conn = psycopg.connect(tmp_pg.url())
        for key in dbs_to_process:
            print(key)
            pg_setup_file = f"pg/{key}.pg.sql"
            exec_query_from_file(pg_setup_file, conn=dev_conn)
        dev_conn.close()

        prod_db: results.database.Database = db(pg_url)
        dev_db: results.database.Database = db(tmp_pg.url())
        diff: str | None = prod_db.schemadiff_as_sql(dev_db)

        logging.info("running db migration")

        if diff:
            with prod_db.t() as t:
                t: results.database.Transaction
                t.q(diff, fail_on_empty=False)
            logging.info("migration applied")
        else:
            logging.info("already up to date")


def _worker(
    pg_url: str,
    db_file: Path,
    table_info: tuple[str, list[str]],
    batch_size: int,
):
    table_name, json_cols = table_info

    with sqlite3.Connection(db_file) as sqlite_conn, psycopg.connect(pg_url) as pg_conn:
        cursor = sqlite_conn.execute(f"SELECT * FROM {table_name} LIMIT 0")
        cols = [desc[0] for desc in cursor.description]
        col_list = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        json_col_idx: dict[str, int] = {}
        for col in json_cols:
            try:
                idx = cols.index(col)
            except ValueError:
                logging.warning(
                    "json col %s not found in table %s, skipping", col, table_name
                )
                continue
            json_col_idx[col] = idx

        total = int(
            sqlite_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        offset = 0

        pbar = tqdm.tqdm(
            desc=f"transferring {table_name}",
            total=total,
            unit="rows",
            leave=False,
        )

        while True:
            batch = list(
                sqlite_conn.execute(
                    f"SELECT {col_list} FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                ).fetchall()
            )

            if not batch:
                break

            for i, b in enumerate(batch):
                row = list(b)
                for col_name, idx in json_col_idx.items():
                    if row[idx] is None:
                        continue
                    try:
                        parsed = json.loads(row[idx])

                        if isinstance(parsed, list):
                            row[idx] = [Jsonb(item) for item in parsed]
                        else:
                            row[idx] = Jsonb(parsed)
                    except (JSONDecodeError, TypeError) as err:
                        logging.error(
                            "failed to parse json column %s in %s db: %s",
                            col_name,
                            db_file,
                            err,
                        )
                batch[i] = row

            with pg_conn.cursor() as cur:
                cur.executemany(cast(LiteralString, insert_sql), batch)
            pg_conn.commit()

            offset += batch_size
            pbar.update(len(batch))

        pbar.close()
