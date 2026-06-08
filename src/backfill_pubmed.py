import logging
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
from pysradb.sraweb import SRAweb
from tqdm import tqdm

from config import Datasources

DATASOURCE_TABLE_MAPPING: dict[Datasources, str] = {
    Datasources.Sra: "sra_studies",
    Datasources.Geo: "geo_series",
    Datasources.ArrayExpress: "arrayexpress_experiments",
    Datasources.Ena: "ena_studies",
}
MISSING_QUERIES: dict[Datasources, str] = {
    Datasources.Sra: "SELECT accession FROM sra_studies WHERE (links IS NULL OR NOT json_valid(links) OR json_extract(links, '$.XREF_LINK.DB') != 'pubmed') AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
    Datasources.Geo: "SELECT accession FROM geo_series WHERE (pubmed_id IS NULL OR pubmed_id = '' OR pubmed_id = '[]') AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
    Datasources.ArrayExpress: "SELECT accession FROM arrayexpress_experiments WHERE (pubmed_id IS NULL OR pubmed_id = '') AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
    Datasources.Ena: "SELECT study_accession FROM ena_studies WHERE (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
}
HAS_ORIGINAL_PMID_QUERIES: dict[Datasources, str] = {
    Datasources.Sra: "SELECT COUNT(*) FROM sra_studies WHERE json_valid(links) AND json_extract(links, '$.XREF_LINK.DB') = 'pubmed' AND json_extract(links, '$.XREF_LINK.ID') IS NOT NULL",
    Datasources.Geo: "SELECT COUNT(*) FROM geo_series WHERE pubmed_id IS NOT NULL AND pubmed_id != '' AND pubmed_id != '[]'",
    Datasources.ArrayExpress: "SELECT COUNT(*) FROM arrayexpress_experiments WHERE pubmed_id IS NOT NULL AND pubmed_id != ''",
    Datasources.Ena: "SELECT 0",
}
BACKFILLED_PMID_QUERIES: dict[Datasources, str] = {
    Datasources.Sra: "SELECT COUNT(*) FROM sra_studies WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    Datasources.Geo: "SELECT COUNT(*) FROM geo_series WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    Datasources.ArrayExpress: "SELECT COUNT(*) FROM arrayexpress_experiments WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    Datasources.Ena: "SELECT COUNT(*) FROM ena_studies WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
}


def backfill_pubmed(
    datasource: Datasources,
    db_file: str,
    keys_file: str,
    num_workers: int,
    batch_size: int,
    is_dry_run: bool,
):
    if not db_file:
        logging.fatal("missing db file (--db-file)")
        sys.exit(1)

    if not keys_file:
        logging.fatal("missing api keys file flag (--pubmed-ncbi-api-keys)")
        sys.exit(1)

    table = DATASOURCE_TABLE_MAPPING[datasource]

    with sqlite3.Connection(db_file) as conn:
        _add_col_if_not_exists(conn, table, "pubmed_id_pysradb", "TEXT")
        _add_col_if_not_exists(conn, table, "pubmed_last_checked", "TEXT")

        missing_query = MISSING_QUERIES[datasource]
        has_original_query = HAS_ORIGINAL_PMID_QUERIES[datasource]
        backfilled_query = BACKFILLED_PMID_QUERIES[datasource]

        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        already_checked = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE pubmed_last_checked IS NOT NULL"
        ).fetchone()[0]
        missing_pubmed_ids = conn.execute(missing_query).fetchall()
        num_has_original = conn.execute(has_original_query).fetchone()[0]
        num_backfilled = conn.execute(backfilled_query).fetchone()[0]

    missing_pubmed_ids = [id for (id,) in missing_pubmed_ids]

    logging.info(
        "%s: %d total\n\t\t\t%d have original pubmed_id\n\t\t\t%d found via pysradb\n\t\t\t%d already checked\n\t\t\t%d to check (%.1f%%)",
        str(datasource),
        total,
        num_has_original,
        num_backfilled,
        already_checked,
        len(missing_pubmed_ids),
        (len(missing_pubmed_ids) * 100) / total if total else 0,
    )

    if is_dry_run or len(missing_pubmed_ids) == 0:
        return

    with open(str(keys_file), "r") as f:
        keys = f.read().splitlines()

    if len(keys) != num_workers:
        num_workers = len(keys)
        logging.warning("using %d as num workers", num_workers)

    worker_data: list[list[str]] = [[] for _ in range(num_workers)]
    for i, pmid in enumerate(missing_pubmed_ids):
        worker_data[i % num_workers].append(pmid)

    with ThreadPoolExecutor(num_workers) as pool:
        futures = [
            pool.submit(
                _worker, keys[i], datasource, worker_data[i], db_file, batch_size, i
            )
            for i in range(num_workers)
        ]

        for f in as_completed(futures):
            f.result()


def _worker(
    api_key: str,
    datasource: Datasources,
    data: list[str],
    db_file: str,
    batch_size: int,
    worker_id: int = 0,
):
    sra = SRAweb(api_key)
    is_sra_related = datasource in (Datasources.Sra, Datasources.Ena)
    col_name = "srp_accession" if is_sra_related else "gse_accession"

    with tqdm(
        total=len(data),
        desc=f"[worker-{worker_id}] processing accessions...",
        unit=" accession",
        position=worker_id,
    ) as pbar:
        for batch in _chunked(data, batch_size):
            df = sra.sra_to_pmid(batch) if is_sra_related else sra.gse_to_pmid(batch)
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            pmid_updates = []

            for _, row in df.iterrows():
                acc = row.get(col_name)
                pmid = _parse_pmid(row.get("pmid"))
                if acc and pmid:
                    pmid_updates.append((pmid, acc))

            with sqlite3.Connection(db_file) as conn:
                table = DATASOURCE_TABLE_MAPPING[datasource]
                conn.execute(f"UPDATE {table} SET pubmed_last_checked = ?", (now,))
                if pmid_updates:
                    conn.executemany(
                        f"UPDATE {table} SET pubmed_id_pysradb = ? WHERE accession = ?",
                        pmid_updates,
                    )
                conn.commit()

            pbar.update(len(batch))


def _add_col_if_not_exists(
    conn: sqlite3.Connection, table: str, column: str, col_type: str
):
    cursor = conn.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def _parse_pmid(pmid) -> str | None:
    if pd.isna(pmid):
        return None
    s = str(pmid).strip()
    if not s:
        return None

    return s


def _chunked(lst: list, batch_size: int):
    for i in range(0, len(lst), batch_size):
        yield lst[i : i + batch_size]
