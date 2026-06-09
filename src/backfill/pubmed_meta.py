import logging
import multiprocessing as mp
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing.sharedctypes import Synchronized
from pathlib import Path
from typing import Callable

import pandas as pd
import tqdm
from pysradb.sraweb import SRAweb

from backfill._utils import _chunked, _parse_pmid
from backfill.pubmed_config import BackfillConfig
from config import Datasource
from helpers import exec_query_from_file

_PUBMED_META_OUTPUT_DIR = "pubmed_meta"


PMID_EXTRACT_QUERIES = {
    "sra": [
        "SELECT DISTINCT json_extract(links, '$.XREF_LINK.ID') FROM sra_studies WHERE json_valid(links) AND json_extract(links, '$.XREF_LINK.DB') = 'pubmed' AND json_extract(links, '$.XREF_LINK.ID') IS NOT NULL",
        "SELECT DISTINCT pubmed_id_pysradb FROM sra_studies WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    ],
    "geo": [
        "SELECT DISTINCT value FROM geo_series, json_each(geo_series.pubmed_id) WHERE pubmed_id IS NOT NULL AND pubmed_id != '' AND pubmed_id != '[]' AND json_valid(pubmed_id)",
        "SELECT DISTINCT pubmed_id_pysradb FROM geo_series WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    ],
    "ae": [
        "SELECT DISTINCT pubmed_id FROM arrayexpress_experiments WHERE pubmed_id IS NOT NULL AND pubmed_id != ''",
        "SELECT DISTINCT pubmed_id_pysradb FROM arrayexpress_experiments WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    ],
    "ena": [
        "SELECT DISTINCT pubmed_id_pysradb FROM ena_studies WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    ],
}
UPSERT_SQL = """
INSERT INTO pubmed_metadata (
    pmid, title, journal, doi, pub_date, authors, issn,
    citation_count, journal_h_index, journal_i10_index,
    journal_2yr_mean_citedness, journal_cited_by_count, journal_works_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (pmid) DO UPDATE SET
    title = EXCLUDED.title,
    journal = EXCLUDED.journal,
    doi = EXCLUDED.doi,
    pub_date = EXCLUDED.pub_date,
    authors = EXCLUDED.authors,
    issn = EXCLUDED.issn,
    citation_count = EXCLUDED.citation_count,
    journal_h_index = EXCLUDED.journal_h_index,
    journal_i10_index = EXCLUDED.journal_i10_index,
    journal_2yr_mean_citedness = EXCLUDED.journal_2yr_mean_citedness,
    journal_cited_by_count = EXCLUDED.journal_cited_by_count,
    journal_works_count = EXCLUDED.journal_works_count
"""
UPDATE_METRICS_SQL = """
UPDATE pubmed_metadata SET
    citation_count             = COALESCE(?, citation_count),
    journal_h_index            = COALESCE(?, journal_h_index),
    journal_i10_index          = COALESCE(?, journal_i10_index),
    journal_2yr_mean_citedness = COALESCE(?, journal_2yr_mean_citedness),
    journal_cited_by_count     = COALESCE(?, journal_cited_by_count),
    journal_works_count        = COALESCE(?, journal_works_count)
WHERE pmid = ?
"""


@dataclass
class Counters:
    total: Synchronized[int]
    found: Synchronized[int]
    errors: Synchronized[int]
    citations: Synchronized[int]
    journals: Synchronized[int]


def backfill_pubmed_meta(cfg: BackfillConfig):
    output_dir = Path(cfg.base_output_dir) / _PUBMED_META_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    meta_db_file = output_dir / "pubmed_meta.db"
    exec_query_from_file("tables/pubmed_meta_tables.sql", db_file=meta_db_file)

    with sqlite3.Connection(cfg.db_file) as conn:
        exec_query_from_file("tables/pubmed_meta_tables.sql", conn=conn)

        all_pmids = _collect_all_pmids(conn, cfg.datasource)
        existing_pmids = _collect_existing_pmids(conn)
        log_prefix = f"pmids: {len(all_pmids)} unique, {len(existing_pmids)} already have metadata"

        # only re-fetch metrics for records missing citation count or journal metrics
        if cfg.fill_metrics:
            missing_metrics = _collect_pmids_with_missing_metrics(conn)
            target = sorted(all_pmids & missing_metrics)
            logging.info(
                "%s, %d missing metrics, filling metrics for %d",
                log_prefix,
                len(missing_metrics),
                len(target),
            )
        # re-fetch metrics for all records
        elif cfg.update_metrics:
            target = sorted(all_pmids & existing_pmids)
            logging.info("%s, updating metrics for %d", log_prefix, target)
        # fetching metadata for pmids who are not yet present in the table
        else:
            target = sorted(all_pmids - existing_pmids)
            logging.info("%s, %d to fetch", log_prefix, len(target))

    if cfg.is_dry_run or not target:
        return

    with open(cfg.ncbi_keys_file, "r") as f:
        ncbi_keys = f.read().splitlines()
    with open(cfg.openalex_keys_file, "r") as f:
        openalex_keys = f.read().splitlines()

    total_batches = (len(target) + cfg.batch_size - 1) // cfg.batch_size
    optimal_num_workers = max(len(ncbi_keys), len(openalex_keys))
    num_workers = cfg.num_workers
    if cfg.num_workers != optimal_num_workers:
        num_workers = optimal_num_workers
        logging.warning("using %d as num workers", optimal_num_workers)

    worker_data: list[list[str]] = [[] for _ in range(num_workers)]
    for i, pmid in enumerate(target):
        worker_data[i % num_workers].append(pmid)

    sql = UPDATE_METRICS_SQL if cfg.update_metrics else UPSERT_SQL
    to_tuple = _row_to_metrics_tuple if cfg.update_metrics else _row_to_tuple

    counters = Counters(
        total=mp.Value("i", 0),
        found=mp.Value("i", 0),
        errors=mp.Value("i", 0),
        citations=mp.Value("i", 0),
        journals=mp.Value("i", 0),
    )

    pbar = tqdm.tqdm(
        total=total_batches,
        desc="backfilling pubmed metadata",
        unit="batch",
        position=0,
    )

    with ThreadPoolExecutor(num_workers) as pool:
        futures = []

        for i in range(num_workers):
            ncbi_key = ncbi_keys[i % len(ncbi_keys)]
            openalex_key = openalex_keys[i % len(openalex_keys)]

            futures.append(
                pool.submit(
                    _worker,
                    meta_db_file,
                    worker_data[i],
                    sql,
                    to_tuple,
                    ncbi_key,
                    openalex_key,
                    cfg.batch_size,
                    cfg.skip_journal_metrics,
                    counters,
                    pbar,
                )
            )

        for f in futures:
            f.result()

    pbar.close()


def _worker(
    db_file: Path,
    data: list[str],
    sql: str,
    to_tuple: Callable[[dict], tuple],
    ncbi_key: str,
    openalex_key: str,
    batch_size: int,
    skip_journal_metrics: bool,
    counters: Counters,
    pbar: tqdm.tqdm,
):
    sra = SRAweb(api_key=ncbi_key, openalex_api_key=openalex_key)
    local_found = 0
    local_errors = 0

    with sqlite3.Connection(db_file) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        for batch in _chunked(data, batch_size):
            try:
                df: pd.DataFrame = sra.pmid_info(batch, skip_journal_metrics)
                rows = []
                batch_citations = 0
                batch_journals = 0

                for _, row in df.iterrows():
                    pmid = _parse_pmid(row.get("pmid"))
                    if pmid:
                        rows.append(to_tuple(row.to_dict()))
                        if pd.notna(row.get("citation_count")):
                            batch_citations += 1
                        if pd.notna(row.get("journal_h_index")):
                            batch_journals += 1

                if rows:
                    conn.executemany(sql, rows)
                    conn.commit()
                    local_found += len(rows)
                    with counters.found.get_lock():
                        counters.found.value += len(rows)

                with counters.citations.get_lock():
                    counters.citations.value += batch_citations
                with counters.journals.get_lock():
                    counters.journals.value += batch_journals
                with counters.total.get_lock():
                    counters.total.value += 1

                pbar.update(1)
                pbar.set_postfix(
                    found=counters.found.value,
                    errors=counters.errors.value,
                    refresh=False,
                )
            except Exception as e:
                local_errors += len(batch)
                with counters.errors.get_lock():
                    counters.errors.value += local_errors
                with counters.total.get_lock():
                    counters.total.value += 1

                pbar.update(1)
                pbar.set_postfix(
                    found=counters.found.value,
                    errors=counters.errors.value,
                    refresh=False,
                )

                logging.exception("batch failed (%d pubmed ids): %s", len(batch), e)

    logging.info(
        "finished: %d metadata rows, %d errors out of %d pubmed ids",
        local_found,
        local_errors,
        len(data),
    )


def _collect_all_pmids(conn: sqlite3.Connection, datasource: Datasource) -> set[str]:
    all_pmids: set[str] = set()

    for query in PMID_EXTRACT_QUERIES[datasource]:
        rows = conn.execute(query).fetchall()
        for (raw,) in rows:
            pmid = _parse_pmid(raw)
            if pmid:
                all_pmids.add(pmid)

    return all_pmids


def _collect_existing_pmids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT pmid FROM pubmed_metadata").fetchall()
    return {r[0] for r in rows}


def _collect_pmids_with_missing_metrics(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """SELECT pmid FROM pubmed_metadata
        WHERE citation_count IS NULL
            OR journal_h_index IS NULL
            OR journal_i10_index IS NULL
            OR journal_2yr_mean_citedness IS NULL
            OR journal_cited_by_count IS NULL
            OR journal_works_count IS NULL"""
    ).fetchall()
    return {r[0] for r in rows}


def _normalize_val(v):
    if v is None:
        return None
    if not (pd.notna(v) if not isinstance(v, str) else bool(v)):
        return None
    return v


def _to_number(v):
    v = _normalize_val(v)

    try:
        return int(v)
    except (ValueError, TypeError):
        pass
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _row_to_tuple(row: dict) -> tuple:
    return (
        str(row.get("pmid", "")),
        _normalize_val(row.get("title")),
        _normalize_val(row.get("journal")),
        _normalize_val(row.get("doi")),
        _normalize_val(row.get("pub_date")),
        _normalize_val(row.get("authors")),
        _normalize_val(row.get("issn")),
        _to_number(row.get("citation_count")),
        _to_number(row.get("journal_h_index")),
        _to_number(row.get("journal_i10_index")),
        _to_number(row.get("journal_2yr_mean_citedness")),
        _to_number(row.get("journal_cited_by_count")),
        _to_number(row.get("journal_works_count")),
    )


def _row_to_metrics_tuple(row: dict) -> tuple:
    return (
        _to_number(row.get("citation_count")),
        _to_number(row.get("journal_h_index")),
        _to_number(row.get("journal_i10_index")),
        _to_number(row.get("journal_2yr_mean_citedness")),
        _to_number(row.get("journal_cited_by_count")),
        _to_number(row.get("journal_works_count")),
        str(row.get("pmid", "")),
    )
