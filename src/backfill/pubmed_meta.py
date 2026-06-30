import functools
import logging
import multiprocessing as mp
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing.sharedctypes import Synchronized
from pathlib import Path

import pandas as pd
import tqdm
from pysradb.sraweb import SRAweb

from backfill.backfill_utils import _chunked, _parse_pmid
from helpers import exec_query_from_file
from pipeline_types import Datasource

logger = logging.getLogger(__name__)


@dataclass
class Counters:
    total: Synchronized[int]
    updated: Synchronized[int]
    errors: Synchronized[int]
    skipped: Synchronized[int] | None
    citations: Synchronized[int] | None
    journals: Synchronized[int] | None


PUBMED_METADATA_OUTPUT_DIR = "pubmed_metadata"

PMID_EXTRACT_QUERIES = {
    "sra": [
        (
            "SELECT DISTINCT json_extract(links, '$.XREF_LINK.ID')"
            " FROM sra_studies WHERE json_valid(links)"
            " AND json_extract(links, '$.XREF_LINK.DB') = 'pubmed'"
            " AND json_extract(links, '$.XREF_LINK.ID') IS NOT NULL"
        ),
        (
            "SELECT DISTINCT pubmed_id_pysradb FROM sra_studies"
            " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
        ),
    ],
    "geo": [
        (
            "SELECT DISTINCT value"
            " FROM geo_series, json_each(geo_series.pubmed_id)"
            " WHERE pubmed_id IS NOT NULL AND pubmed_id != ''"
            " AND pubmed_id != '[]' AND json_valid(pubmed_id)"
        ),
        (
            "SELECT DISTINCT pubmed_id_pysradb FROM geo_series"
            " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
        ),
    ],
    "ae": [
        (
            "SELECT DISTINCT pubmed_id FROM arrayexpress_experiments"
            " WHERE pubmed_id IS NOT NULL AND pubmed_id != ''"
        ),
        (
            "SELECT DISTINCT pubmed_id_pysradb"
            " FROM arrayexpress_experiments"
            " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
        ),
    ],
    "ena": [
        (
            "SELECT DISTINCT pubmed_id_pysradb FROM ena_studies"
            " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
        ),
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
    journal_works_count = EXCLUDED.journal_works_count;
"""

UPDATE_METRICS_SQL = """
UPDATE pubmed_metadata SET
    citation_count             = COALESCE(?, citation_count),
    journal_h_index            = COALESCE(?, journal_h_index),
    journal_i10_index          = COALESCE(?, journal_i10_index),
    journal_2yr_mean_citedness = COALESCE(?, journal_2yr_mean_citedness),
    journal_cited_by_count     = COALESCE(?, journal_cited_by_count),
    journal_works_count        = COALESCE(?, journal_works_count)
WHERE pmid = ?;
"""

JOURNAL_METRICS_UPDATE_SQL = """
UPDATE pubmed_metadata SET
    journal_h_index            = ?,
    journal_i10_index          = ?,
    journal_2yr_mean_citedness = ?,
    journal_cited_by_count     = ?,
    journal_works_count        = ?
WHERE journal = ?;
"""


def backfill_pubmed_metadata(
    base_output_dir: Path,
    geo_db_file: Path | None,
    sra_db_file: Path | None,
    ena_db_file: Path | None,
    ae_db_file: Path | None,
    db_file: Path | None,
    ncbi_keys_file: Path,
    openalex_keys_file: Path,
    num_workers: int,
    batch_size: int,
    fill_metrics: bool,
    update_metrics: bool,
    skip_journal_metrics: bool,
    update_journal_metrics: bool,
    refresh_all_journals_metrics: bool,
    is_dry_run: bool,
) -> None:
    if (
        (not geo_db_file)
        and (not sra_db_file)
        and (not ena_db_file)
        and (not ae_db_file)
    ):
        msg = (
            "expected at least one of --geo-db-file, --sra-db-file,"
            " --ena-db-file or --ae-db-file. recieved none of them"
        )
        raise ValueError(msg)

    output_dir = base_output_dir / PUBMED_METADATA_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (output_dir / "pubmed_metadata.db")

    raw_mapping: dict[Datasource, Path | None] = {
        Datasource.Geo: geo_db_file,
        Datasource.Sra: sra_db_file,
        Datasource.Ena: ena_db_file,
        Datasource.ArrayExpress: ae_db_file,
    }
    mapping: dict[Datasource, Path] = {
        k: v for k, v in raw_mapping.items() if v is not None
    }

    exec_query_from_file("tables/pubmed_metadata.sql", db_file)

    if update_journal_metrics:
        _process_via_per_journal_lookup(
            db_file,
            ncbi_keys_file,
            openalex_keys_file,
            num_workers,
            batch_size,
            refresh_all_journals_metrics,
            is_dry_run,
        )
    else:
        for datasource, ds_db_file in mapping.items():
            _process_via_per_pmid_lookup(
                datasource,
                ds_db_file,
                db_file,
                ncbi_keys_file,
                openalex_keys_file,
                num_workers,
                batch_size,
                fill_metrics,
                update_metrics,
                skip_journal_metrics,
                is_dry_run,
            )


def _process_via_per_journal_lookup(
    db_file: Path,
    ncbi_keys_file: Path,
    openalex_keys_file: Path,
    num_workers: int,
    batch_size: int,
    refresh_all_journals_metrics: bool,
    is_dry_run: bool,
) -> None:
    with sqlite3.Connection(db_file) as conn:
        if refresh_all_journals_metrics:
            target = _collect_all_journals(conn)
        else:
            target = _collect_journals_with_missing_metrics(conn)

    total = len(target)
    logger.info(
        "found %d unique journals to look up (refresh_all=%s)",
        total,
        refresh_all_journals_metrics,
    )

    if is_dry_run or not target:
        return

    ncbi_keys = ncbi_keys_file.read_text().splitlines()
    openalex_keys = openalex_keys_file.read_text().splitlines()

    num_workers, _ = _calc_num_workers_and_batches(
        target,
        num_workers,
        batch_size,
        len(ncbi_keys),
        len(openalex_keys),
    )

    worker_data: list[list[tuple[str, str]]] = [[] for _ in range(num_workers)]
    for i, (journal, issn) in enumerate(target):
        worker_data[i % num_workers].append((journal, issn))

    counters = Counters(
        total=mp.Value("i", 0),
        updated=mp.Value("i", 0),
        errors=mp.Value("i", 0),
        skipped=mp.Value("i", 0),
        citations=None,
        journals=None,
    )

    partial_worker_fn = functools.partial(
        _journal_lookup_worker,
        db_file=db_file,
        sql=JOURNAL_METRICS_UPDATE_SQL,
        to_tuple=_journal_row_to_tuple,
        batch_size=batch_size,
        counters=counters,
    )

    pbar = tqdm.tqdm(
        total=total,
        desc="backfilling pubmed metadata via journal lookups",
        unit="journal",
    )

    _worker_loop(
        partial_worker_fn,
        num_workers,
        worker_data,
        ncbi_keys,
        openalex_keys,
        counters,
        pbar,
    )

    logger.info(
        "journal lookup finished: %d updated, %d errors out of %d journals",
        counters.updated.value,
        counters.errors.value,
        total,
    )


def _process_via_per_pmid_lookup(
    datasource: Datasource,
    datasource_db_file: Path,
    meta_db_file: Path,
    ncbi_keys_file: Path,
    openalex_keys_file: Path,
    num_workers: int,
    batch_size: int,
    fill_metrics: bool,
    update_metrics: bool,
    skip_journal_metrics: bool,
    is_dry_run: bool,
) -> None:
    logger.info("processing %s datasource...", datasource)

    with sqlite3.Connection(datasource_db_file) as conn:
        all_pmids = _collect_all_pmids(conn, datasource)

    with sqlite3.Connection(meta_db_file) as meta_conn:
        existing_pmids = _collect_existing_pmids(meta_conn)
        total_pmids = len(all_pmids)
        existing = len(existing_pmids)
        log_prefix = f"pmids: {total_pmids} unique, {existing} already have metadata"

        if fill_metrics:
            missing_metrics = _collect_pmids_with_missing_metrics(meta_conn)
            target = all_pmids & missing_metrics
            logger.info(
                "%s, %d missing metrics, filling metrics for %d",
                log_prefix,
                len(missing_metrics),
                len(target),
            )
        elif update_metrics:
            target = all_pmids & existing_pmids
            logger.info("%s, updating metrics for %d", log_prefix, len(target))
        else:
            target = all_pmids - existing_pmids
            logger.info("%s, %d to fetch", log_prefix, len(target))

    if is_dry_run or not target:
        return

    ncbi_keys = ncbi_keys_file.read_text().splitlines()
    openalex_keys = openalex_keys_file.read_text().splitlines()

    num_workers, total_batches = _calc_num_workers_and_batches(
        target,
        num_workers,
        batch_size,
        len(ncbi_keys),
        len(openalex_keys),
    )

    worker_data: list[list[str]] = [[] for _ in range(num_workers)]
    for i, pmid in enumerate(target):
        worker_data[i % num_workers].append(pmid)

    sql = UPDATE_METRICS_SQL if update_metrics else UPSERT_SQL
    to_tuple = _pmid_row_to_metrics_tuple if update_metrics else _pmid_row_to_tuple

    counters = Counters(
        total=mp.Value("i", 0),
        updated=mp.Value("i", 0),
        errors=mp.Value("i", 0),
        skipped=None,
        citations=mp.Value("i", 0),
        journals=mp.Value("i", 0),
    )

    partial_worker_fn = functools.partial(
        _pmid_lookup_worker,
        db_file=meta_db_file,
        sql=sql,
        to_tuple=to_tuple,
        batch_size=batch_size,
        skip_journal_metrics=skip_journal_metrics,
        counters=counters,
    )

    pbar = tqdm.tqdm(
        total=total_batches,
        desc="backfilling pubmed metadata via pmid lookups",
        unit="batch",
    )

    _worker_loop(
        partial_worker_fn,
        num_workers,
        worker_data,
        ncbi_keys,
        openalex_keys,
        counters,
        pbar,
    )

    logger.info(
        "pmid lookup finished: %d updated, %d errors out of %d pmids",
        counters.updated.value,
        counters.errors.value,
        len(target),
    )


def _worker_loop(
    worker: functools.partial,
    num_workers: int,
    worker_data: list,
    ncbi_keys: list[str],
    openalex_keys: list[str],
    counters: Counters,
    pbar: tqdm.tqdm,
) -> None:
    with (
        ThreadPoolExecutor(num_workers) as pool,
    ):
        futures = []

        for i in range(num_workers):
            ncbi_key = ncbi_keys[i % len(ncbi_keys)]
            openalex_key = openalex_keys[i % len(openalex_keys)]

            futures.append(
                pool.submit(
                    worker,
                    data=worker_data[i],
                    ncbi_key=ncbi_key,
                    openalex_key=openalex_key,
                    pbar=pbar,
                    counters=counters,
                ),
            )

        for f in futures:
            f.result()


def _journal_lookup_worker(
    db_file: Path,
    data: list[tuple[str, str]],
    ncbi_key: str,
    openalex_key: str,
    sql: str,
    to_tuple: Callable[[dict], tuple],
    batch_size: int,
    counters: Counters,
    pbar: tqdm.tqdm,
) -> None:
    sra = SRAweb(api_key=ncbi_key, openalex_api_key=openalex_key)
    local_updated = 0
    local_errors = 0

    with sqlite3.Connection(db_file) as conn:
        _setup_wal(conn)

        rows = []

        def _flush() -> None:
            nonlocal local_updated, rows
            if rows:
                conn.executemany(sql, rows)
                conn.commit()

                local_updated += len(rows)
                with counters.updated.get_lock():
                    counters.updated.value += len(rows)
                rows = []

        def _pbar_update() -> None:
            pbar.set_postfix(
                updated=counters.updated.value,
                errors=counters.errors.value,
                refresh=False,
            )
            pbar.update(1)

        for journal, issn in data:
            try:
                journal_metrics: dict = sra._openalex_source_lookup(issn, journal)  # noqa: SLF001
                row = (
                    *to_tuple(
                        journal_metrics,
                    ),
                    journal,
                )

                h_index = row[0]
                i10_index = row[1]
                cited_by = row[2]

                if h_index is None or i10_index is None or cited_by is None:
                    if counters.skipped:
                        with counters.skipped.get_lock():
                            counters.skipped.value += 1
                else:
                    rows.append(row)
                    if len(rows) >= batch_size:
                        _flush()

                _pbar_update()
            except Exception:
                local_errors += 1
                with counters.errors.get_lock():
                    counters.errors.value += 1
                with counters.total.get_lock():
                    counters.total.value += 1

                _pbar_update()
                logger.exception("failed to fetch journal lookup (%s)", journal)

        if rows:
            _flush()
    if data:
        with counters.total.get_lock():
            counters.total.value += len(data)


def _pmid_lookup_worker(
    db_file: Path,
    data: list[str],
    ncbi_key: str,
    openalex_key: str,
    sql: str,
    to_tuple: Callable[[dict], tuple],
    batch_size: int,
    skip_journal_metrics: bool,
    counters: Counters,
    pbar: tqdm.tqdm,
) -> None:
    sra = SRAweb(api_key=ncbi_key, openalex_api_key=openalex_key)
    local_updated = 0
    local_errors = 0

    def _pbar_update() -> None:
        pbar.set_postfix(
            updated=counters.updated.value,
            errors=counters.errors.value,
            refresh=False,
        )
        pbar.update(1)

    with sqlite3.Connection(db_file) as conn:
        _setup_wal(conn)

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

                    local_updated += len(rows)
                    with counters.updated.get_lock():
                        counters.updated.value += len(rows)

                with counters.total.get_lock():
                    counters.total.value += 1

                if counters.citations:
                    with counters.citations.get_lock():
                        counters.citations.value += batch_citations

                if counters.journals:
                    with counters.journals.get_lock():
                        counters.journals.value += batch_journals

                _pbar_update()
            except Exception:
                local_errors += len(batch)
                with counters.errors.get_lock():
                    counters.errors.value += len(batch)
                with counters.total.get_lock():
                    counters.total.value += 1

                _pbar_update()
                logger.exception("batch failed (%d pubmed ids)", len(batch))


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
        """
        SELECT pmid FROM pubmed_metadata
            WHERE citation_count IS NULL
                OR journal_h_index IS NULL
                OR journal_i10_index IS NULL
                OR journal_2yr_mean_citedness IS NULL
                OR journal_cited_by_count IS NULL
                OR journal_works_count IS NULL;
        """,
    ).fetchall()
    return {r[0] for r in rows}


def _collect_all_journals(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT DISTINCT journal, issn FROM pubmed_metadata
           WHERE journal IS NOT NULL AND journal != '';
        """,
    ).fetchall()
    return set(rows)


def _collect_journals_with_missing_metrics(
    conn: sqlite3.Connection,
) -> set[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT DISTINCT journal, issn FROM pubmed_metadata
           WHERE journal IS NOT NULL AND journal != ''
             AND (journal_h_index IS NULL
                  OR journal_i10_index IS NULL
                  OR journal_2yr_mean_citedness IS NULL
                  OR journal_cited_by_count IS NULL
                  OR journal_works_count IS NULL);
        """,
    ).fetchall()
    return set(rows)


def _setup_wal(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")


def _calc_num_workers_and_batches(
    target: set,
    num_workers: int,
    batch_size: int,
    num_ncbi_keys: int,
    num_openalex_keys: int,
) -> tuple[int, int]:
    total_batches = (len(target) + batch_size - 1) // batch_size
    optimal_num_workers = max(num_ncbi_keys, num_openalex_keys)
    if num_workers != optimal_num_workers:
        num_workers = optimal_num_workers
        logger.warning("using %d as num workers", optimal_num_workers)

    return (num_workers, total_batches)


def _normalize_val(v: object) -> str | None:
    if v is None:
        return None
    if not (pd.notna(v) if not isinstance(v, str) else bool(v)):
        return None
    return str(v)


def _to_number(v: object) -> int | float | None:
    v = _normalize_val(v)
    if v:
        try:
            return int(v)
        except (ValueError, TypeError):
            pass

        try:
            return float(v)
        except (ValueError, TypeError):
            return None
    else:
        return None


def _pmid_row_to_tuple(row: dict) -> tuple:
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


def _pmid_row_to_metrics_tuple(row: dict) -> tuple:
    return (
        _to_number(row.get("citation_count")),
        _to_number(row.get("journal_h_index")),
        _to_number(row.get("journal_i10_index")),
        _to_number(row.get("journal_2yr_mean_citedness")),
        _to_number(row.get("journal_cited_by_count")),
        _to_number(row.get("journal_works_count")),
        str(row.get("pmid", "")),
    )


def _journal_row_to_tuple(journal: dict) -> tuple:
    return (
        _to_number(journal.get("journal_h_index")),
        _to_number(journal.get("journal_i10_index")),
        _to_number(journal.get("journal_2yr_mean_citedness")),
        _to_number(journal.get("journal_cited_by_count")),
        _to_number(journal.get("journal_works_count")),
    )
