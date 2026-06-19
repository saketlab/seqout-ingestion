import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from pysradb.sraweb import SRAweb
from tqdm import tqdm

from backfill.backfill_utils import _chunked, _parse_pmid
from pipeline_types import Datasource

DATASOURCE_TABLE_MAPPING: dict[Datasource, str] = {
    Datasource.Sra: "sra_studies",
    Datasource.Geo: "geo_series",
    Datasource.ArrayExpress: "arrayexpress_experiments",
    Datasource.Ena: "ena_studies",
}

DATASOURCE_COLUMN_MAPPING: dict[Datasource, str] = {
    Datasource.Sra: "accession",
    Datasource.Geo: "accession",
    Datasource.ArrayExpress: "accession",
    Datasource.Ena: "study_accession",
}

MISSING_QUERIES: dict[Datasource, str] = {
    Datasource.Sra: "SELECT accession FROM sra_studies WHERE (links IS NULL OR NOT json_valid(links) OR json_extract(links, '$.XREF_LINK.DB') != 'pubmed') AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
    Datasource.Geo: "SELECT accession FROM geo_series WHERE (pubmed_id IS NULL OR pubmed_id = '' OR pubmed_id = '[]') AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
    Datasource.ArrayExpress: "SELECT accession FROM arrayexpress_experiments WHERE (pubmed_id IS NULL OR pubmed_id = '') AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
    Datasource.Ena: "SELECT study_accession FROM ena_studies WHERE (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')",
}

HAS_ORIGINAL_PMID_QUERIES: dict[Datasource, str] = {
    Datasource.Sra: "SELECT COUNT(*) FROM sra_studies WHERE json_valid(links) AND json_extract(links, '$.XREF_LINK.DB') = 'pubmed' AND json_extract(links, '$.XREF_LINK.ID') IS NOT NULL",
    Datasource.Geo: "SELECT COUNT(*) FROM geo_series WHERE pubmed_id IS NOT NULL AND pubmed_id != '' AND pubmed_id != '[]'",
    Datasource.ArrayExpress: "SELECT COUNT(*) FROM arrayexpress_experiments WHERE pubmed_id IS NOT NULL AND pubmed_id != ''",
    Datasource.Ena: "SELECT 0",
}

BACKFILLED_PMID_QUERIES: dict[Datasource, str] = {
    Datasource.Sra: "SELECT COUNT(*) FROM sra_studies WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    Datasource.Geo: "SELECT COUNT(*) FROM geo_series WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    Datasource.ArrayExpress: "SELECT COUNT(*) FROM arrayexpress_experiments WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
    Datasource.Ena: "SELECT COUNT(*) FROM ena_studies WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''",
}


def backfill_pubmed_ids(
    geo_db_file: Optional[Path],
    sra_db_file: Optional[Path],
    ena_db_file: Optional[Path],
    ae_db_file: Optional[Path],
    ncbi_keys_file: Path,
    num_workers: int,
    batch_size: int,
    is_dry_run: bool,
    process_all: bool = False,
):
    if (
        (not geo_db_file)
        and (not sra_db_file)
        and (not ena_db_file)
        and (not ae_db_file)
    ):
        raise ValueError(
            "expected at least one of --geo-db-file, --sra-db-file, --ena-db-file or --ae-db-file. recieved none of them"
        )

    raw_mapping: dict[Datasource, Path | None] = {
        Datasource.Geo: geo_db_file,
        Datasource.Sra: sra_db_file,
        Datasource.Ena: ena_db_file,
        Datasource.ArrayExpress: ae_db_file,
    }
    mapping: dict[Datasource, Path] = {
        k: v for k, v in raw_mapping.items() if v is not None
    }

    missing_by_datasource: dict[Datasource, list[str]] = {}
    total = 0
    already_checked = 0
    num_has_original = 0
    num_backfilled = 0

    for datasource, db_file in mapping.items():
        table = DATASOURCE_TABLE_MAPPING[datasource]

        with sqlite3.Connection(db_file) as conn:
            missing_query = MISSING_QUERIES[datasource]
            if not process_all:
                missing_query += " AND pubmed_last_checked IS NULL"
            has_original_query = HAS_ORIGINAL_PMID_QUERIES[datasource]
            backfilled_query = BACKFILLED_PMID_QUERIES[datasource]

            total += conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            already_checked += conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE pubmed_last_checked IS NOT NULL"
            ).fetchone()[0]
            num_has_original += conn.execute(has_original_query).fetchone()[0]
            num_backfilled += conn.execute(backfilled_query).fetchone()[0]

            per_datasource_missing_pubmed_ids = conn.execute(missing_query).fetchall()
            missing_by_datasource[datasource] = [
                id for (id,) in per_datasource_missing_pubmed_ids
            ]

    total_missing = sum(len(v) for v in missing_by_datasource.values())
    logging.info(
        "%d total, across {%s} datasources | %d have original pid | %d via pysradb | %d checked | %d to check (%.2f%%)",
        total,
        ",".join(mapping.keys()),
        num_has_original,
        num_backfilled,
        already_checked,
        total_missing,
        (total_missing * 100) / total if total else 0,
    )

    if is_dry_run or total_missing == 0:
        return

    with open(str(ncbi_keys_file), "r") as f:
        keys = f.read().splitlines()

    optimal_num_workers = len(keys)
    num_workers = num_workers
    if num_workers != optimal_num_workers:
        num_workers = optimal_num_workers
        logging.warning("using %d as num workers", num_workers)

    for datasource, db_file in mapping.items():
        accessions = missing_by_datasource[datasource]
        if not accessions:
            continue

        logging.info("processing %s - %d accessions\n", datasource, len(accessions))
        worker_data: list[list[str]] = [[] for _ in range(num_workers)]
        for i, accession in enumerate(accessions):
            worker_data[i % num_workers].append(accession)

        pbar = tqdm(total=len(accessions), unit="acc", desc=datasource.value)

        with ThreadPoolExecutor(num_workers) as pool:
            futures = [
                pool.submit(
                    _worker,
                    keys[i],
                    datasource,
                    worker_data[i],
                    db_file,
                    batch_size,
                    pbar,
                )
                for i in range(num_workers)
            ]

            for f in as_completed(futures):
                f.result()

        pbar.close()


def _worker(
    api_key: str,
    datasource: Datasource,
    data: list[str],
    db_file: Path,
    batch_size: int,
    pbar: tqdm,
):
    sra = SRAweb(api_key, verbose=False)
    is_sra_related = datasource in (Datasource.Sra, Datasource.Ena)
    col_name = "srp_accession" if is_sra_related else "gse_accession"

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for batch in _chunked(data, batch_size):
        try:
            df = sra.sra_to_pmid(batch) if is_sra_related else sra.gse_to_pmid(batch)
        except KeyError:
            df = pd.DataFrame()

        pmid_updates = []

        for _, row in df.iterrows():
            acc = row.get(col_name)
            pmid = _parse_pmid(row.get("pmid"))
            if acc and pmid:
                pmid_updates.append((pmid, acc))

        with sqlite3.Connection(db_file) as conn:
            conn.execute("PRAGMA busy_timeout=60000")

            table = DATASOURCE_TABLE_MAPPING[datasource]
            column = DATASOURCE_COLUMN_MAPPING[datasource]

            accessions_in_batch = [a for a in batch if a]
            placeholders = ",".join("?" for _ in accessions_in_batch)
            conn.execute(
                f"UPDATE {table} SET pubmed_last_checked = ? WHERE {column} IN ({placeholders})",
                (now, *accessions_in_batch),
            )

            if pmid_updates:
                conn.executemany(
                    f"UPDATE {table} SET pubmed_id_pysradb = ? WHERE {column} = ?",
                    pmid_updates,
                )

            conn.commit()
        pbar.update(len(batch))
