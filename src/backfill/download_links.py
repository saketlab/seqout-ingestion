import csv
import http
import io
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from time import sleep
from typing import Literal

import httpx
from pysradb.sraweb import SRAweb
from tqdm import tqdm

from helpers import exec_query_from_file, sqlite_connect
from pipeline_types import Datasource

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

ENA_FILEREPORT_URL = "https://www.ebi.ac.uk/ena/portal/api/filereport"
FILEREPORT_FIELDS = (
    "run_accession,experiment_accession,fastq_ftp,fastq_md5,fastq_bytes,"
    "sra_ftp,sra_md5,sra_bytes,library_layout"
)


UPSERT_RUN_SQL = """
INSERT INTO run_download_links (
    run_accession, study_accession, experiment_accession, library_layout,
    fastq_ftp, fastq_bytes, fastq_md5,
    sra_ftp, sra_bytes, sra_md5, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (run_accession) DO UPDATE SET
    study_accession      = EXCLUDED.study_accession,
    experiment_accession = EXCLUDED.experiment_accession,
    library_layout       = EXCLUDED.library_layout,
    fastq_ftp            = EXCLUDED.fastq_ftp,
    fastq_bytes          = EXCLUDED.fastq_bytes,
    fastq_md5            = EXCLUDED.fastq_md5,
    sra_ftp              = EXCLUDED.sra_ftp,
    sra_bytes            = EXCLUDED.sra_bytes,
    sra_md5              = EXCLUDED.sra_md5,
    updated_at           = EXCLUDED.updated_at
"""

UPSERT_NCBI_RUN_SQL = """
INSERT INTO run_download_links (
    run_accession, study_accession, experiment_accession,
    ncbi_files, ncbi_sra_url, ncbi_sra_url_aws, ncbi_fetched_at,
    ncbi_sra_normalized_url, ncbi_sra_normalized_bytes,
    ncbi_sra_lite_url, ncbi_sra_lite_bytes,
    ncbi_sra_lite_s3_url, ncbi_sra_lite_gs_url
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (run_accession) DO UPDATE SET
    experiment_accession      = COALESCE(
        EXCLUDED.experiment_accession, run_download_links.experiment_accession
    ),
    ncbi_files                = EXCLUDED.ncbi_files,
    ncbi_sra_url              = EXCLUDED.ncbi_sra_url,
    ncbi_sra_url_aws          = EXCLUDED.ncbi_sra_url_aws,
    ncbi_fetched_at           = EXCLUDED.ncbi_fetched_at,
    ncbi_sra_normalized_url   = EXCLUDED.ncbi_sra_normalized_url,
    ncbi_sra_normalized_bytes = EXCLUDED.ncbi_sra_normalized_bytes,
    ncbi_sra_lite_url         = EXCLUDED.ncbi_sra_lite_url,
    ncbi_sra_lite_bytes       = EXCLUDED.ncbi_sra_lite_bytes,
    ncbi_sra_lite_s3_url      = EXCLUDED.ncbi_sra_lite_s3_url,
    ncbi_sra_lite_gs_url      = EXCLUDED.ncbi_sra_lite_gs_url,
    updated_at                = EXCLUDED.updated_at
"""

UPSERT_LOG_SQL = """
INSERT INTO study_fetch_log (study_accession, status, n_runs, fetched_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (study_accession) DO UPDATE SET
    status     = EXCLUDED.status,
    n_runs     = EXCLUDED.n_runs,
    fetched_at = EXCLUDED.fetched_at
"""

UPSERT_NCBI_LOG_SQL = """
INSERT INTO study_fetch_log (
    study_accession, status, n_runs, fetched_at,
    ncbi_status, ncbi_n_runs, ncbi_fetched_at
) VALUES (?, 'ncbi_only', 0, ?, ?, ?, ?)
ON CONFLICT (study_accession) DO UPDATE SET
    ncbi_status     = EXCLUDED.ncbi_status,
    ncbi_n_runs     = EXCLUDED.ncbi_n_runs,
    ncbi_fetched_at = EXCLUDED.ncbi_fetched_at
"""

STUDY_QUERIES: dict[Datasource, str] = {
    Datasource.Sra: "SELECT accession FROM sra_studies ORDER BY accession",
    Datasource.Ena: "SELECT study_accession FROM ena_studies ORDER BY study_accession",
}


def backfill_download_links(
    base_output_dir: Path,
    sra_db_file: Path | None,
    ena_db_file: Path | None,
    ncbi_keys_file: Path | None,
    num_workers: int,
    is_dry_run: bool,
    source: Literal["ncbi", "ena"],
    limit: int | None = None,
) -> None:
    if not sra_db_file and not ena_db_file:
        msg = "expected at least one of --sra-db-file or --ena-db-file"
        raise ValueError(msg)

    output_dir = base_output_dir / "download_links"
    output_dir.mkdir(exist_ok=True, parents=True)

    output_db_file = output_dir / "download_links.db"
    with sqlite_connect(output_db_file, busy_timeout=120000) as conn:
        exec_query_from_file("tables/download_links.sql", conn=conn)
        exec_query_from_file("tables/study_fetch_log.sql", conn=conn)
        exec_query_from_file("indexes/download_links.sql", conn=conn)
        conn.commit()

    raw_mapping: dict[Datasource, Path | None] = {
        Datasource.Sra: sra_db_file,
        Datasource.Ena: ena_db_file,
    }
    mapping: dict[Datasource, Path] = {
        k: v for k, v in raw_mapping.items() if v is not None
    }

    study_accessions: set[str] = set()
    for datasource, db_file in mapping.items():
        query = STUDY_QUERIES[datasource]
        with sqlite_connect(db_file) as conn:
            rows = conn.execute(query).fetchall()
        accessions = [r[0] for r in rows]
        study_accessions.update(accessions)
        logger.info("%s: %d studies", datasource.value, len(accessions))

    target = sorted(study_accessions)
    if limit is not None:
        target = target[:limit]

    with sqlite_connect(output_db_file, busy_timeout=120000) as conn:
        rows = conn.execute(
            "SELECT DISTINCT study_accession FROM run_download_links"
        ).fetchall()
        existing = {r[0] for r in rows}

    if existing:
        before = len(target)
        target = [a for a in target if a not in existing]
        logger.info(
            "skipping %d studies already present in run_download_links",
            before - len(target),
        )

    logger.info("total studies to process: %d", len(target))

    if is_dry_run or not target:
        return

    keys: list[str] = []
    if ncbi_keys_file:
        keys = ncbi_keys_file.read_text().splitlines()

    if source == "ncbi":
        if not keys:
            msg = "--ncbi-keys-file is required for ncbi source"
            raise ValueError(msg)

        num_workers = min(num_workers, len(keys))
        worker_data: list[list[str]] = [[] for _ in range(num_workers)]
        for i, acc in enumerate(target):
            worker_data[i % num_workers].append(acc)

        pbar = tqdm(total=len(target), unit="study", desc="ncbi download links")

        with ThreadPoolExecutor(num_workers) as pool:
            futures = [
                pool.submit(
                    _ncbi_worker,
                    keys[i],
                    worker_data[i],
                    output_db_file,
                    pbar,
                )
                for i in range(num_workers)
            ]
            for f in as_completed(futures):
                f.result()

        pbar.close()
    elif source == "ena":
        worker_data = [[] for _ in range(num_workers)]
        for i, acc in enumerate(target):
            worker_data[i % num_workers].append(acc)

        pbar = tqdm(total=len(target), unit="study", desc="ena download links")

        with ThreadPoolExecutor(num_workers) as pool:
            futures = [
                pool.submit(
                    _ena_worker,
                    worker_data[i],
                    output_db_file,
                    pbar,
                )
                for i in range(num_workers)
            ]
            for f in as_completed(futures):
                f.result()

        pbar.close()


def _ena_worker(
    studies: list[str],
    output_db_file: Path,
    pbar: tqdm,
) -> None:
    conn = sqlite_connect(str(output_db_file), busy_timeout=120000)
    conn.execute("PRAGMA journal_mode=WAL")

    session = httpx.Client()
    now = datetime.now(UTC).isoformat(timespec="seconds")

    for study in studies:
        runs = _fetch_study_runs(session, study)
        if runs is None:
            conn.execute(UPSERT_LOG_SQL, (study, "error", 0, now))
            conn.commit()
            logger.warning("ena fetch failed for %s", study)
        elif not runs:
            conn.execute(UPSERT_LOG_SQL, (study, "empty", 0, now))
            conn.commit()
        else:
            rows = [
                (
                    run.get("run_accession", ""),
                    study,
                    _empty_to_none(run.get("experiment_accession")),
                    _empty_to_none(run.get("library_layout")),
                    _empty_to_none(run.get("fastq_ftp")),
                    _empty_to_none(run.get("fastq_bytes")),
                    _empty_to_none(run.get("fastq_md5")),
                    _empty_to_none(run.get("sra_ftp")),
                    _empty_to_none(run.get("sra_bytes")),
                    _empty_to_none(run.get("sra_md5")),
                    now,
                )
                for run in runs
            ]

            conn.executemany(UPSERT_RUN_SQL, rows)
            conn.execute(UPSERT_LOG_SQL, (study, "ok", len(rows), now))
            conn.commit()

        pbar.update(1)

    conn.close()
    session.close()


def _ncbi_worker(
    api_key: str,
    studies: list[str],
    output_db_file: Path,
    pbar: tqdm,
) -> None:
    sra = SRAweb(api_key, verbose=False)
    conn = sqlite_connect(output_db_file, busy_timeout=120000)
    conn.execute("PRAGMA journal_mode=WAL")

    now = datetime.now(UTC).isoformat(timespec="seconds")

    for study in studies:
        try:
            response = sra.get_efetch_response("sra", study)
        except Exception as e:
            conn.execute(UPSERT_NCBI_LOG_SQL, (study, now, "error", 0, now))
            conn.commit()
            pbar.update(1)
            logger.warning("ncbi efetch failed for %s: %s", study, e)
            continue

        if response is None:
            conn.execute(UPSERT_NCBI_LOG_SQL, (study, now, "empty", 0, now))
            conn.commit()
            pbar.update(1)
            continue

        if not isinstance(response, list):
            response = [response]

        run_rows = []
        for package in response:
            exp_node = (
                package["EXPERIMENT"]
                if isinstance(package.get("EXPERIMENT"), dict)
                else {}
            )
            exp_acc = exp_node.get("@accession") or None

            run_set = (
                package["RUN_SET"] if isinstance(package.get("RUN_SET"), dict) else {}
            )
            runs = run_set.get("RUN", [])
            if isinstance(runs, dict):
                runs = [runs]

            for run in runs:
                run_acc = run.get("@accession")
                if not run_acc:
                    continue

                ncbi_files = _parse_sra_files(run)
                ncbi_url, aws_url = _extract_best_urls(ncbi_files)
                urls = _extract_ncbi_urls(ncbi_files)
                run_rows.append(
                    (
                        run_acc,
                        study,
                        exp_acc,
                        json.dumps(ncbi_files),
                        ncbi_url,
                        aws_url,
                        now,
                        urls["ncbi_sra_normalized_url"],
                        urls["ncbi_sra_normalized_bytes"],
                        urls["ncbi_sra_lite_url"],
                        urls["ncbi_sra_lite_bytes"],
                        urls["ncbi_sra_lite_s3_url"],
                        urls["ncbi_sra_lite_gs_url"],
                    ),
                )

        if run_rows:
            conn.executemany(UPSERT_NCBI_RUN_SQL, run_rows)
            conn.execute(UPSERT_NCBI_LOG_SQL, (study, now, "ok", len(run_rows), now))
            conn.commit()
        else:
            conn.execute(UPSERT_NCBI_LOG_SQL, (study, now, "empty", 0, now))
            conn.commit()

        pbar.update(1)

    conn.close()


def _fetch_study_runs(
    session: httpx.Client,
    study_accession: str,
    retries: int = 3,
) -> list[dict] | None:
    params = {
        "result": "read_run",
        "fields": FILEREPORT_FIELDS,
        "accession": study_accession,
    }

    for attempt in range(retries):
        try:
            resp = session.get(ENA_FILEREPORT_URL, params=params, timeout=120)
            if resp.status_code == http.HTTPStatus.NO_CONTENT:
                return []

            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text), delimiter="\t")
            return list(reader)
        except httpx.HTTPError:
            if attempt < retries - 1:
                sleep(2 * (attempt + 1))
            else:
                return None
    return None


def _extract_best_urls(ncbi_files: list[dict]) -> tuple[str | None, str | None]:
    type_priority = {"Primary ETL": 2, "SRA Lite": 1}

    ncbi_url = None
    aws_url = None
    best_ncbi_score = 0
    best_aws_score = 0

    for f in ncbi_files:
        score = type_priority.get(f.get("supertype", ""), 0)
        if score == 0:
            continue

        for alt in f.get("alternatives", []):
            if alt.get("free_egress") != "worldwide":
                continue
            if alt.get("access_type") != "anonymous":
                continue

            url = alt.get("url", "")
            org = alt.get("org", "")

            if org == "NCBI" and score > best_ncbi_score:
                ncbi_url = url
                best_ncbi_score = score
            elif org == "AWS" and score > best_aws_score:
                aws_url = url
                best_aws_score = score

    return ncbi_url, aws_url


def _extract_ncbi_urls(ncbi_files: list[dict]) -> dict:
    result = {
        "ncbi_sra_normalized_url": None,
        "ncbi_sra_normalized_bytes": None,
        "ncbi_sra_lite_url": None,
        "ncbi_sra_lite_bytes": None,
        "ncbi_sra_lite_s3_url": None,
        "ncbi_sra_lite_gs_url": None,
    }

    for f in ncbi_files:
        sname = f.get("semantic_name", "")
        size = f.get("size")

        if sname == "SRA Normalized":
            for alt in f.get("alternatives", []):
                if alt.get("org") == "AWS" and alt.get("access_type") == "anonymous":
                    result["ncbi_sra_normalized_url"] = alt.get("url")
                    result["ncbi_sra_normalized_bytes"] = size
                    break
            if not result["ncbi_sra_normalized_url"] and f.get("url"):
                result["ncbi_sra_normalized_url"] = f["url"]
                result["ncbi_sra_normalized_bytes"] = size

        elif sname == "SRA Lite":
            result["ncbi_sra_lite_bytes"] = size
            for alt in f.get("alternatives", []):
                url = alt.get("url", "")
                org = alt.get("org", "")
                if org == "NCBI" and alt.get("free_egress") == "worldwide":
                    result["ncbi_sra_lite_url"] = url
                elif org == "AWS":
                    result["ncbi_sra_lite_s3_url"] = url
                elif org == "GCP":
                    result["ncbi_sra_lite_gs_url"] = url

    return result


def _parse_sra_files(run_dict: dict) -> list[dict]:
    sra_files = run_dict.get("SRAFiles", {})
    if not sra_files:
        return []

    sra_file_list = sra_files.get("SRAFile", [])
    if isinstance(sra_file_list, dict):
        sra_file_list = [sra_file_list]

    results = []
    for sf in sra_file_list:
        entry = {
            "filename": sf.get("@filename", ""),
            "semantic_name": sf.get("@semantic_name", ""),
            "supertype": sf.get("@supertype", ""),
            "url": sf.get("@url", ""),
            "size": sf.get("@size"),
            "md5": sf.get("@md5", ""),
            "sratoolkit": sf.get("@sratoolkit", "0"),
        }

        alts = sf.get("Alternatives", [])
        if isinstance(alts, dict):
            alts = [alts]
        entry["alternatives"] = [
            {
                "url": a.get("@url", ""),
                "free_egress": a.get("@free_egress", ""),
                "access_type": a.get("@access_type", ""),
                "org": a.get("@org", ""),
            }
            for a in alts
        ]
        results.append(entry)
    return results


def _empty_to_none(val: str | None) -> str | None:
    if val is None or val == "":
        return None
    return val
