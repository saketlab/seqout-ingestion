import contextlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

from pysradb.sraweb import SRAweb
from tqdm import tqdm

from backfill.backfill_utils import _chunked, _parse_pmid
from helpers import sqlite_connect
from pipeline_types import Datasource

logger = logging.getLogger(__name__)

DATASOURCE_TABLE_MAPPING: dict[Datasource, str] = {
    Datasource.Sra: "sra_studies",
    Datasource.Geo: "geo_series",
    Datasource.ArrayExpress: "arrayexpress_experiments",
    Datasource.Ena: "ena_studies",
    Datasource.Dra: "dra_studies",
    Datasource.Gea: "gea_experiments",
    Datasource.Gsa: "gsa_studies",
    Datasource.GsaHuman: "gsa_human_studies",
}

DATASOURCE_COLUMN_MAPPING: dict[Datasource, str] = {
    Datasource.Sra: "accession",
    Datasource.Geo: "accession",
    Datasource.ArrayExpress: "accession",
    Datasource.Ena: "study_accession",
    Datasource.Dra: "accession",
    Datasource.Gea: "accession",
    Datasource.Gsa: "accession",
    Datasource.GsaHuman: "accession",
}

MISSING_QUERIES: dict[Datasource, str] = {
    Datasource.Sra: (
        "SELECT accession FROM sra_studies"
        " WHERE (links IS NULL OR NOT json_valid(links)"
        " OR json_extract(links, '$.XREF_LINK.DB') != 'pubmed')"
        " AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
    Datasource.Geo: (
        "SELECT accession FROM geo_series"
        " WHERE (pubmed_id IS NULL OR pubmed_id = '' OR pubmed_id = '[]')"
        " AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
    Datasource.ArrayExpress: (
        "SELECT accession FROM arrayexpress_experiments"
        " WHERE (pubmed_id IS NULL OR pubmed_id = '')"
        " AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
    Datasource.Ena: (
        "SELECT study_accession FROM ena_studies"
        " WHERE (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
    # DDBJ-exclusive only. The SRA/ENA twin of a mirrored DRP is canonical and already
    # carries the pmid; the DDBJ copy would cost ~17k NCBI calls nothing reads.
    Datasource.Dra: (
        "SELECT accession FROM dra_studies"
        " WHERE is_only_ddbj = 1"
        " AND (links IS NULL OR NOT json_valid(links)"
        " OR json_extract(links, '$.XREF_LINK.DB') != 'pubmed')"
        " AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
    Datasource.Gea: (
        "SELECT accession FROM gea_experiments"
        " WHERE (pubmed_id IS NULL OR pubmed_id = '')"
        " AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
    Datasource.Gsa: (
        "SELECT accession FROM gsa_studies"
        " WHERE (pubmed_id IS NULL OR pubmed_id = '' OR pubmed_id = '[]')"
        " AND (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
    Datasource.GsaHuman: (
        "SELECT accession FROM gsa_human_studies"
        " WHERE (pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"
    ),
}

HAS_ORIGINAL_PMID_QUERIES: dict[Datasource, str] = {
    Datasource.Sra: (
        "SELECT COUNT(*) FROM sra_studies"
        " WHERE json_valid(links)"
        " AND json_extract(links, '$.XREF_LINK.DB') = 'pubmed'"
        " AND json_extract(links, '$.XREF_LINK.ID') IS NOT NULL"
    ),
    Datasource.Geo: (
        "SELECT COUNT(*) FROM geo_series"
        " WHERE pubmed_id IS NOT NULL AND pubmed_id != '' AND pubmed_id != '[]'"
    ),
    Datasource.ArrayExpress: (
        "SELECT COUNT(*) FROM arrayexpress_experiments"
        " WHERE pubmed_id IS NOT NULL AND pubmed_id != ''"
    ),
    Datasource.Ena: "SELECT 0",
    Datasource.Dra: "SELECT 0",
    Datasource.Gea: (
        "SELECT COUNT(*) FROM gea_experiments"
        " WHERE pubmed_id IS NOT NULL AND pubmed_id != ''"
    ),
    Datasource.Gsa: (
        "SELECT COUNT(*) FROM gsa_studies"
        " WHERE pubmed_id IS NOT NULL AND pubmed_id != '' AND pubmed_id != '[]'"
    ),
    Datasource.GsaHuman: "SELECT 0",
}

BACKFILLED_PMID_QUERIES: dict[Datasource, str] = {
    Datasource.Sra: (
        "SELECT COUNT(*) FROM sra_studies"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
    Datasource.Geo: (
        "SELECT COUNT(*) FROM geo_series"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
    Datasource.ArrayExpress: (
        "SELECT COUNT(*) FROM arrayexpress_experiments"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
    Datasource.Ena: (
        "SELECT COUNT(*) FROM ena_studies"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
    Datasource.Dra: (
        "SELECT COUNT(*) FROM dra_studies"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
    Datasource.Gea: (
        "SELECT COUNT(*) FROM gea_experiments"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
    Datasource.Gsa: (
        "SELECT COUNT(*) FROM gsa_studies"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
    Datasource.GsaHuman: (
        "SELECT COUNT(*) FROM gsa_human_studies"
        " WHERE pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb != ''"
    ),
}

# CRA/HRA/E-GEAD have no NCBI record to elink. ae_to_pmid: literature search, smallest
# pmid (the originating paper). DRA resolves by BioProject.
SRAWEB_RESOLVERS: dict[Datasource, tuple[str, str]] = {
    Datasource.Sra: ("sra_to_pmid", "srp_accession"),
    Datasource.Ena: ("sra_to_pmid", "srp_accession"),
    Datasource.Geo: ("gse_to_pmid", "gse_accession"),
    Datasource.ArrayExpress: ("gse_to_pmid", "gse_accession"),
    Datasource.Gea: ("ae_to_pmid", "ae_accession"),
    Datasource.Gsa: ("ae_to_pmid", "ae_accession"),
    Datasource.GsaHuman: ("ae_to_pmid", "ae_accession"),
}

PMID_RESOLVERS: dict[Datasource, str] = {
    **{k: v[0] for k, v in SRAWEB_RESOLVERS.items()},
    Datasource.Dra: "_resolve_dra",
}


def backfill_pubmed_ids(
    geo_db_file: Path | None,
    sra_db_file: Path | None,
    ena_db_file: Path | None,
    ae_db_file: Path | None,
    dra_db_file: Path | None,
    gea_db_file: Path | None,
    gsa_db_file: Path | None,
    gsa_human_db_file: Path | None,
    ncbi_keys_file: Path,
    num_workers: int,
    batch_size: int,
    is_dry_run: bool,
    retries: int,
    timeout: int,
    process_all: bool = False,  # noqa:  FBT002
) -> None:
    if not any(
        (
            geo_db_file,
            sra_db_file,
            ena_db_file,
            ae_db_file,
            dra_db_file,
            gea_db_file,
            gsa_db_file,
            gsa_human_db_file,
        ),
    ):
        msg = (
            "expected at least one of --geo-db-file, --sra-db-file, --ena-db-file,"
            " --ae-db-file, --dra-db-file, --gea-db-file, --gsa-db-file or"
            " --gsa-human-db-file. recieved none of them"
        )
        raise ValueError(msg)

    raw_mapping: dict[Datasource, Path | None] = {
        Datasource.Geo: geo_db_file,
        Datasource.Sra: sra_db_file,
        Datasource.Ena: ena_db_file,
        Datasource.ArrayExpress: ae_db_file,
        Datasource.Dra: dra_db_file,
        Datasource.Gea: gea_db_file,
        Datasource.Gsa: gsa_db_file,
        Datasource.GsaHuman: gsa_human_db_file,
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

        with sqlite_connect(db_file) as conn:
            missing_query = MISSING_QUERIES[datasource]
            if not process_all:
                missing_query += " AND pubmed_last_checked IS NULL"
            has_original_query = HAS_ORIGINAL_PMID_QUERIES[datasource]
            backfilled_query = BACKFILLED_PMID_QUERIES[datasource]

            total += conn.execute(
                f"SELECT COUNT(*) FROM {table}",  # noqa: S608
            ).fetchone()[0]
            already_checked += conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE pubmed_last_checked IS NOT NULL",  # noqa: S608
            ).fetchone()[0]
            num_has_original += conn.execute(has_original_query).fetchone()[0]
            num_backfilled += conn.execute(backfilled_query).fetchone()[0]

            per_datasource_missing_pubmed_ids = conn.execute(missing_query).fetchall()
            missing_by_datasource[datasource] = [
                item for (item,) in per_datasource_missing_pubmed_ids
            ]

    total_missing = sum(len(v) for v in missing_by_datasource.values())
    logger.info(
        "%d total, across {%s} datasources | %d have original pid"
        " | %d via pysradb | %d checked | %d to check (%.2f%%)",
        total,
        ",".join(str(d) for d in mapping),
        num_has_original,
        num_backfilled,
        already_checked,
        total_missing,
        (total_missing * 100) / total if total else 0,
    )

    if is_dry_run or total_missing == 0:
        return

    keys = ncbi_keys_file.read_text().splitlines()

    optimal_num_workers = len(keys)
    if num_workers != optimal_num_workers:
        num_workers = optimal_num_workers
        logger.warning("using %d as num workers", num_workers)

    for datasource, db_file in mapping.items():
        accessions = missing_by_datasource[datasource]
        if not accessions:
            continue

        logger.info("processing %s - %d accessions", datasource, len(accessions))
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
                    retries,
                    timeout,
                )
                for i in range(num_workers)
            ]

            for f in as_completed(futures):
                f.result()

        pbar.close()


def _resolve_via_sraweb(
    sra: SRAweb,
    datasource: Datasource,
    batch: list[str],
) -> list[tuple[str, str]]:
    method, col_name = SRAWEB_RESOLVERS[datasource]
    try:
        pm_df = getattr(sra, method)(batch)
    except KeyError:
        return []

    updates: list[tuple[str, str]] = []
    for _, row in pm_df.iterrows():
        acc = row.get(col_name)
        pmid = _parse_pmid(row.get("pmid"))
        if acc and pmid:
            updates.append((pmid, str(acc)))
    return updates


def _dra_bioprojects(db_file: Path) -> dict[str, str]:
    """DRP -> its PRJDB, from the external_id json ingest wrote."""
    with sqlite_connect(db_file) as conn:
        rows = conn.execute(
            "SELECT accession, external_id FROM dra_studies "
            "WHERE external_id IS NOT NULL",
        ).fetchall()

    out: dict[str, str] = {}
    for acc, external_id in rows:
        with contextlib.suppress(json.JSONDecodeError, TypeError, AttributeError):
            bioproject = json.loads(external_id).get("BioProject")
            if bioproject:
                out[acc] = bioproject
    return out


def _resolve_dra(
    sra: SRAweb,
    batch: list[str],
    bioprojects: dict[str, str],
) -> list[tuple[str, str]]:
    """
    DRP -> pmid via its BioProject; DRA studies carry no pubmed link of their own.

    Must go through fetch_bioproject_pmids, which resolves to an NCBI UID first:
    efetch by accession reads only the digits, so PRJDB13786 returns PRJNA13786.
    """
    wanted = {acc: bioprojects[acc] for acc in batch if acc in bioprojects}
    if not wanted:
        return []

    try:
        found = sra.fetch_bioproject_pmids(sorted(set(wanted.values())))
    except Exception:
        logger.exception("fetch_bioproject_pmids failed for a dra batch")
        return []

    updates: list[tuple[str, str]] = []
    for acc, bioproject in wanted.items():
        pmids = found.get(bioproject) or []
        pmid = _parse_pmid(pmids[0]) if pmids else None
        if pmid:
            updates.append((pmid, acc))
    return updates


def _worker(
    api_key: str,
    datasource: Datasource,
    data: list[str],
    db_file: Path,
    batch_size: int,
    pbar: tqdm,
    retries: int,
    timeout: int,
) -> None:
    sra = SRAweb(api_key, retries=retries, timeout=timeout, verbose=False)

    now = datetime.now(UTC).strftime("%Y-%m-%d")
    bioprojects = _dra_bioprojects(db_file) if datasource is Datasource.Dra else {}

    for batch in _chunked(data, batch_size):
        if datasource is Datasource.Dra:
            pmid_updates = _resolve_dra(sra, batch, bioprojects)
        else:
            pmid_updates = _resolve_via_sraweb(sra, datasource, batch)

        with sqlite_connect(db_file) as conn:
            table = DATASOURCE_TABLE_MAPPING[datasource]
            column = DATASOURCE_COLUMN_MAPPING[datasource]

            accessions_in_batch = [a for a in batch if a]
            placeholders = ",".join("?" for _ in accessions_in_batch)
            conn.execute(
                f"""UPDATE {table} SET pubmed_last_checked = ?
                WHERE {column} IN ({placeholders})""",  # noqa: S608
                (now, *accessions_in_batch),
            )

            if pmid_updates:
                conn.executemany(
                    f"UPDATE {table} SET pubmed_id_pysradb = ? WHERE {column} = ?",  # noqa: S608
                    pmid_updates,
                )

            conn.commit()
        pbar.update(len(batch))
