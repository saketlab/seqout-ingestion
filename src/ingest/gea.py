"""
DDBJ Genomic Expression Archive (GEA) ingest.
"""

import datetime
import json
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import httpx
from tqdm import tqdm

from helpers import exec_query_from_file, send_req, sqlite_connect
from ingest.magetab import GEA_IDF_MAPPING, GEA_SDRF_MAPPING, parse_idf, parse_sdrf

logger = logging.getLogger(__name__)

GEA_OUTPUT_DIR = "gea"
DDBJ_BASE_URL = "https://ddbj.nig.ac.jp/public/ddbj_database"
GEA_EXPERIMENT_BASE_URL = f"{DDBJ_BASE_URL}/gea/experiment"
GEA_LIVELIST_URL = f"{GEA_EXPERIMENT_BASE_URL}/livelist.txt"

GEA_BUCKET_SIZE = 1000
_HTTP_NOT_FOUND = 404
_LIVELIST_MIN_COLS = 5
_FILELIST_MIN_COLS = 5
_PUBLIC = "Public"

MAGETAB_SUFFIXES = ("idf.txt", "sdrf.txt", "filelist.txt")

# Upsert, not INSERT OR REPLACE: REPLACE is a delete+insert, which would null the
# columns this statement does not name: pubmed_id_pysradb, pubmed_last_checked
# come from backfill, not from the MAGE-TAB, and a re-ingest must not discard them.
_EXPERIMENT_COLS = (
    "accession",
    "title",
    "organism",
    "description",
    "study_type",
    "bioproject",
    "release_date",
    "submission_date",
    "updated_at",
    "experimental_factors",
    "experimental_factor_types",
    "contact_name",
    "contact_email",
    "contact_affiliation",
    "pubmed_id",
    "protocol_names",
    "protocol_types",
    "protocols_description",
    "attributes_json",
    "data_files",
)
INSERT_EXPERIMENT_SQL = (
    f"INSERT INTO gea_experiments ({', '.join(_EXPERIMENT_COLS)}) "  # noqa: S608
    f"VALUES ({', '.join('?' * len(_EXPERIMENT_COLS))}) "
    "ON CONFLICT(accession) DO UPDATE SET "
    + ", ".join(
        f"{col} = excluded.{col}" for col in _EXPERIMENT_COLS if col != "accession"
    )
)

_SAMPLE_COLS = (
    "source_name",
    "organism",
    "taxon_id",
    "organism_part",
    "tissue",
    "cell_type",
    "genotype",
    "strain",
    "sex",
    "age",
    "biosample",
    "sample_title",
    "sample_description",
    "library_strategy",
    "library_source",
    "library_selection",
    "library_layout",
    "instrument_model",
    "dra_experiment",
    "dra_run",
    "attributes_json",
)

INSERT_SAMPLE_SQL = (
    "INSERT INTO gea_samples "
    "(experiment_accession, source_name, organism, taxon_id, organism_part, "
    "tissue, cell_type, genotype, strain, sex, age, biosample, sample_title, "
    "sample_description, library_strategy, library_source, library_selection, "
    "library_layout, instrument_model, dra_experiment, dra_run, "
    "attributes_json) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)


@dataclass(frozen=True)
class GeaLivelistRow:
    accession: str
    status: str
    updated: str
    published: str
    submitted: str


def experiment_dir_url(accession: str) -> str:
    """E-GEAD-282 -> .../gea/experiment/E-GEAD-000/E-GEAD-282."""
    number = int(accession.rsplit("-", 1)[1])
    bucket_num = (number // GEA_BUCKET_SIZE) * GEA_BUCKET_SIZE
    bucket = f"E-GEAD-{bucket_num:03d}"
    return f"{GEA_EXPERIMENT_BASE_URL}/{bucket}/{accession}"


def parse_gea_livelist(path: Path) -> dict[str, GeaLivelistRow]:
    """livelist.txt -> {E-GEAD accession: row} for public experiments only."""
    rows: dict[str, GeaLivelistRow] = {}

    with path.open(encoding="utf-8", errors="replace") as f:
        header = f.readline()
        if not header.startswith("Accession"):
            msg = f"unexpected gea livelist header: {header[:80]!r}"
            raise ValueError(msg)

        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < _LIVELIST_MIN_COLS:
                continue

            accession, status, updated, published, submitted = cols[:5]
            if status != _PUBLIC:
                continue

            rows[accession] = GeaLivelistRow(
                accession=accession,
                status=status,
                updated=updated,
                published=published,
                submitted=submitted,
            )

    return rows


def parse_filelist(path: Path, accession: str) -> list[dict] | None:
    """filelist.txt -> the experiment's downloadable files, with md5 + size."""
    if not path.exists():
        return None

    base = experiment_dir_url(accession)
    files: list[dict] = []

    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue

            cols = line.rstrip("\n").split("\t")
            if len(cols) < _FILELIST_MIN_COLS:
                continue

            _kind, name, time, size, md5 = cols[:5]
            files.append(
                {
                    "name": name,
                    "url": f"{base}/{name}",
                    "time": time,
                    "size": size,
                    "md5": md5,
                },
            )

    return files or None


def in_date_window(
    value: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> bool:
    try:
        day = datetime.date.fromisoformat(value[:10])
    except ValueError:
        return False
    return start_date <= day <= end_date


def _download_livelist(
    data_dir: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> Path:
    path = data_dir / "livelist.txt"

    r = send_req(
        "GET",
        GEA_LIVELIST_URL,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
    )
    path.write_bytes(r.content)
    r.close()

    return path


def _fetch_experiment_files(
    accession: str,
    out_dir: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    base = experiment_dir_url(accession)
    exp_dir = out_dir / accession
    exp_dir.mkdir(exist_ok=True, parents=True)

    for suffix in MAGETAB_SUFFIXES:
        target = exp_dir / f"{accession}.{suffix}"

        if target.exists() and target.stat().st_size > 0:
            continue

        try:
            r = send_req(
                "GET",
                f"{base}/{accession}.{suffix}",
                num_retries=retries,
                timeout=timeout,
                max_wait=max_timeout,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == _HTTP_NOT_FOUND:
                continue
            raise

        target.write_bytes(r.content)
        r.close()


def _organism_of(samples: list[dict]) -> str | None:
    for sample in samples:
        organism = sample.get("organism")
        if organism:
            return organism
    return None


def _load_experiment(
    conn: sqlite3.Connection,
    exp_dir: Path,
    accession: str,
    live: GeaLivelistRow,
) -> bool:
    idf_path = exp_dir / f"{accession}.idf.txt"
    if not idf_path.exists():
        logger.warning("no idf for %s -- skipping", accession)
        return False

    idf = parse_idf(idf_path, GEA_IDF_MAPPING) or {}
    overflow = idf.get("overflow", {})

    samples = parse_sdrf(
        exp_dir / f"{accession}.sdrf.txt",
        accession,
        GEA_SDRF_MAPPING,
    )
    samples = samples or []

    data_files = parse_filelist(exp_dir / f"{accession}.filelist.txt", accession)

    cursor = conn.cursor()
    cursor.execute(
        INSERT_EXPERIMENT_SQL,
        (
            accession,
            idf.get("title"),
            _organism_of(samples),
            idf.get("description"),
            idf.get("study_type"),
            idf.get("bioproject"),
            idf.get("release_date_idf") or live.published,
            live.submitted,
            idf.get("last_update_date") or live.updated,
            idf.get("experimental_factors"),
            idf.get("experimental_factor_types"),
            idf.get("contact_name"),
            idf.get("contact_email"),
            idf.get("contact_affiliation"),
            idf.get("pubmed_id"),
            idf.get("protocol_names"),
            idf.get("protocol_types"),
            idf.get("protocols_description"),
            json.dumps(overflow) if overflow else None,
            json.dumps(data_files) if data_files else None,
        ),
    )

    # re-ingesting an experiment must not duplicate its samples (the table has a
    # surrogate id, so INSERT OR REPLACE cannot dedupe them)
    cursor.execute(
        "DELETE FROM gea_samples WHERE experiment_accession = ?",
        (accession,),
    )
    cursor.executemany(
        INSERT_SAMPLE_SQL,
        [(accession, *(sample.get(col) for col in _SAMPLE_COLS)) for sample in samples],
    )

    return True


def ingest_gea(
    base_output_dir: Path,
    db_file: Path | None,
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    output_dir = Path(base_output_dir) / GEA_OUTPUT_DIR
    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    livelist_path = _download_livelist(data_dir, retries, timeout, max_timeout)
    live = parse_gea_livelist(livelist_path)
    logger.info("gea livelist: %d public experiments", len(live))

    selected = {
        acc: row
        for acc, row in live.items()
        if in_date_window(row.updated, start_date, end_date)
    }
    logger.info(
        "%d of %d experiments updated in [%s, %s]",
        len(selected),
        len(live),
        start_date,
        end_date,
    )
    if not selected:
        logger.warning("no gea experiments to ingest")
        return

    if db_file is None:
        date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
        db_dir = output_dir / "db" / date_str
        db_dir.mkdir(exist_ok=True, parents=True)
        db_file = db_dir / f"gea_metadata_{date_str}.db"

    files_dir = data_dir / "experiments"
    files_dir.mkdir(exist_ok=True, parents=True)

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=len(selected), unit="exp", desc="downloading gea magetab") as pbar,
    ):
        futures = [
            pool.submit(
                _fetch_experiment_files,
                acc,
                files_dir,
                retries,
                timeout,
                max_timeout,
            )
            for acc in selected
        ]
        for f in as_completed(futures):
            f.result()
            pbar.update(1)

    exec_query_from_file("tables/gea.sql", db_file)

    loaded = 0
    with sqlite_connect(db_file) as conn:
        for acc, row in tqdm(selected.items(), unit="exp", desc="loading gea"):
            exp_dir = files_dir / acc
            if exp_dir.exists() and _load_experiment(conn, exp_dir, acc, row):
                loaded += 1
        conn.commit()

    exec_query_from_file("indexes/gea.sql", db_file)

    logger.info("gea ingest complete: %d experiments -> %s", loaded, db_file)
