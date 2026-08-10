"""
FASTQ links for DRA runs, from DDBJ's fastqlist manifest.

ENA never mirrored the DDBJ-exclusive runs, so `backfill download-links` cannot find
them. fastqlist carries no study/experiment/library_layout; those join from the DRA db.
Existing links are COALESCE'd, never overwritten.
"""

import logging
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

from helpers import download_file, exec_query_from_file, sqlite_connect

logger = logging.getLogger(__name__)

DDBJ_PUBLIC_BASE_URL = "https://ddbj.nig.ac.jp/public"
FASTQLIST_URL = f"{DDBJ_PUBLIC_BASE_URL}/ddbj_database/dra/meta/list/fastqlist"

_RUN_RE = re.compile(r"/(DRR\d+)(?:_\d+)?\.fastq")
_MIN_COLS = 3

UPSERT_SQL = """
INSERT INTO run_download_links (
    run_accession, study_accession, experiment_accession, library_layout,
    fastq_ftp, fastq_bytes, fastq_md5
) VALUES (?,?,?,?,?,?,?)
ON CONFLICT (run_accession) DO UPDATE SET
    study_accession      = COALESCE(run_download_links.study_accession,
                                    excluded.study_accession),
    experiment_accession = COALESCE(run_download_links.experiment_accession,
                                    excluded.experiment_accession),
    library_layout       = COALESCE(run_download_links.library_layout,
                                    excluded.library_layout),
    fastq_ftp            = COALESCE(run_download_links.fastq_ftp,
                                    excluded.fastq_ftp),
    fastq_bytes          = COALESCE(run_download_links.fastq_bytes,
                                    excluded.fastq_bytes),
    fastq_md5            = COALESCE(run_download_links.fastq_md5,
                                    excluded.fastq_md5)
"""


@dataclass(frozen=True)
class FastqGroup:
    fastq_ftp: str
    fastq_md5: str
    fastq_bytes: str


def run_accession_of(file_path: str) -> str | None:
    match = _RUN_RE.search(file_path)
    return match.group(1) if match else None


def group_fastq_rows(
    rows: Iterable[tuple[str, str, str]],
    keep: set[str],
) -> dict[str, FastqGroup]:
    """[(file_path, md5, bytes)] -> {run: FastqGroup}, restricted to `keep`."""
    per_run: dict[str, list[tuple[str, str, str]]] = {}

    for file_path, md5, size in rows:
        run = run_accession_of(file_path)
        if run is None or run not in keep:
            continue
        per_run.setdefault(run, []).append((file_path, md5, size))

    grouped: dict[str, FastqGroup] = {}
    for run, files in per_run.items():
        # sort by path so _1 precedes _2 and md5/bytes stay aligned with the urls
        files.sort(key=lambda f: f[0])
        grouped[run] = FastqGroup(
            fastq_ftp=";".join(f"{DDBJ_PUBLIC_BASE_URL}{f[0]}" for f in files),
            fastq_md5=";".join(f[1] for f in files),
            fastq_bytes=";".join(f[2] for f in files),
        )

    return grouped


def _run_context(dra_db_file: Path) -> dict[str, tuple[str | None, str, str]]:
    """Map run -> (study, experiment, library_layout) for everything we ingested."""
    with sqlite_connect(dra_db_file) as conn:
        rows = conn.execute(
            """
            SELECT r.accession, e.study, e.accession, e.library_layout
            FROM dra_runs r
            LEFT JOIN dra_experiments e ON e.accession = r.experiment
            """,
        ).fetchall()

    return {run: (study, experiment, layout) for run, study, experiment, layout in rows}


def _read_fastqlist(path: Path) -> Iterator[tuple[str, str, str]]:
    """Stream, since the caller drops most rows of a large fastqlist."""
    with path.open(encoding="utf-8", errors="replace") as f:
        header = f.readline()
        if not header.startswith("file_path"):
            msg = f"unexpected fastqlist header: {header[:80]!r}"
            raise ValueError(msg)

        for line in tqdm(f, unit="line", desc="scanning fastqlist"):
            cols = line.rstrip("\n").split("\t")
            if len(cols) < _MIN_COLS:
                continue
            yield (cols[0], cols[1], cols[2])


def backfill_dra_download_links(
    base_output_dir: Path,
    dra_db_file: Path,
    download_links_db_file: Path,
    num_workers: int,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    data_dir = base_output_dir / "dra" / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    fastqlist_path = data_dir / "fastqlist"
    if not fastqlist_path.exists():
        logger.info("downloading fastqlist")
        download_file(
            FASTQLIST_URL,
            fastqlist_path,
            chunk_size=chunk_size,
            num_workers=num_workers,
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
            with_pbar=True,
            validate_content_length=True,
        )

    context = _run_context(dra_db_file)
    logger.info("%d dra runs ingested", len(context))
    if not context:
        logger.warning("no dra runs; nothing to link")
        return

    grouped = group_fastq_rows(_read_fastqlist(fastqlist_path), keep=set(context))
    logger.info("%d of %d dra runs have fastq files", len(grouped), len(context))

    missing_study = [r for r in grouped if context[r][0] is None]
    if missing_study:
        logger.warning(
            "%d runs have no study via dra_experiments and were skipped "
            "(study_accession is NOT NULL); e.g. %s",
            len(missing_study),
            missing_study[:5],
        )

    exec_query_from_file("tables/download_links.sql", download_links_db_file)

    payload = [
        (
            run,
            context[run][0],
            context[run][1],
            context[run][2],
            group.fastq_ftp,
            group.fastq_bytes,
            group.fastq_md5,
        )
        for run, group in grouped.items()
        if context[run][0] is not None
    ]

    with sqlite_connect(download_links_db_file) as conn:
        conn.executemany(UPSERT_SQL, payload)
        conn.commit()

    logger.info("wrote %d dra run download links", len(payload))
