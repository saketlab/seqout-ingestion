#!/usr/bin/env python3

import ast
import csv
import functools
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Any

from helpers import download_file
from sc_enrich.counter._process import (
    FILE_TYPE_PRIORITY,
    _check_disk_space,
    _get_sc_count,
    _resolve_file_type,
)
from sc_enrich.counter.counter import DownloadLink, FileType

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 10 * 1024 * 1024
_RETRIES = 5
_TIMEOUT = 30
_MAX_TIMEOUT = 300


def main() -> None:
    csv_path = Path(sys.argv[1])
    out_dir = Path("results")
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            result = process_accession(
                accession=row["accession"],
                source=row.get("source", ""),
                aliases=row.get("aliases", ""),
                raw_links=row.get("supplementary_data", "[]"),
                output_dir=out_dir,
                chunk_size=_CHUNK_SIZE,
                retries=_RETRIES,
                timeout=_TIMEOUT,
                max_timeout=_MAX_TIMEOUT,
            )
            results.append(result)

    out_path = out_dir / "seqout_sc_count_results.json"
    with out_path.open("w") as f_out:
        json.dump(results, f_out, indent=2)


def process_accession(
    accession: str,
    source: str,
    aliases: str,
    raw_links: str,
    output_dir: Path,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> dict[str, Any]:
    links = _parse_download_links(raw_links)
    links.sort(key=lambda x: FILE_TYPE_PRIORITY.index(x.type))

    acc_dir = output_dir / accession
    acc_dir.mkdir(parents=True, exist_ok=True)

    downloader = functools.partial(
        download_file,
        chunk_size=chunk_size,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
        with_pbar=False,
        validate_content_length=False,
    )

    max_count = 0
    best_type = ""

    for link in links:
        dest_path = acc_dir / link.extract_filename()

        if dest_path.exists():
            result = _resolve_file_type(dest_path, link.type, chunk_size)
            if result is None:
                logger.debug("unable to find file type for %s", dest_path)
                dest_path.unlink(missing_ok=True)
                continue

            file_type, sniffed_path = result
            single_cell_count = _get_sc_count(sniffed_path, file_type, chunk_size)

            if sniffed_path != dest_path:
                sniffed_path.unlink(missing_ok=True)

            if single_cell_count > max_count:
                max_count = single_cell_count
                best_type = file_type.value

            dest_path.unlink(missing_ok=True)
            continue

        if not _check_disk_space(output_dir):
            logger.warning("skipping %s: insufficient disk space", link.url)
            continue

        try:
            dest_path = downloader(
                url=link.parse_url(), dest_path=dest_path, num_workers=1
            )
        except Exception:
            logger.exception("failed to download %s", link.url)
            continue

        result = _resolve_file_type(dest_path, link.type, chunk_size)
        if result is None:
            logger.debug("unable to find file type for %s", dest_path)
            dest_path.unlink(missing_ok=True)
            decompressed = dest_path.with_suffix("")
            if decompressed != dest_path:
                decompressed.unlink(missing_ok=True)
            continue

        file_type, sniffed_path = result
        single_cell_count = _get_sc_count(sniffed_path, file_type, chunk_size)

        if sniffed_path != dest_path:
            sniffed_path.unlink(missing_ok=True)

        if single_cell_count > max_count:
            max_count = single_cell_count
            best_type = file_type.value

        dest_path.unlink(missing_ok=True)

    shutil.rmtree(acc_dir, ignore_errors=True)

    return {
        "accession": accession,
        "source": source,
        "aliases": aliases,
        "single_cell_count": max_count,
        "single_cell_file_format": best_type,
    }


def _parse_download_links(raw: str) -> list[DownloadLink]:
    try:
        items = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        logger.exception("failed to parse download links: %s", raw)
        return []

    return [DownloadLink(type=FileType(t), url=u) for t, u in items]


if __name__ == "__main__":
    main()
