#!/usr/bin/env python3
"""
yuj work item: count single-cell cells for ONE accession.
"""

import functools
import json
import logging
import shutil
import socket
import sys
from pathlib import Path
from typing import Any

from helpers import download_file_ftp
from sc_enrich.counter._process import (
    FILE_TYPE_PRIORITY,
    _check_disk_space,
    _get_sc_count,
    _resolve_file_type,
    last_rds_class,
)
from sc_enrich.counter.counter_common import DownloadLink, FileType, RdsReadError

logger = logging.getLogger(__name__)

_FAILURES_LOG = "rds_read_failures.jsonl"


def _record_rds_failure(
    output_dir: Path, accession: str, url: str, reason: str
) -> None:
    """
    Append an unreadable-RDS failure to results/rds_read_failures.jsonl.

    One worker runs per host at a time, so the append is race-free; each host
    keeps its own log, merged on ``yuj pull``.
    """
    rec = {
        "host": socket.gethostname(),
        "accession": accession,
        "url": url,
        "reason": reason,
    }
    with (output_dir / _FAILURES_LOG).open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


_CHUNK_SIZE = 10 * 1024 * 1024
_RETRIES = 5
_TIMEOUT = 30
_MAX_TIMEOUT = 300

_SIDECAR_DIR = Path("yuj_setup/accessions")
_OUT_DIR = Path("results")

_DEFAULT_SOCKS = "10.195.102.16:1080"


def _read_proxy() -> str | None:
    pf = Path("socks_proxy")
    if not pf.exists():
        return _DEFAULT_SOCKS
    val = pf.read_text().strip()
    return None if not val or val.lower() == "direct" else val


def main() -> None:
    accession = sys.argv[1]
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUT_DIR / f"{accession}.json"
    if out_path.exists():
        return

    meta = json.loads((_SIDECAR_DIR / f"{accession}.json").read_text())

    try:
        result = process_accession(
            accession=meta["accession"],
            source=meta.get("source", ""),
            aliases=meta.get("aliases", ""),
            links=meta.get("links", []),
            output_dir=_OUT_DIR,
            chunk_size=_CHUNK_SIZE,
            retries=_RETRIES,
            timeout=_TIMEOUT,
            max_timeout=_MAX_TIMEOUT,
        )
    except Exception:
        logger.exception("failed accession %s", accession)
        result = {"accession": accession, "error": True}

    tmp = out_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(result, indent=2))
    tmp.replace(out_path)


def process_accession(
    accession: str,
    source: str,
    aliases: str,
    links: list[tuple[str, str]],
    output_dir: Path,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> dict[str, Any]:
    dl_links = [DownloadLink(type=FileType(t), url=u) for t, u in links]
    dl_links.sort(key=lambda x: FILE_TYPE_PRIORITY.index(x.type))

    acc_dir = output_dir / accession
    acc_dir.mkdir(parents=True, exist_ok=True)

    downloader = functools.partial(
        download_file_ftp,
        chunk_size=chunk_size,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
        with_pbar=False,
        socks_proxy=_read_proxy(),
    )

    max_count = 0
    best_type = ""
    best_rds_class = ""

    for link in dl_links:
        dest_path = acc_dir / link.extract_filename()

        if dest_path.exists():
            try:
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
                    best_rds_class = (
                        last_rds_class() if file_type == FileType.Rds else ""
                    )
            except RdsReadError as exc:
                _record_rds_failure(output_dir, accession, link.url, exc.reason)
            except Exception:
                logger.exception("failed to process existing file %s", dest_path)
            finally:
                dest_path.unlink(missing_ok=True)
            continue

        if not _check_disk_space(output_dir):
            logger.warning("skipping %s: insufficient disk space", link.url)
            continue

        try:
            dest_path = downloader(
                url=link.url.replace("https://", "ftp://", 1),
                dest_path=dest_path,
            )
        except Exception:
            logger.exception("failed to download %s", link.url)
            continue

        try:
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
                best_rds_class = (
                    last_rds_class() if file_type == FileType.Rds else ""
                )
        except RdsReadError as exc:
            _record_rds_failure(output_dir, accession, link.url, exc.reason)
        except Exception:
            logger.exception("failed to process %s", link.url)
        finally:
            dest_path.unlink(missing_ok=True)

    shutil.rmtree(acc_dir, ignore_errors=True)

    out: dict[str, Any] = {
        "accession": accession,
        "source": source,
        "aliases": aliases,
        "single_cell_count": max_count,
        "single_cell_file_format": best_type,
    }
    if best_rds_class:
        out["rds_class"] = best_rds_class
    return out


if __name__ == "__main__":
    main()
