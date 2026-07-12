#!/usr/bin/env python3
"""
yuj work item: count single-cell cells for ONE accession.

Invoked by yuj as ``python -m sc_enrich.counter <accession>`` with cwd at the
deploy dir. Reads the accession's download links from
``yuj_setup/accessions/<accession>.json`` (shipped as payload), downloads and
sniffs the supplementary files, and writes ``results/<accession>.json``. yuj
skips items whose output already exists, so this stays idempotent.
"""

import functools
import json
import logging
import shutil
import socket
import sys
from pathlib import Path
from typing import Any

from helpers import (
    FileTooLargeError,
    download_file,
    download_file_ftp,
    download_ftp_head,
    download_http_head,
)
from sc_enrich.counter._process import (
    FILE_TYPE_PRIORITY,
    HEADER_STREAMABLE,
    RELIABLE_TYPES,
    _check_disk_space,
    _get_sc_count,
    _resolve_file_type,
    count_h5_via_http,
    count_tar_via_stream,
    is_cell_major,
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

# bounds only the whole-file path
_MAX_FILE_BYTES = 30 * 1024**3

# mtx count is on the first line
_MTX_HEAD_BYTES = 1 * 1024 * 1024

_SIDECAR_DIR = Path("yuj_setup/accessions")
_OUT_DIR = Path("results")

# fallback when no per-host socks_proxy file
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

    # Write via a temp file so a crash mid-write can't leave a half-written
    # output that yuj's resume would treat as a completed item.
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

    full_download = functools.partial(
        download_file_ftp,
        chunk_size=chunk_size,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
        with_pbar=False,
        socks_proxy=_read_proxy(),
        max_bytes=_MAX_FILE_BYTES,
    )

    def count_link(link: DownloadLink) -> tuple[int, str, str]:  # noqa: PLR0911
        """Fetch and count ONE link. Returns (n_cells, file_format, rds_class)."""
        # EBI serves supplementary files over HTTPS only; ftp:// times out there
        use_http = "ebi.ac.uk" in link.url
        ftp_url = link.url.replace("https://", "ftp://", 1)

        # range-read the h5 shape, no full download
        if link.type in {FileType.H5, FileType.H5ad}:
            n = count_h5_via_http(link.url, link.type)
            if n is not None:
                return (n, link.type.value, "") if n > 0 else (0, "", "")

        # stream tar members over ftp; EBI tars fall through to a full http download
        if link.type == FileType.Tar and not use_http:
            n = count_tar_via_stream(
                link.url, chunk_size, timeout=timeout, socks_proxy=_read_proxy()
            )
            if n is not None:
                return (n, link.type.value, "") if n > 0 else (0, "", "")

        dest_path = acc_dir / link.extract_filename()
        try:
            if link.type in HEADER_STREAMABLE and not is_cell_major(link.url):
                # counted from the first chunk, so fetch only that
                head = _MTX_HEAD_BYTES if link.type == FileType.Mtx else chunk_size
                if use_http:
                    dest_path = download_http_head(
                        link.url,
                        dest_path,
                        max_decompressed=head,
                        num_retries=retries,
                        timeout=timeout,
                        max_wait=max_timeout,
                    )
                else:
                    dest_path = download_ftp_head(
                        ftp_url,
                        dest_path,
                        max_decompressed=head,
                        num_retries=retries,
                        timeout=timeout,
                        max_wait=max_timeout,
                        socks_proxy=_read_proxy(),
                    )
            else:
                if not _check_disk_space(output_dir):
                    logger.warning("skipping %s: insufficient disk space", link.url)
                    return (0, "", "")
                if use_http:
                    dest_path = download_file(
                        link.url,
                        dest_path,
                        chunk_size=chunk_size,
                        num_workers=1,
                        num_retries=retries,
                        timeout=timeout,
                        max_wait=max_timeout,
                        with_pbar=False,
                        validate_content_length=False,
                    )
                else:
                    dest_path = full_download(url=ftp_url, dest_path=dest_path)
        except FileTooLargeError as exc:
            logger.info("skipping oversized file: %s", exc)
            return (0, "", "")
        except Exception:
            logger.exception("failed to download %s", link.url)
            return (0, "", "")

        try:
            result = _resolve_file_type(dest_path, link.type, chunk_size)
            if result is None:
                logger.debug("unable to find file type for %s", dest_path)
                return (0, "", "")
            file_type, sniffed_path = result
            n = _get_sc_count(sniffed_path, file_type, chunk_size)
            if sniffed_path != dest_path:
                sniffed_path.unlink(missing_ok=True)
            rds = last_rds_class() if file_type == FileType.Rds else ""
            return (n, file_type.value, rds) if n > 0 else (0, "", "")  # noqa: TRY300
        except RdsReadError as exc:
            _record_rds_failure(output_dir, accession, link.url, exc.reason)
            return (0, "", "")
        except Exception:
            logger.exception("failed to process %s", link.url)
            return (0, "", "")
        finally:
            dest_path.unlink(missing_ok=True)
            dest_path.with_suffix("").unlink(missing_ok=True)

    max_count = 0
    best_type = ""
    best_rds_class = ""
    for link in dl_links:
        n_cells, file_format, rds_class = count_link(link)
        if n_cells > max_count:
            max_count, best_type, best_rds_class = n_cells, file_format, rds_class
        # only a reliable type ends the search
        if n_cells > 0 and link.type in RELIABLE_TYPES:
            break

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
