r"""
Download every daily NCBI SRA metadata dump in a date range, without processing it.

`just ingest_sra_daily` downloads and parses in one call; this recipe is for
staging a wide backfill window (e.g. weeks) up front, so the slow network
fetch and the CPU-bound parse can be run as separate steps, useful when you
want to verify disk headroom or inspect the raw dumps before committing to a
multi-hour parse.

Usage:
    .venv/bin/python recipes/sra_backfill_download.py \
        --start-date 2026-08-14 --end-date 2026-09-13
    .venv/bin/python recipes/sra_backfill_download.py \
        --start-date 2026-08-14 --end-date 2026-09-13 --workers 3

Safe to re-run: `_download_sra_file_dumps` skips any dump already on disk.
"""

from __future__ import annotations

import argparse
import functools
import logging
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from helpers import download_file  # noqa: E402
from ingest.sra import _download_sra_file_dumps  # noqa: E402
from ingest.sra_daily import _extract_sra_daily_dumps_in_range  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


def _date(s: str) -> date:
    return date.fromisoformat(s)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=_date, required=True)
    parser.add_argument("--end-date", type=_date, required=True)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("/data/share/seqout/sra"),
        help="the sra/ output dir under DATA_DIR (not DATA_DIR itself)",
    )
    parser.add_argument(
        "--workers", type=int, default=3, help="per-file download workers"
    )
    parser.add_argument("--retries", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=30, help="seconds")
    parser.add_argument(
        "--max-timeout", type=int, default=120, help="seconds, backoff cap"
    )
    parser.add_argument(
        "--chunk-size-mb", type=int, default=20, help="download chunk size in MB"
    )
    args = parser.parse_args()

    dumps = _extract_sra_daily_dumps_in_range(args.start_date, args.end_date)
    if not dumps:
        logger.error("no daily dumps found in %s..%s", args.start_date, args.end_date)
        raise SystemExit(1)
    logger.info("%d dump(s) to ensure present: %s", len(dumps), dumps)

    downloader = functools.partial(
        download_file,
        num_retries=args.retries,
        timeout=args.timeout,
        max_wait=args.max_timeout,
        with_pbar=True,
        validate_content_length=True,
    )

    data_dir = _download_sra_file_dumps(
        downloader,
        dumps,
        args.data_dir,
        num_workers=args.workers,
        chunk_size=args.chunk_size_mb * 1024 * 1024,
    )
    logger.info("done, data in %s", data_dir)


if __name__ == "__main__":
    main()
