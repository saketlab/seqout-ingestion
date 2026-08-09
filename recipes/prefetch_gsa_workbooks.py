"""
Pre-fetch CRA workbooks.
uv run python recipes/prefetch_gsa_workbooks.py --output-dir /NAS/saket/seqout-ddbj
"""

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tqdm import tqdm

from ingest.gsa import _enumerate_studies, _fetch_study_xlsx

logging.basicConfig(level=logging.WARNING, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)


def fetch_one(
    acc: str,
    out_dir: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> str:
    dest = out_dir / f"{acc}.xlsx"
    if dest.exists() and dest.stat().st_size > 0:
        return "cached"

    try:
        data = _fetch_study_xlsx(
            acc,
            retries=retries,
            timeout=timeout,
            max_timeout=max_timeout,
        )
    except Exception:
        logger.exception("failed %s", acc)
        return "failed"

    if not data:
        return "empty"

    tmp = dest.with_name(f"{dest.name}.prefetch{os.getpid()}")
    tmp.write_bytes(data)
    if dest.exists() and dest.stat().st_size > 0:
        tmp.unlink(missing_ok=True)  # ingest got there first
        return "cached"
    tmp.rename(dest)
    return "ok"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--retries", type=int, default=20)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--max-timeout", type=int, default=120)
    args = ap.parse_args()

    out_dir = args.output_dir / "gsa" / "data" / "studies"
    out_dir.mkdir(parents=True, exist_ok=True)

    studies = _enumerate_studies(args.retries, args.timeout, args.max_timeout)
    todo = [a for a in reversed(studies) if not (out_dir / f"{a}.xlsx").exists()]
    logger.warning("%d CRA studies, %d workbooks to fetch", len(studies), len(todo))

    counts: dict[str, int] = {}
    with (
        ThreadPoolExecutor(args.workers) as pool,
        tqdm(total=len(todo), unit="wb", desc="prefetching gsa workbooks") as bar,
    ):
        futures = {
            pool.submit(
                fetch_one,
                a,
                out_dir,
                args.retries,
                args.timeout,
                args.max_timeout,
            ): a
            for a in todo
        }
        for f in as_completed(futures):
            counts[f.result()] = counts.get(f.result(), 0) + 1
            bar.update(1)
            bar.set_postfix(counts, refresh=False)

    logger.warning("prefetch done: %s", counts)


if __name__ == "__main__":
    main()
