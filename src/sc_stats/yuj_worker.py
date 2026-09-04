"""
Fleet worker: QC statistics for one accession's matrices per invocation.

    python3 yuj_worker.py <accession>

yuj appends the work item, so the accession is argv[-1]. Download links come
from yuj_setup/accessions/<accession>.json, the same sidecar the counting fleet
reads. Matrices are fetched to scratch, summarised, then deleted; results land
in <accession>.stats.json with a .status sidecar, so "measured, nothing found"
stays distinct from "never measured" the way the quant sweep does it.

Imports h5py and numpy and nothing else from this repo beyond sc_stats, because
the hosts get this package and no wider install.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from pathlib import Path

# fetch is stdlib-only and imports eagerly; sc_stats.stats is lazy, guarded in
# main() for its h5py/numpy dependency.
from sc_stats.fetch import (
    MATRIX_HINTS,
    download,
    is_archive,
    is_dense_table,
    is_wanted,
    unpack,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

_EXIT_USAGE = 2
_EXIT_MISSING_DEPENDENCY = 3

#: past this a study costs more disk than the statistic is worth
MAX_FILE_BYTES = int(os.environ.get("SC_STATS_MAX_BYTES") or 40 * 1024**3)


def _status(out_dir: Path, accession: str, state: str, detail: str = "") -> None:
    (out_dir / f"{accession}.status").write_text(
        f"{state}\n{detail}\n" if detail else f"{state}\n", encoding="utf-8"
    )


def _find_features(matrix: Path) -> Path | None:
    for name in ("features.tsv.gz", "features.tsv", "genes.tsv.gz", "genes.tsv"):
        candidate = matrix.parent / name
        if candidate.exists():
            return candidate
    return None


def _links_for(root: Path, accession: str) -> list[tuple[str, str]] | None:
    sidecar = root / "yuj_setup" / "accessions" / f"{accession}.json"
    if not sidecar.exists():
        return None
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("%s: unreadable sidecar: %s", accession, exc)
        return None
    return [(str(t), str(u)) for t, u in payload.get("links", [])]


def _fetch_all(links: list[tuple[str, str]], work: Path) -> tuple[int, int, int]:
    """Fetch every matrix-bearing link; returns (wanted, fetched, unreadable)."""
    work.mkdir(parents=True, exist_ok=True)
    wanted = fetched = unreadable = 0
    for _kind, url in links:
        if not is_wanted(url):
            continue
        wanted += 1
        name = url.rsplit("/", 1)[-1]
        got = download(url, work / name, MAX_FILE_BYTES)
        if got is None:
            continue
        fetched += 1
        if unpack(got, work / "unpacked") is None and is_archive(got.name):
            unreadable += 1
    return wanted, fetched, unreadable


def _local_matrices(work: Path) -> list[Path]:
    if not work.is_dir():
        return []
    found = [
        p
        for p in sorted(work.rglob("*"))
        if p.is_file()
        and (any(s in p.name.lower() for s in MATRIX_HINTS) or is_dense_table(p.name))
    ]
    # a .mtx and its own .mtx.gz are one matrix; the reader takes either
    compressed = {p.with_suffix("") for p in found if p.suffix == ".gz"}
    return [p for p in found if p not in compressed]


def _write_result(out_file: Path, payload: dict) -> None:
    tmp = out_file.with_suffix(".json.partial")
    tmp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    tmp.replace(out_file)


def _cleanup(work: Path) -> None:
    # scratch only; the statistics are already written to results/
    if work.is_dir() and os.environ.get("SC_STATS_KEEP") != "1":
        shutil.rmtree(work, ignore_errors=True)


def main() -> int:
    if len(sys.argv) < 2:  # noqa: PLR2004
        sys.stderr.write("usage: yuj_worker.py <accession>\n")
        return _EXIT_USAGE
    accession = sys.argv[-1]

    root = Path(os.environ.get("SC_STATS_DIR", Path.home() / "sc-stats"))
    work = root / accession
    out_dir = root / "results"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"{accession}.stats.json"
    if out_file.exists():
        logger.info("%s already done", accession)
        return 0

    try:
        from sc_stats.stats import summarise_matrix
    except ImportError as exc:
        sys.stderr.write(f"FATAL: stats deps missing: {exc}\n")
        return _EXIT_MISSING_DEPENDENCY

    links = _links_for(root, accession)
    if links is None and not work.is_dir():
        _status(out_dir, accession, "no_input", str(work))
        logger.warning("%s: no links sidecar and nothing at %s", accession, work)
        return 0

    wanted = fetched = unreadable = 0
    if links:
        wanted, fetched, unreadable = _fetch_all(links, work)

    matrices = _local_matrices(work)
    if not matrices:
        # a study whose files would not download is not a study without a
        # matrix; recording it as one turns a tooling failure into a finding
        if wanted and not fetched:
            state = "fetch_failed"
        elif unreadable:
            state = "unpack_failed"
        else:
            state = "no_matrix"
        _status(out_dir, accession, state, f"{fetched}/{wanted} files")
        logger.warning("%s: %s (%d/%d fetched)", accession, state, fetched, wanted)
        if state == "no_matrix":
            # terminal, and yuj re-dispatches any study with no result file
            _write_result(
                out_file,
                {
                    "accession": accession,
                    "matrices": [],
                    "failures": [],
                    "state": state,
                },
            )
        _cleanup(work)
        return 0

    rows, failures = [], []
    for i, m in enumerate(matrices, 1):
        logger.info("%s: [%d/%d] %s", accession, i, len(matrices), m.name)
        try:
            row = summarise_matrix(m, _find_features(m))
        except Exception as exc:  # one bad file must not lose the others
            failures.append({"file": m.name, "error": str(exc)[:200]})
            logger.warning("%s: %s failed: %s", accession, m.name, exc)
            continue
        row["accession"] = accession
        rows.append(row)

    _write_result(
        out_file,
        {"accession": accession, "matrices": rows, "failures": failures},
    )
    _status(out_dir, accession, "ok" if rows else "all_failed", f"{len(rows)} matrices")
    logger.info("%s: wrote %d matrix rows", accession, len(rows))
    _cleanup(work)
    return 0


if __name__ == "__main__":
    sys.exit(main())
