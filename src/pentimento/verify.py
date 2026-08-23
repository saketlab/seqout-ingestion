"""
Quality gate for quantifier output before corpus ingest.

Checks parsed files that look finished but say nothing; those would otherwise
read as measured-empty in n_runs_measurable rollups.
"""

from __future__ import annotations

import csv
import logging
import os
from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

META_ROWS = ("__run__", "__total_reads__", "__read_length__")
EXPECTED_HEADER = "virus,reads,kmer_mass,n_unitigs,ref_bp,covered_bp,breadth_frac,class"


def _rows(path: Path) -> tuple[list[str], list[list[str]], dict[str, str]]:
    """Return (header, data rows, metadata) for one .quant.csv."""
    header: list[str] = []
    data: list[list[str]] = []
    meta: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for i, rec in enumerate(csv.reader(fh)):
            if not rec:
                continue
            if i == 0:
                header = rec
            elif rec[0].startswith("__"):
                meta[rec[0]] = rec[1] if len(rec) > 1 else ""
            else:
                data.append(rec)
    return header, data, meta


def check_one(path: Path) -> list[str]:
    """Return the problems found in one result, empty when it is sound."""
    acc = path.name.removesuffix(".quant.csv")
    bad: list[str] = []
    try:
        header, data, meta = _rows(path)
    except OSError:
        return ["unreadable"]

    status_file = path.with_name(f"{acc}.status")
    status = ""
    if status_file.is_file():
        try:
            status = status_file.read_text(errors="replace").strip()
        except OSError:
            status = ""
    else:
        bad.append("no_status_sidecar")

    if status.startswith("absent_from_logan") or meta.get("__status__", "").startswith(
        "absent_from_logan"
    ):
        # The absent path writes its own shorter row set: a 6-column header, no
        # __total_reads__ and no __read_length__, because nothing was measured
        # and inventing those would be a claim. Judge it on its own terms.
        if "__run__" not in meta:
            bad.append("missing_run")
        if data:
            bad.append("absent_but_has_targets")
        return bad

    if ",".join(header) != EXPECTED_HEADER:
        # a pre-breadth 6-column file, or a truncated write
        bad.append(f"header_mismatch({len(header)}col)")
    bad.extend(f"missing_{k.strip('_')}" for k in META_ROWS if k not in meta)
    if status.startswith(("download_failed", "too_large", "abundance_pass")):
        # a failure row with a .quant.csv is contradictory: the corpus treats a
        # present csv as a completed measurement
        if data:
            bad.append("failed_status_but_has_targets")
        return bad

    if not meta.get("__total_reads__"):
        # not fatal, but any abundance fraction over this run is uncomputable
        bad.append("empty_total_reads")

    if data:
        # the silent-zero signature: rows present, every measurement zero, and
        # the status still `ok`
        nonzero = 0
        for r in data:
            try:
                if float(r[1]) > 0 or float(r[2]) > 0:
                    nonzero += 1
            except (ValueError, IndexError):
                bad.append("unparseable_row")
                break
        if data and nonzero == 0:
            bad.append("all_zero_abundance")
    return bad


def verify_dir(directory: Path, sample: int | None = None) -> dict[str, int]:
    """Scan a results directory, returning a count per problem plus totals."""
    counts: Counter[str] = Counter()
    names: Iterator[str] = (
        e.name for e in os.scandir(directory) if e.name.endswith(".quant.csv")
    )
    for i, name in enumerate(names):
        if sample and i >= sample:
            break
        counts["scanned"] += 1
        problems = check_one(directory / name)
        if not problems:
            counts["clean"] += 1
        counts.update(problems)
        if counts["scanned"] % 20_000 == 0:
            logger.info("verified %d", counts["scanned"])
    return dict(counts)


def repair_statuses(directory: Path, *, dry_run: bool = True) -> dict[str, int]:
    """
    Restore `ok` where complete measurements were mislabelled failures.

    A CSV with full metadata and at least one target proves completion. Zero-target
    CSVs stay untouched because clean-empty and failed-early are indistinguishable.
    """
    counts: Counter[str] = Counter()
    for entry in os.scandir(directory):
        if not entry.name.endswith(".quant.csv"):
            continue
        acc = entry.name.removesuffix(".quant.csv")
        status_file = directory / f"{acc}.status"
        try:
            status = status_file.read_text(errors="replace").strip()
        except OSError:
            continue
        if not status.startswith(("download_failed", "too_large", "abundance_pass")):
            continue
        counts["failed_status"] += 1
        try:
            _, data, meta = _rows(directory / entry.name)
        except OSError:
            continue
        if not data or not meta.get("__total_reads__"):
            counts["left_alone_no_proof"] += 1
            continue
        counts["repairable"] += 1
        if not dry_run:
            status_file.write_text("ok\n")
            counts["repaired"] += 1
    return dict(counts)
