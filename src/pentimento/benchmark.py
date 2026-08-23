"""Ingest the internal preflightx comparison against archive metadata."""

from __future__ import annotations

import csv
import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

FIELDS = ("organism", "assay", "sex")

# Score "unknown" as a no-call. Counting it as a disagreement turns the
# comparison into a coverage measure.
_NOCALL = re.compile(
    r"^$|^na$|unknown|undetermined|ambiguous|not determined|inconclusive"
)
_MIXED = re.compile(r"mixed|pooled")

# (pattern, canonical) applied in order; first match wins. Unmatched values
# keep their raw vocabulary and surface as disagreements.
_ORGANISM = (
    (re.compile(r"homo sapiens|human"), "human"),
    (re.compile(r"mus musculus|mus spretus|mouse"), "mouse"),
    (re.compile(r"rattus|rat\b"), "rat"),
)
_SEX = (
    (re.compile(r"^xx$|female|\bf\b"), "female"),
    (re.compile(r"^xy$|male|\bm\b"), "male"),
)
_ASSAY = (
    (re.compile(r"5'|chromium 5"), "10x 5'"),
    (re.compile(r"10x|chromium|3' ?v"), "10x 3'"),
    (re.compile(r"drop-?seq"), "drop-seq"),
    (re.compile(r"smart-?seq|ss2"), "smart-seq"),
    (
        re.compile(r"not single|not-single|bulk|^rna-?seq$|^wgs$|^wxs$|^chip"),
        "not-single-cell",
    ),
    (re.compile(r"scrna|snrna|single"), "single-cell (unspecified)"),
)


def _normalise(field: str, value: str | None) -> str | None:
    """Collapse one side of a comparison into the shared vocabulary."""
    text = (value or "").strip().lower()
    if field == "organism":
        text = text.replace("_", " ").replace("-", " ")
    if _NOCALL.search(text):
        return None
    if field in {"organism", "sex"} and _MIXED.search(text):
        return None
    table = {"organism": _ORGANISM, "sex": _SEX, "assay": _ASSAY}[field]
    for pattern, canonical in table:
        if pattern.search(text):
            return canonical
    return text or None


def load_benchmark(csv_file: Path) -> Iterator[tuple[Any, ...]]:
    """Yield (accession, experiment, field, gt_raw, pred_raw, gt, pred, correct)."""
    if not csv_file.is_file():
        logger.warning("benchmark csv missing, skipping: %s", csv_file)
        return
    with csv_file.open(newline="") as handle:
        for row in csv.DictReader(handle):
            for field in FIELDS:
                gt_raw = row.get(f"gt_{field}")
                pred_raw = row.get(f"pred_{field}")
                gt = _normalise(field, gt_raw)
                pred = _normalise(field, pred_raw)
                # NULL, not 0, when either side is unknown: "we could not check"
                # and "we checked and it was wrong" are different facts.
                correct = None if gt is None or pred is None else int(gt == pred)
                yield (
                    row["accession"],
                    row["experiment"],
                    field,
                    gt_raw or None,
                    pred_raw or None,
                    gt,
                    pred,
                    correct,
                )


def summarise(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Per-field coverage and agreement, straight off the ingested rows."""
    rows = conn.execute(
        """
        SELECT field,
               COUNT(*),
               SUM(pred_norm IS NOT NULL),
               SUM(correct IS NOT NULL),
               SUM(COALESCE(correct, 0))
        FROM preflightx_benchmark
        GROUP BY field
        """
    ).fetchall()
    return {
        field: {
            "n_experiments": n,
            "n_predicted": predicted,
            "n_evaluable": evaluable,
            # None: sex has no evaluable rows. Zero would read as measured-and-wrong.
            "agreement": round(correct / evaluable, 4) if evaluable else None,
        }
        for field, n, predicted, evaluable, correct in rows
    }
