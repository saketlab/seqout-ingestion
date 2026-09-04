"""
Countable expression matrices per study.

Uses sc_evidence.rules.TENX_FILE_TERMS across GEO and ArrayExpress file channels.
has_countable is narrower than matched; matrices count cells x genes.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections import defaultdict
from typing import TYPE_CHECKING

from sc_evidence.rules import TENX_FILE_TERMS, _like_any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# Countable TENX_FILE_TERMS entries, spelled as upstream regex strings.
# A renamed or dropped upstream term raises here before study reclassification.
COUNTABLE_REGEXES = frozenset(
    {
        r"barcodes\.tsv",
        r"features\.tsv",
        r"genes\.tsv",
        r"matrix\.mtx",
        r"filtered_feature_bc_matrix",
        r"raw_feature_bc_matrix",
        r"molecule_info\.h5",
        r"peak_bc_matrix",
    },
)
_KNOWN = {t.regex for t in TENX_FILE_TERMS}
if not COUNTABLE_REGEXES <= _KNOWN:
    msg = (
        "sc_db.matrix.COUNTABLE_REGEXES has drifted from "
        f"TENX_FILE_TERMS: {sorted(COUNTABLE_REGEXES - _KNOWN)}"
    )
    raise RuntimeError(msg)

_COMPILED = [(t.regex, re.compile(t.regex, re.IGNORECASE)) for t in TENX_FILE_TERMS]

# The extractor keeps a short span around the hit; ArrayExpress blobs can be large.
_SPAN = 60


def matched_terms(value: str | None) -> list[str]:
    if not value:
        return []
    return [regex for regex, pattern in _COMPILED if pattern.search(value)]


def _example(value: str | None, terms: list[str]) -> str | None:
    if not value or not terms:
        return None
    hit = re.compile(terms[0], re.IGNORECASE).search(value)
    if hit is None:
        return None
    start = max(hit.start() - _SPAN // 2, 0)
    return value[start : hit.end() + _SPAN // 2]


class MatrixHits:
    """Per-study accumulator for one channel."""

    def __init__(self) -> None:
        """Empty means unmatched; files may still exist."""
        self.n_objects = 0
        self.terms: set[str] = set()
        self.example: str | None = None

    def add(self, value: str | None) -> None:
        """Fold one object's file blob in, keeping the first hit as an example."""
        terms = matched_terms(value)
        if not terms:
            return
        self.n_objects += 1
        self.terms.update(terms)
        if self.example is None:
            self.example = _example(value, terms)

    def row(self, study: str, channel: str) -> tuple:
        """Emit the sc_study_matrix row for this study and channel."""
        return (
            study,
            channel,
            self.n_objects,
            int(bool(self.terms & COUNTABLE_REGEXES)),
            json.dumps(sorted(self.terms)),
            self.example,
        )


def _accumulate(rows: object, channel: str) -> list[tuple]:
    hits: dict[str, MatrixHits] = defaultdict(MatrixHits)
    for study, value in rows:  # type: ignore[misc]
        if study:
            hits[study].add(value)
    return [h.row(study, channel) for study, h in hits.items() if h.n_objects]


def geo_sample_channel(conn: sqlite3.Connection) -> list[tuple]:
    """
    Use evidence rows written by tenx_output_files.

    Re-scanning geo_samples reaches the same answer the long way round; those
    evidence rows are the complete GEO 10x filename census.
    """
    rows = conn.execute(
        "SELECT e.study_accession, e.value FROM single_cell_evidence e "
        "JOIN sc_rule r ON r.id = e.rule_id "
        "WHERE r.rule = 'tenx_output_files'",
    )
    return _accumulate(rows, "geo_sample_tenx")


def geo_series_channel(geo_db_file: Path) -> list[tuple]:
    gate = _like_any("lower(supplementary_data)", TENX_FILE_TERMS)
    with sqlite3.connect(f"file:{geo_db_file}?immutable=1", uri=True) as conn:
        rows = conn.execute(
            "SELECT accession, supplementary_data FROM geo_series "  # noqa: S608
            f"WHERE supplementary_data IS NOT NULL AND {gate}",
        ).fetchall()
    return _accumulate(rows, "geo_series_tenx")


def ae_channel(ae_db_file: Path) -> list[tuple]:
    gate = _like_any("lower(processed_data_files)", TENX_FILE_TERMS)
    with sqlite3.connect(f"file:{ae_db_file}?immutable=1", uri=True) as conn:
        rows = conn.execute(
            "SELECT accession, processed_data_files "  # noqa: S608
            "FROM arrayexpress_experiments "
            f"WHERE processed_data_files IS NOT NULL AND {gate}",
        ).fetchall()
    return _accumulate(rows, "ae_processed")


def ae_uncounted_extensions(ae_db_file: Path) -> int:
    """
    ArrayExpress matrix extensions outside TENX_FILE_TERMS.

    .mtx, .h5ad and .loom are countable costs excluded from the 10x list; rule
    changes belong in evidence terms.
    """
    gate = _like_any("lower(processed_data_files)", TENX_FILE_TERMS)
    with sqlite3.connect(f"file:{ae_db_file}?immutable=1", uri=True) as conn:
        row = conn.execute(
            "SELECT count(*) FROM arrayexpress_experiments "  # noqa: S608
            "WHERE processed_data_files IS NOT NULL "
            "AND (lower(processed_data_files) LIKE '%.mtx%' "
            "  OR lower(processed_data_files) LIKE '%.h5ad%' "
            "  OR lower(processed_data_files) LIKE '%.loom%') "
            f"AND NOT {gate}",
        ).fetchone()
    return int(row[0]) if row else 0
