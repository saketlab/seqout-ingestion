"""
Plate-based single-cell protocols routed around barcode gates.

Plate protocols keep cell identity in filenames; barcode evidence is weak.
Detection uses single_cell_evidence.value.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from sc_evidence.rules import Term, _like_any

if TYPE_CHECKING:
    import sqlite3

logger = logging.getLogger(__name__)

# Same (regex, LIKE) shape as the evidence rules, keeping the SQL gate
# and Python label aligned.
PLATE_TERMS: tuple[Term, ...] = (
    Term(r"smart.?seq", ("%smart-seq%", "%smartseq%", "%smart seq%")),
    Term(r"cel.?seq", ("%cel-seq%", "%celseq%", "%cel seq%")),
    Term(r"mars.?seq", ("%mars-seq%", "%marsseq%")),
    Term(r"scrb.?seq", ("%scrb-seq%", "%scrbseq%")),
    Term(r"quartz.?seq", ("%quartz-seq%", "%quartzseq%")),
    Term(r"strt.?seq", ("%strt-seq%", "%strtseq%")),
    # Plate/microwell instruments: one well, one library, same consequence.
    Term(r"fluidigm", ("%fluidigm%",)),
    Term(r"icell8", ("%icell8%",)),
    Term(r"plate.based", ("%plate-based%", "%plate based%")),
)


def plate_candidates(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """
    Study to plate terms found in evidence text.

    One SQL scan applies the union gate; labels are re-derived so studies
    carrying multiple protocols keep them.
    """
    gate = _like_any("lower(e.value)", PLATE_TERMS)
    hits: dict[str, list[str]] = defaultdict(list)
    rows = conn.execute(
        "SELECT DISTINCT e.study_accession, e.value FROM single_cell_evidence e "  # noqa: S608
        f"WHERE e.study_accession IS NOT NULL AND e.value IS NOT NULL AND {gate}",
    )
    for study, value in rows:
        low = value.lower()
        for term in PLATE_TERMS:
            label = term.regex
            if label in hits[study]:
                continue
            if any(_like(low, pattern) for pattern in term.like):
                hits[study].append(label)
    return {s: sorted(t) for s, t in hits.items() if t}


def _like(value: str, pattern: str) -> bool:
    """SQLite LIKE for the patterns above: only '%' at the ends, '_' escaped."""
    needle = pattern.strip("%").replace(r"\_", "_")
    return needle in value
