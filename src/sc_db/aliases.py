"""
Map queue accessions to the canonical form other stages resolve by.

`sc_db` routes on whatever accession the archive tables carry, so an ENA
BioProject can reach the queue while `unified_metadata` holds the study under
its GSE. A stage keyed on the canonical accession finds nothing and drops the
study without saying so.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from helpers import sqlite_connect

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

logger = logging.getLogger(__name__)

# json_each expands the aliases dict, so one row per (canonical, alias) pair.
# Ordering by canonical_accession keeps the pick stable when an alias appears
# under more than one study.
_REVERSE = """
SELECT j.value, u.canonical_accession
FROM unified_metadata u, json_each(u.aliases) j
JOIN _alias_want w ON w.accession = j.value
ORDER BY u.canonical_accession
"""


def canonical_map(sc_db_file: Path, accessions: Sequence[str]) -> dict[str, str]:
    """Return {given: canonical} for accessions that are an alias of something."""
    if not accessions:
        return {}
    conn = sqlite_connect(f"file:{sc_db_file}?immutable=1", uri=True)
    try:
        conn.execute("CREATE TEMP TABLE _alias_want (accession TEXT PRIMARY KEY)")
        conn.executemany(
            "INSERT OR IGNORE INTO _alias_want VALUES (?)", [(a,) for a in accessions]
        )
        mapping: dict[str, str] = {}
        for alias, canonical in conn.execute(_REVERSE):
            mapping.setdefault(alias, canonical)
    finally:
        conn.close()
    return mapping


def canonicalise(sc_db_file: Path, accessions: Sequence[str]) -> list[str]:
    """Swap each accession for its canonical form, keeping order and uniqueness."""
    # An accession already canonical is absent from the map and passes through.
    mapping = canonical_map(sc_db_file, accessions)
    seen: set[str] = set()
    out: list[str] = []
    for accession in accessions:
        resolved = mapping.get(accession, accession)
        if resolved not in seen:
            seen.add(resolved)
            out.append(resolved)
    logger.info("canonicalised %d of %d accessions", len(mapping), len(accessions))
    return out
