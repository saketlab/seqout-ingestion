#!/usr/bin/env python
"""
Rare-disease joins through MONDO ids and closure descendants.

Substring matching is unsafe: Non-Small Cell Lung Cancer would match small cell
lung carcinoma. Tighten on disease_match_method when precision matters.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from helpers import sqlite_connect

if TYPE_CHECKING:
    from pathlib import Path

#: A MONDO id list plus descendants. An empty ontology_closure falls back
#: to bare ids; self-pairs are absent, hence the UNION.
MONDO_EXPAND = """SELECT descendant_id FROM ontology_closure
     WHERE ontology = 'mondo' AND ancestor_id = ANY(%(mondo)s)
     UNION SELECT unnest(%(mondo)s::text[])"""

#: Predicate over alias os; expects a %(mondo)s list of curies.
#:
#: Arm 2 catches samples with no resolved id but populated disease_ancestors.
#: `IS NULL` keeps arms selective and disjoint, so the planner builds BitmapOr.
#:
#: This shape keeps the arms selective. Ancestors-only matching also misses
#: studies whose id is a closure descendant while disease_ancestors omits the
#: GARD ancestor.
MONDO_SUBSET = """(
    os.disease_ontology_id = ANY(ARRAY({MONDO_EXPAND}))
    OR (os.disease_ontology_id IS NULL
        AND os.disease_ancestors IS NOT NULL
        AND os.disease_ancestors <> ''
        AND mondo_ancestor_ids(os.disease_ancestors) && %(mondo)s::text[])
)"""


def with_subset(template: str) -> str:
    """
    Substitute MONDO_SUBSET into a query template.

    Caller templates start SELECT ... FROM; interpolation trips ruff S608.
    """
    return template.replace(
        "{MONDO_SUBSET}", MONDO_SUBSET.replace("{MONDO_EXPAND}", MONDO_EXPAND)
    )


def mondo_ids(gard_db_file: Path) -> list[str]:
    """Distinct MONDO curies the GARD stage resolved; unmapped dropped."""
    with sqlite_connect(gard_db_file) as conn:
        return sorted(
            curie
            for (curie,) in conn.execute(
                "SELECT DISTINCT mondo_id FROM gard_disease WHERE mondo_id IS NOT NULL"
            )
        )


def gard_labels(gard_db_file: Path) -> dict[str, tuple[str, str]]:
    """MONDO curie -> (gard_id, gard_name), for labelling a report."""
    out: dict[str, tuple[str, str]] = {}
    with sqlite_connect(gard_db_file) as conn:
        for mondo, gard_id, name in conn.execute(
            "SELECT mondo_id, gard_id, gard_name FROM gard_disease "
            "WHERE mondo_id IS NOT NULL ORDER BY gard_id"
        ):
            out.setdefault(mondo, (gard_id, name))
    return out
