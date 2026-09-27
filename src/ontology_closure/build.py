"""
Compute the ontology descendant closure, SQLite-first, for transfer to Postgres.

The Beacon v2 API (pysradb-server) supports `includeDescendantTerms` filtering: a
filter on term T must match samples annotated with T *or any descendant of T*.
The per-sample `{field}_ancestors` column produced by enrichment only follows the
first parent (`get_ancestor_chain` -> `parents[0]`), a single lineage rather than
the full DAG closure -- matching against it would silently under-count. This module
computes the full transitive closure over all parents from the parsed OBO/EFO JSONs.

Stage -> transfer, mirroring the repo's usual convention:
  ontology_closure build           parse ontologies -> full-DAG closure -> SQLite
  ontology_closure verify-sqlite   correctness assertions against the SQLite
  transfer ontology-closure        load the SQLite tables into Postgres
  transfer ontology-closure-verify-pg   EXPLAIN + superset assertions against Postgres

Table `ontology_closure(ontology, ancestor_id, descendant_id)` is scoped by source
ontology so EFO's imported CURIEs can't cross-contaminate disease/tissue/etc.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from enrich.ontology_map import FIELD_ONTOLOGY_MAP, ONTOLOGY_DIR, OntologyIndex
from helpers import exec_query_from_file, sqlite_connect

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# field -> short ontology tag. tissue & development_stage share uberon.
ONTOLOGY_OF = {
    "disease": "mondo",
    "tissue": "uberon",
    "cell_type": "cl",
    "assay": "efo",
    "development_stage": "uberon",
}


def _all_ancestors(
    index: OntologyIndex, tid: str, cache: dict[str, set[str]]
) -> set[str]:
    """All transitive ancestors of `tid` over ALL parents (DAG), cycle-guarded."""
    cached = cache.get(tid)
    if cached is not None:
        return cached
    cache[tid] = set()  # placeholder breaks cycles (OBO imports can loop)
    acc: set[str] = set()
    for parent in index.get_parents(tid):
        acc.add(parent)
        acc |= _all_ancestors(index, parent, cache)
    cache[tid] = acc
    return acc


def compute() -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str]]]:
    """
    Return (closure_rows, term_rows).

    closure_rows: (ontology, ancestor_id, descendant_id) for every term (full DAG).
    term_rows:    (ontology, id, label) for every term -- labels for /filtering_terms
                  and resolve_ontology_term, incl. ancestor CURIEs that are no
                  sample's own term.
    """
    closure: list[tuple[str, str, str]] = []
    terms: list[tuple[str, str, str]] = []
    seen_onto: set[str] = set()
    for field, (fname, keys, prefix) in FIELD_ONTOLOGY_MAP.items():
        onto = ONTOLOGY_OF[field]
        if onto in seen_onto:  # uberon appears for both tissue + development_stage
            continue
        seen_onto.add(onto)
        path = ONTOLOGY_DIR / fname
        if not path.exists():
            logger.warning("missing ontology file %s", path)
            continue
        index = OntologyIndex(path, keys, prefix)
        cache: dict[str, set[str]] = {}
        n_before = len(closure)
        for tid in index.terms_by_id:
            name = index.get_name(tid)
            if name:
                terms.append((onto, tid, name))
            ancestors = _all_ancestors(index, tid, cache)
            closure.extend((onto, anc, tid) for anc in ancestors)
        n_new = len(closure) - n_before
        n_terms = len(index.terms_by_id)
        logger.info("%s: %d terms -> %d closure edges", onto, n_terms, n_new)
    return closure, terms


def build(sqlite_path: Path) -> None:
    closure, terms = compute()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    exec_query_from_file("tables/ontology_closure.sql", db_file=sqlite_path)
    con = sqlite_connect(sqlite_path)
    try:
        con.executemany(
            "INSERT OR IGNORE INTO ontology_closure VALUES (?, ?, ?)", closure
        )
        con.executemany("INSERT OR IGNORE INTO ontology_terms VALUES (?, ?, ?)", terms)
        con.commit()
        (nc,) = con.execute("SELECT count(*) FROM ontology_closure").fetchone()
        (nt,) = con.execute("SELECT count(*) FROM ontology_terms").fetchone()
        logger.info("SQLite %s: %d closure rows, %d term labels", sqlite_path, nc, nt)
    finally:
        con.close()


def verify_sqlite(sqlite_path: Path) -> None:
    con = sqlite_connect(sqlite_path)
    try:
        # neoplasm (MONDO:0005070) must have breast cancer (MONDO:0004992) as
        # a descendant
        hit = con.execute(
            "SELECT 1 FROM ontology_closure WHERE ontology='mondo' "
            "AND ancestor_id='MONDO:0005070' AND descendant_id='MONDO:0004992'"
        ).fetchone()
        if not hit:
            msg = "closure missing known neoplasm -> breast-cancer edge"
            raise AssertionError(msg)
        counts = con.execute(
            "SELECT ontology, count(*) FROM ontology_closure "
            "GROUP BY ontology ORDER BY ontology"
        ).fetchall()
        for onto, n in counts:
            if n == 0:
                msg = f"empty closure for {onto}"
                raise AssertionError(msg)
        logger.info("verify-sqlite OK: %s", dict(counts))
    finally:
        con.close()
