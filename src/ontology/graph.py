"""
Load the extracted artifacts into Neo4j, and stamp the ``has_children`` flag.

The graph has **one node label and three relationship types**::

    (:Term {name, has_children, xrefs})
        -[:MAPS_TO]->      synonym -> canonical term
        -[:SUBCLASS_OF]->  child   -> parent   (from OBO is_a)
        -[:PART_OF]->      part    -> whole    (from OBO BFO_0000050)

``name`` is the only key -- there is no per-ontology id *identifying* a node, so
a term that appears in two ontologies is one merged node. That is intentional:
the app looks terms up by the words a user typed, not by CURIE.

``xrefs`` is what that merge would otherwise throw away: the list of source
CURIEs the term came from, e.g. ``["UBERON:0002107", "MeSH:D008099"]``. It is
the *only* provenance record -- the source is readable off the CURIE prefix, so
nodes carry no separate ``source`` property. Prefixes are not limited to the
eight ontologies in the registry: ``cl.json`` and ``uberon-full.json`` bundle
imported classes, so ``PATO:``, ``GO:``, ``NCBITaxon:`` and others legitimately
appear. Appends are guarded, so re-loading an unchanged artifact writes nothing.

Every edge write is a ``MERGE``, so re-running any of this is safe and
idempotent. Every ``xrefs`` write is a ``MATCH``-guarded ``SET``: provenance
never invents a node. See ``load_xrefs`` for when that distinction matters.
"""

from __future__ import annotations

import csv
import json
import logging
import re
from typing import TYPE_CHECKING, Any

from neo4j import GraphDatabase
from tqdm import tqdm

from ontology.extract import curie, norm

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from neo4j import Driver

    from ontology.sources import Source

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000

#: The uniqueness constraint doubles as the only index in the database, and it
#: backs every name lookup and every MERGE. Relationship traversals are pointer
#: chases -- no index applies and none is needed.
_CONSTRAINT = (
    "CREATE CONSTRAINT term_name_unique IF NOT EXISTS "
    "FOR (t:Term) REQUIRE t.name IS UNIQUE"
)

#: OBO edge predicate -> our relationship type. Anything else is ignored:
#: the remaining OBO relations (develops_from, has_part, ...) do not mean
#: "narrower than" and would make deep-dive wander sideways.
_PRED_REL = {
    "is_a": "SUBCLASS_OF",
    "BFO_0000050": "PART_OF",
    "http://purl.obolibrary.org/obo/BFO_0000050": "PART_OF",
}

_SAFE_IDENT = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

#: Minimum columns in a term-expansions CSV. The third, ``id``, is the
#: canonical term's CURIE; a row is still usable without one, so it is read
#: defensively rather than required.
_CSV_COLS = 2


def connect(uri: str, user: str, password: str) -> Driver:
    """Open a Neo4j driver and fail fast if the server is unreachable."""
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    return driver


def _batched(rows: list[dict[str, str]], size: int) -> Iterator[list[dict[str, str]]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def _run_batches(
    driver: Driver,
    query: str,
    rows: list[dict[str, str]],
    desc: str,
) -> None:
    for chunk in tqdm(list(_batched(rows, BATCH_SIZE)), desc=desc, unit="batch"):
        driver.execute_query(query, rows=chunk, database_="neo4j")


def _checked_label(label: str | None) -> str | None:
    """Reject anything that is not a bare identifier; labels cannot be parameterized."""
    if label and not _SAFE_IDENT.match(label):
        msg = f"unsafe Neo4j label: {label!r}"
        raise ValueError(msg)
    return label


def _tag_node(var: str, label: str | None) -> str:
    """
    ``ON CREATE`` fragment marking a node this source was the first to create.

    On create only, so a node another ontology already contributed is never
    relabelled. See ``Source.label`` for why that is the semantics we want.
    """
    return f" ON CREATE SET {var}:{label}" if label else ""


def _tag_rel(var: str, label: str | None) -> str:
    """``ON CREATE`` fragment stamping ``source`` on a newly created relationship."""
    return f" ON CREATE SET {var}.source = '{label}'" if label else ""


def _append_xref(var: str, id_expr: str = "row.id") -> str:
    """
    Cypher fragment appending ``id_expr`` to ``var``'s ``xrefs`` list, once.

    ``FOREACH`` over a one-or-zero element list is the standard way to write a
    conditional ``SET``; Cypher has no ``SET ... WHERE``. The guard makes the
    write idempotent, which matters more than it looks: without it every re-load
    grows the list and rewrites every publication-carrying node.
    """
    return f"""
    FOREACH (_ IN CASE WHEN {id_expr} <> ''
                        AND NOT {id_expr} IN coalesce({var}.xrefs, [])
                       THEN [1] ELSE [] END |
        SET {var}.xrefs = coalesce({var}.xrefs, []) + {id_expr})
    """


def ensure_constraint(driver: Driver) -> None:
    """Create the :Term(name) uniqueness constraint if it is not there yet."""
    driver.execute_query(_CONSTRAINT, database_="neo4j")


def _read_csv(path: Path) -> list[dict[str, str]]:
    """Read a ``short,long,id`` term-expansions CSV. Missing ids become ``""``."""
    with path.open() as f:
        reader = csv.reader(f)
        next(reader, None)  # header: short,long,id
        return [
            {"short": r[0], "long": r[1], "id": r[2] if len(r) > _CSV_COLS else ""}
            for r in reader
            if len(r) >= _CSV_COLS
        ]


def load_synonyms(driver: Driver, source: Source, root: Path) -> int:
    """
    Load ``<name>_term_expansions.csv`` as MAPS_TO edges, and stamp ``xrefs``.

    The row's id goes onto *both* nodes. A synonym string has no CURIE of its
    own, so it inherits its descriptor's -- that is what makes ``xrefs`` answer
    "which ontology is this spelling from?" for synonyms too, not just for
    canonical terms.
    """
    rows = _read_csv(source.csv_path(root))

    label = _checked_label(source.label)
    query = f"""
    UNWIND $rows AS row
    MERGE (s:Term {{name: row.short}}){_tag_node("s", label)}
    MERGE (l:Term {{name: row.long}}){_tag_node("l", label)}
    MERGE (s)-[r:MAPS_TO]->(l){_tag_rel("r", label)}
    {_append_xref("s")}
    {_append_xref("l")}
    """
    logger.info("%s: loading %d MAPS_TO edges", source.name, len(rows))
    _run_batches(driver, query, rows, f"{source.name} MAPS_TO")
    return len(rows)


def load_xrefs(driver: Driver, source: Source, root: Path) -> int:
    """
    Stamp ``xrefs`` from a term-expansions CSV **without touching any edge**.

    ``load_synonyms`` does two jobs -- MERGE the MAPS_TO edges and record the
    provenance. Use this instead when the edges are already in the graph and the
    CSV's pair set may have drifted from what was originally loaded: a
    regenerated extract that reproduces 99.95% of its pairs would have the
    remaining 0.05% MERGEd in *beside* the originals, which is not a rebuild but
    a slow corruption.

    Every write is MATCH-based, so this can never create a node or a
    relationship. Terms in the CSV that are absent from the graph are skipped.
    """
    rows = _read_csv(source.csv_path(root))

    # Matched independently, so a row still stamps the synonym when the
    # canonical is missing from the graph, and vice versa.
    query = f"""
    UNWIND $rows AS row
    CALL {{
        WITH row
        MATCH (s:Term {{name: row.short}})
        {_append_xref("s")}
    }}
    CALL {{
        WITH row
        MATCH (l:Term {{name: row.long}})
        {_append_xref("l")}
    }}
    """
    logger.info(
        "%s: stamping xrefs from %d rows (no edge writes)", source.name, len(rows)
    )
    _run_batches(driver, query, rows, f"{source.name} xrefs")
    return len(rows)


def load_hierarchy(driver: Driver, source: Source, root: Path) -> dict[str, int]:
    """
    Load an OBO-JSON's hierarchy edges, and stamp ``xrefs`` on its terms.

    The ``edges`` array becomes SUBCLASS_OF (is_a) / PART_OF (BFO_0000050)
    relationships. OBO edges join node *ids*, but our graph is keyed by name,
    so we resolve each id to its normalized label first. Deprecated and
    unlabelled nodes drop out here, which also drops any edge that touched them.

    The provenance pass covers the whole node list, not just the nodes that
    happen to appear in an edge, and is independent of the synonym CSV -- a
    canonical term with no synonyms at all still gets its CURIE. It runs on
    MATCH, so unlike the edge writes above it never invents a node.
    """
    path = source.hierarchy_path(root)
    if path is None:
        return {}

    with path.open() as f:
        graph: dict[str, Any] = json.load(f)["graphs"][0]

    id_to_name = {
        n["id"]: norm(n["lbl"])
        for n in graph["nodes"]
        if n.get("lbl") and not n.get("meta", {}).get("deprecated")
    }

    by_rel: dict[str, list[dict[str, str]]] = {}
    for e in graph.get("edges", []):
        rel = _PRED_REL.get(e.get("pred"))
        if not rel:
            continue
        child, parent = id_to_name.get(e.get("sub")), id_to_name.get(e.get("obj"))
        if child and parent and child != parent:
            by_rel.setdefault(rel, []).append({"c": child, "p": parent})

    label = _checked_label(source.label)
    for rel, rows in by_rel.items():
        query = f"""
        UNWIND $rows AS row
        MERGE (c:Term {{name: row.c}}){_tag_node("c", label)}
        MERGE (p:Term {{name: row.p}}){_tag_node("p", label)}
        MERGE (c)-[r:{rel}]->(p){_tag_rel("r", label)}
        """
        logger.info("%s: loading %d %s edges", source.name, len(rows), rel)
        _run_batches(driver, query, rows, f"{source.name} {rel}")

    prov = [
        {"name": name, "id": cur}
        for node_id, name in id_to_name.items()
        if (cur := curie(node_id))
    ]
    logger.info("%s: stamping xrefs on %d terms", source.name, len(prov))
    _run_batches(
        driver,
        f"""
        UNWIND $rows AS row
        MATCH (t:Term {{name: row.name}})
        {_append_xref("t")}
        """,
        prov,
        f"{source.name} xrefs",
    )

    return {rel: len(rows) for rel, rows in by_rel.items()}


# ---------------------------------------------------------------------------
# has_children
# ---------------------------------------------------------------------------

#: A term X has children iff something points at X, or at any member of X's
#: synonym cluster, via SUBCLASS_OF/PART_OF -- excluding the cluster itself, so
#: a synonym is never counted as a child of its own term. MAPS_TO*0..2 includes
#: X via the *0, and two hops covers synonym-of-a-synonym chains.
#:
#: This is the *same* definition the deep-dive children query uses, which is
#: what makes the flag safe to trust: it can go stale, but it cannot disagree.
_COMPUTE_HAS_CHILDREN = """
UNWIND $names AS nm
MATCH (x:Term {name: nm})
OPTIONAL MATCH (x)-[:MAPS_TO*0..2]-(m:Term)
WITH x, collect(DISTINCT m) AS cluster
UNWIND cluster AS s
OPTIONAL MATCH (grand:Term)-[:SUBCLASS_OF|PART_OF]->(s)
WHERE NOT grand IN cluster
WITH x, count(grand) AS g
SET x.has_children = g > 0
"""


def stamp_has_children(driver: Driver) -> tuple[int, int]:
    """
    Precompute ``has_children`` on every :Term. Run this last, after any load.

    The frontend's deep-dive explorer needs to know whether each node it renders
    can be expanded. Working that out at query time means an undirected synonym
    walk per node, which is slow on dense anatomy hubs (breast, brain). The
    graph is static between rebuilds, so we compute it once and read the flag.

    Staleness is a snapshot, not a hazard: the children query reads
    ``coalesce(child.has_children, false)``, so a missing or stale flag shows a
    term as not-expandable rather than erroring. Re-run after every load.
    """
    records, _, _ = driver.execute_query(
        "MATCH (t:Term) RETURN t.name AS name",
        database_="neo4j",
    )
    names = [r["name"] for r in records]
    logger.info("stamping has_children on %d :Term nodes", len(names))

    for i in tqdm(range(0, len(names), BATCH_SIZE), desc="has_children", unit="batch"):
        driver.execute_query(
            _COMPUTE_HAS_CHILDREN,
            names=names[i : i + BATCH_SIZE],
            database_="neo4j",
        )

    with_children = driver.execute_query(
        "MATCH (t:Term) WHERE t.has_children RETURN count(t) AS n",
        database_="neo4j",
    )[0][0]["n"]
    logger.info("done: %d/%d terms have children", with_children, len(names))
    return len(names), with_children


def stats(driver: Driver) -> dict[str, int]:
    """Count nodes and relationships, to sanity-check a rebuild."""
    out: dict[str, int] = {}
    out["terms"] = driver.execute_query(
        "MATCH (t:Term) RETURN count(t) AS n",
        database_="neo4j",
    )[0][0]["n"]
    out["has_children"] = driver.execute_query(
        "MATCH (t:Term) WHERE t.has_children RETURN count(t) AS n",
        database_="neo4j",
    )[0][0]["n"]
    out["with_xrefs"] = driver.execute_query(
        "MATCH (t:Term) WHERE t.xrefs IS NOT NULL RETURN count(t) AS n",
        database_="neo4j",
    )[0][0]["n"]
    # A CURIE that curie() failed to resolve lands here as junk rather than as
    # an error, so the rebuild counts it. Expect 0; anything else is a
    # curie() regression. CVCL_ is Cellosaurus, the documented exception.
    out["bad_xrefs"] = driver.execute_query(
        "MATCH (t:Term) UNWIND t.xrefs AS x "
        "WITH x WHERE NOT x =~ "
        "'^([A-Za-z][A-Za-z0-9]*:[A-Za-z0-9.-]+|CVCL_[A-Za-z0-9]+)$' "
        "RETURN count(x) AS n",
        database_="neo4j",
    )[0][0]["n"]
    for rel in ("MAPS_TO", "SUBCLASS_OF", "PART_OF"):
        out[rel] = driver.execute_query(
            f"MATCH ()-[r:{rel}]->() RETURN count(r) AS n",
            database_="neo4j",
        )[0][0]["n"]
    return out
