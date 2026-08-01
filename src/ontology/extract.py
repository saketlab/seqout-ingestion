"""
Turn a downloaded ontology into the two artifacts the graph loader wants.

Every source, whatever its native format, is reduced to:

* ``expansions/<name>_term_expansions.csv`` -- columns ``short,long``. One row
  per synonym: ``short`` is the alternative spelling, ``long`` is the canonical
  term. Loaded as ``(:Term {name: short})-[:MAPS_TO]->(:Term {name: long})``.
* an OBO-JSON with an ``edges`` array, for sources we take hierarchy from. Most
  ship one; MeSH does not, so we synthesize it.

Everything is keyed by *normalized name*, never by ontology id -- see ``norm``.
That is why a term shared by two ontologies collapses into a single node.
"""

from __future__ import annotations

import csv
import gzip
import json
import logging
import re
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING, Any, BinaryIO

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from ontology.sources import Source

logger = logging.getLogger(__name__)

#: Synonym predicates we accept from OBO sources. Narrow predicates only:
#: hasBroadSynonym ("cancer" for "breast cancer") and hasNarrowSynonym would
#: both make query expansion change what the user asked for.
_GOOD_PREDS = frozenset({"hasExactSynonym", "hasRelatedSynonym"})

#: Synonyms this short are almost always initialisms that collide with
#: unrelated terms. HGNC is the exception -- gene symbols are legitimately
#: 2 characters (e.g. AR, MB) -- so it gets its own floor.
_MIN_SYNONYM_LEN = 2
_MIN_SYNONYM_LEN_HGNC = 1


def norm(x: str) -> str:
    """
    Normalize a term to its graph key: lowercase, punctuation folded to spaces.

    ``ATAC-seq`` -> ``atac seq``. Every writer and every reader must apply this
    identically or lookups miss. In Cypher, normalize the *parameter* side --
    never ``toLower(t.name)``, which cannot use the index.
    """
    x = x.strip().lower()
    x = re.sub(r"[-_/]", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()


class _PairSet:
    """Collects (synonym, canonical) pairs, applying the per-source filters."""

    def __init__(self, source: Source) -> None:
        self._pairs: set[tuple[str, str]] = set()
        self._generic = source.generic_terms
        self._floor = (
            _MIN_SYNONYM_LEN_HGNC if source.parser == "hgnc" else _MIN_SYNONYM_LEN
        )

    def add(self, short: str, long: str) -> None:
        """Record one synonym pair, unless it is trivial or too generic."""
        short, long = norm(short), norm(long)
        if not short or short == long:
            return
        if short in self._generic or len(short) <= self._floor:
            return
        self._pairs.add((short, long))

    def write(self, path: Path) -> int:
        """Write the pairs to ``path`` as a ``short,long`` CSV. Returns the count."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["short", "long"])
            w.writerows(sorted(self._pairs))
        return len(self._pairs)


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------


def download(source: Source, root: Path, *, workers: int, force: bool) -> Path:
    """
    Fetch a source into ``raw/``, skipping it if the file is already there.

    These are 30 MB - 700 MB files from publishers that rate-limit; re-download
    only with ``force``, or by deleting the file.
    """
    # Imported lazily: helpers pulls in psycopg/socks, and the parsing half of
    # this module has no reason to need a database driver.
    from helpers import download_file

    dest = source.raw_path(root)
    if dest.exists() and not force:
        logger.info("%s: already downloaded (%s)", source.name, dest)
        return dest

    logger.info("%s: downloading %s", source.name, source.url)
    return download_file(
        source.url,
        dest,
        chunk_size=1 << 20,
        num_workers=workers,
        num_retries=5,
        timeout=60,
        max_wait=120,
        with_pbar=True,
        # Publishers serve these gzip-encoded, so the on-disk size legitimately
        # differs from Content-Length.
        validate_content_length=False,
    )


# ---------------------------------------------------------------------------
# parsers -- one per Source.parser kind
# ---------------------------------------------------------------------------


def _load_obo_graph(path: Path) -> dict[str, Any]:
    """Read an OBO-JSON file and return its single graph object."""
    # ponytail: whole-file json.load. ChEBI peaks around 6 GB of RAM; fine on the
    # ingest box and this runs about once a year. Switch to ijson if that stops
    # being true.
    with path.open() as f:
        return json.load(f)["graphs"][0]


def _obo_nodes(graph: dict[str, Any], source: Source) -> Iterator[dict[str, Any]]:
    """Yield the live, labelled, in-scope nodes of an OBO graph."""
    for node in graph["nodes"]:
        if not node.get("lbl"):
            continue
        if node.get("meta", {}).get("deprecated", False):
            continue
        if source.id_prefixes and not node.get("id", "").startswith(source.id_prefixes):
            continue
        yield node


def _extract_obo(source: Source, root: Path) -> int:
    """Pull synonyms out of an OBO-JSON release (MONDO, CL, Uberon, EFO, ChEBI)."""
    graph = _load_obo_graph(source.raw_path(root))
    pairs = _PairSet(source)
    for node in _obo_nodes(graph, source):
        canonical = node["lbl"]
        for syn in node.get("meta", {}).get("synonyms", []):
            if syn.get("pred") in _GOOD_PREDS and syn.get("val"):
                pairs.add(syn["val"], canonical)
    return pairs.write(source.csv_path(root))


def _extract_hgnc(source: Source, root: Path) -> int:
    """
    Pull gene aliases out of the HGNC complete set.

    The approved symbol is canonical; the full name, previous symbols and every
    alias map onto it, so a query using a retired symbol still hits.
    """
    with source.raw_path(root).open() as f:
        docs = json.load(f)["response"]["docs"]

    pairs = _PairSet(source)
    for doc in docs:
        symbol = doc.get("symbol")
        if not symbol:
            continue
        if doc.get("name"):
            pairs.add(doc["name"], symbol)
        for key in ("alias_symbol", "prev_symbol", "prev_name", "alias_name"):
            for alias in doc.get(key, []):
                pairs.add(alias, symbol)
    return pairs.write(source.csv_path(root))


def _extract_cellosaurus(source: Source, root: Path) -> int:
    """
    Pull cell-line name variants out of the Cellosaurus XML.

    The name tagged ``type="identifier"`` is canonical; the rest of the
    name-list are the spellings seen in the literature.
    """
    pairs = _PairSet(source)
    # iterparse + clear(): the dump is ~700 MB and a full DOM is several GB.
    with source.raw_path(root).open("rb") as fh:
        for _event, cell_line in ET.iterparse(fh, events=("end",)):  # noqa: S314
            if cell_line.tag != "cell-line":
                continue
            names = cell_line.find("name-list")
            if names is None:
                cell_line.clear()
                continue

            canonical = next(
                (
                    n.text
                    for n in names.findall("name")
                    if n.attrib.get("type") == "identifier" and n.text
                ),
                None,
            )
            if canonical:
                for name in names.findall("name"):
                    if name.text:
                        pairs.add(name.text, canonical)
            cell_line.clear()
    return pairs.write(source.csv_path(root))


def _open_maybe_gz(path: Path) -> BinaryIO:
    """Open a file for binary reading, transparently decompressing ``.gz``."""
    return gzip.open(path, "rb") if path.suffix == ".gz" else path.open("rb")


def _extract_mesh(source: Source, root: Path) -> int:
    """
    Build both MeSH artifacts: the synonym CSV and a synthesized OBO-JSON.

    MeSH ships no OBO release, but it is a poly-hierarchy encoded in *tree
    numbers*: a descriptor owns one or more strings like ``C04.557.470``, each
    tree number belongs to exactly one descriptor, and a tree number's parent is
    that string minus its last ``.NNN`` segment. So we resolve
    parent-tree -> parent-descriptor and emit ``child is_a parent``.

    Top-level tree numbers (no dot) hang off a single-letter category that is
    not itself a descriptor, so they come out as roots with no edge.
    """
    pairs = _PairSet(source)
    ui_to_name: dict[str, str] = {}
    ui_to_trees: dict[str, list[str]] = {}
    tree_to_ui: dict[str, str] = {}

    # iterparse: the descriptor file is ~340 MB uncompressed.
    with _open_maybe_gz(source.raw_path(root)) as fh:
        for _event, rec in ET.iterparse(fh, events=("end",)):  # noqa: S314
            if rec.tag != "DescriptorRecord":
                continue
            ui_el = rec.find("DescriptorUI")
            name_el = rec.find("DescriptorName/String")
            if (
                ui_el is None
                or ui_el.text is None
                or name_el is None
                or not name_el.text
            ):
                rec.clear()
                continue

            ui = ui_el.text
            ui_to_name[ui] = norm(name_el.text)
            for term in rec.findall(".//Concept/TermList/Term/String"):
                if term.text:
                    pairs.add(term.text, name_el.text)

            trees = [
                t.text for t in rec.findall(".//TreeNumberList/TreeNumber") if t.text
            ]
            ui_to_trees[ui] = trees
            for t in trees:
                tree_to_ui[t] = ui
            rec.clear()

    edges = {
        (ui, parent)
        for ui, trees in ui_to_trees.items()
        for t in trees
        if (parent := tree_to_ui.get(t.rpartition(".")[0])) and parent != ui
    }

    obo_path = root / "derived" / (source.derived_obo or "mesh.json")
    obo_path.parent.mkdir(parents=True, exist_ok=True)
    with obo_path.open("w") as f:
        json.dump(
            {
                "graphs": [
                    {
                        "nodes": [
                            {"id": ui, "lbl": name} for ui, name in ui_to_name.items()
                        ],
                        "edges": [
                            {"sub": c, "pred": "is_a", "obj": p}
                            for c, p in sorted(edges)
                        ],
                    },
                ],
            },
            f,
        )
    logger.info(
        "%s: %d descriptors, %d is_a edges -> %s",
        source.name,
        len(ui_to_name),
        len(edges),
        obo_path,
    )
    return pairs.write(source.csv_path(root))


_PARSERS = {
    "obo": _extract_obo,
    "hgnc": _extract_hgnc,
    "cellosaurus": _extract_cellosaurus,
    "mesh": _extract_mesh,
}


def extract(source: Source, root: Path) -> int:
    """Run the right parser for ``source``. Returns the number of synonym pairs."""
    count = _PARSERS[source.parser](source, root)
    logger.info("%s: %d synonym pairs -> %s", source.name, count, source.csv_path(root))
    return count
