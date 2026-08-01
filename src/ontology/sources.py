"""
The ontology registry: what to download, from where, in what format.

This module is data, not logic. It is the one place to change when an ontology
is added, dropped, or its download URL moves. Everything else in this package
iterates over ``SOURCES``.

Layout of the artifacts, all under ``<output-dir>/ontology/``::

    raw/         the files downloaded verbatim from the publishers
    expansions/  <name>_term_expansions.csv  (columns: short,long)
    derived/     OBO-JSON we synthesize ourselves (MeSH only)

Build order matters and is the order of ``SOURCES``. MeSH is deliberately last
-- see the note on ``Source.label`` below.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pathlib import Path

# Parser kinds. Each maps to a function in extract.py.
#   obo         -- OBO Foundry JSON: {"graphs": [{"nodes": [...], "edges": [...]}]}
#                  Nodes carry `lbl` and `meta.synonyms`; edges carry `pred`.
#                  This is the shape MONDO, CL, Uberon, EFO and ChEBI all ship.
#   hgnc        -- the HGNC "complete set" JSON, a Solr response envelope.
#   cellosaurus -- the Cellosaurus XML dump.
#   mesh        -- the NLM MeSH descriptor XML. Has no OBO-JSON release, so we
#                  synthesize one from its tree numbers.
ParserKind = Literal["obo", "hgnc", "cellosaurus", "mesh"]


@dataclass(frozen=True)
class Source:
    """One ontology and everything needed to turn it into graph artifacts."""

    #: Short key. Names the CSV (``<name>_term_expansions.csv``) and the CLI arg.
    name: str

    #: Human-readable name, for logs and docs.
    title: str

    #: Direct download. Verified reachable 2026-08-02; all are stable, versioned
    #: publisher endpoints that always serve the current release.
    url: str

    #: Filename under ``raw/``. Kept identical to what the publisher calls it so
    #: a file dropped in by hand is picked up without renaming.
    filename: str

    #: Which parser in extract.py handles this file.
    parser: ParserKind

    #: Whether we ingest this ontology's parent/child edges. HGNC (a gene
    #: symbol registry) and Cellosaurus (a cell-line catalogue) have no
    #: hierarchy worth expanding, so they contribute synonyms only.
    hierarchy: bool

    #: Synonyms equal to one of these are dropped. They are true synonyms in the
    #: source but so generic that expanding a query with them matches most of
    #: the corpus. The list is per-ontology because "cell" is noise in CL and
    #: meaningful nowhere else.
    generic_terms: frozenset[str] = frozenset()

    #: OBO parsers only. When set, keep only nodes whose id starts with one of
    #: these. EFO and ChEBI ship their imports (large chunks of Uberon, CL,
    #: MONDO, ...) inside the same file; without this filter we would ingest
    #: those terms twice and attribute them to the wrong ontology.
    id_prefixes: tuple[str, ...] = ()

    #: Optional extra Neo4j label, applied ON CREATE only. Nodes and edges that
    #: this source is the *first* to create get the label (and ``source=<label>``
    #: on the relationship); anything already in the graph is left alone. That
    #: makes the label mean "this term exists only because of <source>", which
    #: is exactly what a frontend on/off toggle needs.
    #:
    #: Only MeSH is tagged today -- it was added last, on purpose, so its tag
    #: is meaningful. If you ever want a toggle for another ontology, set its
    #: label here AND move it to the end of the build order, otherwise it will
    #: claim nodes that a later ontology would have created anyway.
    label: str | None = None

    #: Longer note rendered into the docs and the CLI listing.
    notes: str = ""

    #: Extra files this source produces beyond the synonym CSV.
    derived_obo: str | None = field(default=None)

    def raw_path(self, root: Path) -> Path:
        """Where the downloaded file lives."""
        return root / "raw" / self.filename

    def csv_path(self, root: Path) -> Path:
        """Where the extracted ``short,long`` synonym CSV lives."""
        return root / "expansions" / f"{self.name}_term_expansions.csv"

    def hierarchy_path(self, root: Path) -> Path | None:
        """
        Locate the OBO-JSON whose ``edges`` array feeds the hierarchy load.

        For most sources this is the raw download. MeSH has no OBO release, so
        extract.py writes one into ``derived/`` and we read that instead.
        """
        if not self.hierarchy:
            return None
        if self.derived_obo:
            return root / "derived" / self.derived_obo
        return self.raw_path(root)


# ---------------------------------------------------------------------------
# The registry. Order is build order.
# ---------------------------------------------------------------------------

SOURCES: tuple[Source, ...] = (
    Source(
        name="mondo",
        title="MONDO Disease Ontology",
        url="http://purl.obolibrary.org/obo/mondo.json",
        filename="mondo.json",
        parser="obo",
        hierarchy=True,
        generic_terms=frozenset(
            {"disease", "disorder", "syndrome", "condition", "abnormality"},
        ),
        notes=(
            "Disease vocabulary. Merges OMIM, Orphanet, DOID and NCIt, so it "
            "carries the clinical spellings submitters actually use."
        ),
    ),
    Source(
        name="cl",
        title="Cell Ontology",
        url="http://purl.obolibrary.org/obo/cl.json",
        filename="cl.json",
        parser="obo",
        hierarchy=True,
        generic_terms=frozenset(
            {"cell", "cells", "cell type", "cell line", "part", "region"},
        ),
        notes="Cell types. The hierarchy is what makes 'T cell' expand to its subsets.",
    ),
    Source(
        name="uberon",
        title="Uberon Anatomy Ontology",
        url="http://purl.obolibrary.org/obo/uberon/uberon-full.json",
        filename="uberon-full.json",
        parser="obo",
        hierarchy=True,
        generic_terms=frozenset(
            {"tissue", "organ", "cell", "anatomical structure", "part", "region"},
        ),
        notes=(
            "Anatomy. Use the '-full' release, not plain uberon.json: only the "
            "full one keeps the part_of (BFO_0000050) edges, and anatomy is "
            "mostly part-of rather than is-a."
        ),
    ),
    Source(
        name="efo",
        title="Experimental Factor Ontology",
        url="https://github.com/EBISPOT/efo/releases/latest/download/efo.json",
        filename="efo.json",
        parser="obo",
        hierarchy=True,
        generic_terms=frozenset(
            {
                "disease",
                "disorder",
                "condition",
                "cell",
                "cell line",
                "tissue",
                "organ",
                "sample",
                "assay",
            },
        ),
        id_prefixes=("http://www.ebi.ac.uk/efo/EFO_", "EFO:"),
        notes=(
            "Assays and experimental variables -- this is where 'ATAC-seq' and "
            "'scRNA-seq' come from. EFO bundles its imports, hence id_prefixes."
        ),
    ),
    Source(
        name="chebi",
        title="ChEBI Chemical Entities",
        url="http://purl.obolibrary.org/obo/chebi.json",
        filename="chebi.json",
        parser="obo",
        hierarchy=True,
        generic_terms=frozenset(
            {
                "compound",
                "chemical",
                "drug",
                "metabolite",
                "molecule",
                "agent",
                "solution",
            },
        ),
        id_prefixes=("http://purl.obolibrary.org/obo/CHEBI_", "CHEBI:"),
        notes=(
            "Drugs and small molecules, for treatment/perturbation queries. The "
            "largest download by far (~500 MB of JSON)."
        ),
    ),
    Source(
        name="hgnc",
        title="HGNC Gene Symbols",
        url=(
            "https://storage.googleapis.com/public-download-files/hgnc/json/json/"
            "hgnc_complete_set.json"
        ),
        filename="hgnc_complete_set.json",
        parser="hgnc",
        hierarchy=False,
        generic_terms=frozenset({"gene", "protein", "rna", "dna"}),
        notes=(
            "Approved human gene symbols plus their old symbols and full names, "
            "so a query for a retired symbol still finds the study. A registry, "
            "not an ontology -- no hierarchy."
        ),
    ),
    Source(
        name="cellosaurus",
        title="Cellosaurus Cell Lines",
        url="https://ftp.expasy.org/databases/cellosaurus/cellosaurus.xml",
        filename="cellosaurus.xml",
        parser="cellosaurus",
        hierarchy=False,
        generic_terms=frozenset({"cell", "cells", "cell line"}),
        notes=(
            "Cell-line names and their many spellings (MCF-7 / MCF7 / MCF 7). "
            "A flat catalogue, so synonyms only. ~700 MB of XML; parsed "
            "streaming."
        ),
    ),
    # MeSH is LAST on purpose. Its `label` tags whatever it creates first, so it
    # only means "MeSH-only term" if every other ontology has already been
    # loaded. Moving it earlier silently changes what the frontend toggle hides.
    Source(
        name="mesh",
        title="MeSH (NLM Medical Subject Headings)",
        # The year in the filename changes every November. Bump both the URL and
        # `filename` when NLM publishes the new edition; the old one keeps
        # working until then.
        url="https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc2026.gz",
        filename="desc2026.gz",
        parser="mesh",
        hierarchy=True,
        derived_obo="mesh.json",
        notes=(
            "Descriptors only. Supplementary concept records (chemicals) and "
            "qualifiers are deliberately skipped: they overlap ChEBI and would "
            "add ~250k mostly-chemical nodes for little search benefit."
        ),
        label="MeSH",
    ),
)

BY_NAME: dict[str, Source] = {s.name: s for s in SOURCES}
