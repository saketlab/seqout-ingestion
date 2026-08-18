"""Offline checks for the ontology extractors. No network, no Neo4j."""

import csv
import dataclasses
import gzip
import json
import re
from pathlib import Path

import pytest

from ontology.extract import curie, extract, norm
from ontology.graph import _append_xref, _checked_label, _tag_node, _tag_rel
from ontology.sources import BY_NAME, SOURCES, Source

#: Same shape graph.py's ``bad_xrefs`` stat asserts on. CVCL_ is Cellosaurus,
#: which issues no PREFIX:local id and is the one documented exception.
_VALID_XREF = re.compile(r"[A-Za-z][A-Za-z0-9]*:[A-Za-z0-9.-]+|CVCL_[A-Za-z0-9]+")


def read_csv(path):
    """The (synonym, canonical) pairs, ignoring provenance."""
    with path.open() as f:
        reader = csv.reader(f)
        next(reader)
        return {(r[0], r[1]) for r in reader}


def read_csv_ids(path):
    """The full (synonym, canonical, id) triples."""
    with path.open() as f:
        reader = csv.reader(f)
        assert next(reader) == ["short", "long", "id"]
        return {tuple(r) for r in reader}


def run(source: Source, tmp_path, raw_bytes: bytes):
    raw = source.raw_path(tmp_path)
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(raw_bytes)
    extract(source, tmp_path)
    return read_csv(source.csv_path(tmp_path))


def obo(nodes, edges=()):
    return json.dumps({"graphs": [{"nodes": nodes, "edges": list(edges)}]}).encode()


def test_norm_folds_punctuation_and_case():
    assert norm("  ATAC-seq ") == "atac seq"
    assert norm("CD4/CD8_positive") == "cd4 cd8 positive"
    assert norm("T  cell") == "t cell"


def test_obo_synonym_filters(tmp_path):
    src = BY_NAME["cl"]
    pairs = run(
        src,
        tmp_path,
        obo(
            [
                {
                    "id": "CL:1",
                    "lbl": "T cell",
                    "meta": {
                        "synonyms": [
                            {"pred": "hasExactSynonym", "val": "T-lymphocyte"},
                            # broad synonyms change what the user asked for
                            {"pred": "hasBroadSynonym", "val": "leukocyte"},
                            # generic_terms entry, dropped
                            {"pred": "hasExactSynonym", "val": "cell"},
                            # <= 2 chars, dropped
                            {"pred": "hasExactSynonym", "val": "TC"},
                        ],
                    },
                },
                # deprecated nodes never enter the graph
                {
                    "id": "CL:2",
                    "lbl": "old cell",
                    "meta": {
                        "deprecated": True,
                        "synonyms": [
                            {"pred": "hasExactSynonym", "val": "obsolete thing"}
                        ],
                    },
                },
                {"id": "CL:3", "meta": {}},  # unlabelled
            ],
        ),
    )
    assert pairs == {("t lymphocyte", "t cell")}


def test_obo_id_prefixes_drop_imported_terms(tmp_path):
    """EFO ships its imports; only EFO-native ids may contribute synonyms."""
    src = BY_NAME["efo"]
    nodes = [
        {
            "id": "http://www.ebi.ac.uk/efo/EFO_1",
            "lbl": "ATAC-seq",
            "meta": {
                "synonyms": [{"pred": "hasExactSynonym", "val": "ATAC sequencing"}]
            },
        },
        {
            "id": "http://purl.obolibrary.org/obo/UBERON_1",
            "lbl": "liver",
            "meta": {"synonyms": [{"pred": "hasExactSynonym", "val": "hepatic organ"}]},
        },
    ]
    assert run(src, tmp_path, obo(nodes)) == {("atac sequencing", "atac seq")}


def test_hgnc_keeps_two_character_symbols(tmp_path):
    """Gene symbols really are that short; the generic 2-char floor would eat them."""
    src = BY_NAME["hgnc"]
    payload = {
        "response": {
            "docs": [
                {
                    "symbol": "AR",
                    "name": "androgen receptor",
                    "prev_symbol": ["DHTR"],
                    "alias_name": ["NR3C4"],
                },
                {"name": "no symbol here"},
            ],
        },
    }
    pairs = run(src, tmp_path, json.dumps(payload).encode())
    assert ("androgen receptor", "ar") in pairs
    assert ("dhtr", "ar") in pairs
    assert ("nr3c4", "ar") in pairs


def test_cellosaurus_uses_the_identifier_name_as_canonical(tmp_path):
    src = BY_NAME["cellosaurus"]
    xml = b"""<Cellosaurus><cell-line-list>
      <cell-line><name-list>
        <name type="identifier">MCF-7</name>
        <name type="synonym">MCF7</name>
        <name type="synonym">Michigan Cancer Foundation-7</name>
      </name-list></cell-line>
      <cell-line><name-list><name type="synonym">orphan</name></name-list></cell-line>
    </cell-line-list></Cellosaurus>"""
    pairs = run(src, tmp_path, xml)
    # "MCF7" -> "mcf7" but "MCF-7" -> "mcf 7": norm() folds hyphens to spaces and
    # does not remove them, so the two spellings stay distinct nodes joined by
    # MAPS_TO. That edge is the whole point -- it is what lets a search for
    # "MCF7" find a study that only ever wrote "MCF-7".
    assert pairs == {
        ("mcf7", "mcf 7"),
        ("michigan cancer foundation 7", "mcf 7"),
    }
    # the second cell-line has no type="identifier" name, so it contributes nothing
    assert not any("orphan" in p for p in pairs)


MESH_XML = b"""<DescriptorRecordSet>
  <DescriptorRecord>
    <DescriptorUI>D001</DescriptorUI>
    <DescriptorName><String>Neoplasms</String></DescriptorName>
    <TreeNumberList><TreeNumber>C04</TreeNumber></TreeNumberList>
    <ConceptList><Concept><TermList>
      <Term><String>Tumors</String></Term>
      <Term><String>Cancer</String></Term>
    </TermList></Concept></ConceptList>
  </DescriptorRecord>
  <DescriptorRecord>
    <DescriptorUI>D002</DescriptorUI>
    <DescriptorName><String>Breast Neoplasms</String></DescriptorName>
    <TreeNumberList><TreeNumber>C04.588</TreeNumber></TreeNumberList>
    <ConceptList><Concept><TermList>
      <Term><String>Breast Cancer</String></Term>
    </TermList></Concept></ConceptList>
  </DescriptorRecord>
</DescriptorRecordSet>"""


def test_mesh_builds_hierarchy_from_tree_numbers(tmp_path):
    src = BY_NAME["mesh"]
    pairs = run(src, tmp_path, gzip.compress(MESH_XML))
    assert ("tumors", "neoplasms") in pairs
    assert ("breast cancer", "breast neoplasms") in pairs

    graph = json.loads(src.hierarchy_path(tmp_path).read_text())["graphs"][0]
    names = {n["id"]: n["lbl"] for n in graph["nodes"]}
    # Ids carry the MeSH: prefix. The hierarchy load would not care -- it
    # resolves ids against this same file -- but it also runs curie() over them
    # to stamp xrefs, and a bare "D001" names no registry.
    assert names == {"MeSH:D001": "neoplasms", "MeSH:D002": "breast neoplasms"}
    assert all(curie(node_id) == node_id for node_id in names)
    # C04.588 -> parent tree C04 -> D001. C04 is top-level, so it gets no edge.
    assert graph["edges"] == [
        {"sub": "MeSH:D002", "pred": "is_a", "obj": "MeSH:D001"},
    ]


def test_mesh_reads_plain_xml_too(tmp_path):
    src = dataclasses.replace(BY_NAME["mesh"], filename="desc.xml")
    assert ("cancer", "neoplasms") in run(src, tmp_path, MESH_XML)


# ---------------------------------------------------------------------------
# Provenance: the xrefs list
# ---------------------------------------------------------------------------


def test_curie_normalizes_obo_purls():
    assert curie("http://purl.obolibrary.org/obo/MONDO_0005148") == "MONDO:0005148"
    assert curie("http://www.ebi.ac.uk/efo/EFO_0000001") == "EFO:0000001"
    # only the FIRST underscore becomes the colon; locals may contain more
    assert curie("http://purl.obolibrary.org/obo/CHEBI_15377") == "CHEBI:15377"
    # already a CURIE (MeSH's synthesized OBO) -- passes through unchanged
    assert curie("MeSH:D008099") == "MeSH:D008099"


def test_curie_recovers_registries_hidden_in_the_url_path():
    """The whole reason curie() is not a one-line rsplit."""
    # last path segment alone would be a bare "1014", naming no registry
    assert curie("http://identifiers.org/ncbigene/1014") == "NCBIGene:1014"
    assert curie("https://identifiers.org/hgnc/5") == "HGNC:5"
    # registry in a query parameter
    assert curie("https://www.genenames.org/tools/search?hgnc_id=10000") == "HGNC:10000"


def test_curie_rejects_everything_that_is_not_a_term_id():
    assert curie("http://purl.obolibrary.org/obo/mondo#excluded_subClassOf") == ""
    assert curie("http://purl.org/dc/elements/1.1/description") == ""
    assert curie("https://orcid.org/0000-0001-5208-3432") == ""
    assert curie("http://identifiers.org/unknownregistry/42") == ""
    assert curie("") == ""


def test_obo_synonyms_carry_their_descriptors_curie(tmp_path):
    """A synonym string has no id of its own, so it inherits the canonical's."""
    src = BY_NAME["cl"]
    run(
        src,
        tmp_path,
        obo(
            [
                {
                    "id": "http://purl.obolibrary.org/obo/CL_0000084",
                    "lbl": "T cell",
                    "meta": {
                        "synonyms": [
                            {"pred": "hasExactSynonym", "val": "T-lymphocyte"},
                            {"pred": "hasExactSynonym", "val": "T cells"},
                        ],
                    },
                },
            ],
        ),
    )
    assert read_csv_ids(src.csv_path(tmp_path)) == {
        ("t lymphocyte", "t cell", "CL:0000084"),
        ("t cells", "t cell", "CL:0000084"),
    }


def test_hgnc_uses_the_published_curie(tmp_path):
    src = BY_NAME["hgnc"]
    payload = {
        "response": {
            "docs": [
                {"symbol": "AR", "name": "androgen receptor", "hgnc_id": "HGNC:644"},
                # no hgnc_id: still a usable synonym pair, just no provenance
                {"symbol": "MB", "name": "myoglobin"},
            ],
        },
    }
    run(src, tmp_path, json.dumps(payload).encode())
    assert read_csv_ids(src.csv_path(tmp_path)) == {
        ("androgen receptor", "ar", "HGNC:644"),
        ("myoglobin", "mb", ""),
    }


def test_cellosaurus_uses_the_primary_accession(tmp_path):
    """CVCL_ ids are bare, not PREFIX:local -- the documented CURIE exception."""
    src = BY_NAME["cellosaurus"]
    xml = b"""<Cellosaurus><cell-line-list>
      <cell-line>
        <accession-list>
          <accession type="primary">CVCL_0031</accession>
          <accession type="secondary">CVCL_RETIRED</accession>
        </accession-list>
        <name-list>
          <name type="identifier">MCF-7</name>
          <name type="synonym">MCF7</name>
        </name-list>
      </cell-line>
    </cell-line-list></Cellosaurus>"""
    run(src, tmp_path, xml)
    assert read_csv_ids(src.csv_path(tmp_path)) == {("mcf7", "mcf 7", "CVCL_0031")}


def test_mesh_synonyms_carry_the_descriptor_ui(tmp_path):
    src = BY_NAME["mesh"]
    run(src, tmp_path, gzip.compress(MESH_XML))
    assert read_csv_ids(src.csv_path(tmp_path)) == {
        ("tumors", "neoplasms", "MeSH:D001"),
        ("cancer", "neoplasms", "MeSH:D001"),
        ("breast cancer", "breast neoplasms", "MeSH:D002"),
    }


def test_every_extracted_id_is_a_valid_curie(tmp_path):
    """
    The shape graph.py's ``bad_xrefs`` counter checks for, enforced offline.

    A curie() regression writes junk into xrefs rather than raising, so it is
    invisible until someone queries the graph. This is the cheap early warning.
    """
    src = BY_NAME["mondo"]
    run(
        src,
        tmp_path,
        obo(
            [
                {
                    "id": "http://purl.obolibrary.org/obo/MONDO_0005148",
                    "lbl": "type 2 diabetes mellitus",
                    "meta": {"synonyms": [{"pred": "hasExactSynonym", "val": "T2DM"}]},
                },
            ],
        ),
    )
    ids = {r[2] for r in read_csv_ids(src.csv_path(tmp_path))} - {""}
    assert ids
    assert all(_VALID_XREF.fullmatch(i) for i in ids), ids


def test_append_xref_is_guarded_against_duplicates():
    """Re-loading an unchanged artifact must not grow the list."""
    frag = _append_xref("s")
    assert "NOT row.id IN coalesce(s.xrefs, [])" in frag
    assert "row.id <> ''" in frag
    assert "SET s.xrefs = coalesce(s.xrefs, []) + row.id" in frag


def test_only_mesh_is_tagged_and_it_is_built_last():
    """The :MeSH tag only means 'MeSH-only term' if MeSH loads after everything."""
    tagged = [s.name for s in SOURCES if s.label]
    assert tagged == ["mesh"]
    assert SOURCES[-1].name == "mesh"


def test_label_injection_is_rejected():
    assert _checked_label(None) is None
    assert _checked_label("MeSH") == "MeSH"
    with pytest.raises(ValueError, match="unsafe Neo4j label"):
        _checked_label("MeSH` DETACH DELETE n //")


def test_tag_clauses_are_empty_without_a_label():
    assert _tag_node("s", None) == ""
    assert _tag_rel("r", None) == ""
    assert _tag_node("s", "MeSH") == " ON CREATE SET s:MeSH"
    assert _tag_rel("r", "MeSH") == " ON CREATE SET r.source = 'MeSH'"


def test_sources_are_internally_consistent():
    root = Path()
    for s in SOURCES:
        assert s.hierarchy == (s.hierarchy_path(root) is not None)
        assert not (s.derived_obo and not s.hierarchy)
        assert not (s.id_prefixes and s.parser != "obo")
