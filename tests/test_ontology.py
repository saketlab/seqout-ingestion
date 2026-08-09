"""Offline checks for the ontology extractors. No network, no Neo4j."""

import csv
import dataclasses
import gzip
import json
from pathlib import Path

import pytest

from ontology.extract import extract, norm
from ontology.graph import _checked_label, _tag_node, _tag_rel
from ontology.sources import BY_NAME, SOURCES, Source


def read_csv(path):
    with path.open() as f:
        reader = csv.reader(f)
        next(reader)
        return {(r[0], r[1]) for r in reader}


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
    assert names == {"D001": "neoplasms", "D002": "breast neoplasms"}
    # C04.588 -> parent tree C04 -> D001. C04 is top-level, so it gets no edge.
    assert graph["edges"] == [{"sub": "D002", "pred": "is_a", "obj": "D001"}]


def test_mesh_reads_plain_xml_too(tmp_path):
    src = dataclasses.replace(BY_NAME["mesh"], filename="desc.xml")
    assert ("cancer", "neoplasms") in run(src, tmp_path, MESH_XML)


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
