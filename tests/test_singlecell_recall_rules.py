"""The extra single-cell evidence rules in sc_enrich.recall_rules. Offline.

Fixtures are protocol strings copied from real records (accession noted), so a
regex change that loses a real positive or re-admits a known false positive fails.
"""

import pytest

from sc_enrich import recall_rules as rr

PROTOCOL_POSITIVE = [
    # GSE152183 GSM Extract-Protocol
    "10X Genomics 3' Version 2 Single Cell Kit",
    # GSE137444
    "Flow Cytometry or MACS.\n10X Genomics V2 chemistry.",
    # GSE167272
    "Gene expression libraries were prepared using Chromium Single Cell 3′ V3 Reagent Kits",  # noqa: RUF001
    # GSE278281
    "Chromium Next GEM Single Cell 5’ reagent kits v2 (dual index) with featured barcode technology",  # noqa: RUF001
    # GSE197981: 10x Genomics only next to single-cell wording
    "RNA libraries were prepared for sequencing using standard 10X genomics protocols\nsingle-cell RNA-seq",
    # GSE249330
    "Cells were captured with the BD Rhapsody Single-Cell Analysis System",
    # SRP470208
    "Raw counts data was imported into R for downstream processing. using Cellranger count.",
    # SRP555845 library title
    "scARC library, group 70305, 6hr snATAC-seq FASTQs",
    # plate chemistry with single-cell wording in the same record
    "Single cells were sorted into 384-well plates and processed with MARS-seq",
]

PROTOCOL_NEGATIVE = [
    # 10x linked-read genome (audit FP class: PRJNA383593-style)
    "10x Genomics Chromium Genome linked-read library, de novo assembly with Supernova",
    # buffers and magnification are not platforms
    "Cells were washed in 10x PBS and imaged at 10x magnification",
    # chromium the metal
    "Soil samples contaminated with chromium were extracted for RNA",
    # Nanodrop is not Drop-seq
    "RNA quality was checked by Nanodrop sequencing on NovaSeq",
    # GeoMx ROI (audit FP class spatial_roi)
    "GeoMx Digital Spatial Profiler whole transcriptome atlas, single cell resolution not claimed",
    # bulk library in the same record
    "Bulk RNA-seq libraries of sorted cells; 10x Genomics single cell data in a companion series",
    # plain Smart-seq2 (low-input bulk is common) is not a platform on its own
    "Libraries were prepared with Smart-seq2 from 500 sorted cells",
    # plate chemistries used as bulk preps (GSE247422, GSE288746)
    "RNA libraries for RNA seq was prepared using MARS-seq inhouse protocol",
    "Liver and white adipose tissue RNA-seq libraries were made with SmartSeq3",
]


@pytest.mark.parametrize("text", PROTOCOL_POSITIVE)
def test_protocol_positive(text):
    assert rr.protocol_says_single_cell(text)


@pytest.mark.parametrize("text", PROTOCOL_NEGATIVE)
def test_protocol_negative(text):
    assert not rr.protocol_says_single_cell(text)


def test_libsource_vetoes():
    assert (
        rr.libsource_vetoed(
            "Prochlorococcus marinus", "SCGC single cell genome", ["WGS"]
        )
        == "microbial_organism"
    )
    assert (
        rr.libsource_vetoed(
            "Homo sapiens", "JGI single cell genome sequencing", ["WGS"]
        )
        == "genome_project_title"
    )
    assert (
        rr.libsource_vetoed("Homo sapiens", "H3K27ac profiling", ["ChIP-Seq"])
        == "non_rna_strategies_only"
    )
    assert (
        rr.libsource_vetoed("Homo sapiens", "PBMC atlas", ["RNA-Seq", "AMPLICON"])
        is None
    )


def test_whitelist_miss_is_not_a_bulk_verdict():
    # SRP345341: 4/4 libraries declared single-cell, only whitelist misses
    assert rr.declared_overrules_whitelist_miss(
        4, 4, n_runs_bulk_r1len=0, n_runs_bulk_any=4, n_runs_sc=0
    )
    # an R1-length bulk call is real read structure and stands (SRP336897)
    assert not rr.declared_overrules_whitelist_miss(
        128, 128, n_runs_bulk_r1len=95, n_runs_bulk_any=95, n_runs_sc=0
    )
    # a minority declaration does not overrule (SRP291341, 2/20)
    assert not rr.declared_overrules_whitelist_miss(
        2, 20, n_runs_bulk_r1len=0, n_runs_bulk_any=20, n_runs_sc=0
    )


def test_merge_status():
    assert (
        rr.merge_status("measured_single_cell", "measured_not_single_cell")
        == "measured_single_cell"
    )
    assert rr.merge_status(
        "measured_not_single_cell", "text_indicates_single_cell"
    ) == ("measured_not_single_cell_text_disagrees")
    assert rr.merge_status("not_screened", rr.PROTOCOL_STATUS) == rr.PROTOCOL_STATUS


def _r(acc, status, modality=None):
    return (acc, rr.IS_SC[status], status, modality, None, None, None, 0, 0, 0)


def test_canonicalise_moves_alias_keyed_rows():
    staged = {
        "SRP1": _r("SRP1", "measured_single_cell", "scRNA-seq"),
        "GSE1": _r("GSE1", "text_indicates_single_cell"),
    }
    out, moved = rr.canonicalise(staged, {"SRP1": "GSE1", "GSE1": "GSE1"}.get)
    assert moved == 1
    assert set(out) == {"GSE1"}
    assert out["GSE1"][2] == "measured_single_cell"
    assert out["GSE1"][3] == "scRNA-seq"


def test_text_promotion_vetoes():
    # development-sample false positives restored by the abstention fix
    assert rr.text_promotion_vetoed(
        "GRNsmile heat shock stress [PRO-seq]", ""
    )  # SRP316802
    assert rr.text_promotion_vetoed(
        "Limb gene transcription [ChIP-seq]", ""
    )  # SRP496654
    assert rr.text_promotion_vetoed(
        "Starling genome", "The 10x reads were assembled using SUPERNOVA"
    )
    assert rr.text_promotion_vetoed(
        "Adm lungs", "Bulk RNA sequencing of mice lungs, integrated with scRNA-seq"
    )
    # a single-cell tag is not a veto
    assert not rr.text_promotion_vetoed("Kidney atlas [scRNA-seq]", "")
    assert not rr.text_promotion_vetoed("Islets [single-cell RNA-seq]", "")


def test_vetoed_text_is_not_restored_but_new_rows_keep_the_old_rule():
    staged = {"A": _r("A", "not_screened")}
    out, added = rr.apply_text(staged, {"A": None, "B": None}, vetoed={"A", "B"})
    assert out["A"][2] == "not_screened"
    assert out["B"][2] == "text_indicates_single_cell"
    assert added == 1


def test_text_verdict_survives_an_all_abstain_read_pass():
    # SRP333476: pentimento staged it not_screened, title matches SC_TEXT_REGEX
    out, added = rr.apply_text(
        {"SRP333476": _r("SRP333476", "not_screened")}, {"SRP333476": None}
    )
    assert added == 1
    assert out["SRP333476"][:3] == ("SRP333476", 1, "text_indicates_single_cell")


def test_additive_never_overrides_a_verdict():
    staged = {"A": _r("A", "measured_not_single_cell"), "B": _r("B", "not_screened")}
    out, added = rr.apply_additive(staged, ["A", "B", "C"], rr.PROTOCOL_STATUS)
    assert added == 2
    assert out["A"][2] == "measured_not_single_cell"
    assert out["B"][2] == out["C"][2] == rr.PROTOCOL_STATUS
    assert all(len(r) == 10 for r in out.values())


def test_a_canonical_resolves_to_itself():
    # SRP twin canonical listed as an alias of its GSE: keep its own verdict
    amap = {"SRP9": "GSE9", "GSE9": "GSE9", "SRP8": "GSE8"}
    canon = {"GSE9", "SRP9", "GSE8"}
    assert rr.resolve_canonical("SRP9", amap, {}, canon) == "SRP9"
    assert rr.resolve_canonical("SRP8", amap, {}, canon) == "GSE8"
    assert rr.resolve_canonical("PRJEB1", amap, {"PRJEB1": "SRP8"}, canon) == "GSE8"
    assert rr.resolve_canonical("PRJEB2", amap, {}, canon) is None
