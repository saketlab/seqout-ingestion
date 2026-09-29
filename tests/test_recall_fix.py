"""The classification rules in recipes/recall_fix/build.py.

Offline. The two things worth guarding are the ones that were wrong on the first
attempt: treating a preflightx abstention as a bulk verdict, and letting the text
regex drift out of step with sc_enrich's.
"""

import importlib.util
import pathlib
import re

import pytest

_BUILD = (
    pathlib.Path(__file__).resolve().parent.parent
    / "recipes"
    / "recall_fix"
    / "build.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("recall_fix_build", _BUILD)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover - psycopg absent in some envs
        pytest.skip(f"recall_fix build not importable: {exc}")
    return mod


def _row(**kw):
    base = {
        "matrix_says_sc": False,
        "sample_assay_sc": False,
        "n_runs_sc": 0,
        "n_runs_bulk": 0,
        "n_runs_atac": 0,
        "n_counted": 0,
    }
    return {**base, **kw}


def test_counted_matrix_is_single_cell():
    assert _load()._classify(_row(matrix_says_sc=True, n_counted=4)) == (
        1,
        "measured_single_cell",
    )


def test_positive_beats_a_bulk_call_on_another_run():
    # bulk_vs_sc and assay_is_single_cell are independent calls off different
    # evidence; one run reading bulk does not unmake a single-cell study.
    assert _load()._classify(_row(n_runs_sc=2, n_runs_bulk=9))[0] == 1


def test_abstention_alone_is_not_a_negative():
    # 'bulk_or_5prime_broken' and 'uncertain' never reach n_runs_bulk, so a study
    # whose every run abstained must come back unscreened, not 0. This is the
    # bug that manufactured 1.19M false negatives in the first place.
    assert _load()._classify(_row()) == (None, "not_screened")


def test_explicit_bulk_verdict_is_a_negative():
    assert _load()._classify(_row(n_runs_bulk=3)) == (0, "measured_not_single_cell")


def test_atac_counts_as_single_cell():
    # scATAC trips assay_is_single_cell=FALSE because that call is a barcode
    # whitelist hit against RNA chemistries.
    assert _load()._classify(_row(n_runs_atac=5, n_runs_bulk=5))[0] == 1


def test_text_regex_matches_what_sc_enrich_matches():
    from sc_enrich.classify import SC_PATTERNS

    mod = _load()
    ours = re.compile(mod.SC_TEXT_REGEX, re.IGNORECASE)
    shared = [
        "we performed scRNA-seq on liver",
        "single-cell RNA sequencing (scRNA-seq)",
        "snRNA-seq of frozen nuclei",
        "10x Genomics Chromium",
        "Drop-seq profiling",
        "CITE-seq surface markers",
    ]
    for text in shared:
        assert SC_PATTERNS.search(text), text
        assert ours.search(text), text
    for text in ["bulk RNA-seq of whole liver", "ChIP-seq for H3K27ac"]:
        assert not SC_PATTERNS.search(text), text
        assert not ours.search(text), text


def test_text_regex_covers_the_vocabulary_gap():
    # GSE158723 read FALSE because it says "droplet-based" and never
    # "scRNA-seq".
    mod = _load()
    ours = re.compile(mod.SC_TEXT_REGEX, re.IGNORECASE)
    assert ours.search(
        "profiling the transcriptome of more than 25,000 individual liver cells "
        "using droplet-based RNA-sequencing"
    )
