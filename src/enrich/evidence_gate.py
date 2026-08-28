"""
Archive-declared inputs, extraction, and the cell-line EVIDENCE gate.
"""

from __future__ import annotations

import copy
import os
import re

import pandas as pd

from enrich.archive_fields import augment as _augment
from enrich.cellosaurus import BACKPROP_FIELDS
from enrich.cellosaurus import load_lookup as _cellosaurus_lookup

WIDE_TEXT_COLS = [
    "Description",
    "description",
    "Sample Title",
    "sample_title",
    "Sample_Title",
    "SampleTitle",
    "Title",
    "title",
    "source_name",
    "source name",
    "Source Name",
    "sample_name",
    "Sample Name",
    "SampleName",
    "Library Name",
    "library_name",
]
DETAIL_COL = "sample_detail"

_NARROW_TEXT_COLS = ["Description", "Sample Title", "source_name", "source name"]


def _text_cols() -> list[str]:
    return (
        WIDE_TEXT_COLS
        if os.environ.get("ENRICH_TEXT_COLS") == "wide"
        else _NARROW_TEXT_COLS
    )


_REP_STRICT = [
    re.compile(r"(?i)[_\-. ]?reps?[_\-. ]?\d+\b"),
    re.compile(r"(?i)[_\-. ]?replicates?[_\-. ]?\d+\b"),
    re.compile(r"(?i)\b(?:lane|run|batch)[_\-. ]?\d+\b"),
]
_REP_AGGRESSIVE = _REP_STRICT + [re.compile(r"[_\-. ]\d{1,3}$")]


def _strip_replicate(s: str) -> str:
    mode = os.environ.get("ENRICH_REPLICATE", "strict")
    if mode == "off":
        return s
    for rx in _REP_AGGRESSIVE if mode == "aggressive" else _REP_STRICT:
        s = rx.sub("", s)
    return re.sub(r"\s+", " ", s).strip(" _-.")


_NULLISH = {
    "",
    "-",
    "na",
    "n/a",
    "none",
    "null",
    "nan",
    "unknown",
    "not applicable",
    "other",
    "not determined",
}
_SQUASH_MATCH_LEN = 4

# Cellosaurus aliases these to cell lines: "ko" -> K-562, "wt" -> STA-WT-1,
# "na" -> NA. Text saying "CRISPR KO" would otherwise vouch for an asserted
# K-562. Blocked as evidence only; CELL_LINE_PLACEHOLDERS handles a submitter
# who writes one as the cell line.
_AMBIGUOUS_EVIDENCE_NAMES = frozenset(
    {
        "ko",
        "wt",
        "na",
        "nt",
        "ctl",
        "ctr",
        "neg",
        "pos",
        "es",
        "ips",
        "pbs",
        "dmso",
        "wtc",
        "sc",
        "shc",
        "mut",
        "ref",
        "std",
        "tc",
        "jm",
        "nsg",
    }
)

_CELLOSAURUS_BACKPROP = BACKPROP_FIELDS

_EXTRACTED_COLS = {
    "age",
    "sex",
    "ethnicity",
    "phenotype",
    "cell_type",
    "tissue",
    "strain",
    "disease",
    "assay",
    "assay_category",
    "cell_line",
    "treatment",
    "development_stage",
    "sample_type",
    "genetic_modification",
    "organism",
    "taxid",
    "tissue_primary_site",
    "tissue_site_type",
}


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    return str(v).strip().lower() in _NULLISH


def _squash(v) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(v).lower())


def apply_wide_sample_detail(samples: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Mirror row free text so dedup cannot broadcast one label across distinct rows."""
    present = [c for c in _text_cols() if c in samples.columns]
    if not present:
        return samples, 0
    joined = (
        samples[present]
        .astype(str)
        .apply(lambda r: " | ".join(v.strip() for v in r if not _blank(v)), axis=1)
    )
    joined = joined.map(_strip_replicate)
    if joined.nunique(dropna=True) < 2:
        return samples, 0
    out = samples.copy()
    out[DETAIL_COL] = joined.replace("", None)
    return out, int(joined.ne("").sum())


def cell_line_names(value: str, lookup: dict | None = None) -> set[str]:
    """Expand synonyms so source text naming H9 can support a WA09 assignment."""
    lookup = _cellosaurus_lookup() if lookup is None else lookup
    names = {value}
    entry = lookup.get(_squash(value))
    if entry:
        if entry.get("canonical_name"):
            names.add(entry["canonical_name"])
        names.update(entry.get("synonyms") or [])
    return {s for n in names if (s := _squash(n))}


def cell_line_supported(value, evidence, lookup: dict | None = None) -> bool:
    """Treat unjudgeable rows as supported because over-refusing deletes recoverable facts."""
    if _blank(value):
        return True
    names = cell_line_names(str(value), lookup)
    if not names:
        return True
    squashed, raw = evidence if isinstance(evidence, tuple) else (evidence, "")
    for n in names:
        if len(n) >= _SQUASH_MATCH_LEN:
            if n in squashed:
                return True
        elif n in _AMBIGUOUS_EVIDENCE_NAMES:
            continue
        elif raw and re.search(rf"\b{re.escape(n)}\b", raw):
            return True
    return False


def build_evidence_text(project, samples: pd.DataFrame) -> pd.Series:
    """Study text counts because a line named once in the study can support every sample."""
    study = " ".join(
        str(getattr(project, f, "") or "")
        for f in ("title", "description", "overall_design")
    )
    study_sq, study_raw = _squash(study), study.lower()
    # ignore enriched fields because a value can otherwise support itself
    samples = samples.drop(
        columns=[c for c in samples.columns if c in _EXTRACTED_COLS], errors="ignore"
    )
    rows = samples.apply(lambda r: " ".join(str(v) for v in r.values), axis=1)
    return rows.map(lambda r: (study_sq + _squash(r), study_raw + " " + r.lower()))


def apply_evidence_gate(project, output_df, cello_df, provenance_df, ontology_df):
    """Refuse absent cell-line assertions and clear fields donated by that line."""
    frames = [f for f in (cello_df, ontology_df) if f is not None]
    if not frames or provenance_df is None:
        return output_df, cello_df, provenance_df, ontology_df

    samples = getattr(project, "samples", None)
    if samples is None or not isinstance(samples, pd.DataFrame) or samples.empty:
        return output_df, cello_df, provenance_df, ontology_df

    # gate only on range indexes because upstream frames mix label and position alignment
    if not samples.index.equals(pd.RangeIndex(len(samples))):
        return output_df, cello_df, provenance_df, ontology_df

    lookup = _cellosaurus_lookup()
    evidence = build_evidence_text(project, samples)
    ref = frames[0]
    refused = 0

    for i in ref.index:
        value = ref.loc[i, "cell_line"] if "cell_line" in ref.columns else None
        if _blank(value):
            continue
        text = evidence.get(i, ("", ""))
        if cell_line_supported(value, text, lookup):
            continue
        refused += 1
        for frame in frames:
            if "cell_line" in frame.columns:
                frame.loc[i, "cell_line"] = None
            for f in _CELLOSAURUS_BACKPROP:
                col = f"{f}_source"
                src = (
                    provenance_df.loc[i, col] if col in provenance_df.columns else None
                )
                if f in frame.columns and src == "cellosaurus":
                    frame.loc[i, f] = None
            for c in ("tissue_primary_site", "tissue_site_type"):
                if c in frame.columns:
                    frame.loc[i, c] = None
        if "cell_line_source" in provenance_df.columns:
            provenance_df.loc[i, "cell_line_source"] = "evidence_refused"
    if refused:
        provenance_df.attrs["evidence_refused"] = refused
    return output_df, cello_df, provenance_df, ontology_df


_PATCHED = False


def patch_llm_interface() -> None:
    """Disable reasoning by default and retry parse failures so one malformed answer does not abort a study."""
    global _PATCHED
    if _PATCHED:
        return
    try:
        import langchain_ollama
    except ImportError:
        _PATCHED = True
        return

    reasoning = os.environ.get("ENRICH_REASONING", "0") == "1"
    retries = int(os.environ.get("ENRICH_RETRIES", "2"))

    cls = langchain_ollama.ChatOllama
    _orig_init = cls.__init__

    def _init(self, *a, **kw):
        kw.setdefault("reasoning", reasoning)
        return _orig_init(self, *a, **kw)

    _orig_wso = cls.with_structured_output

    def _wso(self, *a, **kw):
        runnable = _orig_wso(self, *a, **kw)
        if retries <= 0:
            return runnable
        from langchain_core.exceptions import OutputParserException

        return runnable.with_retry(
            retry_if_exception_type=(
                OutputParserException,
                ValueError,
                KeyError,
                TypeError,
            ),
            stop_after_attempt=retries + 1,
        )

    cls.__init__ = _init  # type: ignore[method-assign]
    cls.with_structured_output = _wso  # type: ignore[method-assign]
    _PATCHED = True


def enrich_df(project, *args, **kwargs):
    """Archive augmentation must run before enrichment, and the evidence gate after enrichment."""
    patch_llm_interface()
    from enrich.extract import enrich_df as _extract_df

    aug = _augment(project)
    samples = getattr(aug, "samples", None)
    if isinstance(samples, pd.DataFrame) and not samples.empty:
        frame, n_detail = apply_wide_sample_detail(samples)
        aug.samples = frame
        aug.samples.attrs["sample_detail"] = n_detail

    pre = (
        aug.samples.copy()
        if isinstance(getattr(aug, "samples", None), pd.DataFrame)
        else None
    )

    out = _extract_df(aug, *args, **kwargs)
    if not (isinstance(out, tuple) and len(out) == 4):
        return out

    if os.environ.get("ENRICH_TISSUE_GUARD") == "1":
        from enrich.archive_fields import apply_guard

        out = apply_guard(*out)

    if pre is not None:
        aug = copy.copy(aug)
        aug.samples = pre
    return apply_evidence_gate(aug, *out)


def _demo() -> None:
    lookup = {
        "hela": {"canonical_name": "HeLa", "synonyms": ["HELA", "He-La"]},
        "wa09": {"canonical_name": "WA09", "synonyms": ["WA 09", "H9", "H9 ESC"]},
        "k562": {"canonical_name": "K-562", "synonyms": ["K562", "KO", "K 562"]},
        "hek293t": {"canonical_name": "HEK293T", "synonyms": ["293T", "HEK-293T"]},
    }

    ev = lambda s: (_squash(s), s.lower())
    assert (
        cell_line_supported("HeLa", ev("blood human T cell ABE NTC day3"), lookup)
        is False
    )

    assert (
        cell_line_supported("K-562", ev("K562 genomic DNA mammalian"), lookup) is True
    )

    assert cell_line_supported("WA09", ev("H9 hESC differentiation"), lookup) is True

    assert (
        cell_line_supported("WA09", (_squash("xxH9999xx"), "xxh9999xx"), lookup)
        is False
    )
    ko = ("crisprkoscreen", "crispr ko screen")
    assert cell_line_supported("K-562", ko, lookup) is True

    assert cell_line_supported(None, ("", ""), lookup) is True
    assert cell_line_supported("", ("", ""), lookup) is True

    assert cell_line_supported("HEK293T", ev("HEK-293 T cells"), lookup) is True

    df = pd.DataFrame(
        {
            "library_strategy": ["WGS", "WGS"],
            "Title": ["1ugBxb1_d3_rep1", "1ugCre_d3_rep1"],
        }
    )
    out, n = apply_wide_sample_detail(df)
    assert n == 0 and DETAIL_COL not in out.columns, out

    os.environ["ENRICH_TEXT_COLS"] = "wide"
    try:
        out, n = apply_wide_sample_detail(df)
        assert n == 2 and out[DETAIL_COL].nunique() == 2, out
        assert DETAIL_COL not in df.columns, "must copy, not alias the caller's frame"
    finally:
        os.environ.pop("ENRICH_TEXT_COLS", None)

    d2 = pd.DataFrame(
        {
            "library_strategy": ["WGS", "WGS"],
            "Description": ["Library name: SP000009", "Library name: SP000010"],
        }
    )
    out, n = apply_wide_sample_detail(d2)
    assert n == 2 and out[DETAIL_COL].nunique() == 2, out

    out, n = apply_wide_sample_detail(pd.DataFrame({"Description": ["same", "same"]}))
    assert n == 0 and DETAIL_COL not in out.columns

    once, _ = apply_wide_sample_detail(d2)
    twice, _ = apply_wide_sample_detail(once)
    assert list(once[DETAIL_COL]) == list(twice[DETAIL_COL]), (
        list(once[DETAIL_COL]),
        list(twice[DETAIL_COL]),
    )

    class _P:
        accession = "X"
        title = "A study of neurons"
        description = None
        overall_design = None

    pr = _P()
    pr.samples = pd.DataFrame({"Sample": ["S1"], "cell type": ["human T cell"]})
    assert (
        cell_line_supported("HeLa", build_evidence_text(pr, pr.samples).iloc[0])
        is False
    )
    pr.samples["cell_line"] = "HeLa"
    assert (
        cell_line_supported("HeLa", build_evidence_text(pr, pr.samples).iloc[0])
        is False
    ), "the gate must not read extract's own output as evidence"

    # placeholder names are rejected because Cellosaurus has real records for tokens like na and wt
    from enrich.cellosaurus import CELLOSAURUS_DONATED_FIELDS
    from enrich.cellosaurus import lookup as cellosaurus_lookup

    for junk in ("N/A", "NA", "n.a.", "WT", "wild type", "none", "unknown", "control"):
        assert cellosaurus_lookup(junk) is None, f"placeholder resolved: {junk!r}"
    for real, expect in (("K562", "K-562"), ("HeLa", "HeLa"), ("A549", "A-549")):
        got = cellosaurus_lookup(real)
        assert got and got["canonical_name"] == expect, (real, got)
    assert (
        "age" in _CELLOSAURUS_BACKPROP and "cell_line" not in _CELLOSAURUS_BACKPROP
    ), _CELLOSAURUS_BACKPROP
    assert set(_CELLOSAURUS_BACKPROP) == set(CELLOSAURUS_DONATED_FIELDS) - {
        "cell_line"
    }, (sorted(_CELLOSAURUS_BACKPROP), sorted(CELLOSAURUS_DONATED_FIELDS))

    print("enrich.evidence_gate self-check: all assertions passed")


if __name__ == "__main__":
    _demo()
