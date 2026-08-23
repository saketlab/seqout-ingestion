"""
Archive-declared inputs, extraction, and the tissue-overlap override guard.
"""

from __future__ import annotations

import copy
import re

import pandas as pd

from enrich.cellosaurus import BACKPROP_FIELDS, CELLOSAURUS_DONATED_FIELDS

_CELLOSAURUS_BACKPROP = BACKPROP_FIELDS
_NULLISH = {"", "-", "na", "none", "null", "nan", "unknown", "not applicable"}

STRATEGY_COL = "library_strategy"
DESIGN_ATTR = "design_description"
# sample_detail keeps rows differing only in free text from collapsing in dedup and inheriting one label
FALLBACK_TEXT_COLS = ["Description", "Sample Title", "source_name", "source name"]
DETAIL_COL = "sample_detail"

_BLANKISH = {
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


def _norm(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = re.sub(r"[^a-z0-9]+", " ", str(v).lower()).strip()
    return None if (not s or s in _NULLISH) else s


def _tissues_overlap(llm_tissue, cvcl_site) -> bool:
    a, b = _norm(llm_tissue), _norm(cvcl_site)
    if a is None or b is None:
        return True
    if a == b or (len(a) >= 3 and a in b) or (len(b) >= 3 and b in a):
        return True
    return bool(set(a.split()) & set(b.split()))


def should_refuse_override(
    cell_line_source, tissue_source, llm_tissue, cvcl_primary_site
) -> bool:
    """Refuse a matched cell line that contradicts high-trust tissue; the match found a line the sample never was."""
    return (
        cell_line_source == "cellosaurus"
        and tissue_source == "llm_primary"
        and cvcl_primary_site is not None
        and not _tissues_overlap(llm_tissue, cvcl_primary_site)
    )


def apply_guard(output_df, cello_df, provenance_df, ontology_df):
    """Apply the Cellosaurus override guard to output frames."""
    if cello_df is None or provenance_df is None or len(cello_df) == 0:
        return output_df, cello_df, provenance_df, ontology_df

    def psrc(idx, field):
        col = f"{field}_source"
        return provenance_df.loc[idx, col] if col in provenance_df.columns else None

    refused = 0
    for i in cello_df.index:
        site = (
            cello_df.loc[i, "tissue_primary_site"]
            if "tissue_primary_site" in cello_df.columns
            else None
        )
        if not should_refuse_override(
            psrc(i, "cell_line"), psrc(i, "tissue"), cello_df.loc[i, "tissue"], site
        ):
            continue
        refused += 1
        for frame in (cello_df, ontology_df):
            if frame is None:
                continue
            frame.loc[i, "cell_line"] = None
            for f in _CELLOSAURUS_BACKPROP:
                if f in frame.columns and psrc(i, f) == "cellosaurus":
                    frame.loc[i, f] = None
            for c in ("tissue_primary_site", "tissue_site_type"):
                if c in frame.columns:
                    frame.loc[i, c] = None
        if "cell_line_source" in provenance_df.columns:
            provenance_df.loc[i, "cell_line_source"] = "guard_refused"
    if refused:
        provenance_df.attrs["guard_refused"] = refused
    return output_df, cello_df, provenance_df, ontology_df


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    return str(v).strip().lower() in _BLANKISH


def apply_library_strategy(samples: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Seed an assay column from the declared library_strategy."""
    out = samples.copy()
    if STRATEGY_COL not in samples.columns:
        return out, 0

    existing = next((c for c in out.columns if c.lower() == "assay"), None)
    if existing is None:
        out["assay"] = None
        existing = "assay"

    fill = out[existing].map(_blank) & ~out[STRATEGY_COL].map(_blank)
    # strip strategy because the canonical lookup does not strip
    out.loc[fill, existing] = out.loc[fill, STRATEGY_COL].map(lambda v: str(v).strip())
    return out, int(fill.sum())


def apply_sample_detail(samples: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Mirror per-row free text into a dedup-visible column."""
    present = [c for c in FALLBACK_TEXT_COLS if c in samples.columns]
    if not present or DETAIL_COL in samples.columns:
        return samples, 0

    out = samples
    joined = (
        out[present]
        .astype(str)
        .apply(lambda r: " | ".join(v.strip() for v in r if not _blank(v)), axis=1)
    )
    if joined.nunique(dropna=True) < 2:
        return out, 0
    out = out.copy()
    out[DETAIL_COL] = joined.replace("", None)
    return out, int(joined.ne("").sum())


def apply_design_description(project) -> bool:
    """Fold the study design text into the project description."""
    design = getattr(project, DESIGN_ATTR, None)
    if _blank(design):
        return False
    design = str(design).strip()
    current = getattr(project, "description", None)
    current = "" if _blank(current) else str(current).strip()
    if design in current:
        return False
    project.description = f"{current}\n\nEXPERIMENTAL DESIGN: {design}".strip()
    return True


def augment(project):
    """Return a copy of project with the archive-declared fields wired in."""
    samples = getattr(project, "samples", None)
    if samples is None or not isinstance(samples, pd.DataFrame) or samples.empty:
        return project

    aug = copy.copy(project)
    frame, n_assay = apply_library_strategy(samples)
    frame, n_detail = apply_sample_detail(frame)
    aug.samples = frame
    appended = apply_design_description(aug)
    aug.samples.attrs["assay_from_strategy"] = n_assay
    aug.samples.attrs["sample_detail"] = n_detail
    aug.samples.attrs["design_appended"] = appended
    return aug


def enrich_df(project, *args, **kwargs):
    """Augment must run before enrich, and guard after enrich."""
    from enrich.extract import enrich_df as _extract_df

    # expected_taxid and expected_species are a separate earlier guard
    out = _extract_df(augment(project), *args, **kwargs)
    if isinstance(out, tuple) and len(out) == 4:
        return apply_guard(*out)
    return out


def _demo() -> None:
    """Self-check."""

    class _P:
        def __init__(self, samples, **kw):
            self.samples = samples
            for k, v in kw.items():
                setattr(self, k, v)

    df = pd.DataFrame(
        {STRATEGY_COL: ["WGS", " RNA-Seq ", "WGS"], "assay": [None, None, "scRNA-seq"]}
    )
    out, n = apply_library_strategy(df)
    assert n == 2, n
    assert list(out["assay"]) == ["WGS", "RNA-Seq", "scRNA-seq"], list(out["assay"])
    assert df["assay"].isna().sum() == 2, "input frame was mutated"

    bare = pd.DataFrame({"x": [1, 2]})
    out2, n2 = apply_library_strategy(bare)
    assert n2 == 0 and out2 is not bare

    d = pd.DataFrame({"Description": ["tumor", "matched normal"]})
    out3, n3 = apply_sample_detail(d)
    assert n3 == 2 and out3[DETAIL_COL].nunique() == 2
    same = pd.DataFrame({"Description": ["x", "x"]})
    out4, n4 = apply_sample_detail(same)
    assert n4 == 0 and DETAIL_COL not in out4.columns

    p = _P(pd.DataFrame({"a": [1]}), description="Base.", design_description="Trio.")
    assert apply_design_description(p) is True
    assert "EXPERIMENTAL DESIGN: Trio." in p.description
    assert apply_design_description(p) is False, "not idempotent"

    orig = _P(
        pd.DataFrame({STRATEGY_COL: ["WGS"], "Description": ["tumor"]}),
        description="B.",
        design_description="Trio.",
    )
    a1 = augment(orig)
    a2 = augment(a1)
    assert a1.samples.attrs["assay_from_strategy"] == 1
    assert a2.samples.attrs["assay_from_strategy"] == 0, "augment not idempotent"
    assert a2.samples.attrs["design_appended"] is False
    assert "assay" not in orig.samples.columns, "caller's frame was mutated"

    assert "age" in _CELLOSAURUS_BACKPROP
    assert "cell_line" not in _CELLOSAURUS_BACKPROP
    assert set(_CELLOSAURUS_BACKPROP) == set(CELLOSAURUS_DONATED_FIELDS) - {"cell_line"}

    print("enrich.archive_fields self-check: all assertions passed")


if __name__ == "__main__":
    _demo()
