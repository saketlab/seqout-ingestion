"""
Promote a study's single-cell modality into its assay call.

library_strategy has no single-cell vocabulary, so a single-cell study reads as
bulk RNA-seq in dominant_assay_l1/l2. Every modality is itself a raw term in
assay_mapping.csv; adding one means adding a row there, not editing this module.

assay_l1_counts and assay_l2_counts are left alone, holding what the library
strategies said.
"""

from __future__ import annotations

#: the bulk assay each modality refines; promotion needs that counterpart or no
#: information, so a ChIP-seq study is not relabelled scRNA-seq
_REFINES: dict[str, frozenset[str]] = {
    "scRNA-seq": frozenset({"Transcriptomic"}),
    "snRNA-seq": frozenset({"Transcriptomic"}),
    "Spatial Transcriptomics": frozenset({"Transcriptomic"}),
    "scATAC-seq": frozenset({"Chromatin Accessibility"}),
    "snATAC-seq": frozenset({"Chromatin Accessibility"}),
    "Multiome": frozenset({"Transcriptomic", "Chromatin Accessibility"}),
    "Other single-cell": frozenset({"Transcriptomic"}),
}

#: Promotable whatever the modality: nothing was derived, the derived answer
#: carries no information, or the row is already single-cell and only its L2
#: needs sharpening.
_UNINFORMATIVE: frozenset[str | None] = frozenset(
    {None, "", "Other", "Single Cell", "Spatial"}
)

#: `sc_enrich classify` reads study text; pentimento reads the reads. Where they
#: disagree the reads win, so a text-derived modality on a study the reads
#: screened as NOT single-cell must not be promoted.
_CONTRADICTED = frozenset(
    {"measured_not_single_cell", "measured_not_single_cell_text_disagrees"}
)


def promote(
    modality: str | None,
    status: str | None,
    assay_l1: str | None,
    assay_l2: str | None,
    assay_map: dict[str, tuple[str, str]],
) -> tuple[str, str] | None:
    """
    Return the (assay_l1, assay_l2) this modality upgrades the row to, or None.

    assay_map is raw_term -> (canonical_assay, category), from assay_mapping.csv.
    """
    if not modality or status in _CONTRADICTED:
        return None
    mapped = assay_map.get(modality) or assay_map.get(modality.casefold())
    if not mapped:
        # no row in assay_mapping.csv promotes to nothing
        return None
    canonical, category = mapped
    if (category, canonical) == (assay_l1, assay_l2):
        return None
    if assay_l1 in _UNINFORMATIVE or assay_l1 in _REFINES.get(modality, frozenset()):
        return category, canonical
    return None
