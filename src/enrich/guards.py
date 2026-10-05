"""
Post-hoc correction tiers.

Guard mapping:

  guard_h9                 <- detect_h9_v4.py + apply_h9_v4.py
  guard_organism            <- build_organism_truth_v4.py + apply_organism_v4.py
                               (source-DB sweep remains post-hoc)
  guard_placeholder         <- enrich.cellosaurus.CELL_LINE_PLACEHOLDERS (F5)
  guard_declared            <- ENA typed sample columns
  guard_insdc_assay         <- INSDC library triple
  guard_cell_line_species   <- Cellosaurus species vs the settled organism

"""

from __future__ import annotations

import functools
import re
from datetime import datetime

import pandas as pd

from enrich.cellosaurus import CELL_LINE_PLACEHOLDERS, CELLOSAURUS_DONATED_FIELDS
from enrich.cellosaurus import load_lookup as cellosaurus_lookup_table
from enrich.cellosaurus import normalize_cell_line_name as _normalize_cell_line_name

FRAMEWORK_VERSION = "2026-08-22-inline"

LOG_COLUMNS = (
    "study_accession",
    "sample",
    "table_name",
    "field",
    "old_value",
    "new_value",
    "action",
    "reason",
    "source_value",
    "detector",
    "framework_version",
    "ts",
)

FRAME_TABLE = {
    "output": "enriched_samples",
    "cello": "cellosaurus_samples",
    "ontology": "ontology_samples",
    "provenance": "provenance_samples",
}

_ID_CANDIDATES = ("Sample", "Accession", "experiment_accession")

_ONTOLOGY_SUFFIXES = (
    "_ontology_id",
    "_ontology_name",
    "_ontology_score",
    "_match_method",
    "_ancestors",
)


def _ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _blank(v) -> bool:
    return (
        v is None
        or v is pd.NA
        or (isinstance(v, float) and pd.isna(v))
        or str(v).strip() == ""
    )


def id_column(df: pd.DataFrame) -> str | None:
    for c in _ID_CANDIDATES:
        if df is not None and c in df.columns:
            return c
    return None


def _sample_ids(frames: dict) -> pd.Series:
    for key in ("ontology", "cello", "output"):
        df = frames.get(key)
        col = id_column(df) if df is not None else None
        if col:
            return df[col]
    raise KeyError("no sample id column in any frame")


class _Log:
    """Collects audit rows in enrichment_corrections_log order."""

    def __init__(self, study: str):
        self.study = study
        self.rows: list[tuple] = []
        self.ts = _ts()

    def add(
        self,
        sample,
        table,
        field,
        old,
        new,
        action,
        reason,
        detector,
        source_value=None,
    ):
        self.rows.append(
            (
                self.study,
                sample,
                table,
                field,
                None if old is None else str(old),
                None if new is None else str(new),
                action,
                reason,
                source_value,
                detector,
                FRAMEWORK_VERSION,
                self.ts,
            )
        )

    def __len__(self):
        return len(self.rows)


def _set(
    frames: dict,
    key: str,
    i,
    field: str,
    value,
    log: _Log,
    sample,
    action: str,
    reason: str,
    detector: str,
    source_value=None,
    clear_ontology: bool = True,
):
    """Write ``value`` into one frame/cell and audit it if it actually changed."""
    df = frames.get(key)
    if df is None or field not in df.columns or i not in df.index:
        return False
    old = df.at[i, field]
    same = (_blank(old) and _blank(value)) or (
        not _blank(old) and not _blank(value) and str(old) == str(value)
    )
    if same:
        return False
    df.at[i, field] = value
    log.add(
        sample,
        FRAME_TABLE[key],
        field,
        None if _blank(old) else old,
        value,
        action,
        reason,
        detector,
        source_value,
    )
    if clear_ontology and key == "ontology":
        for suf in _ONTOLOGY_SUFFIXES:
            col = f"{field}{suf}"
            if col in df.columns and not _blank(df.at[i, col]):
                df.at[i, col] = None
    return True


def _prov(provenance_df, i, field, suffix="_source"):
    col = f"{field}{suffix}"
    if (
        provenance_df is None
        or col not in provenance_df.columns
        or i not in provenance_df.index
    ):
        return None
    v = provenance_df.at[i, col]
    return None if _blank(v) else str(v)


def _set_prov(provenance_df, i, col, value, log, sample, reason, detector):
    if (
        provenance_df is None
        or col not in provenance_df.columns
        or i not in provenance_df.index
    ):
        return
    old = provenance_df.at[i, col]
    if (_blank(old) and _blank(value)) or str(old) == str(value):
        return
    provenance_df.at[i, col] = value
    log.add(
        sample,
        "provenance_samples",
        col,
        None if _blank(old) else old,
        value,
        "update",
        reason,
        detector,
    )


# shared study/sample text


def project_text(project) -> str:
    return " ".join(
        str(getattr(project, f, "") or "")
        for f in ("title", "description", "overall_design")
    )


def row_texts(samples: pd.DataFrame) -> pd.Series:
    """Per-row concatenation of every sample column, as one lowercase string."""
    if samples is None or not isinstance(samples, pd.DataFrame) or samples.empty:
        return pd.Series(dtype=object)
    return samples.apply(lambda r: " ".join(str(v) for v in r.values), axis=1)


# G1. wrong_h9
#
# "H9" is two cell lines.  Cellosaurus resolves the bare string to CVCL_1240, a
# Sezary-syndrome T-cell lymphoma line (53Y, Male, Peripheral blood).  In stem
# cell papers "H9" means WA09/CVCL_9773, a female hESC line (Blastocyst).  The
# lookup has no context, so every hESC study that writes "H9" inherits a
# lymphoma patient's demographics.
#

HESC_RE = re.compile(
    r"embryonic stem|\bh[\- ]?esc|\bhes\b|\bes cell|pluripotent|reprogramm"
    r"|wa[\- ]?09|definitive endoderm|organoid|gastruloid|sclerotome"
    r"|neural crest|cardiomyocyte|differentiat|teratoma|trophoblast",
    re.IGNORECASE,
)
LYMPHOMA_RE = re.compile(
    r"sezary|s[eé]zary|lymphoma|cutaneous t|\bhiv\b|\bcd4\b|leukemi", re.IGNORECASE
)

H9_DETECTOR = "wrong_h9"
H9_REASON = "wrong_h9_CVCL1240_to_WA09"

# F2: import the shared constant rather than mirroring it locally.
DONATED_FIELDS = CELLOSAURUS_DONATED_FIELDS


def _entry_donations(entry: dict) -> dict:
    """What ``entry`` would donate, in enriched-field terms."""
    tissue = entry.get("tissue") or None
    return {
        "cell_line": entry.get("canonical_name"),
        "age": entry.get("age") or None,
        "sex": entry.get("sex") or None,
        "ethnicity": entry.get("population") or None,
        "tissue": tissue,
        "disease": entry.get("disease") or None,
        "sample_type": "Cell Line",
        "organism": entry.get("species") or None,
        "taxid": entry.get("taxid") or None,
        "tissue_site_type": entry.get("site_type") or None,
        "tissue_primary_site": tissue,
    }


def guard_h9(project, frames: dict, log: _Log, lookup: dict | None = None) -> int:
    """
    Re-donate WA09 over every H9 assignment made in an hESC context.
    """
    onto = frames.get("ontology")
    if onto is None or "cell_line" not in onto.columns:
        return 0
    lookup = cellosaurus_lookup_table() if lookup is None else lookup
    wa09 = lookup.get("wa09")
    if not wa09:
        return 0
    donate = _entry_donations(wa09)

    samples = getattr(project, "samples", None)
    texts = row_texts(samples)
    study = project_text(project)
    ids = _sample_ids(frames)
    prov = frames.get("provenance")
    n = 0

    for i in onto.index:
        if _normalize_cell_line_name(onto.at[i, "cell_line"] or "") != "h9":
            continue
        txt = study + " " + (texts.get(i, "") if len(texts) else "")
        if LYMPHOMA_RE.search(txt):
            continue  # genuine CVCL_1240 context: leave alone
        if not HESC_RE.search(txt):
            continue  # no evidence either way: never guess
        sample = ids.get(i)
        for field, value in donate.items():
            if field in ("taxid", "tissue_primary_site", "tissue_site_type"):
                continue
            # only fields the wrong entry donated; an LLM-set value stands
            if field != "cell_line" and _prov(prov, i, field) != "cellosaurus":
                continue
            for key in ("output", "cello", "ontology"):
                _set(
                    frames,
                    key,
                    i,
                    field,
                    value,
                    log,
                    sample,
                    "correct_known",
                    H9_REASON,
                    H9_DETECTOR,
                    source_value=wa09.get("cvcl_id"),
                )
        for field in ("taxid", "tissue_primary_site", "tissue_site_type"):
            for key in ("cello", "ontology"):
                _set(
                    frames,
                    key,
                    i,
                    field,
                    donate[field],
                    log,
                    sample,
                    "correct_known",
                    H9_REASON,
                    H9_DETECTOR,
                    source_value=wa09.get("cvcl_id"),
                )
        _set_prov(
            prov,
            i,
            "cell_line_source",
            "corrected_source",
            log,
            sample,
            H9_REASON,
            H9_DETECTOR,
        )
        n += 1
    return n


# G2. organism

ORGANISM_DETECTOR = "wrong_organism"
ORGANISM_REASON = "wrong_organism_vs_source_taxid"

_ORGANISM_COLS = ("Organism", "organism", "Scientific Name", "scientific_name")
_TAXID_COLS = ("Taxon ID", "taxon_id", "taxid", "TaxID")


def source_species(samples: pd.DataFrame) -> pd.DataFrame:
    """(taxid, organism) the submitter stated per row, as far as the frame says."""
    if samples is None or not isinstance(samples, pd.DataFrame) or samples.empty:
        return pd.DataFrame(columns=["taxid", "organism"])
    tax = next((c for c in _TAXID_COLS if c in samples.columns), None)
    org = next((c for c in _ORGANISM_COLS if c in samples.columns), None)
    out = pd.DataFrame(index=samples.index, columns=["taxid", "organism"], dtype=object)
    if tax:
        out["taxid"] = samples[tax].map(lambda v: None if _blank(v) else str(v).strip())
    if org:
        out["organism"] = samples[org].map(
            lambda v: None if _blank(v) else str(v).strip()
        )
        # A GEO sample frame states the organism by NAME and carries no taxid at
        # all, while the corpus detector keys on taxid.
        missing = out["taxid"].isna() & out["organism"].notna()
        if missing.any():
            out.loc[missing, "taxid"] = out.loc[missing, "organism"].map(_species_taxid)
    return out


@functools.lru_cache(maxsize=1)
def _species_map() -> dict:
    """
    Extract's name -> taxid table, or a compact fallback.
    """
    try:
        from enrich.extract import _SPECIES_TAXID

        return dict(_SPECIES_TAXID)
    except Exception:
        return {
            "homo sapiens": "9606",
            "human": "9606",
            "mus musculus": "10090",
            "mouse": "10090",
            "rattus norvegicus": "10116",
            "rat": "10116",
            "danio rerio": "7955",
            "zebrafish": "7955",
            "drosophila melanogaster": "7227",
            "caenorhabditis elegans": "6239",
            "saccharomyces cerevisiae": "4932",
            "arabidopsis thaliana": "3702",
            "gallus gallus": "9031",
            "sus scrofa": "9823",
            "bos taurus": "9913",
            "macaca mulatta": "9544",
            "cricetulus griseus": "10029",
        }


def _species_taxid(name):
    if _blank(name):
        return None
    key = re.sub(r"[^a-z0-9]+", " ", str(name).lower()).strip()
    return _species_map().get(key)


def guard_organism(project, frames: dict, log: _Log) -> int:
    """Restore taxid+organism from the submitter's own statement."""
    onto = frames.get("ontology")
    if onto is None or "taxid" not in onto.columns:
        return 0
    src = source_species(getattr(project, "samples", None))
    if src.empty or src["taxid"].isna().all():
        return 0
    ids = _sample_ids(frames)
    n = 0
    for i in onto.index:
        if i not in src.index:
            continue
        stax, sorg = src.at[i, "taxid"], src.at[i, "organism"]
        if _blank(stax):
            continue
        cur = onto.at[i, "taxid"]
        if _blank(cur) or str(cur).strip() == str(stax):
            continue  # nothing asserted, or already agrees
        sample = ids.get(i)
        changed = False
        for key in ("cello", "ontology"):
            changed |= _set(
                frames,
                key,
                i,
                "taxid",
                stax,
                log,
                sample,
                "correct_source",
                ORGANISM_REASON,
                ORGANISM_DETECTOR,
                source_value=stax,
            )
            if not _blank(sorg):
                changed |= _set(
                    frames,
                    key,
                    i,
                    "organism",
                    sorg,
                    log,
                    sample,
                    "correct_source",
                    ORGANISM_REASON,
                    ORGANISM_DETECTOR,
                    source_value=stax,
                )
        n += int(changed)
    return n


# G7. cell_line species mismatch
#
# Fires only for a pinned allowlist of names Cellosaurus resolves
# unambiguously (re-checked against the live lookup's cvcl_id every call),
# crossed with organisms verified to have no xenograft/EV-transfer paradigm
# for those lines. Widening either list needs the same per-case evidence.
#
# A flat Cellosaurus lookup can silently return the wrong entry for an
# ambiguous name: "E14"/"R1"/"B16" each collapse onto one unrelated line
# (enrich.cellosaurus.load_lookup). Xenograft/EV-transfer experiments into
# mouse/rat/zebrafish legitimately produce a species mismatch (e.g.
# SRP457338: HeLa cells dosed with mouse-donor EVs).

ALLOWED_CELL_LINES = {
    "hela": "CVCL_0030",
    "k562": "CVCL_0004",
    "mcf7": "CVCL_0031",
    "hepg2": "CVCL_0027",
    "jurkat": "CVCL_0065",
    "a549": "CVCL_0023",
    "hct116": "CVCL_0291",
}

SPECIES_SAFE_TAXIDS = {
    "4932": "Saccharomyces cerevisiae",
    "4896": "Schizosaccharomyces pombe",
    "7227": "Drosophila melanogaster",
    "6239": "Caenorhabditis elegans",
    "3702": "Arabidopsis thaliana",
}

SPECIES_DETECTOR = "wrong_cell_line_species"
SPECIES_REASON = "cell_line_species_mismatch_vs_organism"

_SPECIES_BACKPROP_FIELDS = tuple(
    f for f in DONATED_FIELDS if f not in ("cell_line", "organism")
)
_SPECIES_SITE_FIELDS = ("tissue_primary_site", "tissue_site_type")


_NO_DONATION_CHECK = object()


def _matches_donation(current, donated) -> bool:
    """Only clear a field still holding exactly what this entry donated."""
    if _blank(donated):
        return _blank(current)
    if _blank(current):
        return False
    return str(current).strip().lower() == str(donated).strip().lower()


def _clear_species_field(
    frames: dict,
    i,
    field: str,
    key_frames: tuple[str, ...],
    log: _Log,
    sample,
    pinned_cvcl: str,
    donated=_NO_DONATION_CHECK,
) -> None:
    for key_frame in key_frames:
        if donated is not _NO_DONATION_CHECK:
            df = frames.get(key_frame)
            if df is None or field not in df.columns or i not in df.index:
                continue
            if not _matches_donation(df.at[i, field], donated):
                continue  # value has since diverged from the donation
        _set(
            frames,
            key_frame,
            i,
            field,
            None,
            log,
            sample,
            "null",
            SPECIES_REASON,
            SPECIES_DETECTOR,
            source_value=pinned_cvcl,
        )


def guard_cell_line_species(
    project, frames: dict, log: _Log, lookup: dict | None = None
) -> int:
    onto = frames.get("ontology")
    if onto is None or "cell_line" not in onto.columns or "taxid" not in onto.columns:
        return 0
    lookup = cellosaurus_lookup_table() if lookup is None else lookup
    ids = _sample_ids(frames)
    prov = frames.get("provenance")
    n = 0
    for i in onto.index:
        cl = onto.at[i, "cell_line"]
        if _blank(cl):
            continue
        key = _normalize_cell_line_name(cl)
        pinned_cvcl = ALLOWED_CELL_LINES.get(key)
        if pinned_cvcl is None:
            continue  # not on the pinned, verified-unambiguous allowlist
        entry = lookup.get(key)
        if entry is None or entry.get("cvcl_id") != pinned_cvcl:
            continue  # lookup no longer agrees with the pin: abstain
        cur_taxid = onto.at[i, "taxid"]
        if _blank(cur_taxid):
            continue
        cur_taxid = str(cur_taxid).strip()
        if cur_taxid not in SPECIES_SAFE_TAXIDS:
            continue  # not a verified-incompatible organism: abstain
        sample = ids.get(i)
        donate = _entry_donations(entry)

        _clear_species_field(
            frames,
            i,
            "cell_line",
            ("output", "cello", "ontology"),
            log,
            sample,
            pinned_cvcl,
        )
        for field in _SPECIES_BACKPROP_FIELDS:
            if _prov(prov, i, field) == "cellosaurus":
                _clear_species_field(
                    frames,
                    i,
                    field,
                    ("output", "cello", "ontology"),
                    log,
                    sample,
                    pinned_cvcl,
                    donate.get(field),
                )
        if _prov(prov, i, "tissue") == "cellosaurus":
            for field in _SPECIES_SITE_FIELDS:
                _clear_species_field(
                    frames,
                    i,
                    field,
                    ("cello", "ontology"),
                    log,
                    sample,
                    pinned_cvcl,
                    donate.get(field),
                )
        n += 1
    return n


# G3. cell-line placeholders in the OUTPUT
#
# enrich.cellosaurus.CELL_LINE_PLACEHOLDERS stops "N/A" resolving to Cellosaurus NA
# (CVCL_7082, donor 86Y) and "WT" to STA-WT-1 (1Y9M).  That closes the import.

PLACEHOLDER_DETECTOR = "cell_line_placeholder"
PLACEHOLDER_REASON = "placeholder_is_not_an_identification"

# F5: the same set that gates the Cellosaurus lookup, imported not restated.


def guard_placeholder(project, frames: dict, log: _Log) -> int:
    onto = frames.get("ontology")
    if onto is None or "cell_line" not in onto.columns:
        return 0
    ids = _sample_ids(frames)
    n = 0
    for i in onto.index:
        vals = {
            k: frames[k].at[i, "cell_line"]
            for k in ("output", "cello", "ontology")
            if frames.get(k) is not None and "cell_line" in frames[k].columns
        }
        hits = [
            k
            for k, v in vals.items()
            if not _blank(v) and _normalize_cell_line_name(v) in CELL_LINE_PLACEHOLDERS
        ]
        if not hits:
            continue
        sample = ids.get(i)
        for key in hits:
            _set(
                frames,
                key,
                i,
                "cell_line",
                None,
                log,
                sample,
                "null",
                PLACEHOLDER_REASON,
                PLACEHOLDER_DETECTOR,
            )
        n += 1
    return n


# G3b. Species as tissue
#
# Submitters sometimes put the species in a column named "tissue", and an
# Organism column used to be fuzzy-matched as tissue. Either way a species
# name is never a tissue, and mapping it gives a tissue ontology term such as
# "Homo sapiens".

SPECIES_TISSUE_DETECTOR = "species_as_tissue"
SPECIES_TISSUE_REASON = "tissue_value_is_a_species_name"
TAXON_TERM_REASON = "tissue_mapped_to_a_taxon_term"


_PLURAL_SPECIES = {"mice": "mouse", "humans": "human", "rats": "rat"}


def _species_like(value) -> bool:
    """A species name, allowing plurals and a trailing sample number ("Mice", "Human1")."""
    if _blank(value):
        return False
    key = re.sub(r"[^a-z ]+", " ", str(value).lower()).strip()
    key = re.sub(r"\s+", " ", key)
    return _species_taxid(_PLURAL_SPECIES.get(key, key)) is not None


def _taxon_named(tissue, taxon_name) -> bool:
    """The tissue text is the taxon itself (species "Xenopus Laevis", genus "Mus")."""
    if _blank(tissue) or _blank(taxon_name):
        return False
    t = str(tissue).strip().lower()
    name = str(taxon_name).strip().lower()
    return t == name or t == name.split(" <")[0] or t == name.split()[0]


def guard_species_tissue(project, frames: dict, log: _Log) -> int:
    ids = _sample_ids(frames)
    onto = frames.get("ontology")
    has_ids = onto is not None and "tissue_ontology_id" in onto.columns
    clear_text, clear_term = set(), set()
    for key in ("output", "cello", "ontology"):
        df = frames.get(key)
        if df is not None and "tissue" in df.columns:
            clear_text |= {i for i in df.index if _species_like(df.at[i, "tissue"])}
    if has_ids:
        for i in onto.index:
            if not str(onto.at[i, "tissue_ontology_id"]).startswith("NCBITaxon:"):
                continue
            name = (
                onto.at[i, "tissue_ontology_name"]
                if "tissue_ontology_name" in onto.columns
                else None
            )
            # NCBITaxon:1 is the taxonomy root: "Root" is a plant tissue, keep it
            if str(onto.at[i, "tissue_ontology_id"]) != "NCBITaxon:1" and _taxon_named(
                onto.at[i, "tissue"], name
            ):
                clear_text.add(i)
            else:
                clear_term.add(i)
    for i in clear_text:
        for key in ("output", "cello", "ontology"):
            _set(
                frames,
                key,
                i,
                "tissue",
                None,
                log,
                ids.get(i),
                "null",
                SPECIES_TISSUE_REASON,
                SPECIES_TISSUE_DETECTOR,
            )
    for i in clear_term - clear_text:
        for col in [c for c in onto.columns if c.startswith("tissue_ontology_")] + [
            c for c in ("tissue_match_method", "tissue_ancestors") if c in onto.columns
        ]:
            _set(
                frames,
                "ontology",
                i,
                col,
                None,
                log,
                ids.get(i),
                "null",
                TAXON_TERM_REASON,
                SPECIES_TISSUE_DETECTOR,
                clear_ontology=False,
            )
    return len(clear_text | clear_term)


# G4. Bad labels
#
#   provenance.cell_line_source = 'cellosaurus'
#   AND (cell_line_original_source = 'llm_fallback' OR cell_line_original_column IS NULL)
#   AND provenance.tissue_source = 'llm_primary'
#   AND ontology.cell_line IS NOT NULL AND ontology.tissue IS NOT NULL
#   AND lower(ontology.tissue) NOT LIKE '%' || first_token(cellosaurus.tissue_primary_site) || '%'
#
# Detection and action are separate: tissue disagreement does not prove the
# cell line is wrong. The default policy reports this predicate and lets
# evidence_gate decide whether the cell line is named in the study;

BADLABEL_DETECTOR = "provenance_bad_label"
BADLABEL_REASON_CL = "cellosaurus_override_vs_llm_tissue"
BADLABEL_REASON_CASCADE = "cascade_from_wrong_cellosaurus_cell_line"


def bad_label_rows(frames: dict) -> list:
    """Row positions matching build_badlabel_v4.py's predicate."""
    onto, cello, prov = (
        frames.get("ontology"),
        frames.get("cello"),
        frames.get("provenance"),
    )
    if onto is None or prov is None:
        return []
    if "cell_line" not in onto.columns or "tissue" not in onto.columns:
        return []
    hits = []
    for i in onto.index:
        if _prov(prov, i, "cell_line") != "cellosaurus":
            continue
        orig_src = _prov(prov, i, "cell_line", "_original_source")
        orig_col = _prov(prov, i, "cell_line", "_original_column")
        if not (orig_src == "llm_fallback" or orig_col is None):
            continue
        if _prov(prov, i, "tissue") != "llm_primary":
            continue
        if _blank(onto.at[i, "cell_line"]) or _blank(onto.at[i, "tissue"]):
            continue
        site = ""
        if (
            cello is not None
            and "tissue_primary_site" in cello.columns
            and i in cello.index
        ):
            v = cello.at[i, "tissue_primary_site"]
            site = "" if _blank(v) else str(v)
        # SQL: substr(site, 1, instr(site || ',', ',') - 1)  -> first comma token
        head = (site + ",").split(",")[0].lower()
        if head and head in str(onto.at[i, "tissue"]).lower():
            continue
        hits.append(i)
    return hits


def guard_bad_label(project, frames: dict, log: _Log, policy: str = "evidence") -> int:
    hits = bad_label_rows(frames)
    if policy != "legacy" or not hits:
        return len(hits)
    ids = _sample_ids(frames)
    prov = frames.get("provenance")
    for i in hits:
        sample = ids.get(i)
        for field in DONATED_FIELDS:
            if field != "cell_line" and _prov(prov, i, field) != "cellosaurus":
                continue
            reason = (
                BADLABEL_REASON_CL if field == "cell_line" else BADLABEL_REASON_CASCADE
            )
            for key in ("output", "cello", "ontology"):
                _set(
                    frames,
                    key,
                    i,
                    field,
                    None,
                    log,
                    sample,
                    "null",
                    reason,
                    BADLABEL_DETECTOR,
                )
        for field in ("tissue_primary_site", "tissue_site_type"):
            for key in ("cello", "ontology"):
                _set(
                    frames,
                    key,
                    i,
                    field,
                    None,
                    log,
                    sample,
                    "null",
                    BADLABEL_REASON_CL,
                    BADLABEL_DETECTOR,
                )
        _set_prov(
            prov,
            i,
            "cell_line_source",
            "corrected_null",
            log,
            sample,
            BADLABEL_REASON_CL,
            BADLABEL_DETECTOR,
        )
    return len(hits)


# G5. ENA typed columns
#
# Declared-field rules: NULL means not declared, not contradiction. Fill empty
# fields, but avoid general overrides because string disagreement often reflects
# matcher limits rather than wrong data. The only override below is provenance-
# gated. Age is fill-only until comparison is unit-aware.

DECLARED_DETECTOR = "ena_declared"
DECLARED_FILL_REASON = "filled_from_ena_declared_column"
DECLARED_OVERRIDE_REASON = "cellosaurus_disease_vs_ena_declared"
DECLARED_SOURCE = "ena_declared"

# enriched field -> the ENA columns that inform it, most specific first.
DECLARED_FILL = {
    "cell_type": ("ena_cell_type",),
    "tissue": ("ena_tissue_type", "ena_tissue_lib"),
    "disease": ("ena_disease",),
    "development_stage": ("ena_dev_stage",),
    "strain": ("ena_strain", "ena_sub_strain"),
    "sex": ("ena_sex", "ena_host_sex"),
    "age": ("ena_age",),
    "cell_line": ("ena_cell_line",),
}

_DECLARED_NULLISH = {
    "na",
    "n",
    "none",
    "null",
    "nan",
    "unknown",
    "unspecified",
    "notapplicable",
    "notavailable",
    "notcollected",
    "missing",
    "notprovided",
    "notdetermined",
    "other",
    "tbd",
    "pooled",
}

_HEALTHY = {
    "normal",
    "healthy",
    "control",
    "none",
    "notapplicable",
    "na",
    "nodisease",
    "healthycontrol",
    "wildtype",
    "wt",
    "uninfected",
}


def _get(df, i, field):
    """Cell value, or None when the frame simply has no such column."""
    if df is None or field not in df.columns or i not in df.index:
        return None
    return df.at[i, field]


def _norm(v) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(v).lower()) if not _blank(v) else ""


_STOPWORDS = {
    "the",
    "a",
    "of",
    "and",
    "cell",
    "cells",
    "type",
    "positive",
    "negative",
    "sample",
    "tissue",
    "human",
    "primary",
}


def _tokens(v) -> set[str]:
    """Significant tokens of a label, singularised. Empty when uninformative."""
    if _blank(v):
        return set()
    raw = [t for t in re.split(r"[^a-z0-9]+", str(v).lower()) if len(t) >= 2]
    out = {t[:-1] if (len(t) > 3 and t.endswith("s")) else t for t in raw}
    out -= _STOPWORDS
    return out


def values_agree(a, b) -> bool:
    """
    Only a clear mismatch counts as disagreement.

    Substring containment counts as agreement (Sh-5Y/SH-SY5Y, Cd4+ T Cells/
    CD4-positive alpha-beta T cell), and the healthy family is one value
    (Healthy/normal). Under-firing is safer here than overwriting correct values.
    """
    x, y = _norm(a), _norm(b)
    if not x or not y:
        return True  # abstention is not contradiction
    if x == y or x in y or y in x:
        return True
    if x in _HEALTHY and y in _HEALTHY:
        return True
    # Punctuated abbreviations of one name, such as "Sh-5Y" vs "SH-SY5Y", need
    # subsequence matching. Bound by length ratio so it cannot swallow conflicts;
    # and "adult" is not a subsequence of "juvenile" at all.
    short, long = (x, y) if len(x) <= len(y) else (y, x)
    if len(short) >= 3 and len(short) / len(long) >= 0.5:
        it = iter(long)
        if all(ch in it for ch in short):
            return True
    # Ontology phrasing of the same thing: "Cd4+ T Cells" vs "CD4-positive,
    # alpha-beta T cell"
    ta, tb = _tokens(a), _tokens(b)
    if ta and tb:
        small, big = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
        if small <= big:
            return True
    return False


def guard_declared(project, frames: dict, log: _Log) -> dict:
    """Fill from ENA's typed columns; override only the cell-line disease leak."""
    onto = frames.get("ontology")
    samples = getattr(project, "samples", None)
    stats = {
        "declared_filled": 0,
        "declared_override": 0,
        "declared_agree": 0,
        "declared_conflict_kept": 0,
    }
    if onto is None or samples is None or not isinstance(samples, pd.DataFrame):
        return stats
    if not samples.index.equals(pd.RangeIndex(len(samples))):
        return stats
    prov = frames.get("provenance")
    ids = _sample_ids(frames)

    for i in onto.index:
        if i not in samples.index:
            continue
        sample = ids.get(i)
        for field, cols in DECLARED_FILL.items():
            declared = next(
                (
                    samples.at[i, c]
                    for c in cols
                    if c in samples.columns and not _blank(samples.at[i, c])
                ),
                None,
            )
            if _blank(declared) or _norm(declared) in _DECLARED_NULLISH:
                continue  # NOT-DECLARED
            # Typed ENA columns can carry the same placeholders as free text;
            # filling cell_line from "WT" would re-open the F5 defect.
            if (
                field == "cell_line"
                and _normalize_cell_line_name(declared) in CELL_LINE_PLACEHOLDERS
            ):
                continue
            current = _get(onto, i, field)
            if _blank(current):
                for key in ("output", "cello", "ontology"):
                    _set(
                        frames,
                        key,
                        i,
                        field,
                        str(declared),
                        log,
                        sample,
                        "fill",
                        DECLARED_FILL_REASON,
                        DECLARED_DETECTOR,
                        source_value=str(declared),
                    )
                _set_prov(
                    prov,
                    i,
                    f"{field}_source",
                    DECLARED_SOURCE,
                    log,
                    sample,
                    DECLARED_FILL_REASON,
                    DECLARED_DETECTOR,
                )
                _set_prov(
                    prov,
                    i,
                    f"{field}_column",
                    cols[0],
                    log,
                    sample,
                    DECLARED_FILL_REASON,
                    DECLARED_DETECTOR,
                )
                stats["declared_filled"] += 1
                continue
            if values_agree(current, declared):
                stats["declared_agree"] += 1
                continue
            # The only override: Cellosaurus supplied a disease, while ENA declares
            # the sample healthy/normal. Broader Cellosaurus-vs-ENA disagreement
            # includes abbreviation pairs and is kept for review instead.
            if (
                field == "disease"
                and _prov(prov, i, "disease") == "cellosaurus"
                and _norm(declared) in _HEALTHY
                and _norm(current) not in _HEALTHY
            ):
                for key in ("output", "cello", "ontology"):
                    _set(
                        frames,
                        key,
                        i,
                        field,
                        str(declared),
                        log,
                        sample,
                        "correct_source",
                        DECLARED_OVERRIDE_REASON,
                        DECLARED_DETECTOR,
                        source_value=str(declared),
                    )
                _set_prov(
                    prov,
                    i,
                    "disease_source",
                    DECLARED_SOURCE,
                    log,
                    sample,
                    DECLARED_OVERRIDE_REASON,
                    DECLARED_DETECTOR,
                )
                stats["declared_override"] += 1
            else:
                stats["declared_conflict_kept"] += 1
    return stats


# G6. the INSDC library
#
# The INSDC library triple bounds what the model may say; text can refine within
# that bound. If the enriched assay is outside the declared strategy, the
# declaration wins and the model value is audited. OTHER and mixed samples are
# recorded but not majority-voted.

INSDC_DETECTOR = "insdc_assay"
INSDC_SOURCE = "insdc_declared"
INSDC_FILL_REASON = "filled_from_insdc_library_strategy"
INSDC_OVERRIDE_REASON = "assay_outside_declared_library_strategy"


def _normalize_assay(value):
    try:
        from enrich.extract import _normalize_assay as f

        return f(value)
    except Exception:
        return value


def _assay_category(value):
    try:
        from enrich.extract import _get_assay_category as f

        return f(value)
    except Exception:
        return None


def guard_insdc_assay(project, frames: dict, log: _Log) -> dict:
    from enrich.declared import (
        CONFLICT_COL,
        TRIPLE_COLS,
        UNINFORMATIVE,
        strategy_families,
    )

    STRAT_COL = TRIPLE_COLS[0]

    onto = frames.get("ontology")
    samples = getattr(project, "samples", None)
    stats = {
        "assay_declared_agree": 0,
        "assay_declared_fill": 0,
        "assay_declared_override": 0,
        "assay_conflict_kept": 0,
        "assay_unjudgeable": 0,
    }
    if onto is None or samples is None or not isinstance(samples, pd.DataFrame):
        return stats
    if STRAT_COL not in samples.columns:
        return stats
    if not samples.index.equals(pd.RangeIndex(len(samples))):
        return stats
    prov = frames.get("provenance")
    ids = _sample_ids(frames)

    for i in onto.index:
        if i not in samples.index:
            continue
        raw = samples.at[i, STRAT_COL]
        if _blank(raw):
            continue
        declared = {s.strip() for s in str(raw).split(";") if s.strip()}
        informative = declared - UNINFORMATIVE
        sample = ids.get(i)
        if not informative:
            continue
        mixed = (
            CONFLICT_COL in samples.columns and not _blank(samples.at[i, CONFLICT_COL])
        ) or len(declared) > 1
        current = _get(onto, i, "assay")

        if _blank(current):
            if mixed:
                stats["assay_conflict_kept"] += 1
                continue
            value = _normalize_assay(sorted(informative)[0])
            for key in ("output", "cello", "ontology"):
                _set(
                    frames,
                    key,
                    i,
                    "assay",
                    value,
                    log,
                    sample,
                    "fill",
                    INSDC_FILL_REASON,
                    INSDC_DETECTOR,
                    source_value=raw,
                )
                _set(
                    frames,
                    key,
                    i,
                    "assay_category",
                    _assay_category(value),
                    log,
                    sample,
                    "fill",
                    INSDC_FILL_REASON,
                    INSDC_DETECTOR,
                )
            _set_prov(
                prov,
                i,
                "assay_source",
                INSDC_SOURCE,
                log,
                sample,
                INSDC_FILL_REASON,
                INSDC_DETECTOR,
            )
            _set_prov(
                prov,
                i,
                "assay_column",
                STRAT_COL,
                log,
                sample,
                INSDC_FILL_REASON,
                INSDC_DETECTOR,
            )
            stats["assay_declared_fill"] += 1
            continue

        fams = strategy_families(current)
        if not fams:
            stats["assay_unjudgeable"] += 1  # never refuse what we cannot judge
            continue
        if fams & informative:
            stats["assay_declared_agree"] += 1
            continue
        if mixed:
            # the sample's own experiments disagree; there is no single
            # declaration to be outside of.  Record, do not act.
            stats["assay_conflict_kept"] += 1
            continue
        value = _normalize_assay(sorted(informative)[0])
        for key in ("output", "cello", "ontology"):
            _set(
                frames,
                key,
                i,
                "assay",
                value,
                log,
                sample,
                "correct_source",
                INSDC_OVERRIDE_REASON,
                INSDC_DETECTOR,
                source_value=raw,
            )
            _set(
                frames,
                key,
                i,
                "assay_category",
                _assay_category(value),
                log,
                sample,
                "correct_source",
                INSDC_OVERRIDE_REASON,
                INSDC_DETECTOR,
            )
        _set_prov(
            prov,
            i,
            "assay_source",
            INSDC_SOURCE,
            log,
            sample,
            INSDC_OVERRIDE_REASON,
            INSDC_DETECTOR,
        )
        _set_prov(
            prov,
            i,
            "assay_column",
            STRAT_COL,
            log,
            sample,
            INSDC_OVERRIDE_REASON,
            INSDC_DETECTOR,
        )
        stats["assay_declared_override"] += 1
    return stats


# evidence bookkeeping
#

EVIDENCED_SOURCE = "cellosaurus_evidenced"
EVIDENCE_DETECTOR = "cellosaurus_evidence_gate"


def mark_evidence_confirmed(
    project, frames: dict, log: _Log, supported, vocab: str = "v5"
) -> int:
    """Stamp cell_line_source for rows whose cell line is named in the study."""
    if vocab != "v5":
        return 0
    onto, prov = frames.get("ontology"), frames.get("provenance")
    if onto is None or prov is None or "cell_line" not in onto.columns:
        return 0
    ids = _sample_ids(frames)
    n = 0
    for i in onto.index:
        if _prov(prov, i, "cell_line") != "cellosaurus":
            continue
        v = onto.at[i, "cell_line"]
        if _blank(v) or not supported(i, v):
            continue
        _set_prov(
            prov,
            i,
            "cell_line_source",
            EVIDENCED_SOURCE,
            log,
            ids.get(i),
            "cell_line_named_in_source_text",
            EVIDENCE_DETECTOR,
        )
        n += 1
    return n


def evidence_supported_fn(project, pre_samples):
    """
    evidence_gate's evidence test, bound to a study's PRE-enrichment text.

    Returns ``None`` when evidence_gate is unavailable, which callers treat as "do
    not mark" rather than as an error.
    """
    try:
        import copy as _copy

        from enrich import evidence_gate
    except ImportError:
        return None
    holder = _copy.copy(project)
    holder.samples = pre_samples
    lookup = evidence_gate._cellosaurus_lookup()
    evidence = evidence_gate.build_evidence_text(holder, pre_samples)

    def supported(i, value):
        return evidence_gate.cell_line_supported(
            value, evidence.get(i, ("", "")), lookup
        )

    return supported


# entry point


def apply_inline_guards(
    project,
    output_df,
    cello_df,
    provenance_df,
    ontology_df,
    *,
    tierc_policy: str = "evidence",
    vocab: str = "v5",
    supported=None,
    declared: bool = True,
):
    """
    Run every (a)-class guard over the four frames.

    Returns ``(frames_tuple, audit_rows, stats)``.  ``audit_rows`` are ready for
    ``INSERT INTO enrichment_corrections_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?)``.
    """
    study = getattr(project, "accession", None) or "?"
    log = _Log(study)
    frames = {
        "output": output_df,
        "cello": cello_df,
        "provenance": provenance_df,
        "ontology": ontology_df,
    }
    stats = {}
    # order matters: H9 first (it REWRITES a cell line the others would judge),
    # then placeholders (they REMOVE one), then organism, then bad_label last so
    # it sees the settled state, the dependency order run_v4_tiers.sh uses.
    stats["h9"] = guard_h9(project, frames, log)
    stats["placeholder"] = guard_placeholder(project, frames, log)
    stats["organism"] = guard_organism(project, frames, log)
    stats["species_tissue"] = guard_species_tissue(project, frames, log)
    # declared values last among the fill guards: they must see the settled
    # cell-line state, because the one disease override is gated on a
    # provenance the H9 guard can rewrite.
    if declared:
        stats.update(guard_declared(project, frames, log))
        stats.update(guard_insdc_assay(project, frames, log))
    if supported is not None:
        stats["evidenced"] = mark_evidence_confirmed(
            project, frames, log, supported, vocab
        )
    # after organism/declared settle taxid; a species-mismatched cell_line was
    # never valid, no evidence gate needed.
    stats["cell_line_species"] = guard_cell_line_species(project, frames, log)
    stats["bad_label"] = guard_bad_label(project, frames, log, tierc_policy)
    stats["audit_rows"] = len(log)
    return (
        (frames["output"], frames["cello"], frames["provenance"], frames["ontology"]),
        log.rows,
        stats,
    )


def _demo() -> None:
    """Self-check on the shapes each guard exists for.  No LLM, no network."""
    lookup = {
        "wa09": {
            "canonical_name": "WA09",
            "cvcl_id": "CVCL_9773",
            "age": "Blastocyst stage",
            "sex": "Female",
            "species": "Homo sapiens",
            "taxid": "9606",
            "tissue": "Blastocyst",
            "site_type": "In situ",
        }
    }

    class _P:
        accession = "GSETEST"
        title = "Directed differentiation of H9 human embryonic stem cells"
        description = None
        overall_design = None

    def frames_for(
        cell_line,
        tissue,
        disease,
        age,
        sex,
        taxid="9606",
        organism="Homo sapiens",
        site="Peripheral Blood",
        tissue_source="cellosaurus",
    ):
        base = dict(
            Sample=["GSM1"],
            cell_line=[cell_line],
            tissue=[tissue],
            disease=[disease],
            age=[age],
            sex=[sex],
            ethnicity=["Caucasian"],
            sample_type=["Cell Line"],
            cell_type=[None],
            strain=[None],
            development_stage=[None],
            assay=[None],
            assay_category=[None],
        )
        out = pd.DataFrame(base)
        cel = pd.DataFrame(
            {
                **base,
                "organism": [organism],
                "taxid": [taxid],
                "tissue_primary_site": [site],
                "tissue_site_type": ["In situ"],
            }
        )
        ont = cel.copy()
        ont["tissue_ontology_id"] = "UBERON:0000178"
        prov = pd.DataFrame(
            {
                "Sample": ["GSM1"],
                "cell_line_source": ["cellosaurus"],
                "cell_line_original_source": ["llm_fallback"],
                "cell_line_original_column": [None],
                "tissue_source": [tissue_source],
                "disease_source": ["cellosaurus"],
                "age_source": ["cellosaurus"],
                "sex_source": ["cellosaurus"],
                "ethnicity_source": ["cellosaurus"],
                "sample_type_source": ["cellosaurus"],
                "organism_source": ["cellosaurus"],
                "assay_source": [None],
                "assay_column": [None],
                "cell_type_source": [None],
                "strain_source": [None],
                "development_stage_source": [None],
                "sex_column": [None],
                "cell_type_column": [None],
                "tissue_column": [None],
                "disease_column": [None],
                "age_column": [None],
                "strain_column": [None],
                "development_stage_column": [None],
                "cell_line_column": [None],
            }
        )
        return out, cel, prov, ont

    # 1. wrong_h9: hESC study, H9 resolved to the lymphoma line -> re-donate WA09.
    p = _P()
    p.samples = pd.DataFrame({"Sample": ["GSM1"], "source": ["H9 hESC, day 12"]})
    out, cel, prov, ont = frames_for(
        "H9", "Peripheral Blood", "Sezary Syndrome", "53Y", "Male"
    )
    (o2, c2, p2, n2), rows, st = apply_inline_guards(p, out, cel, prov, ont)
    assert st["h9"] == 1, st
    assert n2.at[0, "cell_line"] == "WA09" and n2.at[0, "sex"] == "Female", n2.iloc[0]
    assert n2.at[0, "age"] == "Blastocyst stage", n2.at[0, "age"]
    assert _blank(n2.at[0, "disease"]), n2.at[0, "disease"]
    assert _blank(n2.at[0, "ethnicity"]), n2.at[0, "ethnicity"]
    assert n2.at[0, "tissue"] == "Blastocyst", n2.at[0, "tissue"]
    assert n2.at[0, "tissue_primary_site"] == "Blastocyst"
    assert _blank(n2.at[0, "tissue_ontology_id"]), n2.at[0, "tissue_ontology_id"]
    assert p2.at[0, "cell_line_source"] == "corrected_source"
    assert all(len(r) == len(LOG_COLUMNS) for r in rows)
    assert {r[9] for r in rows} == {"wrong_h9"}, {r[9] for r in rows}

    # 2. a genuine Sezary study keeps CVCL_1240; the guard must not fire.
    q = _P()
    q.title = "Sezary syndrome cutaneous T-cell lymphoma H9 line"
    q.samples = pd.DataFrame({"Sample": ["GSM1"], "source": ["H9 lymphoma"]})
    out, cel, prov, ont = frames_for(
        "H9", "Peripheral Blood", "Sezary Syndrome", "53Y", "Male"
    )
    (_, _, _, n3), _, st = apply_inline_guards(q, out, cel, prov, ont)
    assert st["h9"] == 0 and n3.at[0, "cell_line"] == "H9", st

    # 3. no evidence either way -> never guess.
    r = _P()
    r.title = "A study"
    r.samples = pd.DataFrame({"Sample": ["GSM1"], "s": ["x"]})
    out, cel, prov, ont = frames_for(
        "H9", "Peripheral Blood", "Sezary Syndrome", "53Y", "Male"
    )
    (_, _, _, n4), _, st = apply_inline_guards(r, out, cel, prov, ont)
    assert st["h9"] == 0 and n4.at[0, "cell_line"] == "H9"

    # 4. placeholder: "N/A" as a cell line is not an identification.
    s = _P()
    s.title = "x"
    s.samples = pd.DataFrame({"Sample": ["GSM1"], "s": ["x"]})
    out, cel, prov, ont = frames_for("N/A", "Liver", None, None, None)
    (o5, c5, _, n5), rows5, st = apply_inline_guards(s, out, cel, prov, ont)
    assert st["placeholder"] == 1 and _blank(n5.at[0, "cell_line"])
    assert _blank(o5.at[0, "cell_line"]) and _blank(c5.at[0, "cell_line"])
    assert {r[9] for r in rows5} >= {"cell_line_placeholder"}
    for junk in ("WT", "wild type", "control", "n.a.", "Unknown", "-"):
        assert (
            _normalize_cell_line_name(junk) in CELL_LINE_PLACEHOLDERS or junk == "-"
        ), junk

    # 5. organism: the submitter says mouse, Cellosaurus said human.
    t = _P()
    t.title = "x"
    t.samples = pd.DataFrame(
        {"Sample": ["GSM1"], "Organism": ["Mus musculus"], "Taxon ID": ["10090"]}
    )
    out, cel, prov, ont = frames_for("K-562", "Blood", None, None, None)
    (_, c6, _, n6), rows6, st = apply_inline_guards(t, out, cel, prov, ont)
    assert st["organism"] == 1, st
    assert n6.at[0, "taxid"] == "10090" and n6.at[0, "organism"] == "Mus musculus"
    assert c6.at[0, "taxid"] == "10090"
    assert {r[6] for r in rows6 if r[9] == "wrong_organism"} == {"correct_source"}

    # 5b. A GEO frame states the organism by NAME and carries no taxid. Resolve
    #     it locally, or the whole GEO half stays (b) and needs the source-DB sweep.
    u = _P()
    u.title = "x"
    u.samples = pd.DataFrame({"Sample": ["GSM1"], "Organism": ["Mus musculus"]})
    out, cel, prov, ont = frames_for("K-562", "Blood", None, None, None)
    (_, _, _, n5b), _, st = apply_inline_guards(u, out, cel, prov, ont)
    assert st["organism"] == 1 and n5b.at[0, "taxid"] == "10090", st
    # 5c. an unrecognised organism name resolves to nothing and is left alone --
    #     never guess a taxid.
    u.samples = pd.DataFrame({"Sample": ["GSM1"], "Organism": ["Nothosaurus fake"]})
    out, cel, prov, ont = frames_for("K-562", "Blood", None, None, None)
    _, _, st = apply_inline_guards(u, out, cel, prov, ont)
    assert st["organism"] == 0, st

    # 5d. cell_line species mismatch, in scope: K-562 (pinned allowlist) on a
    # yeast sample (pinned safe-organism list). No xenograft ambiguity is
    # possible for either, so G7 must retract K-562 and what it donated.
    yeast = _P()
    yeast.title = "x"
    yeast.samples = pd.DataFrame(
        {
            "Sample": ["GSM1"],
            "Organism": ["Saccharomyces cerevisiae"],
            "Taxon ID": ["4932"],
        }
    )
    out, cel, prov, ont = frames_for(
        "K-562",
        "Pleural effusion",
        "Blast phase chronic myelogenous leukemia, BCR-ABL1 positive",
        None,
        None,
    )
    log_d = _Log("GSETEST")
    frames_d = {"output": out, "cello": cel, "provenance": prov, "ontology": ont}
    guard_h9(yeast, frames_d, log_d)
    guard_placeholder(yeast, frames_d, log_d)
    guard_organism(yeast, frames_d, log_d)
    n_species = guard_cell_line_species(yeast, frames_d, log_d)
    assert n_species == 1, n_species
    assert _blank(frames_d["ontology"].at[0, "cell_line"])
    assert _blank(frames_d["ontology"].at[0, "tissue"])
    assert _blank(frames_d["ontology"].at[0, "disease"])
    assert frames_d["ontology"].at[0, "taxid"] == "4932"
    assert frames_d["ontology"].at[0, "organism"] == "Saccharomyces cerevisiae"
    assert {r[9] for r in log_d.rows if r[3] == "cell_line"} == {
        "wrong_cell_line_species"
    }

    # 5e. HeLa/K-562/etc. xenografts into mice are a real experimental
    # design, so mouse stays off the safe-organism list even for a pinned
    # name.
    mono = _P()
    mono.title = "x"
    mono.samples = pd.DataFrame(
        {"Sample": ["GSM1"], "Organism": ["Mus musculus"], "Taxon ID": ["10090"]}
    )
    out, cel, prov, ont = frames_for("K-562", "Pleural effusion", None, None, None)
    log_e = _Log("GSETEST")
    frames_e = {"output": out, "cello": cel, "provenance": prov, "ontology": ont}
    guard_organism(mono, frames_e, log_e)
    n_species_mouse = guard_cell_line_species(mono, frames_e, log_e)
    assert n_species_mouse == 0, n_species_mouse
    assert frames_e["ontology"].at[0, "cell_line"] == "K-562"

    # 5f. matching species: K-562 on a human sample is never touched.
    human = _P()
    human.title = "x"
    human.samples = pd.DataFrame(
        {"Sample": ["GSM1"], "Organism": ["Homo sapiens"], "Taxon ID": ["9606"]}
    )
    out, cel, prov, ont = frames_for("K-562", "Pleural effusion", None, None, None)
    log_f = _Log("GSETEST")
    frames_f = {"output": out, "cello": cel, "provenance": prov, "ontology": ont}
    guard_organism(human, frames_f, log_f)
    n_species_ok = guard_cell_line_species(human, frames_f, log_f)
    assert n_species_ok == 0, n_species_ok

    # 5g. an ambiguous, unpinned name (E14: several real cell lines share the
    # key) never fires regardless of organism; only the pinned allowlist
    # triggers this guard.
    e14 = _P()
    e14.title = "x"
    e14.samples = pd.DataFrame(
        {"Sample": ["GSM1"], "Organism": ["Mus musculus"], "Taxon ID": ["10090"]}
    )
    out, cel, prov, ont = frames_for("E14", "Blastocyst", None, None, None)
    log_g = _Log("GSETEST")
    frames_g = {"output": out, "cello": cel, "provenance": prov, "ontology": ont}
    guard_organism(e14, frames_g, log_g)
    n_species_e14 = guard_cell_line_species(e14, frames_g, log_g)
    assert n_species_e14 == 0, n_species_e14
    assert frames_g["ontology"].at[0, "cell_line"] == "E14"

    # 5h. a value that has already diverged from the donation (e.g. a later
    # pipeline stage overwrote it without updating provenance) is left alone
    # even when the field's *_source still says cellosaurus.
    yeast2 = _P()
    yeast2.title = "x"
    yeast2.samples = pd.DataFrame(
        {
            "Sample": ["GSM1"],
            "Organism": ["Saccharomyces cerevisiae"],
            "Taxon ID": ["4932"],
        }
    )
    out, cel, prov, ont = frames_for("K-562", "Cancer", None, None, None)
    log_h = _Log("GSETEST")
    frames_h = {"output": out, "cello": cel, "provenance": prov, "ontology": ont}
    guard_organism(yeast2, frames_h, log_h)
    n_species_diverged = guard_cell_line_species(yeast2, frames_h, log_h)
    assert n_species_diverged == 1, n_species_diverged
    assert _blank(frames_h["ontology"].at[0, "cell_line"])
    assert frames_h["ontology"].at[0, "tissue"] == "Cancer"

    # 6. bad_label predicate: llm tissue "Hippocampus" vs CVCL site "Kidney".
    v = _P()
    v.title = "x"
    v.samples = pd.DataFrame({"Sample": ["GSM1"], "s": ["HEK293T"]})
    out, cel, prov, ont = frames_for(
        "HEK293T",
        "Hippocampus",
        None,
        None,
        None,
        site="Kidney",
        tissue_source="llm_primary",
    )
    _, _, st = apply_inline_guards(v, out, cel, prov, ont)
    assert st["bad_label"] == 1, st  # detected, not acted on
    out, cel, prov, ont = frames_for(
        "HEK293T",
        "Hippocampus",
        None,
        None,
        None,
        site="Kidney",
        tissue_source="llm_primary",
    )
    (o7, _, p7, n7), rows7, st = apply_inline_guards(
        v, out, cel, prov, ont, tierc_policy="legacy"
    )
    assert st["bad_label"] == 1 and _blank(n7.at[0, "cell_line"])
    assert _blank(o7.at[0, "cell_line"])
    assert p7.at[0, "cell_line_source"] == "corrected_null"
    assert {r[9] for r in rows7} == {"provenance_bad_label"}

    # 6b. matching site -> not a bad_label at all.
    out, cel, prov, ont = frames_for(
        "HEK293T",
        "Kidney Epithelium",
        None,
        None,
        None,
        site="Kidney",
        tissue_source="llm_primary",
    )
    _, _, st = apply_inline_guards(v, out, cel, prov, ont)
    assert st["bad_label"] == 0, st

    # 8. ENA declared columns: FILL where we said nothing.
    w = _P()
    w.title = "x"
    w.samples = pd.DataFrame(
        {
            "Sample": ["GSM1"],
            "ena_cell_type": ["human T cells"],
            "ena_tissue_type": ["blood"],
            "ena_sex": ["female"],
            "ena_age": ["6 to 12"],
        }
    )
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    (o8, _, p8, n8), rows8, st = apply_inline_guards(w, out, cel, prov, ont)
    assert st["declared_filled"] == 4, st
    assert n8.at[0, "cell_type"] == "human T cells" and n8.at[0, "tissue"] == "blood"
    assert n8.at[0, "sex"] == "female" and n8.at[0, "age"] == "6 to 12"
    assert o8.at[0, "cell_type"] == "human T cells"
    assert p8.at[0, "sex_source"] == "ena_declared"
    assert {r[9] for r in rows8} == {"ena_declared"}, {r[9] for r in rows8}

    # 8a2. ENA's own placeholders must not sneak a cell line back in.
    w.samples = pd.DataFrame(
        {
            "Sample": ["GSM1"],
            "ena_cell_line": ["WT"],
            "ena_cell_type": ["N/A"],
            "ena_strain": ["C57BL/6J"],
        }
    )
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    (_, _, _, n8a), _, st = apply_inline_guards(w, out, cel, prov, ont)
    assert st["declared_filled"] == 1, st  # strain only
    assert _blank(n8a.at[0, "cell_line"]) and _blank(n8a.at[0, "cell_type"])
    assert n8a.at[0, "strain"] == "C57BL/6J"

    # 8b. A NULL ENA column is abstention, not contradiction.
    w.samples = pd.DataFrame(
        {"Sample": ["GSM1"], "ena_cell_type": [None], "ena_disease": [""]}
    )
    out, cel, prov, ont = frames_for(
        "HeLa", "Cervix", "Cervical Adenocarcinoma", "30Y6M", "Female"
    )
    (_, _, _, n8b), _, st = apply_inline_guards(w, out, cel, prov, ont)
    assert (
        st["declared_filled"] == 0 and n8b.at[0, "disease"] == "Cervical Adenocarcinoma"
    )

    # 8c. the override: Cellosaurus supplied disease, ENA declares normal.
    w.samples = pd.DataFrame({"Sample": ["GSM1"], "ena_disease": ["normal"]})
    out, cel, prov, ont = frames_for(
        "HeLa", "Cervix", "Cervical Adenocarcinoma", "30Y6M", "Female"
    )
    (_, _, p8c, n8c), _, st = apply_inline_guards(w, out, cel, prov, ont)
    assert st["declared_override"] == 1, st
    assert (
        n8c.at[0, "disease"] == "normal"
        and p8c.at[0, "disease_source"] == "ena_declared"
    )

    # 8d. the same disagreement with a NON-cellosaurus provenance is left alone.
    out, cel, prov, ont = frames_for(
        "HeLa", "Cervix", "Cervical Adenocarcinoma", "30Y6M", "Female"
    )
    prov.at[0, "disease_source"] = "llm_primary"
    (_, _, _, n8d), _, st = apply_inline_guards(w, out, cel, prov, ont)
    assert st["declared_override"] == 0 and st["declared_conflict_kept"] == 1, st
    assert n8d.at[0, "disease"] == "Cervical Adenocarcinoma"

    # 8e. the matcher must not mint corrections out of synonym pairs.
    for a, b in (
        ("Healthy", "normal"),
        ("Sh-5Y", "SH-SY5Y"),
        ("Cd4+ T Cells", "CD4-positive, alpha-beta T cell"),
        ("WA09", "WA-09"),
        (None, "anything"),
        ("anything", None),
    ):
        assert values_agree(a, b), (a, b)
    assert not values_agree("HeLa", "Cryopreserved primary human hepatocytes")
    assert not values_agree("Juvenile", "adult")

    # 9. the INSDC triple: inside the bound -> keep the refinement.
    y = _P()
    y.title = "x"
    y.samples = pd.DataFrame(
        {
            "Sample": ["GSM1"],
            "insdc_library_strategy": ["RNA-Seq"],
            "insdc_library_source": ["TRANSCRIPTOMIC SINGLE CELL"],
        }
    )
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    ont["assay"] = "Smart-Seq2"
    cel["assay"] = "Smart-Seq2"
    out["assay"] = "Smart-Seq2"
    (_, _, _, n9), _, st = apply_inline_guards(y, out, cel, prov, ont)
    assert st["assay_declared_agree"] == 1 and n9.at[0, "assay"] == "Smart-Seq2", st

    # 9b. Declared RNA-Seq, enriched "Scrrbs" -> declaration wins.
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    for f in (out, cel, ont):
        f["assay"] = "Scrrbs"
        f["assay_category"] = "Epigenomics"
    (o9, _, p9, n9b), rows9, st = apply_inline_guards(y, out, cel, prov, ont)
    assert st["assay_declared_override"] == 1, st
    assert n9b.at[0, "assay"] == "RNA-Seq" and o9.at[0, "assay"] == "RNA-Seq"
    assert p9.at[0, "assay_source"] == "insdc_declared"
    old = [r for r in rows9 if r[3] == "assay" and r[2] == "ontology_samples"][0]
    assert old[4] == "Scrrbs" and old[5] == "RNA-Seq" and old[6] == "correct_source", (
        old
    )

    # 9c. a MIXED sample is never majority-voted; recorded, not acted on.
    z = _P()
    z.title = "x"
    z.samples = pd.DataFrame(
        {
            "Sample": ["GSM1"],
            "insdc_library_strategy": ["Bisulfite-Seq;RNA-Seq"],
            "insdc_conflict": ["1"],
        }
    )
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    for f in (out, cel, ont):
        f["assay"] = "Scrrbs"
    (_, _, _, n9c), _, st = apply_inline_guards(z, out, cel, prov, ont)
    # Matching one declared strategy is agreement for a mixed sample.
    assert st["assay_declared_agree"] == 1 and n9c.at[0, "assay"] == "Scrrbs", st
    # ...but an assay outside EVERY declared strategy of a mixed sample is left
    # alone. There is no single declaration for it to be outside of, and a
    # majority vote would erase the fact that the experiments disagree.
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    for f in (out, cel, ont):
        f["assay"] = "CUT&Tag"
    (_, _, _, n9c2), _, st = apply_inline_guards(z, out, cel, prov, ont)
    assert st["assay_conflict_kept"] == 1 and n9c2.at[0, "assay"] == "CUT&Tag", st

    # 9d. OTHER carries no constraint in either direction.
    q2 = _P()
    q2.title = "x"
    q2.samples = pd.DataFrame({"Sample": ["GSM1"], "insdc_library_strategy": ["OTHER"]})
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    for f in (out, cel, ont):
        f["assay"] = "CUT&Run"
    (_, _, _, n9d), _, st = apply_inline_guards(q2, out, cel, prov, ont)
    assert st["assay_declared_override"] == 0 and n9d.at[0, "assay"] == "CUT&Run"

    # 9e. an assay we cannot map to any strategy family is never overridden.
    out, cel, prov, ont = frames_for(None, None, None, None, None)
    for f in (out, cel, ont):
        f["assay"] = "Something Nobody Has Heard Of"
    (_, _, _, n9e), _, st = apply_inline_guards(y, out, cel, prov, ont)
    assert st["assay_unjudgeable"] == 1, st
    assert n9e.at[0, "assay"] == "Something Nobody Has Heard Of"

    # 7. the donated-field set IS the shared constant, not a copy that agrees
    # with it today. Identity, not equality: a copy is what stranded the ages.
    assert DONATED_FIELDS is CELLOSAURUS_DONATED_FIELDS

    print("enrich.guards self-check: all assertions passed")


if __name__ == "__main__":
    _demo()
