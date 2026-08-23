"""
Submitter-declared metadata, attached to a Project before enrichment.
"""

from __future__ import annotations

import copy
import json
import os
import sqlite3

import pandas as pd

from helpers import sqlite_connect

ENA_DB = os.environ.get("ENA_DB")
SRA_DB = os.environ.get("SRA_DB")

# ENA typed sample columns we consume, and the enriched field each informs. Keep
# the list to fields the extractor also produces so the two can be compared.
ENA_FIELD_MAP = {
    "cell_line": "cell_line",
    "cell_type": "cell_type",
    "tissue_type": "tissue",
    "tissue_lib": "tissue",
    "disease": "disease",
    "dev_stage": "development_stage",
    "strain": "strain",
    "sub_strain": "strain",
    "sex": "sex",
    "host_sex": "sex",
    "age": "age",
}
# Column order matters for the two-source fields: first non-blank wins, and the
# non-host / non-lib variant is the more specific one.
ENA_COLUMNS = (
    "cell_line",
    "cell_type",
    "tissue_type",
    "tissue_lib",
    "disease",
    "dev_stage",
    "strain",
    "sub_strain",
    "sex",
    "host_sex",
    "age",
)

# Prefix for raw declared values on the sample frame. Do not use enriched field
# names: extract chooses one source column per field, and a partly-NULL declared
# column would hijack that choice.
ENA_PREFIX = "ena_"

# Keep the triple under INSDC-prefixed names, not "library_strategy".
# archive_fields.apply_library_strategy seeds `assay` from that exact column;
# using it here would bypass the SEED_STRATEGIES boundary below.
TRIPLE_COLS = (
    "insdc_library_strategy",
    "insdc_library_source",
    "insdc_library_selection",
)
# The column archive_fields keys on. Written only for seeded rows.
SEED_COL = "library_strategy"
CONFLICT_COL = "insdc_conflict"
ANALYTE_COL = "insdc_analyte"  # library_source, verbatim
MODALITY_COL = "insdc_assay_modality"  # library_strategy, verbatim

SEED_STRATEGIES = {
    "WGS",
    "WXS",
    "WGA",
    "WCS",
    "AMPLICON",
    "Hi-C",
    "miRNA-Seq",
    "ncRNA-Seq",
    "RIP-Seq",
    "MNase-Seq",
    "DNase-Hypersensitivity",
    "Targeted-Capture",
    "RAD-Seq",
    "GBS",
    "Tn-Seq",
    "CLONE",
    "POOLCLONE",
    "SELEX",
    "FL-cDNA",
    "Synthetic-Long-Read",
    "CTS",
    "ChIA-PET",
    "FAIRE-seq",
    "EST",
}
UNINFORMATIVE = {"OTHER", "", None}

STRATEGY_KEYS = {
    "RNA-Seq": (
        "rna-seq",
        "rnaseq",
        "scrna",
        "snrna",
        "smart-seq",
        "smartseq",
        "10x",
        "cite-seq",
        "drop-seq",
        "seq-well",
        "cel-seq",
        "mars-seq",
        "sci-rna",
        "split-seq",
        "parse",
        "bd rhapsody",
        "spatial",
        "visium",
        "slide-seq",
        "quantseq",
        "quant-seq",
        "bulk rna",
        "trac",
        "perturb",
        "tomo-seq",
        "cage",
        "gro-seq",
        "pro-seq",
        "slam-seq",
        "net-seq",
        "tt-seq",
        "multiome",
        "microarray",
        "expression",
    ),
    "ssRNA-seq": ("rna-seq", "ssrna"),
    "miRNA-Seq": ("mirna", "small rna", "smrna"),
    "ncRNA-Seq": ("ncrna", "lncrna", "small rna"),
    "ChIP-Seq": (
        "chip-seq",
        "chipseq",
        "chip",
        "cut&",
        "cutand",
        "chip-chip",
        "dap-seq",
    ),
    "ATAC-seq": ("atac",),
    "DNase-Hypersensitivity": ("dnase", "dhs"),
    "MNase-Seq": ("mnase",),
    "Bisulfite-Seq": (
        "bisulfite",
        "wgbs",
        "rrbs",
        "snmc",
        "sn-m3c",
        "methyl",
        "medip",
        "brbs",
    ),
    "WGS": ("wgs", "whole genome", "genome seq", "scdna"),
    "WXS": ("wxs", "wes", "exome"),
    "Hi-C": ("hi-c", "hic", "hichip", "4c", "5c", "capture-c", "chia-pet", "micro-c"),
    "AMPLICON": ("amplicon", "16s", "18s", "its", "metabarcod"),
    "Targeted-Capture": ("targeted", "capture", "panel", "cite-seq", "adt"),
    "RIP-Seq": ("rip-seq", "clip", "par-clip", "irclip"),
    "RAD-Seq": ("rad-seq", "radseq"),
    "WGA": ("wga", "whole genome ampl"),
    "Tn-Seq": ("tn-seq", "tnseq", "transposon"),
    "GBS": ("gbs", "genotyping by sequencing"),
}

RNA_SOURCES = {
    "TRANSCRIPTOMIC",
    "TRANSCRIPTOMIC SINGLE CELL",
    "METATRANSCRIPTOMIC",
    "VIRAL RNA",
}
DNA_SOURCES = {"GENOMIC", "GENOMIC SINGLE CELL", "METAGENOMIC"}


def _blank(v) -> bool:
    return (
        v is None
        or (isinstance(v, float) and pd.isna(v))
        or str(v).strip() in ("", "-")
    )


def strategy_families(assay) -> set[str]:
    """Declared strategies an enriched assay string is compatible with."""
    a = (str(assay) if not _blank(assay) else "").lower()
    if not a:
        return set()
    return {fam for fam, keys in STRATEGY_KEYS.items() if any(k in a for k in keys)}


def analyte_of(sources) -> str | None:
    s = {x for x in sources if x}
    if not s:
        return None
    if s <= RNA_SOURCES:
        return "RNA"
    if s <= DNA_SOURCES:
        return "DNA"
    return None


class DeclaredSource:
    """Read-only view of the declared metadata for a study."""

    def __init__(self, ena_db: str | None = None, sra_db: str | None = None):
        ena_db = ena_db or ENA_DB
        sra_db = sra_db or SRA_DB
        self.ena = self._open(ena_db)
        self.sra = self._open(sra_db)
        self._secondary_map: dict[str, str] | None = None

    @staticmethod
    def _open(path):
        if not path:
            return None  # unset means unavailable, not an error
        try:
            return sqlite_connect(
                f"file:{path}?immutable=1", uri=True, check_same_thread=False
            )
        except sqlite3.Error:
            return None

    def resolve(self, accession: str) -> tuple[str | None, str | None]:
        """(bioproject, secondary_study) for a GSE / SRP / ERP / DRP / PRJ*."""
        if self.ena is None or not accession:
            return None, None
        if accession.startswith("PRJ"):
            return accession, None
        if accession.startswith(("GSE", "E-GEOD-")):
            r = self.ena.execute(
                "SELECT study_accession, secondary_study_accession FROM ena_studies "
                "WHERE geo_accession=?",
                (accession,),
            ).fetchone()
            return (r[0], r[1]) if r else (None, None)
        # SRP/ERP/DRP: secondary_study_accession is not indexed in the SQLite
        # build, so one scan builds the whole map and every later lookup is free.
        if self._secondary_map is None:
            self._secondary_map = {
                s: p
                for p, s in self.ena.execute(
                    "SELECT study_accession, secondary_study_accession FROM ena_studies "
                    "WHERE secondary_study_accession IS NOT NULL"
                )
            }
        return self._secondary_map.get(accession), accession

    def declared(self, accession: str) -> dict:
        """
        Returns {key: record} where key is a GSM / SRS-ERS-DRS / SAMN-SAMEA /
        SRX-ERX and record carries the ENA typed columns plus the per-sample set
        of INSDC triples.
        """
        prj, srp = self.resolve(accession)
        out: dict[str, dict] = {}
        if self.ena is None or prj is None:
            return out

        cols = ", ".join(ENA_COLUMNS)
        samples = self.ena.execute(
            f"SELECT sample_accession, secondary_sample_accession, sample_alias, {cols} "
            f"FROM ena_samples WHERE study_accession=?",
            (prj,),
        ).fetchall()
        by_samn: dict[str, dict] = {}
        for row in samples:
            samn, secondary, alias = row[0], row[1], row[2]
            rec = {
                ENA_PREFIX + c: (None if _blank(v) else str(v).strip())
                for c, v in zip(ENA_COLUMNS, row[3:])
            }
            rec["_samn"] = samn
            rec["_strategies"] = set()
            rec["_sources"] = set()
            rec["_selections"] = set()
            rec["_experiments"] = set()
            by_samn[samn] = rec
            for k in (samn, secondary, alias):
                if k:
                    out[k] = rec

        for samn, exp, strat, src, sel in self.ena.execute(
            "SELECT sample_accession, experiment_accession, library_strategy, "
            "library_source, library_selection FROM ena_experiments "
            "WHERE study_accession=?",
            (prj,),
        ):
            rec = by_samn.get(samn)
            if rec is None:
                continue
            rec["_experiments"].add(exp)
            if not _blank(strat):
                rec["_strategies"].add(str(strat).strip())
            if not _blank(src):
                rec["_sources"].add(str(src).strip())
            if not _blank(sel):
                rec["_selections"].add(str(sel).strip())
            if exp:
                out.setdefault(exp, rec)

        if any(r["_strategies"] for r in by_samn.values()):
            return out
        self._fill_from_sra(srp or accession, out)
        return out

    def _fill_from_sra(self, srp: str | None, out: dict) -> None:
        if self.sra is None or not srp or not srp.startswith(("SRP", "ERP", "DRP")):
            return
        for acc, strat, src, sel, samples_json in self.sra.execute(
            "SELECT accession, library_strategy, library_source, library_selection, "
            "samples FROM sra_experiments WHERE study=?",
            (srp,),
        ):
            try:
                srs_list = json.loads(samples_json) if samples_json else []
            except (TypeError, ValueError):
                srs_list = []
            if isinstance(srs_list, str):
                srs_list = [srs_list]
            for srs in srs_list:
                rec = out.get(srs)
                if rec is None:
                    continue
                rec["_experiments"].add(acc)
                for v, key in (
                    (strat, "_strategies"),
                    (src, "_sources"),
                    (sel, "_selections"),
                ):
                    if not _blank(v):
                        rec[key].add(str(v).strip())


_ID_COLS = (
    "Sample",
    "Accession",
    "sample_accession",
    "secondary_sample_accession",
    "Sample Alias",
    "experiment_accession",
)


def annotate(project, source: DeclaredSource | None = None, *, seed_assay: bool = True):
    """
    Return a copy of ``project`` carrying its declared columns.
    """
    stats = {
        "resolved": 0,
        "rows": 0,
        "ena_values": 0,
        "triple_rows": 0,
        "conflict_rows": 0,
        "assay_seeded": 0,
    }
    samples = getattr(project, "samples", None)
    if samples is None or not isinstance(samples, pd.DataFrame) or samples.empty:
        return project, stats
    src = source or DeclaredSource()
    table = src.declared(getattr(project, "accession", "") or "")
    if not table:
        return project, stats
    stats["resolved"] = 1

    keys = [c for c in _ID_COLS if c in samples.columns]
    frame = samples.copy()
    ena_cols = {ENA_PREFIX + c: [] for c in ENA_COLUMNS}
    strat, srcs, sels, analyte, modality, conflict, assay_seed = (
        [],
        [],
        [],
        [],
        [],
        [],
        [],
    )

    for _, row in frame.iterrows():
        rec = None
        for k in keys:
            v = row.get(k)
            if not _blank(v):
                rec = table.get(str(v).strip())
                if rec is not None:
                    break
        if rec is None:
            for c in ena_cols:
                ena_cols[c].append(None)
            strat.append(None)
            srcs.append(None)
            sels.append(None)
            analyte.append(None)
            modality.append(None)
            conflict.append(None)
            assay_seed.append(None)
            continue
        stats["rows"] += 1
        for c in ENA_COLUMNS:
            v = rec.get(ENA_PREFIX + c)
            ena_cols[ENA_PREFIX + c].append(v)
            if v:
                stats["ena_values"] += 1
        st, so, se = (
            sorted(rec["_strategies"]),
            sorted(rec["_sources"]),
            sorted(rec["_selections"]),
        )
        strat.append(";".join(st) or None)
        srcs.append(";".join(so) or None)
        sels.append(";".join(se) or None)
        modality.append(";".join(st) or None)
        analyte.append(";".join(so) or None)
        mixed = len(st) > 1 or len(so) > 1
        conflict.append("1" if mixed else None)
        if st:
            stats["triple_rows"] += 1
        if mixed:
            stats["conflict_rows"] += 1
        seed = None
        single_cell = any("SINGLE CELL" in x.upper() for x in so)
        if seed_assay and len(st) == 1 and st[0] in SEED_STRATEGIES and not single_cell:
            seed = st[0]
            stats["assay_seeded"] += 1
        assay_seed.append(seed)

    for c, vals in ena_cols.items():
        if any(v is not None for v in vals):
            frame[c] = vals
    for col, vals in (
        (TRIPLE_COLS[0], strat),
        (TRIPLE_COLS[1], srcs),
        (TRIPLE_COLS[2], sels),
        (ANALYTE_COL, analyte),
        (MODALITY_COL, modality),
        (CONFLICT_COL, conflict),
    ):
        if any(v is not None for v in vals):
            frame[col] = vals
    if any(v is not None for v in assay_seed):
        frame[SEED_COL] = assay_seed
        existing = next((c for c in frame.columns if c.lower() == "assay"), None)
        if existing is None:
            frame["assay"] = assay_seed
        else:
            fill = frame[existing].map(_blank)
            frame.loc[fill, existing] = pd.Series(assay_seed, index=frame.index)[fill]

    aug = copy.copy(project)
    aug.samples = frame
    return aug, stats
