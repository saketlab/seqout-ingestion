"""
Sample metadata enrichment pipeline.

Matches columns semantically, asks the LLM for unsettled fields, then applies
Cellosaurus post-processing and ontology mapping.
"""

import concurrent.futures
import json
import math
import os
import re
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from rapidfuzz import fuzz as _rfuzz
from rich.console import Console
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm
from typing_extensions import TypedDict

from enrich import cellosaurus as _cello
from enrich.prompts import PROMPTS

console = Console()
_FUZZ_THRESHOLD = 75  # rapidfuzz score threshold for column matching (0-100)
_EMBEDDING_MODELS = {}

load_dotenv()


# Resolved at call time by enrich.cellosaurus, which owns the env-var layering
# (CELLOSAURUS_LOOKUP / DISEASE_PRIMARY_TISSUE -> ONTOLOGY_DIR -> site default).
_ontology_path = _cello.ontology_path


# Metastatic/effusion sampling sites. For cancer cell lines, use the
# disease's primary organ.
METASTATIC_SITE_KEYWORDS = {
    "pleural effusion",
    "ascites",
    "pericardial effusion",
    "peritoneal effusion",
}
METASTATIC_SITE_PREFIXES = (
    "lymph node",
    "left supraclavicular lymph node",
    "right supraclavicular lymph node",
    "axillary lymph node",
    "cervical lymph node",
    "inguinal lymph node",
    "mesenteric lymph node",
)

# Known cell type terms that the LLM sometimes puts in cell_line by mistake
KNOWN_CELL_TYPES_NOT_LINES = {
    "monocytes",
    "macrophages",
    "fibroblasts",
    "neurons",
    "lymphocytes",
    "t cells",
    "b cells",
    "neutrophils",
    "dendritic cells",
    "epithelial cells",
    "endothelial cells",
    "astrocytes",
    "microglia",
    "keratinocytes",
    "cardiomyocytes",
    "hepatocytes",
    "chondrocytes",
    "osteoblasts",
    "osteoclasts",
    "platelets",
    "erythrocytes",
    "granulocytes",
    "nk cells",
    "mast cells",
    "adipocytes",
    "myocytes",
    "pericytes",
    "podocytes",
    "spermatocytes",
    "thymocytes",
    "melanocytes",
}


CELLOSAURUS_DONATED_FIELDS = _cello.CELLOSAURUS_DONATED_FIELDS
CELL_LINE_PLACEHOLDERS = _cello.CELL_LINE_PLACEHOLDERS


_normalize_cell_line_name = _cello.normalize_cell_line_name
load_cellosaurus = _cello.load_lookup
load_disease_primary_tissue = _cello.load_disease_primary_tissue


def _is_metastatic_tissue(tissue: str) -> bool:
    """Check if a tissue name indicates a metastatic/effusion sampling site."""
    t = tissue.lower().strip()
    if t in METASTATIC_SITE_KEYWORDS:
        return True
    for prefix in METASTATIC_SITE_PREFIXES:
        if t == prefix or t.startswith(prefix + ",") or t.startswith(prefix + " "):
            return True
    return False


def get_primary_tissue(disease: str) -> str | None:
    """
    Look up the primary organ for a disease.

    Metastatic-site hits trigger shorter disease-name prefix lookups, e.g.
    "blast phase CML" to "CML".
    """
    mapping = load_disease_primary_tissue()
    d = disease.lower().strip()
    result = mapping.get(d)
    if result and not _is_metastatic_tissue(result):
        return result

    # Try shorter prefixes, e.g. "blast phase chronic myelogenous ...".
    parts = d.split()
    for start in range(1, len(parts)):
        shorter = " ".join(parts[start:])
        candidate = mapping.get(shorter)
        if candidate and not _is_metastatic_tissue(candidate):
            return candidate

    # Also try stripping common qualifiers from the start
    for prefix in ("blast phase ", "childhood ", "adult ", "juvenile ", "infantile "):
        if d.startswith(prefix):
            candidate = mapping.get(d[len(prefix) :])
            if candidate and not _is_metastatic_tissue(candidate):
                return candidate

    return result  # return whatever we found (may be metastatic or None)


# The placeholder gate (F5) lives with the constant it reads.
cellosaurus_lookup = _cello.lookup


CELLOSAURUS_SPECIES_GUARD_REFUSED = "species_guard_refused"

# Sources for which _record_provenance keeps the pre-override source as
# {field}_original_source
_PROVENANCE_TWO_LEVEL_SOURCES = ("cellosaurus", CELLOSAURUS_SPECIES_GUARD_REFUSED)

# common names and binomials map to NCBI taxid, so a caller may pass either
# expected_taxid="10090" or expected_species="mouse" / "Mus musculus".
_SPECIES_TAXID = {
    "homo sapiens": "9606",
    "human": "9606",
    "mus musculus": "10090",
    "mouse": "10090",
    "house mouse": "10090",
    "rattus norvegicus": "10116",
    "rat": "10116",
    "cricetulus griseus": "10029",
    "chinese hamster": "10029",
    "mesocricetus auratus": "10036",
    "danio rerio": "7955",
    "zebrafish": "7955",
    "drosophila melanogaster": "7227",
    "fruit fly": "7227",
    "caenorhabditis elegans": "6239",
    "saccharomyces cerevisiae": "4932",
    "arabidopsis thaliana": "3702",
    "gallus gallus": "9031",
    "chicken": "9031",
    "sus scrofa": "9823",
    "pig": "9823",
    "bos taurus": "9913",
    "cattle": "9913",
    "cow": "9913",
    "ovis aries": "9940",
    "sheep": "9940",
    "oryctolagus cuniculus": "9986",
    "rabbit": "9986",
    "canis lupus familiaris": "9615",
    "canis familiaris": "9615",
    "dog": "9615",
    "felis catus": "9685",
    "cat": "9685",
    "macaca mulatta": "9544",
    "rhesus macaque": "9544",
    "macaca fascicularis": "9541",
    "chlorocebus aethiops": "9534",
    "pan troglodytes": "9598",
    "chimpanzee": "9598",
    "xenopus laevis": "8355",
    "equus caballus": "9796",
    "horse": "9796",
}


def _normalize_species(value) -> str | None:
    """Lowercase alphanumeric form of a species name or taxid, or None if blank."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()
    return text or None


def species_taxid(value) -> str | None:
    """NCBI taxid for a taxid string or a species/common name; None if unrecognised."""
    text = _normalize_species(value)
    if not text:
        return None
    if text.isdigit():
        return text
    return _SPECIES_TAXID.get(text)


def _split_species(value) -> list:
    """Split a species field into parts (Cellosaurus writes hybrids as 'A; B')."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, (list, tuple, set)):
        parts = [str(v) for v in value]
    else:
        parts = re.split(r"[;,/]", str(value))
    return [p.strip() for p in parts if p and p.strip()]


def _species_axes(*values) -> tuple:
    """Return (taxids, names) sets for the given species/taxid values."""
    taxids, names = set(), set()
    for value in values:
        for part in _split_species(value):
            norm = _normalize_species(part)
            if not norm:
                continue
            resolved = species_taxid(part)
            if resolved:
                taxids.add(resolved)
            if not norm.isdigit():
                names.add(norm)
    return taxids, names


_WARNED_SPECIES: set = set()


def _warn_unresolved_species(names) -> None:
    """Warn for unresolved species."""
    for name in sorted(names):
        if name in _WARNED_SPECIES:
            continue
        _WARNED_SPECIES.add(name)
        console.print(
            f"[yellow]Cellosaurus species gate: cannot resolve expected species "
            f"{name!r} to an NCBI taxid; the gate is inactive for it. Pass "
            f"expected_taxid instead, or add it to enrich.extract._SPECIES_TAXID.[/yellow]"
        )


def cellosaurus_species_conflict(
    entry,
    expected_taxid: str | None = None,
    expected_species: str | None = None,
) -> bool:
    """
    True when a Cellosaurus match has a different resolved organism.

    Conflict requires one taxid on each side.
    """
    if not entry:
        return False
    exp_taxids, exp_names = _species_axes(expected_taxid, expected_species)
    if not exp_taxids:
        if exp_names:
            _warn_unresolved_species(exp_names)
        return False
    got_taxids, _ = _species_axes(entry.get("taxid"), entry.get("species"))
    if not got_taxids:
        return False
    return exp_taxids.isdisjoint(got_taxids)


def apply_cellosaurus_entries(
    output_df,
    record_provenance,
    tissue_from_embedding_flags,
    expected_taxid: str | None = None,
    expected_species: str | None = None,
):
    """
    Fill/override fields from the Cellosaurus entry matching each row's cell_line.
    """
    cello_df = output_df.copy()
    cello_df["organism"] = None
    cello_df["taxid"] = None
    species_refused = set()

    for i, row in cello_df.iterrows():
        cell_line_val = row.get("cell_line")
        if not cell_line_val or (
            isinstance(cell_line_val, float) and pd.isna(cell_line_val)
        ):
            continue

        entry = cellosaurus_lookup(str(cell_line_val))
        if cellosaurus_species_conflict(entry, expected_taxid, expected_species):
            # Name collision across species. Refuse the override and leave the LLM
            # tissue and cell_type intact.
            species_refused.add(i)
            record_provenance(i, "cell_line", CELLOSAURUS_SPECIES_GUARD_REFUSED)
            entry = None
        if not entry:
            # cell line not in Cellosaurus, so restore the LLM values
            # that were blanked in the "clear cell_type/tissue" step above.
            for field in ("tissue", "cell_type"):
                current = row.get(field)
                if not current or (isinstance(current, float) and pd.isna(current)):
                    original = output_df.loc[i, field]
                    if original and not (
                        isinstance(original, float) and pd.isna(original)
                    ):
                        cello_df.loc[i, field] = original
            continue

        # Canonical cell line name
        cello_df.loc[i, "cell_line"] = entry["canonical_name"]
        record_provenance(i, "cell_line", "cellosaurus")

        # Age: fill from Cellosaurus if LLM didn't extract one
        if entry.get("age"):
            current_age = row.get("age")
            if not current_age or (
                isinstance(current_age, float) and pd.isna(current_age)
            ):
                cello_df.loc[i, "age"] = entry["age"]
                record_provenance(i, "age", "cellosaurus")

        # Sex: override from Cellosaurus; cell-line metadata is authoritative.
        if entry.get("sex"):
            cello_df.loc[i, "sex"] = entry["sex"]
            record_provenance(i, "sex", "cellosaurus")

        # Ethnicity: fill from Cellosaurus population if LLM didn't extract one
        if entry.get("population"):
            current_eth = row.get("ethnicity")
            if not current_eth or (
                isinstance(current_eth, float) and pd.isna(current_eth)
            ):
                cello_df.loc[i, "ethnicity"] = entry["population"]
                record_provenance(i, "ethnicity", "cellosaurus")

        # tissue: override unless it came from an embedding-matched column
        # (embedding match = metadata explicitly had a tissue column, so trust it)
        tissue_from_emb = tissue_from_embedding_flags.get(i, False)
        if entry.get("tissue"):
            # Also check: if current tissue value IS a cell line name, override it
            current_tissue = row.get("tissue")
            tissue_is_cell_line = (
                current_tissue and cellosaurus_lookup(str(current_tissue)) is not None
            )
            if not tissue_from_emb or not current_tissue or tissue_is_cell_line:
                cello_df.loc[i, "tissue"] = _title_case(entry["tissue"])
                record_provenance(i, "tissue", "cellosaurus")

        # Disease: override from Cellosaurus for cancer cell lines and empty disease.
        if entry.get("disease"):
            cello_df.loc[i, "disease"] = _title_case(entry["disease"])
            record_provenance(i, "disease", "cellosaurus")

        # Sample type: if Cellosaurus found it, it's a cell line
        cello_df.loc[i, "sample_type"] = "Cell Line"
        record_provenance(i, "sample_type", "cellosaurus")

        # Organism: species and taxonomy ID from Cellosaurus
        if entry.get("species"):
            cello_df.loc[i, "organism"] = entry["species"]
            record_provenance(i, "organism", "cellosaurus")
        if entry.get("taxid"):
            cello_df.loc[i, "taxid"] = entry["taxid"]
    return cello_df, species_refused


CANONICAL_ASSAY = {
    "rna-seq": "RNA-Seq",
    "rnaseq": "RNA-Seq",
    "mrna-seq": "RNA-Seq",
    "bulk rna-seq": "RNA-Seq",
    "total rna-seq": "RNA-Seq",
    "3' rna-seq": "RNA-Seq",
    "3' mrna-seq": "RNA-Seq",
    "cdna-seq": "RNA-Seq",
    "cdna": "RNA-Seq",
    "3' quantseq": "QuantSeq",
    "quantseq": "QuantSeq",
    "cage": "CAGE",
    "gro-seq": "GRO-Seq",
    "pro-seq": "PRO-Seq",
    "slam-seq": "SLAM-Seq",
    "net-seq": "NET-Seq",
    "tt-seq": "TT-Seq",
    "mirna-seq": "miRNA-Seq",
    "small rna-seq": "Small RNA-Seq",
    "smrna-seq": "Small RNA-Seq",
    "scrna-seq": "scRNA-Seq",
    "snrna-seq": "snRNA-Seq",
    "smart-seq2": "Smart-Seq2",
    "smartseq2": "Smart-Seq2",
    "smart-seq": "Smart-Seq",
    "smartseq": "Smart-Seq",
    "smartseq3": "Smart-Seq3",
    "sci-rna-seq": "sci-RNA-Seq",
    "cite-seq": "CITE-Seq",
    "multiome": "Multiome",
    "scdna-seq": "scDNA-Seq",
    "chip-seq": "ChIP-Seq",
    "chipseq": "ChIP-Seq",
    "chip-sequencing": "ChIP-Seq",
    "chip": "ChIP-Seq",
    "cut&tag": "CUT&Tag",
    "cut&run": "CUT&Run",
    "cutandtag": "CUT&Tag",
    "cutandrun": "CUT&Run",
    "dap-seq": "DAP-Seq",
    "damid": "DamID",
    "damid-seq": "DamID",
    "atac-seq": "ATAC-Seq",
    "atacseq": "ATAC-Seq",
    "scatac-seq": "scATAC-Seq",
    "snatac-seq": "snATAC-Seq",
    "faire-seq": "FAIRE-Seq",
    "mnase-seq": "MNase-Seq",
    "dnase-seq": "DNase-Seq",
    "methylation-seq": "Methylation-Seq",
    "methyl-seq": "Methylation-Seq",
    "methylc-seq": "Methylation-Seq",
    "bisulfite-seq": "Bisulfite-Seq",
    "bs-seq": "Bisulfite-Seq",
    "rrbs": "RRBS",
    "rrbs-seq": "RRBS",
    "wgbs": "WGBS",
    "whole-genome bisulfite sequencing": "WGBS",
    "scbs-seq": "scBS-Seq",
    "medip-seq": "MeDIP-Seq",
    "medipseq": "MeDIP-Seq",
    "dna methylation": "Methylation-Seq",
    "dna methylation profiling": "Methylation-Seq",
    "genomic dna methylation": "Methylation-Seq",
    "genomic dna methylation profiling": "Methylation-Seq",
    "hi-c": "Hi-C",
    "hic": "Hi-C",
    "micro-c": "Micro-C",
    "hichip": "HiChIP",
    "plac-seq": "PLAC-Seq",
    "4c-seq": "4C-Seq",
    "5c": "5C",
    "capture-c": "Capture-C",
    "clip-seq": "CLIP-Seq",
    "eclip": "eCLIP",
    "iclip": "iCLIP",
    "rip-seq": "RIP-Seq",
    "whole genome sequencing": "WGS",
    "whole-genome sequencing": "WGS",
    "genomic dna sequencing": "WGS",
    "genomic dna-seq": "WGS",
    "dna-seq": "WGS",
    "wxs": "WXS",
    "wes": "WXS",
    "whole exome sequencing": "WXS",
    "exome sequencing": "WXS",
    "exome-seq": "WXS",
    "targeted sequencing": "Targeted Sequencing",
    "amplicon-seq": "Amplicon-Seq",
    "amplicon sequencing": "Amplicon-Seq",
    "rad-seq": "RAD-Seq",
    "radseq": "RAD-Seq",
    "ddrad": "ddRAD-Seq",
    "ddrad-seq": "ddRAD-Seq",
    "gwas": "GWAS",
    "16s rrna gene sequencing": "16S rRNA Sequencing",
    "16s rrna": "16S rRNA Sequencing",
    "16s rrna-seq": "16S rRNA Sequencing",
    "16s": "16S rRNA Sequencing",
    "18s-seq": "18S rRNA Sequencing",
    "metagenome-seq": "Metagenomics",
    "metagenome sequencing": "Metagenomics",
    "metagenomic sequencing": "Metagenomics",
    "metagenomic-seq": "Metagenomics",
    "metagenome": "Metagenomics",
    "shotgun metagenome sequencing": "Metagenomics",
    "metatranscriptomics": "Metatranscriptomics",
    "metatranscriptome-seq": "Metatranscriptomics",
    "microarray": "Microarray",
    "expression array": "Expression Array",
    "gene expression array": "Expression Array",
    "expression microarray": "Expression Array",
    "dna microarray": "Microarray",
    "snp array": "SNP Array",
    "snp genotyping array": "SNP Array",
    "snp chip": "SNP Array",
    "affymetrix snp array": "SNP Array",
    "affymetrix snp 6.0": "SNP Array",
    "methylation array": "Methylation Array",
    "dna methylation array": "Methylation Array",
    "dna methylation microarray": "Methylation Array",
    "450k": "Methylation Array (450K)",
    "infinium 450k": "Methylation Array (450K)",
    "epic array": "Methylation Array (EPIC)",
    "infinium epic": "Methylation Array (EPIC)",
    "cgh": "Array CGH",
    "acgh": "Array CGH",
    "cgh array": "Array CGH",
    "array cgh": "Array CGH",
    "array-cgh": "Array CGH",
    "arraycgh": "Array CGH",
    "agilent cgh": "Array CGH",
    "oligo-array cgh": "Array CGH",
    "chip-chip": "ChIP-chip",
    "chip-on-chip": "ChIP-chip",
    "genotyping": "Genotyping Array",
    "protein array": "Protein Array",
    "cgh (comparative genomic hybridization)": "Array CGH",
    "cgh-array": "Array CGH",
    "dna-cgh": "Array CGH",
    "array-seq": "Microarray",
    "visium": "Visium",
    "10x visium": "Visium",
    "visium spatial gene expression": "Visium",
    "visium spatial transcriptomics": "Visium",
    "visium 10x": "Visium",
    "xenium": "Xenium",
    "xenium in situ gene expression": "Xenium",
    "xenium in situ": "Xenium",
    "visium hd": "Visium HD",
    "10x visium hd": "Visium HD",
    "visium hd spatial gene expression": "Visium HD",
    "cosmx": "CosMx SMI",
    "cosmx smi": "CosMx SMI",
    "nanostring cosmx": "CosMx SMI",
    "nanostring cosmx smi": "CosMx SMI",
    "geomx": "GeoMx DSP",
    "geomx dsp": "GeoMx DSP",
    "nanostring geomx": "GeoMx DSP",
    "nanostring geomx dsp": "GeoMx DSP",
    "geomx digital spatial profiling": "GeoMx DSP",
    "slide-seq": "Slide-Seq",
    "slide-seq2": "Slide-SeqV2",
    "slide-seqv2": "Slide-SeqV2",
    "slideseq": "Slide-Seq",
    "slideseq2": "Slide-SeqV2",
    "stereo-seq": "Stereo-Seq",
    "stereoseq": "Stereo-Seq",
    "merfish": "MERFISH",
    "seqfish": "seqFISH",
    "seqfish+": "seqFISH+",
    "spatial transcriptomics": "Spatial Transcriptomics",
    "seq-scope": "Seq-Scope",
    "seqscope": "Seq-Scope",
    "hdst": "HDST",
    "dbit-seq": "DBiT-Seq",
    "pixel-seq": "Pixel-Seq",
    "open-st": "Open-ST",
    "curio seeker": "Curio Seeker",
    "codex": "CODEX",
    "phenocycler": "PhenoCycler",
    "codex/phenocycler": "PhenoCycler",
    "resolve biosciences": "Molecular Cartography",
    "molecular cartography": "Molecular Cartography",
    "eelfish": "EEL FISH",
    "osmfish": "osmFISH",
    "starmapplus": "STARmap PLUS",
    "starmap": "STARmap",
    "cel-seq": "CEL-Seq",
    "cel-seq2": "CEL-Seq2",
    "celseq2": "CEL-Seq2",
    "mars-seq": "MARS-Seq",
    "drop-seq": "Drop-Seq",
    "dropseq": "Drop-Seq",
    "droplet-seq": "Drop-Seq",
    "indrops": "inDrops",
    "indrop": "inDrops",
    "indrops-2": "inDrops-2",
    "indrops2": "inDrops-2",
    "sci-seq": "sci-Seq",
    "sci-atac-seq": "sci-ATAC-Seq",
    "sn-atac-seq": "snATAC-Seq",
    "sccite-seq": "scCITE-Seq",
    "scwgs": "scWGS",
    "single-cell rna-seq": "scRNA-Seq",
    "3' scrnaseq": "scRNA-Seq 3'",
    "3' scrna-seq": "scRNA-Seq 3'",
    "5' scrna-seq": "scRNA-Seq 5'",
    "scrna-seq 3' v3": "scRNA-Seq 3' v3",
    "scrna-seq 3' v2": "scRNA-Seq 3' v2",
    "scrna-seq 3' v2/v3": "scRNA-Seq 3' v2/v3",
    "pip-seq": "PIPseq",
    "pipseq": "PIPseq",
    "pip-seq v2": "PIPseq v2",
    "pipseq v2": "PIPseq v2",
    "pip-seq v3": "PIPseq v3",
    "pipseq v3": "PIPseq v3",
    "pip-seq v10": "PIPseq v10",
    "pip-seq v20": "PIPseq v20",
    "pipseq v20": "PIPseq v20",
    "parse": "Parse Biosciences",
    "parse biosciences": "Parse Biosciences",
    "evercode": "Parse Biosciences Evercode",
    "evercode wt": "Parse Biosciences Evercode WT",
    "parse biosciences evercode wt": "Parse Biosciences Evercode WT",
    "evercode wt mini": "Parse Biosciences Evercode WT Mini",
    "parse biosciences evercode wt mini": "Parse Biosciences Evercode WT Mini",
    "evercode wt mega": "Parse Biosciences Evercode WT Mega",
    "parse biosciences evercode wt mega": "Parse Biosciences Evercode WT Mega",
    "evercode wt penta": "Parse Biosciences Evercode WT Penta",
    "parse biosciences evercode wt penta": "Parse Biosciences Evercode WT Penta",
    "evercode tcr": "Parse Biosciences Evercode TCR",
    "parse biosciences evercode tcr": "Parse Biosciences Evercode TCR",
    "evercode bcr": "Parse Biosciences Evercode BCR",
    "parse biosciences evercode bcr": "Parse Biosciences Evercode BCR",
    "evercode fixation": "Parse Biosciences Evercode Fixation",
    "parse biosciences evercode fixation": "Parse Biosciences Evercode Fixation",
    "evercode ffpe": "Parse Biosciences Evercode FFPE",
    "parse biosciences evercode ffpe": "Parse Biosciences Evercode FFPE",
    "gene select": "Parse Biosciences Gene Select",
    "parse biosciences gene select": "Parse Biosciences Gene Select",
    "crispr detect": "Parse Biosciences CRISPR Detect",
    "parse biosciences crispr detect": "Parse Biosciences CRISPR Detect",
    "scale biosciences": "Scale Biosciences",
    "scale bio": "Scale Biosciences",
    "scaleplex": "Scale Biosciences ScalePlex",
    "bd rhapsody": "BD Rhapsody",
    "bd rhapsody v1": "BD Rhapsody v1",
    "bd-rhapsody-v1": "BD Rhapsody v1",
    "bd rhapsody eb": "BD Rhapsody Enhanced Beads",
    "bd-rhapsody-eb": "BD Rhapsody Enhanced Beads",
    "rhapsody": "BD Rhapsody",
    "microwell-seq": "Microwell-Seq",
    "seq-well": "Seq-Well",
    "seqwell": "Seq-Well",
    "split-seq": "SPLiT-Seq",
    "splitseq": "SPLiT-Seq",
    "split-pool": "SPLiT-Seq",
    "tapestri": "Tapestri",
    "quartz-seq": "Quartz-Seq",
    "quartzseq": "Quartz-Seq",
    "quartz-seq2": "Quartz-Seq2",
    "quartzseq2": "Quartz-Seq2",
    "strt-seq": "STRT-Seq",
    "strt-seq-2i": "STRT-Seq-2i",
    "strt-seq-c1": "STRT-Seq-C1",
    "vasa-seq": "VASA-Seq",
    "vasa-seq-drop": "VASA-Seq-drop",
    "mcscrb-seq": "mcSCRB-Seq",
    "scrbseq": "mcSCRB-Seq",
    "scrb-seq": "mcSCRB-Seq",
    "surecell": "SureCell",
    "scifi-rna-seq": "scifi-RNA-Seq",
    "scifiseq": "scifi-RNA-Seq",
    "sci-rna-seq3": "sci-RNA-Seq3",
    "issaac-seq": "ISSAAC-Seq",
    "dogmaseq": "DOGMAseq",
    "dogmaseq-dig": "DOGMAseq-DIG",
    "dogmaseq-lll": "DOGMAseq-LLL",
    "dogma-seq": "DOGMAseq",
    "snmct-seq": "snmCT-Seq",
    "sn-m3c-seq": "sn-m3C-Seq",
    "snm3c-seq": "sn-m3C-Seq",
    "pi-atac-seq": "Pi-ATAC-Seq",
    "sugarseq": "SugarSeq",
    "sugar-seq": "SugarSeq",
    "merscope": "MERSCOPE",
    "nanopore sequencing": "Nanopore Sequencing",
    "nanopore": "Nanopore Sequencing",
    "ont": "Nanopore Sequencing",
    "pacbio": "PacBio Sequencing",
    "smrt sequencing": "PacBio Sequencing",
    "merip-seq": "MeRIP-Seq",
    "m6a-seq": "MeRIP-Seq",
    "m6a-ip-seq": "MeRIP-Seq",
    "chec-seq": "ChEC-Seq",
    "eclip-seq": "eCLIP",
    "iclip-seq": "iCLIP",
    "par-clip": "PAR-CLIP",
    "hits-clip": "HITS-CLIP",
    "cut&run-seq": "CUT&Run",
    "cut & run": "CUT&Run",
    "cut&tag-seq": "CUT&Tag",
    "cut&tag-sequencing": "CUT&Tag",
    "chic": "ChIC",
    "tn5-seq": "ATAC-Seq",
    "ribo-seq": "Ribo-Seq",
    "ribosome profiling": "Ribo-Seq",
    "cage-seq": "CAGE",
    "totalrna-seq": "RNA-Seq",
    "polya rna-seq": "RNA-Seq",
    "smallrna-seq": "Small RNA-Seq",
    "srna-seq": "Small RNA-Seq",
    "csrna-seq": "Small RNA-Seq",
    "gdna-seq": "WGS",
    "genome sequencing": "WGS",
    "ngs": "WGS",
    "crispr screen": "CRISPR Screen",
    "shrna-seq": "shRNA Screen",
    "tcr-seq": "TCR-Seq",
    "bcr-seq": "BCR-Seq",
    "3'-seq": "3'-Seq",
    "3' end sequencing": "3'-Seq",
    "3'-tagseq": "Tag-Seq",
    "3' sage-seq": "SAGE",
    "icshape": "icSHAPE",
    "chirp-seq": "ChIRP-Seq",
    "chart-seq": "CHART-Seq",
    "char-seq": "ChAR-Seq",
    "clip": "CLIP-Seq",
    "mbd-seq": "MBD-Seq",
    "hic-seq": "Hi-C",
    "schi-c": "scHi-C",
    "bisulfite sequencing": "Bisulfite-Seq",
    "dna methylation-seq": "Methylation-Seq",
    "chromatin rna-seq": "RNA-Seq",
    "cut&runseq": "CUT&Run",
    "snmultiome": "snMultiome",
    "snmultiome-seq": "snMultiome",
    "scmultiome-seq": "Multiome",
    "scmultiome atac + gene expression": "Multiome",
    "scmultiome-atac + gene expression": "Multiome",
    "scmultiome-atac+gene": "Multiome",
    "scmultiome": "Multiome",
    "share-seq": "SHARE-Seq",
    "shareseq": "SHARE-Seq",
    "share-seq2": "SHARE-Seq2",
    "paired-seq": "Paired-Seq",
    "snare-seq": "SNARE-Seq",
    "snare-seq2": "SNARE-Seq2",
    "scrna-seq 3'gex v3": "scRNA-Seq 3' v3",
    "dna copy number profiling": "Array CGH",
    "pars-seq": "PARS-Seq",
    "at-seq": "AT-Seq",
    "smart-3seq": "Smart-3SEQ",
    "rrna-seq": "rRNA-Seq",
    "3'utr-seq": "3'UTR-Seq",
    "nrna-seq": "nRNA-Seq",
    "3' 3ʹ dge": "3' DGE",
    "3' 3' dge": "3' DGE",
    "nascent rna-seq": "Nascent RNA-Seq",
    "circular rna-seq": "circRNA-Seq",
    "bruuv-seq": "BruUV-Seq",
    "nucrna-seq": "Nuclear RNA-Seq",
    "bulkrna-seq": "RNA-Seq",
    "bru-seq": "Bru-Seq",
    "5' rna-seq": "RNA-Seq",
    "3'reads": "3'-Seq",
    "ncut&tag": "nCUT&Tag",
    "genomic dna damid": "DamID",
    "recut&run": "reCUT&Run",
    "chip-exo": "ChIP-exo",
    "chic-seq": "ChIC",
    "genomic dna pulldown": "ChIP-Seq",
    "damidseq-dear": "DamID",
    "c&run": "CUT&Run",
    "nucleosome mapping": "MNase-Seq",
    "genomic dna starr-seq": "STARR-Seq",
    "mpra": "MPRA",
    "starr-seq": "STARR-Seq",
    "m3c-ip-seq": "m3C-Seq",
    "capture hi-c": "Capture Hi-C",
    "snhi-c": "snHi-C",
    "chicago": "Capture Hi-C",
    "whole genome bisulfite sequencing": "WGBS",
    "bisulfite-sequencing": "Bisulfite-Seq",
    "whole genome bisulfite-seq": "WGBS",
    "mh-seq": "MH-Seq",
    "dna methylation analysis": "Methylation-Seq",
    "genomic dna methylation-seq": "Methylation-Seq",
    "dna methylation sequencing": "Methylation-Seq",
    "gbs": "GBS",
    "scvdj-seq": "scVDJ-Seq",
    "iso-seq": "Iso-Seq",
    "ont wgs": "Nanopore Sequencing",
    "pacbio hifi": "PacBio HiFi",
    "pacbio ccs": "PacBio HiFi",
    "18s rrna sequencing": "18S rRNA Sequencing",
    "shotgun sequencing": "Metagenomics",
    "genomic dna hybridization": "Microarray",
    "nimblegen": "Microarray",
    "genomic dna array": "Microarray",
    "m1a-seq": "m1A-Seq",
    "pseudo-seq": "Pseudo-Seq",
    "rpf": "Ribo-Seq",
    "stem-seq": "STEM-Seq",
    "schi-c and scrna-seq": "scHi-C + scRNA-Seq",
    "c&run-atac-seq": "CUT&Run + ATAC-Seq",
    "smart-seq+5'": "Smart-Seq",
    "scnt-seq": "SCNT-Seq",
    "matq-seq": "MATQ-Seq",
    "smart-seq v4 ultra low input rna kit for sequencing": "Smart-Seq",
    "smart-seq v4": "Smart-Seq",
    "scirna-seq": "sci-RNA-Seq",
    "sci-plex sci-rna-seq": "sci-RNA-Seq",
    "sci-3' v3": "sci-RNA-Seq3",
    "sbliss-seq": "sBLISS",
    "digenome-seq": "Digenome-Seq",
    "crispra-screen": "CRISPRa Screen",
    "shrna screen": "shRNA Screen",
    "pacbio sequencing": "PacBio Sequencing",
    "3'mrna-seq": "RNA-Seq",
    "3c-seq": "3C-Seq",
    "chap-seq": "ChAP-Seq",
    "chap": "ChAP-Seq",
    "ct-seq": "CT-Seq",
    "nanopore-sequencing": "Nanopore Sequencing",
    "pacbio rs ii": "PacBio Sequencing",
    "sci-plex rna-seq": "sci-RNA-Seq",
    "scnanorna-seq": "snRNA-Seq",
    "spatial gene expression": "Spatial Transcriptomics",
    "vdj": "scVDJ-Seq",
    "pro-cap": "PRO-Cap",
    "kas-seq": "KAS-Seq",
    "imargi": "iMARGI",
    "nanti-cage": "NANTI-CAGE",
    "replication timing by brdu-seq": "Repli-Seq",
    "repli-seq": "Repli-Seq",
    "mtdna-seq": "WGS",
    "mirna sequencing": "miRNA-Seq",
    "microrna sequencing": "miRNA-Seq",
    "microrna-seq": "miRNA-Seq",
    "micro rna-seq": "miRNA-Seq",
    "small rna sequencing": "Small RNA-Seq",
    "mrna sequencing": "RNA-Seq",
    "total rna sequencing": "RNA-Seq",
    "rna sequencing": "RNA-Seq",
    "transcriptome sequencing": "RNA-Seq",
    "single cell rna-seq": "scRNA-Seq",
    "single-cell rna sequencing": "scRNA-Seq",
    "single cell rna sequencing": "scRNA-Seq",
    "single-nucleus rna-seq": "snRNA-Seq",
    "single nucleus rna-seq": "snRNA-Seq",
    "single-nucleus rna sequencing": "snRNA-Seq",
    "atac": "ATAC-Seq",
    "scatac": "scATAC-Seq",
    "snatac": "snATAC-Seq",
    "single-cell atac": "scATAC-Seq",
    "single-cell atac-seq": "scATAC-Seq",
    "single-nucleus atac": "snATAC-Seq",
    "single-nucleus atac-seq": "snATAC-Seq",
    "bulk atac-seq": "ATAC-Seq",
    "16s sequencing": "16S rRNA Sequencing",
    "16s rrna sequencing": "16S rRNA Sequencing",
    "16s metagenomics": "16S rRNA Sequencing",
    "16s amplicon sequencing": "16S rRNA Sequencing",
    "shotgun metagenomics": "Metagenomics",
    "long read sequencing": "Long-read Sequencing",
    "long-read sequencing": "Long-read Sequencing",
    "oxford nanopore": "Nanopore Sequencing",
    "oxford nanopore sequencing": "Nanopore Sequencing",
    "minion": "Nanopore Sequencing",
    "promethion": "Nanopore Sequencing",
    "smrt": "PacBio Sequencing",
    "hifi": "PacBio HiFi",
    "hifi sequencing": "PacBio HiFi",
    "pacbio hifi sequencing": "PacBio HiFi",
    "methylation sequencing": "Methylation-Seq",
    "chip sequencing": "ChIP-Seq",
    "chromatin immunoprecipitation sequencing": "ChIP-Seq",
    "chromatin immunoprecipitation": "ChIP-Seq",
    "ncrna-seq": "ncRNA-Seq",
    "ncrna sequencing": "ncRNA-Seq",
    "non-coding rna-seq": "ncRNA-Seq",
    "wxs (exome)": "WXS",
    "targeted capture": "Targeted Capture",
    "tiling array": "Tiling Array",
    "est": "EST",
    "expressed sequence tag": "EST",
    "ssrna-seq": "ssRNA-Seq",
    "strand-specific rna-seq": "ssRNA-Seq",
    "tn-seq": "Tn-Seq",
    "tnseq": "Tn-Seq",
    "genotyping-by-sequencing": "GBS",
    "chia-pet": "ChIA-PET",
    "nome-seq": "NOMe-Seq",
    "mre-seq": "MRE-Seq",
    "dip-seq": "DIP-Seq",
    "rip-chip": "RIP-chip",
    "mpss": "MPSS",
    "tcc": "TCC",
    "cts": "CTS",
    "inverse rrna": "Inverse rRNA",
    "clone sequencing": "Clone Sequencing",
    "clone end sequencing": "Clone Sequencing",
    "pooled clone sequencing": "Clone Sequencing",
    "synthetic long read": "Synthetic Long Read",
    "full-length cdna": "Full-length cDNA",
    "whole chromosome sequencing": "WGS",
    "ncrna microarray": "ncRNA Microarray",
    "rnai microarray": "RNAi Microarray",
    "finishing": "Finishing",
    "selex": "SELEX",
}


# Regex-based fallback normalization for patterns not caught by exact lookup
def _normalize_10x(value: str) -> str:
    """
    Parse 10x Genomics assay strings, preserving direction (3'/5') and version.
    """
    s_lower = value.lower()

    # Pre-extract version and direction once (used by all branches)
    ver_m = re.search(r"v\s*(\d+(?:\.\d+)?)", value, re.IGNORECASE)
    version = f"v{ver_m.group(1)}" if ver_m else None
    ver_suffix = f" {version}" if version else ""

    direction_m = re.search(r"([35])\s*[''ʹ]", value)
    direction = f"{direction_m.group(1)}'" if direction_m else None

    # Detect special products first
    if (
        "multiome" in s_lower
        or "coassay" in s_lower
        or "atac + gene" in s_lower
        or "atac+gene" in s_lower
    ):
        return f"10x Multiome{ver_suffix}"
    if "visium" in s_lower:
        hd = " HD" if "hd" in s_lower else ""
        extra = " FFPE" if "ffpe" in s_lower else ""
        return f"10x Visium{hd}{extra}{ver_suffix}"
    if "ffpe" in s_lower or "fixed rna" in s_lower:
        return "10x Fixed RNA Profiling"
    if re.search(r"\batac\b", s_lower):
        return f"10x ATAC{ver_suffix}"
    if "immune profiling" in s_lower:
        d = f" {direction}" if direction else " 5'"
        return f"10x{d} Immune Profiling"

    # Detect modality prefix (snRNA-Seq, snATAC-Seq, etc.)
    modality = None
    if re.search(r"\bsn\s*rna|nuclei|nucleus", s_lower):
        modality = "snRNA-Seq"
    elif re.search(r"\bsn\s*atac", s_lower):
        modality = "snATAC-Seq"

    parts = ["10x"]
    if direction:
        parts.append(direction)
    if version:
        parts.append(version)
    if modality:
        parts.append(modality)

    return " ".join(parts) if len(parts) > 1 else "10x"


_10X_PATTERN = re.compile(
    r"(?:10x|chromium|nextgem|next\s*gem)",
    re.IGNORECASE,
)

_ASSAY_REGEX_MAP = [
    # Order matters: more specific patterns first
    (
        re.compile(
            r"(?:genomic dna |dna )?(?:immunoprecipitation|chip).*(?:microarray|array|tiling)",
            re.IGNORECASE,
        ),
        "ChIP-chip",
    ),
    (
        re.compile(
            r"(?:genomic dna |dna )?(?:immunoprecipitation|chip).*(?:seq|sequencing)",
            re.IGNORECASE,
        ),
        "ChIP-Seq",
    ),
    (
        re.compile(r"(?:genomic dna |dna )?microarray|tiling array", re.IGNORECASE),
        "Microarray",
    ),
    (
        re.compile(
            r"(?:genomic dna |dna )?(?:extraction|isolation|library prep|labeling)",
            re.IGNORECASE,
        ),
        None,
    ),  # non-assay value
    (
        re.compile(r"^genomic dna$", re.IGNORECASE),
        None,
    ),  # bare "Genomic DNA" names a material
    (re.compile(r"affymetrix.*(?:snp|genotyp)", re.IGNORECASE), "SNP Array"),
    (re.compile(r"affymetrix.*(?:tiling|array)", re.IGNORECASE), "Microarray"),
    (re.compile(r"agilent.*(?:cgh|array)", re.IGNORECASE), "Array CGH"),
    (
        re.compile(r"illumina.*(?:450k?|epic|methylation)", re.IGNORECASE),
        "Methylation Array",
    ),
    (re.compile(r"illumina.*(?:beadchip|infinium|snp)", re.IGNORECASE), "SNP Array"),
    # 10x handled by _normalize_10x(); see _normalize_assay()
    (re.compile(r"smarter.*(?:single.cell|scrna|sc rna)", re.IGNORECASE), "scRNA-Seq"),
    (re.compile(r"smarter.*(?:stranded|total).*rna", re.IGNORECASE), "RNA-Seq"),
    (re.compile(r"nugen.*ovation", re.IGNORECASE), "RNA-Seq"),
    # Genomic DNA plus specific assay resolves to the assay.
    (re.compile(r"genomic dna\s+cut\s*&\s*run", re.IGNORECASE), "CUT&Run"),
    (re.compile(r"genomic dna\s+cut\s*&\s*tag", re.IGNORECASE), "CUT&Tag"),
    (re.compile(r"genomic dna\s+(?:atac|tn5)", re.IGNORECASE), "ATAC-Seq"),
    (re.compile(r"genomic dna\s+(?:mnase|nuclease)", re.IGNORECASE), "MNase-Seq"),
    (re.compile(r"genomic dna\s+nimblegen\s*(?:array)?", re.IGNORECASE), "Microarray"),
    (re.compile(r"genomic dna\s+(?:ip|immunoprecip)", re.IGNORECASE), "ChIP-Seq"),
    (
        re.compile(
            r"genomic dna\s+(?:shear|sonicati|digest|ligat|purif|pcr)", re.IGNORECASE
        ),
        None,
    ),  # sample prep
    (
        re.compile(r"affymetrix.*(?:cytoscan|genechip|genome.wide)", re.IGNORECASE),
        "Microarray",
    ),
    (re.compile(r"agilent.*(?:sureprint|sureselect)", re.IGNORECASE), "Microarray"),
    (re.compile(r"visium", re.IGNORECASE), "Visium"),
    (
        re.compile(r"(?:qrt-pcr|rt-pcr|qpcr|real.time pcr)", re.IGNORECASE),
        None,
    ),  # sequencing-control value
    (re.compile(r"western blot", re.IGNORECASE), None),  # rejected value
    (
        re.compile(r"whole.?genome.?amplif|^wga$", re.IGNORECASE),
        None,
    ),  # sample prep
    (
        re.compile(r"^(?:input|igg|control)$", re.IGNORECASE),
        None,
    ),  # ChIP control value
    (
        re.compile(r"^(?:enhancer|promoter)$", re.IGNORECASE),
        None,
    ),  # genomic feature
    (
        re.compile(r"^(?:enrich|enrichment)$", re.IGNORECASE),
        None,
    ),  # ambiguous value
    (
        re.compile(r"^(?:s\d+p|h3k\d+|ab\d+)", re.IGNORECASE),
        None,
    ),  # antibody/histone target
]


def _get_embedding_model(model_name: str, *, verbose: bool = True):
    global _EMBEDDING_MODELS
    if model_name not in _EMBEDDING_MODELS:
        if verbose:
            console.print("Setting up embedding model...")
        hf_token = os.getenv("HF_TOKEN")
        if hf_token:
            os.environ.setdefault("HF_TOKEN", hf_token)
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
        _EMBEDDING_MODELS[model_name] = SentenceTransformer(model_name)
        if verbose:
            console.print("Embedding model set up complete")
    return _EMBEDDING_MODELS[model_name]


def _fuzz_match_keys(
    term: str,
    keys: list[str],
    threshold: int = _FUZZ_THRESHOLD,
) -> list[tuple[str, float]]:
    """
    Return [(key, score)] for keys with rapidfuzz token_sort_ratio >= threshold.

    Score is normalized to 0-1 range (from rapidfuzz's 0-100) for backward
    compatibility with the provenance tracking that expects cosine-sim-like scores.
    """
    if not keys:
        return []
    results = []
    term_lower = term.lower()
    for key in keys:
        score = _rfuzz.token_sort_ratio(term_lower, key.lower())
        if score >= threshold:
            results.append((key, score / 100.0))
    return results


def _title_case(value: str) -> str:
    """Title-case that handles apostrophes and hyphens correctly."""
    return re.sub(
        r"[A-Za-z]+('[A-Za-z]+)?",
        lambda m: m.group(0).capitalize(),
        value,
    )


def _normalize_assay(value: str) -> str | None:
    """
    Canonical casing for assay names.

    Exact known values win, then 10x normalization, regex variants and title case.
    Returns None for non-assay values.
    """
    canonical = CANONICAL_ASSAY.get(value.lower())
    if canonical:
        return canonical
    # 10x Genomics: dedicated parser preserves chemistry details
    if _10X_PATTERN.search(value):
        return _normalize_10x(value)
    # Regex fallback for complex patterns (e.g., "Genomic DNA ChIP-Seq")
    for pattern, replacement in _ASSAY_REGEX_MAP:
        if pattern.search(value):
            return replacement  # None means outside the assay vocabulary.
    return _title_case(value)


_ASSAY_CATEGORIES_CSV = Path(__file__).with_name("assay_categories.csv")


def _load_assay_categories() -> dict[str, str]:
    """Load assay-to-category mapping from CSV file."""
    import csv as _csv

    mapping: dict[str, str] = {}
    if not _ASSAY_CATEGORIES_CSV.exists():
        return mapping
    with open(_ASSAY_CATEGORIES_CSV, newline="") as f:
        for row in _csv.DictReader(f):
            mapping[row["assay"]] = row["category"]
    return mapping


ASSAY_CATEGORY: dict[str, str] = _load_assay_categories()


def _get_assay_category(normalized_assay: str | None) -> str | None:
    """
    Look up the broad category for a normalized assay name.
    """
    if not normalized_assay:
        return None
    cat = ASSAY_CATEGORY.get(normalized_assay)
    if cat:
        return cat
    # 10x variants: match by product type
    if normalized_assay.startswith("10x"):
        lower = normalized_assay.lower()
        if "visium" in lower:
            return "Spatial Transcriptomics"
        if "atac" in lower:
            return "Single-cell Epigenomics"
        if "multiome" in lower:
            return "Single-cell Multi-omics"
        # Default 10x GEX (3'/5'/generic)
        return "Single-cell Transcriptomics"
    return None


# Provenance tracking

# Per-field provenance is stored as: {source, column, score}
#   source: "embedding" | "llm_primary" | "llm_fallback" | "cellosaurus" | "direct"
#   column: matched column name (or None for fallback/cellosaurus)
#   score:  cosine similarity score (or None)

PROVENANCE_SOURCES = (
    "embedding",  # direct value from embedding-matched column (sex, age, strain, etc.)
    "llm_primary",  # LLM extraction from embedding-matched columns
    "llm_fallback",  # LLM extraction from Description/Sample Title/source_name
    "cellosaurus",  # post-processing from Cellosaurus lookup
    "direct",  # exact column name match (e.g., assay column)
)


def enrich_df(
    project,
    enrichment_backend="EssentialAI/rnj-1",
    embedding_model="abhinand/MedEmbed-large-v0.1",
    cellosaurus: bool = True,
    verbose: bool = False,
    debug: bool = False,
    expected_taxid: str | None = None,
    expected_species: str | None = None,
):
    """
    Enrich a project's sample metadata.

    Returns enriched, Cellosaurus-adjusted, provenance and ontology DataFrames.
    Species gates refuse Cellosaurus matches that contradict the expected
    taxid/name.
    """
    # Maps row_index to {field: {source, column, score}}.
    _provenance: dict[int, dict[str, dict]] = {}

    def _record_provenance(
        row_idx: int,
        field: str,
        source: str,
        column: str | None = None,
        score: float | None = None,
    ):
        if row_idx not in _provenance:
            _provenance[row_idx] = {}
        existing = _provenance[row_idx].get(field)
        if existing and source in _PROVENANCE_TWO_LEVEL_SOURCES:
            # Two-level tracking: preserve original source, mark cellosaurus override
            _provenance[row_idx][field] = {
                "source": source,
                "column": column,
                "score": round(score, 4) if score is not None else None,
                "original_source": existing.get("source"),
                "original_column": existing.get("column"),
                "original_score": existing.get("score"),
            }
        else:
            _provenance[row_idx][field] = {
                "source": source,
                "column": column,
                "score": round(score, 4) if score is not None else None,
            }

    def _debug_attrs(node: str, keys: list[str], attributes: dict):
        if not debug:
            return
        console.print(f"debug {node} keys: {keys}")

    def _coerce_str(value):
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if isinstance(value, str):
            return value
        return str(value)

    def _normalized(value):
        text = _coerce_str(value)
        if text is None:
            return None
        return text.strip().lower()

    def _is_garbage(value: str | None) -> bool:
        if value is None:
            return True
        text = value.strip()
        if not text or text == "-":
            return True
        if len(text) <= 1:
            return True
        try:
            float(text)
            return True
        except ValueError:
            pass
        if text.startswith(("http://", "https://", "ftp://", "/")):
            return True
        return False

    def _dedup_keys(keys: list[str]) -> list[str]:
        return list(dict.fromkeys(keys))

    def _collect_values(state, keys: list[str]) -> dict:
        result = {}
        for k in keys:
            v = _coerce_str(state["attributes"].get(k))
            if v and _normalized(v) not in na_strings and v != "-":
                result[k] = v
        return result

    def _fallback_info(state) -> dict:
        # bulk API keys: GEO sends "Title"/"Source", SRA/ENA/DDBJ send "Sample Title" + "Title" (experiment)
        fallback_keys = [
            "Description",
            "Title",
            "Sample Title",
            "Source",
            "source_name",
            "source name",
        ]
        return {
            k: v
            for k in fallback_keys
            if (v := _coerce_str(state["attributes"].get(k)))
            and _normalized(v) not in na_strings
            and v != "-"
        }

    def filter_dict(d: dict, keys: list[str]) -> str:
        return json.dumps({k: d[k] for k in keys if k in d})

    @tool
    def age_to_hours(value: float, unit: str | None = None) -> float | None:
        """Convert an age value to hours. If unit is None, assume years."""
        if value is None:
            return None
        normalized_unit = (unit or "years").strip().lower()
        if normalized_unit in {"hour", "hours", "hr", "hrs", "h"}:
            return value
        if normalized_unit in {"day", "days", "d"}:
            return value * 24
        if normalized_unit in {"week", "weeks", "w"}:
            return value * 24 * 7
        if normalized_unit in {"month", "months", "mo", "mos"}:
            return value * 24 * 30
        if normalized_unit in {"year", "years", "yr", "yrs", "y"}:
            return value * 24 * 365
        return value * 24 * 365

    if not hasattr(project, "samples"):
        raise TypeError("project must have a 'samples' attribute with a DataFrame")

    detailed_df = project.samples
    project_context = {
        "title": _coerce_str(getattr(project, "title", None)),
        "description": _coerce_str(getattr(project, "description", None)),
        "overall_design": _coerce_str(getattr(project, "overall_design", None)),
    }
    project_context_json = json.dumps(project_context)

    if verbose:
        console.print("Enriching metadata in progress...")

    if enrichment_backend.startswith("vllm/"):
        vllm_model = enrichment_backend.removeprefix("vllm/")
        vllm_base_url = os.environ.get("VLLM_BASE_URL", "http://localhost:8000/v1")
        llm = ChatOpenAI(
            model=vllm_model,
            base_url=vllm_base_url,
            api_key="EMPTY",  # type: ignore
            temperature=0,
        )
    else:
        from langchain_ollama import ChatOllama

        ollama_kwargs = {}
        ollama_url = os.environ.get("OLLAMA_BASE_URL")
        if ollama_url:
            ollama_kwargs["base_url"] = ollama_url.rstrip("/").removesuffix("/v1")
        llm = ChatOllama(
            model=enrichment_backend,
            temperature=0,
            timeout=120,
            num_predict=512,
            **ollama_kwargs,
        )

    # Embedding model loads on first cache miss; fuzzy matches avoid startup.

    _embedding_model_instance = None
    _key_embeddings_cache = None  # tensor, lazily computed

    def _ensure_embeddings(keys: list[str]):
        nonlocal _embedding_model_instance, _key_embeddings_cache
        if _key_embeddings_cache is None:
            _embedding_model_instance = _get_embedding_model(
                embedding_model, verbose=verbose
            )
            _key_embeddings_cache = _embedding_model_instance.encode(
                keys, convert_to_tensor=True
            )
        return _embedding_model_instance, _key_embeddings_cache

    def _embedding_match(term: str, keys: list[str]) -> list[tuple[str, float]]:
        """Semantic embedding fallback used when fuzzy match finds nothing."""
        # Empty candidate set returns an empty match list.
        if not keys:
            return []
        model, embs = _ensure_embeddings(keys)
        query = model.encode(term, convert_to_tensor=True)
        similarities = util.cos_sim(query, embs).squeeze(0)
        if similarities.dim() == 0:
            s = similarities.item()
            return [(keys[0], s)] if s > 0.8 else []
        result = []
        for i, similarity in enumerate(similarities):
            s = similarity.item()
            if s > 0.8:
                result.append((keys[i], s))
        return result

    def get_relevant_keys_with_scores(
        term: str, embeddings, keys: list[str]
    ) -> list[tuple[str, float]]:
        """Fast fuzz first, embedding only if fuzz finds nothing."""
        # Pass 1: rapidfuzz
        fuzz_hits = _fuzz_match_keys(term, keys)
        if fuzz_hits:
            return fuzz_hits
        # Pass 2: semantic embedding (lazy-loaded)
        return _embedding_match(term, keys)

    def get_relevant_keys(term: str, embeddings, keys: list[str]) -> list[str]:
        return [k for k, _ in get_relevant_keys_with_scores(term, embeddings, keys)]

    def get_best_score(
        terms: list[str], embeddings, keys: list[str], matched_keys: list[str]
    ) -> dict[str, float]:
        """For each matched key, find the best score across query terms."""
        scores: dict[str, float] = {}
        for term in terms:
            for key, score in get_relevant_keys_with_scores(term, embeddings, keys):
                if key in matched_keys:
                    scores[key] = max(scores.get(key, 0), score)
        return scores

    def get_sex_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("sex", embeddings, keys)
            + get_relevant_keys("gender", embeddings, keys)
        )

    def get_age_keys(embeddings, keys: list[str]):
        return _dedup_keys(get_relevant_keys("age", embeddings, keys))

    def get_cell_type_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("cell type", embeddings, keys)
            + get_relevant_keys("histological", embeddings, keys)
        )

    def get_tissue_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("tissue", embeddings, keys)
            + get_relevant_keys("organ", embeddings, keys)
            + get_relevant_keys("body site", embeddings, keys)
            + get_relevant_keys("tumor site", embeddings, keys)
        )

    def get_tissue_scores(
        embeddings, keys: list[str], matched: list[str]
    ) -> dict[str, float]:
        return get_best_score(
            ["tissue", "organ", "body site", "tumor site"],
            embeddings,
            keys,
            matched,
        )

    def get_phenotype_keys(embeddings, keys: list[str]):
        return _dedup_keys(get_relevant_keys("phenotype", embeddings, keys))

    def get_strain_keys(embeddings, keys: list[str]):
        return _dedup_keys(get_relevant_keys("strain", embeddings, keys))

    def get_ethnicity_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("ethnicity", embeddings, keys)
            + get_relevant_keys("population", embeddings, keys)
        )

    def get_disease_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("disease", embeddings, keys)
            + get_relevant_keys("disease state", embeddings, keys)
            + get_relevant_keys("condition", embeddings, keys)
            + get_relevant_keys("tumor", embeddings, keys)
            + get_relevant_keys("diagnosis", embeddings, keys)
            + get_relevant_keys("infection", embeddings, keys)
            + get_relevant_keys("cancer", embeddings, keys)
            + get_relevant_keys("study disease", embeddings, keys)
            + get_relevant_keys("subject status", embeddings, keys)
        )

    def get_cell_line_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("cell line", embeddings, keys)
            + get_relevant_keys("cellline", embeddings, keys)
        )

    def get_treatment_keys(embeddings, keys: list[str]):
        treatment_terms = {
            "treatment",
            "vaccine",
            "drug",
            "diet",
            "condition",
            "perturbation",
            "stimulus",
            "compound",
            "intervention",
            "exposure",
        }
        matched = [key for key in keys if key.lower() in treatment_terms]
        relevant = (
            get_relevant_keys("treatment", embeddings, keys)
            + get_relevant_keys("vaccine", embeddings, keys)
            + get_relevant_keys("drug treatment", embeddings, keys)
            + get_relevant_keys("perturbation", embeddings, keys)
            + get_relevant_keys("compound", embeddings, keys)
            + get_relevant_keys("stimulus", embeddings, keys)
        )
        return _dedup_keys(matched + relevant)

    def get_development_stage_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("developmental stage", embeddings, keys)
            + get_relevant_keys("life stage", embeddings, keys)
            + get_relevant_keys("development stage", embeddings, keys)
            + get_relevant_keys("age group", embeddings, keys)
        )

    def get_sample_type_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("sample type", embeddings, keys)
            + get_relevant_keys("tissue type", embeddings, keys)
            + get_relevant_keys("material type", embeddings, keys)
        )

    def get_genetic_modification_keys(embeddings, keys: list[str]):
        return _dedup_keys(
            get_relevant_keys("genotype", embeddings, keys)
            + get_relevant_keys("genetic modification", embeddings, keys)
            + get_relevant_keys("knockout", embeddings, keys)
            + get_relevant_keys("transgene", embeddings, keys)
            + get_relevant_keys("mutation", embeddings, keys)
        )

    def get_refined_keys(raw_keys: list[str]) -> list[str]:
        keys = []
        for key in raw_keys:
            refined_key = key.rstrip("0123456789")
            keys.append(
                refined_key.replace("ArrayExpress-", "")
                .lower()
                .replace("-", " ")
                .replace("%", "")
                .replace("*", "")
            )
        return keys

    system_prompt = PROMPTS["chat_template_system"]
    user_prompt = PROMPTS["chat_template_user"]

    na_strings = [
        "missing",
        "not applicable",
        "unknown",
        "na",
        "n/a",
        "not collected",
        "none",
        "null",
        "nan",
    ]

    prompt_template = ChatPromptTemplate(
        [("system", system_prompt), ("user", user_prompt)]
    )

    def build_prompt(sys_prompt: str, info: str):
        return prompt_template.invoke(
            {
                "sys_prompt": sys_prompt,
                "project_context": project_context_json,
                "info": info,
            }
        )

    class TissueOutput(TypedDict):
        """Inferred tissue name."""

        tissue: str | None

    class DiseaseOutput(TypedDict):
        """Inferred disease name."""

        disease: str | None

    class AssayOutput(TypedDict):
        """Inferred assay name."""

        assay: str | None

    class CellTypeOutput(TypedDict):
        """Inferred cell type name."""

        cell_type: str | None

    class CellLineOutput(TypedDict):
        """Inferred cell line name."""

        cell_line: str | None

    class TreatmentOutput(TypedDict):
        """Inferred treatment name."""

        treatment: str | None

    class EthnicityOutput(TypedDict):
        """Inferred ethnicity."""

        ethnicity: str | None

    class DevelopmentStageOutput(TypedDict):
        """Inferred development stage."""

        development_stage: str | None

    class SampleTypeOutput(TypedDict):
        """Inferred sample type."""

        sample_type: str | None

    class GeneticModificationOutput(TypedDict):
        """Inferred genetic modification."""

        genetic_modification: str | None

    class State(TypedDict):
        attributes: dict
        embeddings: object
        raw_keys: list[str]
        row_index: int  # NEW: for provenance tracking
        age: str | None
        sex: str | None
        ethnicity: str | None
        phenotype: str | None
        cell_type: str | None
        tissue: str | None
        strain: str | None
        disease: str | None
        assay: str | None
        cell_line: str | None
        treatment: str | None
        development_stage: str | None
        sample_type: str | None
        genetic_modification: str | None
        # Provenance flags used by Cellosaurus step
        _tissue_from_embedding: bool

    def estimate_age(state: State):
        row_idx = state["row_index"]
        keys = get_age_keys(state["embeddings"], state["raw_keys"])
        if not keys:
            return {"age": None}
        age_values = _collect_values(state, keys)
        if not age_values:
            return {"age": None}
        _debug_attrs("estimate_age", list(age_values.keys()), state["attributes"])
        age_text = next((v for v in age_values.values() if not _is_garbage(v)), None)
        if not age_text:
            return {"age": None}

        matched_col = next(iter(age_values))
        scores = get_best_score(
            ["age"], state["embeddings"], state["raw_keys"], [matched_col]
        )

        sys_prompt = PROMPTS["age_primary"]
        prompt = build_prompt(sys_prompt, json.dumps({"age": age_text}))
        llm_with_tools = llm.bind_tools([age_to_hours])
        result = llm_with_tools.invoke(prompt)
        tool_calls = getattr(result, "tool_calls", [])
        for call in tool_calls:
            if call.get("name") == "age_to_hours":
                # The LLM sometimes passes a non-numeric value (e.g. "P21",
                # "adult"), which fails the tool's float validation.
                try:
                    age_hours = age_to_hours.invoke(call.get("args", {}))
                except Exception:
                    return {"age": None}
                _record_provenance(
                    row_idx, "age", "embedding", matched_col, scores.get(matched_col)
                )
                return {"age": str(age_hours) if age_hours is not None else None}
        return {"age": None}

    def estimate_sex(state: State):
        row_idx = state["row_index"]
        keys = get_sex_keys(state["embeddings"], state["raw_keys"])
        if not keys:
            return {"sex": None}
        scores = get_best_score(
            ["sex", "gender"], state["embeddings"], state["raw_keys"], keys
        )
        for key in keys:
            _debug_attrs("estimate_sex", [key], state["attributes"])
            sex_value = _normalized(state["attributes"].get(key))
            if sex_value in ["male", "m"]:
                _record_provenance(row_idx, "sex", "embedding", key, scores.get(key))
                return {"sex": "Male"}
            if sex_value in ["female", "f"]:
                _record_provenance(row_idx, "sex", "embedding", key, scores.get(key))
                return {"sex": "Female"}
        return {"sex": None}

    def estimate_tissue(state: State):
        row_idx = state["row_index"]
        keys = get_tissue_keys(state["embeddings"], state["raw_keys"])
        values = _collect_values(state, keys) if keys else {}

        if values:
            _debug_attrs("estimate_tissue", list(values.keys()), state["attributes"])
            scores = get_tissue_scores(
                state["embeddings"], state["raw_keys"], list(values.keys())
            )
            best_col = max(scores, key=scores.get) if scores else next(iter(values))
            info_json = json.dumps(values)
            sys_prompt = PROMPTS["tissue_primary"]
            prompt = build_prompt(sys_prompt, info_json)
            structured_llm = llm.with_structured_output(TissueOutput)
            result = structured_llm.invoke(prompt)
            tissue_value = _coerce_str(result["tissue"] if result else None)  # type: ignore
            if (
                tissue_value
                and _normalized(tissue_value) not in na_strings
                and not _is_garbage(tissue_value)
            ):
                _record_provenance(
                    row_idx, "tissue", "llm_primary", best_col, scores.get(best_col)
                )
                return {
                    "tissue": _title_case(tissue_value),
                    "_tissue_from_embedding": True,
                }

        # Fallback
        fallback = _fallback_info(state)
        if not fallback:
            return {"tissue": None, "_tissue_from_embedding": False}
        _debug_attrs(
            "estimate_tissue_fallback", list(fallback.keys()), state["attributes"]
        )
        sys_prompt = PROMPTS["tissue_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(TissueOutput)
        result = structured_llm.invoke(prompt)
        tissue_value = _coerce_str(result["tissue"] if result else None)  # type: ignore
        if (
            tissue_value
            and _normalized(tissue_value) not in na_strings
            and not _is_garbage(tissue_value)
        ):
            _record_provenance(row_idx, "tissue", "llm_fallback")
            return {
                "tissue": _title_case(tissue_value),
                "_tissue_from_embedding": False,
            }
        return {"tissue": None, "_tissue_from_embedding": False}

    def estimate_strain(state: State):
        row_idx = state["row_index"]
        keys = get_strain_keys(state["embeddings"], state["raw_keys"])
        if not keys:
            return {"strain": None}
        scores = get_best_score(
            ["strain"], state["embeddings"], state["raw_keys"], keys
        )
        for key in keys:
            if "host" in key.lower():
                continue
            _debug_attrs("estimate_strain", [key], state["attributes"])
            value = _coerce_str(state["attributes"].get(key))
            if (
                value
                and _normalized(value) not in na_strings
                and not _is_garbage(value)
            ):
                _record_provenance(row_idx, "strain", "embedding", key, scores.get(key))
                return {"strain": _title_case(value)}
        return {"strain": None}

    def estimate_phenotype(state: State):
        row_idx = state["row_index"]
        keys = get_phenotype_keys(state["embeddings"], state["raw_keys"])
        if not keys:
            return {"phenotype": None}
        scores = get_best_score(
            ["phenotype"], state["embeddings"], state["raw_keys"], keys
        )
        for key in keys:
            if "host" in key.lower():
                continue
            _debug_attrs("estimate_phenotype", [key], state["attributes"])
            value = _coerce_str(state["attributes"].get(key))
            if (
                value
                and _normalized(value) not in na_strings
                and not _is_garbage(value)
            ):
                _record_provenance(
                    row_idx, "phenotype", "embedding", key, scores.get(key)
                )
                return {"phenotype": _title_case(value)}
        return {"phenotype": None}

    def estimate_cell_type(state: State):
        row_idx = state["row_index"]
        keys = get_cell_type_keys(state["embeddings"], state["raw_keys"])
        values = _collect_values(state, keys) if keys else {}

        if values:
            clean = {k: v for k, v in values.items() if not _is_garbage(v)}
            if clean:
                _debug_attrs(
                    "estimate_cell_type", list(clean.keys()), state["attributes"]
                )
                scores = get_best_score(
                    ["cell type", "histological"],
                    state["embeddings"],
                    state["raw_keys"],
                    list(clean.keys()),
                )
                best_col = max(scores, key=scores.get) if scores else next(iter(clean))
                info_json = json.dumps(clean)
                sys_prompt = PROMPTS["cell_type_primary"]
                prompt = build_prompt(sys_prompt, info_json)
                structured_llm = llm.with_structured_output(CellTypeOutput)
                result = structured_llm.invoke(prompt)
                ct_value = _coerce_str(result["cell_type"] if result else None)  # type: ignore
                if (
                    ct_value
                    and _normalized(ct_value) not in na_strings
                    and not _is_garbage(ct_value)
                ):
                    _record_provenance(
                        row_idx,
                        "cell_type",
                        "llm_primary",
                        best_col,
                        scores.get(best_col),
                    )
                    return {"cell_type": _title_case(ct_value)}

        fallback = _fallback_info(state)
        if not fallback:
            return {"cell_type": None}
        _debug_attrs(
            "estimate_cell_type_fallback", list(fallback.keys()), state["attributes"]
        )
        sys_prompt = PROMPTS["cell_type_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(CellTypeOutput)
        result = structured_llm.invoke(prompt)
        ct_value = _coerce_str(result["cell_type"] if result else None)  # type: ignore
        if (
            ct_value
            and _normalized(ct_value) not in na_strings
            and not _is_garbage(ct_value)
        ):
            _record_provenance(row_idx, "cell_type", "llm_fallback")
            return {"cell_type": _title_case(ct_value)}
        return {"cell_type": None}

    def estimate_ethnicity(state: State):
        row_idx = state["row_index"]
        keys = get_ethnicity_keys(state["embeddings"], state["raw_keys"])
        values = _collect_values(state, keys) if keys else {}
        if not values:
            return {"ethnicity": None}

        clean = {k: v for k, v in values.items() if not _is_garbage(v)}
        if not clean:
            return {"ethnicity": None}

        _debug_attrs("estimate_ethnicity", list(clean.keys()), state["attributes"])
        scores = get_best_score(
            ["ethnicity", "population"],
            state["embeddings"],
            state["raw_keys"],
            list(clean.keys()),
        )
        best_col = max(scores, key=scores.get) if scores else next(iter(clean))
        info_json = json.dumps(clean)
        sys_prompt = PROMPTS["ethnicity_primary"]
        prompt = build_prompt(sys_prompt, info_json)
        structured_llm = llm.with_structured_output(EthnicityOutput)
        result = structured_llm.invoke(prompt)
        eth_value = _coerce_str(result["ethnicity"] if result else None)  # type: ignore
        if (
            eth_value
            and _normalized(eth_value) not in na_strings
            and not _is_garbage(eth_value)
        ):
            _record_provenance(
                row_idx, "ethnicity", "llm_primary", best_col, scores.get(best_col)
            )
            return {"ethnicity": _title_case(eth_value)}
        return {"ethnicity": None}

    def estimate_disease(state: State):
        row_idx = state["row_index"]
        disease_keys = get_disease_keys(state["embeddings"], state["raw_keys"])
        tissue_keys = get_tissue_keys(state["embeddings"], state["raw_keys"])
        llm_context_keys = _dedup_keys(disease_keys + tissue_keys)
        _debug_attrs("estimate_disease", llm_context_keys, state["attributes"])
        values = _collect_values(state, llm_context_keys)

        if values:
            scores = get_best_score(
                [
                    "disease",
                    "disease state",
                    "condition",
                    "tumor",
                    "diagnosis",
                    "infection",
                    "cancer",
                    "study disease",
                    "subject status",
                ],
                state["embeddings"],
                state["raw_keys"],
                list(values.keys()),
            )
            best_col = max(scores, key=scores.get) if scores else next(iter(values))
            info_json = json.dumps(values)
            sys_prompt = PROMPTS["disease_primary"]
            prompt = build_prompt(sys_prompt, info_json)
            structured_llm = llm.with_structured_output(DiseaseOutput)
            result = structured_llm.invoke(prompt)
            disease_value = _coerce_str(result["disease"] if result else None)  # type: ignore
            if (
                disease_value
                and _normalized(disease_value) not in na_strings
                and not _is_garbage(disease_value)
            ):
                _record_provenance(
                    row_idx, "disease", "llm_primary", best_col, scores.get(best_col)
                )
                return {"disease": _title_case(disease_value)}

        fallback = _fallback_info(state)
        if not fallback:
            return {"disease": None}
        _debug_attrs(
            "estimate_disease_fallback", list(fallback.keys()), state["attributes"]
        )
        sys_prompt = PROMPTS["disease_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(DiseaseOutput)
        result = structured_llm.invoke(prompt)
        disease_value = _coerce_str(result["disease"] if result else None)  # type: ignore
        if (
            disease_value
            and _normalized(disease_value) not in na_strings
            and not _is_garbage(disease_value)
        ):
            _record_provenance(row_idx, "disease", "llm_fallback")
            return {"disease": _title_case(disease_value)}
        return {"disease": None}

    def estimate_assay(state: State):
        row_idx = state["row_index"]
        # 1. Check explicit "assay" column
        explicit_assay_keys = [
            key for key in state["raw_keys"] if key.lower() == "assay"
        ]
        if explicit_assay_keys:
            key = explicit_assay_keys[0]
            _debug_attrs("estimate_assay", [key], state["attributes"])
            assay_value = _coerce_str(state["attributes"].get(key))
            if (
                assay_value
                and _normalized(assay_value) not in na_strings
                and not _is_garbage(assay_value)
            ):
                normalized = _normalize_assay(assay_value)
                if normalized is not None:
                    _record_provenance(row_idx, "assay", "direct", key)
                    return {"assay": normalized}

        # 2. Check protocol / molecule columns
        protocol_keys = [key for key in state["raw_keys"] if "protocol" in key.lower()]
        molecule_keys = [key for key in state["raw_keys"] if key.lower() == "molecule"]
        assay_context_keys = _dedup_keys(protocol_keys + molecule_keys)

        # 3. Fallback: Library column
        library_value = _coerce_str(state["attributes"].get("Library"))
        if (
            library_value
            and library_value != "-"
            and _normalized(library_value) not in na_strings
        ):
            assay_context_keys.append("Library")

        if not assay_context_keys:
            return {"assay": None}

        _debug_attrs("estimate_assay", assay_context_keys, state["attributes"])
        info_json = filter_dict(state["attributes"], assay_context_keys)
        sys_prompt = PROMPTS["assay_primary"]
        prompt = build_prompt(sys_prompt, info_json)
        structured_llm = llm.with_structured_output(AssayOutput)
        result = structured_llm.invoke(prompt)
        assay_value = _coerce_str(result["assay"] if result else None)  # type: ignore
        if (
            assay_value
            and _normalized(assay_value) not in na_strings
            and not _is_garbage(assay_value)
        ):
            normalized = _normalize_assay(assay_value)
            if normalized is not None:
                _record_provenance(
                    row_idx,
                    "assay",
                    "llm_primary",
                    assay_context_keys[0] if assay_context_keys else None,
                )
                return {"assay": normalized}
        return {"assay": None}

    def estimate_cell_line(state: State):
        row_idx = state["row_index"]
        keys = get_cell_line_keys(state["embeddings"], state["raw_keys"])
        values = _collect_values(state, keys) if keys else {}

        if values:
            clean = {k: v for k, v in values.items() if not _is_garbage(v)}
            if clean:
                _debug_attrs(
                    "estimate_cell_line", list(clean.keys()), state["attributes"]
                )
                scores = get_best_score(
                    ["cell line", "cellline"],
                    state["embeddings"],
                    state["raw_keys"],
                    list(clean.keys()),
                )
                best_col = max(scores, key=scores.get) if scores else next(iter(clean))
                info_json = json.dumps(clean)
                sys_prompt = PROMPTS["cell_line_primary"]
                prompt = build_prompt(sys_prompt, info_json)
                structured_llm = llm.with_structured_output(CellLineOutput)
                result = structured_llm.invoke(prompt)
                cl_value = _coerce_str(result["cell_line"] if result else None)  # type: ignore
                if (
                    cl_value
                    and _normalized(cl_value) not in na_strings
                    and not _is_garbage(cl_value)
                ):
                    _record_provenance(
                        row_idx,
                        "cell_line",
                        "llm_primary",
                        best_col,
                        scores.get(best_col),
                    )
                    return {"cell_line": _title_case(cl_value)}

        fallback = _fallback_info(state)
        if not fallback:
            return {"cell_line": None}
        _debug_attrs(
            "estimate_cell_line_fallback", list(fallback.keys()), state["attributes"]
        )
        sys_prompt = PROMPTS["cell_line_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(CellLineOutput)
        result = structured_llm.invoke(prompt)
        cl_value = _coerce_str(result["cell_line"] if result else None)  # type: ignore
        if (
            cl_value
            and _normalized(cl_value) not in na_strings
            and not _is_garbage(cl_value)
        ):
            _record_provenance(row_idx, "cell_line", "llm_fallback")
            return {"cell_line": _title_case(cl_value)}
        return {"cell_line": None}

    def estimate_treatment(state: State):
        row_idx = state["row_index"]
        keys = get_treatment_keys(state["embeddings"], state["raw_keys"])
        values = _collect_values(state, keys) if keys else {}

        if values:
            clean = {k: v for k, v in values.items() if not _is_garbage(v)}
            if clean:
                _debug_attrs(
                    "estimate_treatment", list(clean.keys()), state["attributes"]
                )
                scores = get_best_score(
                    [
                        "treatment",
                        "vaccine",
                        "drug treatment",
                        "perturbation",
                        "compound",
                        "stimulus",
                    ],
                    state["embeddings"],
                    state["raw_keys"],
                    list(clean.keys()),
                )
                best_col = max(scores, key=scores.get) if scores else next(iter(clean))
                info_json = json.dumps(clean)
                sys_prompt = PROMPTS["treatment_primary"]
                prompt = build_prompt(sys_prompt, info_json)
                structured_llm = llm.with_structured_output(TreatmentOutput)
                result = structured_llm.invoke(prompt)
                tx_value = _coerce_str(result["treatment"] if result else None)  # type: ignore
                if (
                    tx_value
                    and _normalized(tx_value) not in na_strings
                    and not _is_garbage(tx_value)
                ):
                    _record_provenance(
                        row_idx,
                        "treatment",
                        "llm_primary",
                        best_col,
                        scores.get(best_col),
                    )
                    return {"treatment": _title_case(tx_value)}

        fallback = _fallback_info(state)
        if not fallback:
            return {"treatment": None}
        _debug_attrs(
            "estimate_treatment_fallback", list(fallback.keys()), state["attributes"]
        )
        sys_prompt = PROMPTS["treatment_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(TreatmentOutput)
        result = structured_llm.invoke(prompt)
        tx_value = _coerce_str(result["treatment"] if result else None)  # type: ignore
        if (
            tx_value
            and _normalized(tx_value) not in na_strings
            and not _is_garbage(tx_value)
        ):
            _record_provenance(row_idx, "treatment", "llm_fallback")
            return {"treatment": _title_case(tx_value)}
        return {"treatment": None}

    def estimate_development_stage(state: State):
        row_idx = state["row_index"]
        keys = get_development_stage_keys(state["embeddings"], state["raw_keys"])
        age_keys = get_age_keys(state["embeddings"], state["raw_keys"])
        context_keys = _dedup_keys(keys + age_keys)
        values = _collect_values(state, context_keys) if context_keys else {}

        if values:
            clean = {k: v for k, v in values.items() if not _is_garbage(v)}
            if clean:
                _debug_attrs(
                    "estimate_development_stage",
                    list(clean.keys()),
                    state["attributes"],
                )
                scores = get_best_score(
                    [
                        "developmental stage",
                        "life stage",
                        "development stage",
                        "age group",
                    ],
                    state["embeddings"],
                    state["raw_keys"],
                    list(clean.keys()),
                )
                best_col = max(scores, key=scores.get) if scores else next(iter(clean))
                info_json = json.dumps(clean)
                sys_prompt = PROMPTS["development_stage_primary"]
                prompt = build_prompt(sys_prompt, info_json)
                structured_llm = llm.with_structured_output(DevelopmentStageOutput)
                result = structured_llm.invoke(prompt)
                ds_value = _coerce_str(result["development_stage"] if result else None)  # type: ignore
                if (
                    ds_value
                    and _normalized(ds_value) not in na_strings
                    and not _is_garbage(ds_value)
                ):
                    _record_provenance(
                        row_idx,
                        "development_stage",
                        "llm_primary",
                        best_col,
                        scores.get(best_col),
                    )
                    return {"development_stage": _title_case(ds_value)}

        fallback = _fallback_info(state)
        if not fallback:
            return {"development_stage": None}
        _debug_attrs(
            "estimate_development_stage_fallback",
            list(fallback.keys()),
            state["attributes"],
        )
        sys_prompt = PROMPTS["development_stage_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(DevelopmentStageOutput)
        result = structured_llm.invoke(prompt)
        ds_value = _coerce_str(result["development_stage"] if result else None)  # type: ignore
        if (
            ds_value
            and _normalized(ds_value) not in na_strings
            and not _is_garbage(ds_value)
        ):
            _record_provenance(row_idx, "development_stage", "llm_fallback")
            return {"development_stage": _title_case(ds_value)}
        return {"development_stage": None}

    def estimate_sample_type(state: State):
        row_idx = state["row_index"]
        keys = get_sample_type_keys(state["embeddings"], state["raw_keys"])
        cell_line_keys = get_cell_line_keys(state["embeddings"], state["raw_keys"])
        context_keys = _dedup_keys(keys + cell_line_keys)
        values = _collect_values(state, context_keys) if context_keys else {}

        if values:
            clean = {k: v for k, v in values.items() if not _is_garbage(v)}
            if clean:
                _debug_attrs(
                    "estimate_sample_type", list(clean.keys()), state["attributes"]
                )
                scores = get_best_score(
                    ["sample type", "tissue type", "material type"],
                    state["embeddings"],
                    state["raw_keys"],
                    list(clean.keys()),
                )
                best_col = max(scores, key=scores.get) if scores else next(iter(clean))
                info_json = json.dumps(clean)
                sys_prompt = PROMPTS["sample_type_primary"]
                prompt = build_prompt(sys_prompt, info_json)
                structured_llm = llm.with_structured_output(SampleTypeOutput)
                result = structured_llm.invoke(prompt)
                st_value = _coerce_str(result["sample_type"] if result else None)  # type: ignore
                if (
                    st_value
                    and _normalized(st_value) not in na_strings
                    and not _is_garbage(st_value)
                ):
                    _record_provenance(
                        row_idx,
                        "sample_type",
                        "llm_primary",
                        best_col,
                        scores.get(best_col),
                    )
                    return {"sample_type": _title_case(st_value)}

        fallback = _fallback_info(state)
        if not fallback:
            return {"sample_type": None}
        _debug_attrs(
            "estimate_sample_type_fallback", list(fallback.keys()), state["attributes"]
        )
        sys_prompt = PROMPTS["sample_type_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(SampleTypeOutput)
        result = structured_llm.invoke(prompt)
        st_value = _coerce_str(result["sample_type"] if result else None)  # type: ignore
        if (
            st_value
            and _normalized(st_value) not in na_strings
            and not _is_garbage(st_value)
        ):
            _record_provenance(row_idx, "sample_type", "llm_fallback")
            return {"sample_type": _title_case(st_value)}
        return {"sample_type": None}

    def estimate_genetic_modification(state: State):
        row_idx = state["row_index"]
        keys = get_genetic_modification_keys(state["embeddings"], state["raw_keys"])
        values = _collect_values(state, keys) if keys else {}

        if values:
            clean = {k: v for k, v in values.items() if not _is_garbage(v)}
            if clean:
                _debug_attrs(
                    "estimate_genetic_modification",
                    list(clean.keys()),
                    state["attributes"],
                )
                scores = get_best_score(
                    [
                        "genotype",
                        "genetic modification",
                        "knockout",
                        "transgene",
                        "mutation",
                    ],
                    state["embeddings"],
                    state["raw_keys"],
                    list(clean.keys()),
                )
                best_col = max(scores, key=scores.get) if scores else next(iter(clean))
                info_json = json.dumps(clean)
                sys_prompt = PROMPTS["genetic_modification_primary"]
                prompt = build_prompt(sys_prompt, info_json)
                structured_llm = llm.with_structured_output(GeneticModificationOutput)
                result = structured_llm.invoke(prompt)
                gm_value = _coerce_str(
                    result["genetic_modification"] if result else None  # type: ignore
                )
                if (
                    gm_value
                    and _normalized(gm_value) not in na_strings
                    and not _is_garbage(gm_value)
                ):
                    _record_provenance(
                        row_idx,
                        "genetic_modification",
                        "llm_primary",
                        best_col,
                        scores.get(best_col),
                    )
                    return {"genetic_modification": _title_case(gm_value)}

        fallback = _fallback_info(state)
        if not fallback:
            return {"genetic_modification": None}
        _debug_attrs(
            "estimate_genetic_modification_fallback",
            list(fallback.keys()),
            state["attributes"],
        )
        sys_prompt = PROMPTS["genetic_modification_fallback"]
        prompt = build_prompt(sys_prompt, json.dumps(fallback))
        structured_llm = llm.with_structured_output(GeneticModificationOutput)
        result = structured_llm.invoke(prompt)
        gm_value = _coerce_str(result["genetic_modification"] if result else None)  # type: ignore
        if (
            gm_value
            and _normalized(gm_value) not in na_strings
            and not _is_garbage(gm_value)
        ):
            _record_provenance(row_idx, "genetic_modification", "llm_fallback")
            return {"genetic_modification": _title_case(gm_value)}
        return {"genetic_modification": None}

    workflow_graph_builder = StateGraph(State)

    workflow_graph_builder.add_node("estimate_age", estimate_age)
    workflow_graph_builder.add_node("estimate_sex", estimate_sex)
    workflow_graph_builder.add_node("estimate_tissue", estimate_tissue)
    workflow_graph_builder.add_node("estimate_strain", estimate_strain)
    workflow_graph_builder.add_node("estimate_phenotype", estimate_phenotype)
    workflow_graph_builder.add_node("estimate_cell_type", estimate_cell_type)
    workflow_graph_builder.add_node("estimate_ethnicity", estimate_ethnicity)
    workflow_graph_builder.add_node("estimate_disease", estimate_disease)
    workflow_graph_builder.add_node("estimate_assay", estimate_assay)
    workflow_graph_builder.add_node("estimate_cell_line", estimate_cell_line)
    workflow_graph_builder.add_node("estimate_treatment", estimate_treatment)
    workflow_graph_builder.add_node(
        "estimate_development_stage", estimate_development_stage
    )
    workflow_graph_builder.add_node("estimate_sample_type", estimate_sample_type)
    workflow_graph_builder.add_node(
        "estimate_genetic_modification", estimate_genetic_modification
    )

    for node in [
        "estimate_age",
        "estimate_sex",
        "estimate_tissue",
        "estimate_strain",
        "estimate_phenotype",
        "estimate_cell_type",
        "estimate_ethnicity",
        "estimate_disease",
        "estimate_assay",
        "estimate_cell_line",
        "estimate_treatment",
        "estimate_development_stage",
        "estimate_sample_type",
        "estimate_genetic_modification",
    ]:
        workflow_graph_builder.add_edge(START, node)
        workflow_graph_builder.add_edge(node, END)

    workflow = workflow_graph_builder.compile()

    ENRICHED_COLS = [
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
    ]

    NON_BIOLOGICAL_COLUMNS = {
        "Accession",
        "Sample",
        "Sample Alias",
        "Sample_Alias",
        "sample_alias",
        "External Id",
        "External_Id",
        "external_id",
        "Submitter Id",
        "Submitter_Id",
        "submitter_id",
        "SRA accession",
        "SRA_accession",
        "sra_accession",
        "experiment_accession",
        "Experiment_Accession",
        "run_accession",
        "Run_Accession",
        "BioSample",
        "biosample",
        "geo_accession",
        "GEO_accession",
        # Sample name / title variants
        "sample_name",
        "Sample Name",
        "sample name",
        "Sample_Name",
        "SampleName",
        "samplename",
        "sample_title",
        "Sample Title",
        "sample title",
        "Sample_Title",
        "SampleTitle",
        "sampletitle",
        "Title",
        "title",
        # Description
        "Description",
        "description",
        "Sample Description",
        "sample_description",
        "Sample_Description",
        # Sequencing metadata
        "Library",
        "library",
        "Library Name",
        "library_name",
        "Library_Name",
        "Layout",
        "layout",
        "Platform",
        "platform",
        "Instrument",
        "instrument",
        "instrument_model",
        "Instrument_Model",
        # Taxonomy
        "Scientific Name",
        "scientific_name",
        "Scientific_Name",
        "Taxon ID",
        "taxon_id",
        "Taxon_ID",
        # ENA / INSDC tracking
        "ENA-FIRST-PUBLIC",
        "ENA-LAST-UPDATE",
        "ENA first public",
        "ENA last update",
        "ena_first_public",
        "ena_last_update",
        "INSDC center name",
        "INSDC center alias",
        "INSDC first public",
        "INSDC last update",
        "INSDC status",
        "insdc_center_name",
        "insdc_center_alias",
        # Submission / center
        "center_name",
        "Center_Name",
        "center name",
        "submission_accession",
        "Submission_Accession",
    }
    non_bio_lower = {c.lower() for c in NON_BIOLOGICAL_COLUMNS}

    all_cols = list(detailed_df.columns)
    embedding_cols = [c for c in all_cols if c.lower() not in non_bio_lower]
    refined_keys = get_refined_keys(embedding_cols)
    key_embeddings = (
        refined_keys  # tensor created lazily if fuzz miss triggers embedding
    )

    # Per-row enrichment

    def _enrich_row(item):
        i, each_row = item
        state = workflow.invoke(
            {
                "attributes": each_row.to_dict(),
                "embeddings": key_embeddings,
                "raw_keys": embedding_cols,
                "row_index": i,
                "_tissue_from_embedding": False,
            }  # type: ignore
        )
        enriched = {col: state.get(col) for col in ENRICHED_COLS}
        tissue_from_emb = state.get("_tissue_from_embedding", False)
        return i, enriched, tissue_from_emb

    rows = list(detailed_df[all_cols].iterrows())
    tissue_from_embedding_flags: dict[int, bool] = {}

    # Deduplicate by attribute fingerprint before LLM calls, then broadcast
    # results across replicate rows.

    def _row_fingerprint(row_dict: dict) -> str:
        """Create a hashable fingerprint from row values (excluding sample ID)."""
        # Deduplicate on biological metadata columns.
        vals = []
        for col in embedding_cols:
            v = row_dict.get(col)
            if v is None or (isinstance(v, float) and v != v):  # NaN check
                vals.append("")
            else:
                vals.append(str(v).strip())
        return "\t".join(vals)

    fp_to_representative: dict[str, int] = {}  # fingerprint -> first row index
    duplicate_map: dict[int, int] = {}  # row_index -> representative row_index
    unique_rows: list[tuple[int, object]] = []  # (index, row) for unique entries

    for idx, row in rows:
        fp = _row_fingerprint(row.to_dict())
        if fp in fp_to_representative:
            duplicate_map[idx] = fp_to_representative[fp]
        else:
            fp_to_representative[fp] = idx
            unique_rows.append((idx, row))

    n_deduped = len(rows) - len(unique_rows)
    if verbose and n_deduped > 0:
        console.print(
            f"Deduplicated: {len(rows)} rows → {len(unique_rows)} unique "
            f"({n_deduped} duplicates skipped)"
        )

    # Enrich unique rows.
    start_time = time.perf_counter()
    unique_results: dict[int, tuple[dict, bool]] = {}  # idx -> (enriched, tissue_flag)

    if unique_rows:
        _max_threads = int(os.environ.get("ENRICH_MAX_THREADS", "2"))
        max_workers = min(_max_threads, len(unique_rows))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            row_iter = executor.map(_enrich_row, unique_rows)
            if verbose:
                row_iter = tqdm(row_iter, total=len(unique_rows), desc="Enriching rows")
            for i, enriched, tissue_from_emb in row_iter:
                unique_results[i] = (enriched, tissue_from_emb)
                for col, value in enriched.items():
                    detailed_df.loc[i, col] = value  # type: ignore
                tissue_from_embedding_flags[i] = tissue_from_emb

    # Broadcast results to duplicate rows
    for dup_idx, rep_idx in duplicate_map.items():
        enriched, tissue_from_emb = unique_results[rep_idx]
        for col, value in enriched.items():
            detailed_df.loc[dup_idx, col] = value  # type: ignore
        tissue_from_embedding_flags[dup_idx] = tissue_from_emb
        # Copy provenance from representative
        if rep_idx in _provenance:
            _provenance[dup_idx] = _provenance[rep_idx].copy()

    elapsed = time.perf_counter() - start_time
    if verbose:
        console.print(f"Enrichment completed in {elapsed:.2f} seconds.")

    # Clear cell_type/tissue when the LLM found a cell_line. Parallel nodes often
    # echo the line name as cell_type; Cellosaurus fills tissue/cell_type from the
    # cell line identity afterward.
    for i in range(len(detailed_df)):
        cell_line_val = (
            detailed_df.loc[i, "cell_line"]
            if "cell_line" in detailed_df.columns
            else None
        )
        if not cell_line_val or (
            isinstance(cell_line_val, float) and math.isnan(cell_line_val)
        ):
            continue
        prov = _provenance.get(i, {})
        cl_source = prov.get("cell_line", {}).get("source", "")
        if cl_source in ("llm_primary", "llm_fallback"):
            # Clear cell_type; Cellosaurus will set it from cell line identity
            ct_source = prov.get("cell_type", {}).get("source", "")
            if ct_source != "embedding":
                # Clear when cell_type came from the LLM; direct metadata columns stay
                # authoritative.
                detailed_df.loc[i, "cell_type"] = None
            # Clear tissue; Cellosaurus will fill from cell line site of origin
            tissue_from_emb = tissue_from_embedding_flags.get(i, False)
            if not tissue_from_emb:
                detailed_df.loc[i, "tissue"] = None

    if verbose:
        cleared = sum(
            1
            for i in range(len(detailed_df))
            if _provenance.get(i, {}).get("cell_line", {}).get("source")
            in ("llm_primary", "llm_fallback")
            and detailed_df.loc[i, "cell_line"] is not None
            and not (
                isinstance(detailed_df.loc[i, "cell_line"], float)
                and math.isnan(detailed_df.loc[i, "cell_line"])
            )
        )
        console.print(
            f"Cleared cell_type/tissue for {cleared} rows with LLM-extracted cell_line (Cellosaurus will fill)."
        )

    # Determine sample ID column

    id_col = None
    if "Sample" in detailed_df.columns:
        id_col = "Sample"
    elif "Accession" in detailed_df.columns:
        id_col = "Accession"
    elif "experiment_accession" in detailed_df.columns:
        id_col = "experiment_accession"

    if id_col is None:
        raise KeyError(
            "Expected one of ['Sample', 'Accession', 'experiment_accession'] "
            "in detailed_df."
        )

    # assay_category is derived, not from the graph; ensure column exists
    if "assay_category" not in detailed_df.columns:
        detailed_df["assay_category"] = None

    output_df = detailed_df[[id_col] + ENRICHED_COLS].copy()
    for col in ENRICHED_COLS:
        if col in output_df.columns:
            output_df[col] = (
                output_df[col].astype("object").where(output_df[col].notna(), None)
            )

    # Populate assay_category from normalized assay values
    output_df["assay_category"] = output_df["assay"].map(_get_assay_category)

    # Fix 4: Move misclassified cell types out of cell_line
    # LLM sometimes places cell type terms (e.g. "Monocytes") or patient
    # identifiers (e.g. "Patient-19") in the cell_line column.

    for i in range(len(output_df)):
        cl = output_df.loc[i, "cell_line"] if "cell_line" in output_df.columns else None
        if not cl or (isinstance(cl, float) and pd.isna(cl)):
            continue
        cl_str = str(cl).strip()
        cl_lower = cl_str.lower()
        is_misclassified = (
            cl_lower in KNOWN_CELL_TYPES_NOT_LINES or cl_lower.startswith("patient")
        )
        if is_misclassified:
            # Move to cell_type when it is empty.
            ct = (
                output_df.loc[i, "cell_type"]
                if "cell_type" in output_df.columns
                else None
            )
            if not ct or (isinstance(ct, float) and pd.isna(ct)):
                output_df.loc[i, "cell_type"] = cl_str
                _record_provenance(i, "cell_type", "reclassified_from_cell_line")
            output_df.loc[i, "cell_line"] = None

    # Cellosaurus post-processing

    if cellosaurus:
        if verbose:
            console.print("Applying Cellosaurus post-processing...")
        cello_df, species_refused = apply_cellosaurus_entries(
            output_df,
            _record_provenance,
            tissue_from_embedding_flags,
            expected_taxid,
            expected_species,
        )
    else:
        cello_df = output_df.copy()
        cello_df["organism"] = None
        cello_df["taxid"] = None
        species_refused = set()

    # Add tissue_primary_site and tissue_site_type. For cancer cell lines,
    # Cellosaurus tissue may be a metastatic/effusion site; primary site records
    # the disease origin.

    cello_df["tissue_primary_site"] = None
    cello_df["tissue_site_type"] = None

    for i, row in cello_df.iterrows():
        if i in species_refused:
            # The species gate refused this row's override, but cell_line is still
            # the LLM's own string and still name-matches the foreign entry, so
            # re-deriving the primary site here would leak what we just refused.
            continue
        cell_line_val = row.get("cell_line")
        if not cell_line_val or (
            isinstance(cell_line_val, float) and pd.isna(cell_line_val)
        ):
            continue
        entry = cellosaurus_lookup(str(cell_line_val))
        if not entry:
            continue

        site_type = entry.get("site_type", "")
        tissue = entry.get("tissue", "")
        disease = entry.get("disease", "")

        cello_df.loc[i, "tissue_site_type"] = site_type or None

        if site_type == "Metastatic" or (tissue and _is_metastatic_tissue(tissue)):
            # Look up primary organ from disease
            primary = get_primary_tissue(disease) if disease else None
            if primary and not _is_metastatic_tissue(primary):
                cello_df.loc[i, "tissue_primary_site"] = _title_case(primary)
            else:
                # Primary site unresolved; leave as None
                cello_df.loc[i, "tissue_primary_site"] = None
        elif tissue:
            # In situ: primary site is the tissue itself
            cello_df.loc[i, "tissue_primary_site"] = _title_case(tissue)

    # Build ontology DataFrame (cellosaurus + UBERON/MONDO/CL/EFO).

    from enrich.ontology_map import map_terms

    ontology_df = map_terms(cello_df)

    # Build provenance DataFrame

    prov_rows = []
    for i, row in output_df.iterrows():
        prov_row = {id_col: row[id_col]}
        for field in ENRICHED_COLS:
            prov = _provenance.get(i, {}).get(field, {})
            prov_row[f"{field}_source"] = prov.get("source")
            prov_row[f"{field}_score"] = prov.get("score")
            prov_row[f"{field}_column"] = prov.get("column")
            # Two-level provenance: original source before cellosaurus override
            if prov.get("original_source"):
                prov_row[f"{field}_original_source"] = prov.get("original_source")
                prov_row[f"{field}_original_column"] = prov.get("original_column")
                prov_row[f"{field}_original_score"] = prov.get("original_score")
        prov_rows.append(prov_row)
    provenance_df = pd.DataFrame(prov_rows)
    if species_refused:
        # same shape as archive_fields's provenance_df.attrs["guard_refused"]
        provenance_df.attrs[CELLOSAURUS_SPECIES_GUARD_REFUSED] = len(species_refused)

    return output_df, cello_df, provenance_df, ontology_df


# Dual GSE+SRP enrichment


def _completeness_score(df: pd.DataFrame, enriched_cols: list[str]) -> float:
    """Fraction of non-null enriched cells across the DataFrame."""
    if df.empty:
        return 0.0
    present = [c for c in enriched_cols if c in df.columns]
    if not present:
        return 0.0
    total = len(df) * len(present)
    filled = sum(df[c].notna().sum() for c in present)
    return filled / total


def _remap_srp_to_gsm(srp_df: pd.DataFrame, srp_project) -> pd.DataFrame:
    """Remap SRP rows from SRX identifiers to GSM identifiers."""
    if "Sample Alias" not in srp_project.samples.columns:
        return srp_df

    # Build SRX-to-GSM mapping from original project samples.
    srx_to_gsm = {}
    for _, row in srp_project.samples.iterrows():
        srx = row.get("Accession")
        gsm = row.get("Sample Alias")
        if srx and gsm:
            srx_to_gsm[srx] = gsm

    # The enriched output uses either 'Sample' or 'Accession' as the ID column
    id_col = None
    for col in ("Accession", "Sample"):
        if col in srp_df.columns:
            id_col = col
            break

    if id_col is None:
        return srp_df

    remapped = srp_df.copy()
    remapped[id_col] = remapped[id_col].map(lambda x: srx_to_gsm.get(x, x))
    if id_col == "Accession":
        remapped = remapped.rename(columns={"Accession": "Sample"})
    return remapped


def enrich_df_dual(
    project,
    enrichment_backend="EssentialAI/rnj-1",
    embedding_model="abhinand/MedEmbed-large-v0.1",
    cellosaurus: bool = True,
    verbose: bool = False,
    debug: bool = False,
    borrow_columns: bool = False,
    expected_taxid: str | None = None,
    expected_species: str | None = None,
):
    """
    Enrich paired GSE/SRP metadata and keep the fuller result.

    The losing side can lend columns with better fill rate when borrow_columns is
    set. Returns the same four frames as enrich_df.
    """
    from enrich.projects import fetch_projects, find_counterpart

    acc = project.accession
    counterpart_acc = find_counterpart(acc)

    if not counterpart_acc:
        if verbose:
            console.print(
                f"No counterpart found for {acc}, using single-source enrichment."
            )
        return enrich_df(
            project,
            enrichment_backend=enrichment_backend,
            embedding_model=embedding_model,
            cellosaurus=cellosaurus,
            verbose=verbose,
            debug=debug,
            expected_taxid=expected_taxid,
            expected_species=expected_species,
        )

    if verbose:
        console.print(f"Found counterpart: {acc} <-> {counterpart_acc}")

    # Fetch counterpart project
    try:
        counterpart_projects = fetch_projects([counterpart_acc], timeout=180)
        if not counterpart_projects or counterpart_projects[0].samples.empty:
            if verbose:
                console.print(
                    f"  Counterpart {counterpart_acc} has no samples, using {acc} only."
                )
            return enrich_df(
                project,
                enrichment_backend=enrichment_backend,
                embedding_model=embedding_model,
                cellosaurus=cellosaurus,
                verbose=verbose,
                debug=debug,
                expected_taxid=expected_taxid,
                expected_species=expected_species,
            )
        counterpart_project = counterpart_projects[0]
    except Exception as e:
        if verbose:
            console.print(f"  Failed to fetch counterpart {counterpart_acc}: {e}")
        return enrich_df(
            project,
            enrichment_backend=enrichment_backend,
            embedding_model=embedding_model,
            cellosaurus=cellosaurus,
            verbose=verbose,
            debug=debug,
            expected_taxid=expected_taxid,
            expected_species=expected_species,
        )

    # Enrich primary
    if verbose:
        console.print(f"\n--- Enriching primary: {acc} ---")
    primary_enriched, primary_cello, primary_prov, primary_onto = enrich_df(
        project,
        enrichment_backend=enrichment_backend,
        embedding_model=embedding_model,
        cellosaurus=cellosaurus,
        verbose=verbose,
        debug=debug,
        expected_taxid=expected_taxid,
        expected_species=expected_species,
    )

    # Enrich counterpart
    if verbose:
        console.print(f"\n--- Enriching counterpart: {counterpart_acc} ---")
    counter_enriched, counter_cello, counter_prov, counter_onto = enrich_df(
        counterpart_project,
        enrichment_backend=enrichment_backend,
        embedding_model=embedding_model,
        cellosaurus=cellosaurus,
        verbose=verbose,
        debug=debug,
        expected_taxid=expected_taxid,
        expected_species=expected_species,
    )

    # Enriched columns (excluding ID and assay_category which is derived)
    score_cols = [
        "age",
        "sex",
        "ethnicity",
        "phenotype",
        "cell_type",
        "tissue",
        "strain",
        "disease",
        "assay",
        "cell_line",
        "treatment",
        "development_stage",
        "sample_type",
        "genetic_modification",
    ]

    primary_score = _completeness_score(primary_cello, score_cols)
    counter_score = _completeness_score(counter_cello, score_cols)

    if verbose:
        console.print(
            f"\nCompleteness: {acc}={primary_score:.1%}, "
            f"{counterpart_acc}={counter_score:.1%}"
        )

    def _remap_all(dfs, srp_project):
        return tuple(_remap_srp_to_gsm(df, srp_project) for df in dfs)

    if not borrow_columns:
        # Winner-takes-all: use the more complete side entirely
        if counter_score > primary_score:
            if verbose:
                console.print(
                    f"  -> Using counterpart {counterpart_acc} (more complete)"
                )
            if acc.startswith("SRP") and counterpart_acc.startswith("GSE"):
                return counter_enriched, counter_cello, counter_prov, counter_onto
            return _remap_all(
                (counter_enriched, counter_cello, counter_prov, counter_onto),
                counterpart_project,
            )
        if verbose:
            console.print(f"  -> Using primary {acc}")
        if acc.startswith("SRP") and counterpart_acc.startswith("GSE"):
            return _remap_all(
                (primary_enriched, primary_cello, primary_prov, primary_onto),
                project,
            )
        return primary_enriched, primary_cello, primary_prov, primary_onto

    # Per-column merging (borrow_columns=True)
    # Remap SRP side to GSM IDs so both sides share the same sample identifiers.
    if acc.startswith("SRP") and counterpart_acc.startswith("GSE"):
        primary_enriched, primary_cello, primary_prov, primary_onto = _remap_all(
            (primary_enriched, primary_cello, primary_prov, primary_onto), project
        )
    elif acc.startswith("GSE") and counterpart_acc.startswith("SRP"):
        counter_enriched, counter_cello, counter_prov, counter_onto = _remap_all(
            (counter_enriched, counter_cello, counter_prov, counter_onto),
            counterpart_project,
        )

    # Use the higher-completeness side as base, borrow from the other.
    if counter_score > primary_score:
        base_enriched, donor_enriched = counter_enriched.copy(), primary_enriched
        base_cello, donor_cello = counter_cello.copy(), primary_cello
        base_prov, donor_prov = counter_prov.copy(), primary_prov
        base_onto, donor_onto = counter_onto.copy(), primary_onto
        base_label, donor_label = counterpart_acc, acc
    else:
        base_enriched, donor_enriched = primary_enriched.copy(), counter_enriched
        base_cello, donor_cello = primary_cello.copy(), counter_cello
        base_prov, donor_prov = primary_prov.copy(), counter_prov
        base_onto, donor_onto = primary_onto.copy(), counter_onto
        base_label, donor_label = acc, counterpart_acc

    donor_id_col = "Sample" if "Sample" in donor_cello.columns else "Accession"

    borrowed_cols = []
    for col in score_cols:
        if col not in base_cello.columns and col not in donor_cello.columns:
            continue
        base_fill = base_cello[col].notna().mean() if col in base_cello.columns else 0.0
        donor_fill = (
            donor_cello[col].notna().mean() if col in donor_cello.columns else 0.0
        )

        if donor_fill > base_fill:
            for base_df, donor_df, d_id in [
                (base_enriched, donor_enriched, donor_id_col),
                (base_cello, donor_cello, donor_id_col),
                (base_onto, donor_onto, donor_id_col),
            ]:
                if col in donor_df.columns:
                    d_lookup = dict(zip(donor_df[d_id], donor_df[col]))
                    b_id = "Sample" if "Sample" in base_df.columns else "Accession"
                    donor_vals = base_df[b_id].map(d_lookup)
                    if col in base_df.columns:
                        base_df[col] = donor_vals.fillna(base_df[col])
                    else:
                        base_df[col] = donor_vals

            for suffix in ("_source", "_column", "_score"):
                prov_col = col + suffix
                if prov_col in donor_prov.columns:
                    d_lookup = dict(zip(donor_prov[donor_id_col], donor_prov[prov_col]))
                    p_id = "Sample" if "Sample" in base_prov.columns else "Accession"
                    donor_vals = base_prov[p_id].map(d_lookup)
                    if prov_col in base_prov.columns:
                        base_prov[prov_col] = donor_vals.fillna(base_prov[prov_col])
                    else:
                        base_prov[prov_col] = donor_vals

            borrowed_cols.append(f"{col} ({donor_fill:.0%} vs {base_fill:.0%})")

    if verbose:
        if borrowed_cols:
            console.print(
                f"  -> Base: {base_label}, borrowed from {donor_label}: "
                + ", ".join(borrowed_cols)
            )
        else:
            console.print(f"  -> Using {base_label} (no columns to borrow)")

    return base_enriched, base_cello, base_prov, base_onto
