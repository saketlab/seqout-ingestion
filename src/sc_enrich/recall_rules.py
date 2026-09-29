"""
Extra single-cell evidence for the recall_fix study_single_cell build.

The recall_fix build (recipes/recall_fix/build.py) calls a study single-cell from
pentimento reads, a title/description regex and the archive library_source.
Checked against Arc scBaseCount and CZI Census, the studies it misses fall into a
few classes. This module holds the rules for them as pure functions, so they can
be tested offline and replayed on a local copy. The Postgres feeds are in
sc_enrich.recall_feeds.

  protocol text   the platform is named only in per-experiment or per-GSM
                  protocol text ("Chromium Next GEM Single Cell 3' v3.1"), which
                  the study-level regex never reads
  library_source  the archive declares TRANSCRIPTOMIC/GENOMIC SINGLE CELL.
                  Microbial single-cell genomes and chromatin-only studies are
                  vetoed
  whitelist miss  the only negative read calls are barcode-whitelist misses (VDJ,
                  feature-barcode, non-10x libraries), not R1-length bulk calls,
                  and the archive declares most libraries single-cell
  alias keys      a verdict staged under an archive accession (SRP/ERP/PRJEB)
                  that is not the unified_metadata canonical. transfer.py joins
                  on the canonical, so such rows were dropped
  text abstain    a study whose every pentimento run abstained was staged
                  'not_screened', and the text pass then never set its verdict.
                  Restored, with the audit's text vetoes on the restored rows
  external        optional and labelled: the study is in an external single-cell
                  resource (scBaseCount, Census), kept separate so its share of a
                  recovery can be reported

Left out because precision is below the 0.85 catalogue baseline
(seqout-paper singlecell_coverage_gaps 05_precision): wider title wording (0.71),
unread study fields (0.60), ontology assay terms (0.40), Smart-seq-only mentions,
sc_stats_study matrix-file counts on their own, and study-level pentimento_study
cell counts (0.58 on this branch's development sample: ChIP-seq SubSeries, bulk
RNA and microarray studies carry spurious counts).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Container, Iterable, Mapping

# Statuses this module writes. Each sets is_single_cell = 1.
PROTOCOL_STATUS = "protocol_indicates_single_cell"
LIBSOURCE_STATUS = "library_source_indicates_single_cell"
EXTERNAL_STATUS = "external_resource_indicates_single_cell"

_Q = "['’′`]"  # noqa: RUF001 - prime marks used for 3'/5' in kit names
_S = r"[\s\-]"

# Named single-cell platforms and kits, as they appear in protocol text. Bare
# "10x" is not enough: it matches buffers ("10x PBS"), magnification and
# linked-read genomes, so it must sit next to a kit word.
PLATFORM_RX = re.compile(
    rf"Chromium{_S}*(?:TM\s*)?(?:Single{_S}?Cell|Next{_S}?GEM|Controller|X\b|iX\b"
    r"|Connect|Flex)"
    rf"|Next{_S}?GEM\s+(?:Single|Automated|Chip|Gel)|\bGEM{_S}X\b"
    rf"|Single{_S}?Cell\s+[35]{_Q}?\s*(?:Reagent|Library|Gene|GEX|Kit|v\d|RNA|Chip"
    r"|Gel|HT\b|Feature)"
    rf"|\b10x\s*(?:Genomics\s*)?(?:Chromium|[35]{_Q}\s*(?:v\d|GEX|gene|kit|library"
    r"|single|reagent|chemistry)|v[1-4](?:\.1)?\s*(?:chemistry|kit|reagent)"
    rf"|single{_S}?(?:cell|nucle\w*)|sc(?:RNA|ATAC)|sn(?:RNA|ATAC)|Multiome"
    r"|V\(D\)J|Visium|Xenium|Flex\b)"
    r"|Cell\s?Ranger|\bSTARsolo\b"
    r"|BD\s*Rhapsody|Evercode|Parse\s+Biosciences|\bScale\s?Bio|Singleron|GEXSCOPE"
    r"|DNBelab\s*C4|MobiDrop|SeekOne|\bPIP[\s\-]?seq|Fluent\s+Bio|ICELL8"
    rf"|Fluidigm\s+C1|\bC1\s+(?:Single{_S}?Cell|IFC|system)"
    rf"|(?<!nano)Drop{_S}?seq|\binDrops?\b|Seq{_S}Well|SPLiT{_S}?seq"
    rf"|sci{_S}?RNA{_S}?seq|Microwell{_S}?seq|HIVE\s+(?:scRNA|single)|Honeycomb\s+Bio"
    rf"|\b(?:sc|sn)ATAC{_S}?seq|sci{_S}?ATAC|Single{_S}?Cell\s+ATAC"
    rf"|\bVisium\b|Slide{_S}?seq|Stereo{_S}?seq",
    re.IGNORECASE,
)

# Plate chemistries double as bulk library preps (development sample: bulk
# MARS-seq GSE247422, CEL-Seq2 on larval RNA GSE130274, Smart-seq3 on tissue
# GSE288746). Like "10x Genomics", they count only with single-cell wording in
# the same record, e.g. "standard 10X genomics protocols | single-cell RNA-seq".
_TENX = re.compile(r"\b10x\s*Genomics\b", re.IGNORECASE)
_PLATE = re.compile(
    rf"CEL{_S}?seq2?|MARS{_S}?seq|Smart{_S}?seq3|STRT{_S}?seq|Quartz{_S}?seq2|VASA{_S}?seq"
    rf"|FLASH{_S}?seq",
    re.IGNORECASE,
)
_SC_WORDS = re.compile(
    rf"single{_S}?(?:cell|nucle(?:us|i))|\bsc{_S}?RNA|\bsn{_S}?RNA|\bscATAC|\bsnATAC"
    r"|droplet",
    re.IGNORECASE,
)

# Records that name a platform but are not single-cell omics. Audited FP classes:
# 10x linked-read / GemCode genomes, spatial ROI (GeoMx), bulk libraries.
PROTOCOL_VETO_RX = re.compile(
    rf"linked{_S}?reads?|GemCode|Chromium\s+Genome|Long\s?Ranger|Supernova"
    rf"|de\s+novo\s+assembl|genome\s+assembly|high{_S}molecular{_S}weight\s+DNA"
    r"|\bHMW\s+DNA|GeoMx|\bbulk\s+(?:RNA|ATAC|sequencing|librar|transcriptom|population)",
    re.IGNORECASE,
)


def protocol_says_single_cell(text: str | None) -> bool:
    """Say whether one experiment/GSM record names a single-cell platform or kit."""
    if not text or PROTOCOL_VETO_RX.search(text):
        return False
    if PLATFORM_RX.search(text):
        return True
    needs_context = _TENX.search(text) or _PLATE.search(text)
    return bool(needs_context and _SC_WORDS.search(text))


def platform_snippet(text: str, width: int = 90) -> str:
    """Return the matched platform with some context, for review."""
    m = PLATFORM_RX.search(text) or _TENX.search(text) or _PLATE.search(text)
    if not m:
        return text[: 2 * width]
    return text[max(0, m.start() - width) : m.end() + width].replace("\n", " ")


# Microbial single-cell amplified genomes (JGI/SCGC) declare GENOMIC SINGLE CELL
# legitimately, but they are genomes, not single-cell omics studies.
MICROBIAL_ORGANISM_RX = re.compile(
    r"bacteri|archae|metagenome|Candidatus|\bsp\.|phage|virus|Pelagibacter"
    r"|Prochlorococcus|Synechococcus",
    re.IGNORECASE,
)
GENOME_PROJECT_TITLE_RX = re.compile(
    r"\bSCGC\b|\bJGI\b|Genome sequencing(?: and assembly)?$|genome assembly"
    r"|reference genome"
    rf"|linked{_S}read|Chromium Genome|Microbial Dark Matter",
    re.IGNORECASE,
)
# Strategies that cannot carry a single-cell RNA or ATAC library. A study whose
# every library is one of these is a library_source mislabel (audit: CRA009475).
CHROMATIN_ONLY = frozenset(
    {
        "ChIP-Seq",
        "ChIP",
        "MNase-Seq",
        "MeDIP-Seq",
        "MBD-Seq",
        "MRE-Seq",
        "DNase-Hypersensitivity",
        "FAIRE-seq",
        "ChIA-PET",
        "Hi-C",
        "Tethered Chromatin Conformation Capture",
        "SELEX",
        "Tn-Seq",
        "miRNA-Seq",
    }
)


# A bracketed SubSeries tag naming a non-single-cell assay ("[ChIP-seq]",
# "[PRO-seq]", "[bulk RNA-seq]") marks a bulk companion of a single-cell paper.
_TAG = re.compile(r"\[([^\]]{1,60})\]")
_NON_SC_ASSAY = re.compile(
    r"\b(?:ChIP|CUT&(?:Tag|RUN)|ATAC|PRO|GRO|Micro-?C|Hi-?C|HiChIP|bulk|WGS|WES|exome"
    r"|methyl\w*|bisulfite|RRBS|Ribo|CLIP|MPRA|SLAM|PARS|RNA-?seq|RIP|4C|DamID)\b",
    re.IGNORECASE,
)


def text_promotion_vetoed(title: str | None, description: str | None) -> bool:
    """
    Say whether a text-only verdict should not be restored on an abstained study.

    The audit's false-positive classes for the text path: bulk companions tagged
    in the title, 10x linked-read genomes, and bulk studies citing single-cell
    data ("bulk RNA sequencing ... integrated with single-cell RNA sequencing").
    """
    for tag in _TAG.findall(title or ""):
        if _NON_SC_ASSAY.search(tag) and not _SC_WORDS.search(tag):
            return True
    return bool(PROTOCOL_VETO_RX.search(f"{title or ''} | {description or ''}"))


def libsource_vetoed(
    organism: str | None, title: str | None, strategies: Iterable[str]
) -> str | None:
    """Reason a declared SINGLE CELL library_source is not trusted, else None."""
    if organism and MICROBIAL_ORGANISM_RX.search(organism):
        return "microbial_organism"
    if title and GENOME_PROJECT_TITLE_RX.search(title):
        return "genome_project_title"
    strat = {s for s in strategies if s}
    if strat and strat <= CHROMATIN_ONLY:
        return "non_rna_strategies_only"
    return None


def whitelist_miss_only(
    n_runs_bulk_r1len: int, n_runs_bulk_any: int, n_runs_sc: int
) -> bool:
    """
    Say whether the only negative read calls are barcode-whitelist misses.

    `assay_is_single_cell = FALSE` means no read matched an RNA whitelist. VDJ,
    feature-barcode, ATAC and non-10x libraries do the same, so on its own it is
    an abstention, not a bulk verdict. An R1-length 'bulk' call is read structure
    and is left alone.
    """
    return n_runs_sc == 0 and n_runs_bulk_any > 0 and n_runs_bulk_r1len == 0


DECLARED_MAJORITY = 0.5


def declared_overrules_whitelist_miss(
    lib_n_sc: int, lib_n_exp: int, **counts: int
) -> bool:
    """Let SINGLE CELL declared on most libraries outrank whitelist misses."""
    if lib_n_exp <= 0 or lib_n_sc / lib_n_exp < DECLARED_MAJORITY:
        return False
    return whitelist_miss_only(**counts)


# Which status wins when two archive keys of one canonical study both carry a
# verdict. A measured positive beats everything, as in _classify(). A measured
# negative beats declared/protocol/text evidence; text plus a measured negative is
# recorded as the disagreement it is.
_RANK = {
    "measured_single_cell": 7,
    "measured_not_single_cell_text_disagrees": 6,
    "measured_not_single_cell": 5,
    LIBSOURCE_STATUS: 4,
    PROTOCOL_STATUS: 3,
    "text_indicates_single_cell": 2,
    EXTERNAL_STATUS: 1,
    "not_screened": 0,
}
IS_SC = {
    "measured_single_cell": 1,
    "measured_not_single_cell_text_disagrees": 0,
    "measured_not_single_cell": 0,
    LIBSOURCE_STATUS: 1,
    PROTOCOL_STATUS: 1,
    "text_indicates_single_cell": 1,
    EXTERNAL_STATUS: 1,
    "not_screened": None,
}


def merge_status(a: str | None, b: str | None) -> str:
    """Combine two keys' statuses for one canonical study."""
    a, b = a or "not_screened", b or "not_screened"
    if {a, b} == {"measured_not_single_cell", "text_indicates_single_cell"}:
        return "measured_not_single_cell_text_disagrees"
    return max((a, b), key=_RANK.__getitem__)


def resolve_canonical(
    acc: str,
    alias_map: Mapping[str, str],
    twins: Mapping[str, str],
    canonicals: Container[str] = frozenset(),
) -> str | None:
    """
    Return the unified_metadata canonical for an archive accession.

    A canonical resolves to itself. That differs from the matview's alias_map
    (min canonical wins), which would move an SRP twin canonical's own verdict
    onto its GSE twin and leave the SRP row unset. Otherwise alias_map covers
    unified_metadata.aliases, and `twins` is the archive cross-link
    (sra_studies.alias SRP <-> GSE, ena_studies secondary PRJEB <-> ERP), used
    only when the accession is not an alias.
    """
    if acc in canonicals:
        return acc
    if acc in alias_map:
        return alias_map[acc]
    twin = twins.get(acc)
    if twin is None:
        return None
    return twin if twin in canonicals else alias_map.get(twin)


# Passes over the staged table. A staged row is the study_single_cell tuple:
# (acc, is_single_cell, status, modality, modality_evidence, max_cells,
#  max_genes, n_counted, n_runs_screened, n_runs_single_cell).
# Each pass returns a new dict and a count; nothing is mutated in place.
Row = tuple
Staged = dict[str, Row]
_EMPTY_TAIL = (None, None, 0, 0, 0)  # max_cells .. n_runs_single_cell


def canonicalise(
    staged: Staged, resolve: Callable[[str], str | None]
) -> tuple[Staged, int]:
    """
    Re-key rows staged under an archive accession onto their canonical study.

    transfer.py updates unified_metadata by canonical_accession, so a verdict
    staged under the SRP of a GSE-canonical study never lands. Keys that do not
    resolve are kept (they may be canonicals missing from the map).
    """
    out: Staged = {}
    moved = 0
    for acc, old in staged.items():
        key = resolve(acc) or acc
        moved += key != acc
        row = (key, *old[1:])
        prev = out.get(key)
        if prev is None:
            out[key] = row
            continue
        status = merge_status(prev[2], row[2])
        # evidence columns come from the row whose verdict won; a modality known
        # under either key is kept
        win, other = (prev, row) if _RANK[prev[2]] >= _RANK[row[2]] else (row, prev)
        modality, mod_ev = (win[3], win[4]) if win[3] else (other[3], other[4])
        out[key] = (key, IS_SC[status], status, modality, mod_ev, *win[5:])
    return out, moved


def apply_text(
    staged: Staged,
    text_hits: Mapping[str, str | None],
    vetoed: Container[str] = frozenset(),
    corroborated: Container[str] | None = None,
) -> tuple[Staged, int]:
    """
    Title/description regex pass, with the abstention bug fixed.

    A study whose every pentimento run abstained is staged 'not_screened' by the
    read pass. The old loop then only filled modality, so the text verdict was
    lost. Here 'not_screened' gets the text verdict unless the study is in
    `vetoed` (text_promotion_vetoed) or, when `corroborated` is given, lacks
    independent archive evidence (a protocol-text hit or an unvetoed declared
    library_source). Text alone restored at 0.80 precision, below the 0.85 bar.
    A study with no row keeps the old rule.
    """
    out = dict(staged)
    added = 0
    for acc, modality in text_hits.items():
        prev = out.get(acc)
        restoring = prev is not None and prev[2] == "not_screened"
        if restoring and (
            acc in vetoed or (corroborated is not None and acc not in corroborated)
        ):
            continue
        if prev is None or restoring:
            keep_mod = prev[3] if prev else None
            keep_ev = prev[4] if prev else None
            out[acc] = (
                acc,
                1,
                "text_indicates_single_cell",
                modality or keep_mod,
                "text_regex" if modality else keep_ev,
                *(prev[5:] if prev else _EMPTY_TAIL),
            )
            added += 1
        elif prev[2] == "measured_not_single_cell":
            out[acc] = (*prev[:2], "measured_not_single_cell_text_disagrees", *prev[3:])
        elif prev[3] is None and modality:
            out[acc] = (*prev[:3], modality, "text_regex", *prev[5:])
    return out, added


def apply_additive(
    staged: Staged, accs: Iterable[str], status: str
) -> tuple[Staged, int]:
    """Fill studies with no verdict ('not_screened' or absent). Never overrides."""
    out = dict(staged)
    added = 0
    for acc in accs:
        prev = out.get(acc)
        if prev is not None and prev[2] != "not_screened":
            continue
        tail = prev[3:] if prev else (None, None, *_EMPTY_TAIL)
        out[acc] = (acc, IS_SC[status], status, *tail)
        added += 1
    return out, added


def apply_whitelist_override(
    staged: Staged,
    declared: Mapping[str, tuple[int, int]],
    reads: Mapping[str, tuple[int, int, int]],
) -> tuple[Staged, int]:
    """
    Let declared SINGLE CELL libraries outrank a negative of whitelist misses.

    declared[acc] = (n_sc, n_exp); reads[acc] = (n_runs_sc, n_runs_bulk,
    n_runs_bulk_r1len).
    """
    out = dict(staged)
    flipped = 0
    for acc, (n_sc, n_exp) in declared.items():
        prev = out.get(acc)
        if prev is None or prev[1] != 0:
            continue
        n_runs_sc, n_bulk, n_bulk_r1 = reads.get(acc, (0, 0, 0))
        if declared_overrules_whitelist_miss(
            n_sc,
            n_exp,
            n_runs_bulk_r1len=n_bulk_r1,
            n_runs_bulk_any=n_bulk,
            n_runs_sc=n_runs_sc,
        ):
            out[acc] = (acc, 1, LIBSOURCE_STATUS, *prev[3:])
            flipped += 1
    return out, flipped
