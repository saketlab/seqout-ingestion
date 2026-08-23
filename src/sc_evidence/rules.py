"""
Rule table for single_cell_evidence builds.

Each rule carries a SQLite gate plus Python extraction regex. Invariants: explicit
case-folding, raw matched text, and regex scoped to LIKE traps.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass, field

RULE_VERSION = "v2"

# Longest value we keep. Long enough to re-judge a match by eye, short
# enough to avoid carrying protocol paragraphs across the ArrayExpress rows.
MAX_VALUE_LEN = 200

# Raw-text context around a prose match lets the stored value support review.
_CONTEXT = 40


@dataclass(frozen=True)
class Term:
    """
    One alternative of a rule predicate, in both dialects.

    `regex` is the Python form; `like` lists lowercase SQLite LIKE patterns because
    the predicate folds the column with `lower()`.
    """

    regex: str
    like: tuple[str, ...]


def _like_any(expr: str, terms: Sequence[Term]) -> str:
    """
    Build the SQL predicate matching `expr` against any term.

    Callers pass case-folded expr. ESCAPE preserves literal underscores while
    plain underscores cover prototype `.?` alternations.
    """
    patterns = [p for t in terms for p in t.like]
    return "(" + " OR ".join(f"{expr} LIKE '{p}' ESCAPE '\\'" for p in patterns) + ")"


def _sql_in(values: Sequence[str]) -> str:
    """Render a SQL IN list. Values are module constants, never user input."""
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def _context_re(terms: Sequence[Term]) -> re.Pattern[str]:
    """Python extractor: the literal match, with a little raw context."""
    alternation = "|".join(t.regex for t in terms)
    return re.compile(
        rf".{{0,{_CONTEXT}}}(?:{alternation}).{{0,{_CONTEXT}}}",
        re.IGNORECASE,
    )


# term sets

# Substring traps live in regex because SQLite LIKE has no lookaround. The SQL
# gate stays wide; Python narrows and build._iter_rule_rows counts dropped rows.
#
# Known traps: `single.?nucle` hits SNP/polymorphism studies; `10x` hits
# sequencer codes, catalogue ids, DOIs, indexes and sample ids; `drop.?seq`
# hits "Nanodrop sequencing".
#
# Rejected seqspec terms: bare fluidigm names bulk Access Array/BioMark;
# pip-seq names protein interaction profile sequencing outside enriched
# vocabulary; seqwell, quartz and pi-atac collide with prose. Bare decomposed
# tokens (`sci`, `sn`, `cel`, `pip`, `parse`) collide too.

# Prose wording for SRA/ENA experiment text. GENERIC_TERMS are broad signals;
# PLATFORM_TERMS below is the named-protocol vocabulary.
GENERIC_TERMS: tuple[Term, ...] = (
    Term(r"single.?cell", ("%singlecell%", "%single_cell%")),
    Term(r"single.?nucle(?!otide)", ("%singlenucle%", "%single_nucle%")),
    Term(r"scrna", ("%scrna%",)),
    Term(r"snrna", ("%snrna%",)),
    Term(r"scatac", ("%scatac%",)),
    Term(r"sn[-_. ]?atac", ("%snatac%", "%sn_atac%")),
    Term(r"(?<![A-Za-z0-9])10x", ("%10x%",)),
    Term(r"chromium", ("%chromium%",)),
    Term(r"smart.?seq", ("%smartseq%", "%smart_seq%")),
    Term(r"(?<!nano)drop.?seq", ("%dropseq%", "%drop_seq%")),
    Term(r"cite.?seq", ("%citeseq%", "%cite_seq%")),
    Term(r"multiome", ("%multiome%",)),
    Term(r"visium", ("%visium%",)),
)

# Named-protocol vocabulary, derived from preflightx's vendored seqspec assays
# and display table spellings. Plate and combinatorial protocols often have no
# CellRanger output, no `10x` token and no INSDC single-cell declaration, so the
# generic terms miss them. Terms below were checked against raw matched values;
# rejected candidates are listed in the trap notes above.
PLATFORM_TERMS: tuple[Term, ...] = (
    # CEL-seq / CEL-seq2. `(?!l)` stops `cel.?seq` matching `cellseq`.
    Term(r"cel(?!l)[-_. ]?seq", ("%celseq%", "%cel_seq%")),
    # MARS-seq. `(?!uenc)` guards facility wording around MARS sequencing.
    Term(r"mars[-_. ]?seq(?!uenc)", ("%marsseq%", "%mars_seq%")),
    # SCRB-seq / mcSCRB-seq / msSCRB-seq.
    Term(r"(?:mc|ms)?scrb[-_. ]?seq", ("%scrbseq%", "%scrb_seq%")),
    # Microwell-seq. `seq` keeps platform prose such as microwell filters out.
    Term(r"microwell[-_. ]?seq", ("%microwellseq%", "%microwell_seq%")),
    # Parse Biosciences (SPLiT-seq commercialised). Avoid bare `parse`, which is
    # ordinary English; anchor on the company name and product line.
    Term(r"parse ?bio|evercode", ("%parsebio%", "%parse_bio%", "%evercode%")),
    # sci-RNA-seq / sci-RNA-seq3 and their ATAC sibling. Avoid bare `sci`; it is
    # too broad outside the assay names.
    Term(r"(?<![a-z0-9])sci[-_. ]?rna", ("%scirna%", "%sci_rna%")),
    Term(r"(?<![a-z0-9])sci[-_. ]?atac", ("%sciatac%", "%sci_atac%")),
    Term(r"(?<![a-z0-9])scifi", ("%scifi%",)),
    # Seq-Well requires the separator. Solid `seqwell` is seqWell Inc., which
    # sells bulk library-prep kits; the Shalek-lab assay is written Seq-Well.
    Term(r"seq[-_.]well", ("%seq_well%",)),
    # SHARE-seq and SPLiT-seq. `(?!uenc)` guards demultiplexing script names.
    Term(r"share[-_. ]?seq(?!uenc)", ("%shareseq%", "%share_seq%")),
    Term(r"split[-_. ]?seq(?!uenc)", ("%splitseq%", "%split_seq%")),
    # STRT-seq. Guards are per term; STRT sequencing is the assay phrasing.
    Term(r"(?<![a-z0-9])strt[-_. ]?seq", ("%strtseq%", "%strt_seq%")),
    # SureCell / ddSEQ (Bio-Rad). `(?<![a-z])ddseq` excludes `maddseq`.
    Term(r"surecell", ("%surecell%",)),
    Term(r"(?<![a-z])ddseq", ("%ddseq%",)),
    Term(r"vasa[-_. ]?seq", ("%vasaseq%", "%vasa_seq%")),
    # BD Rhapsody requires adjacent `BD`; bare `rhapsody` hit a flower cultivar
    # and a PacBio sample, so most new SRA studies were wrong. Cost: SRP308295 is
    # missed with "BD targeted multiplex Rhapsody analysis pipeline".
    Term(r"bd[-_. ]?rhapsody", ("%bdrhapsody%", "%bd_rhapsody%")),
    Term(r"issaac", ("%issaac%",)),
    Term(r"dogma[-_. ]?seq", ("%dogmaseq%", "%dogma_seq%")),
    # snmC-seq / snmCT-seq / sn-m3C-seq, the single-nucleus methylome family.
    # The largest single contribution in the whole exercise: 647 + 129 studies
    # on the enrichment channel alone, none of which carries an
    # `assay_category` and none of which any shipped term matched.
    Term(r"snm(?:3c|ct|c)\d?[-_. ]?seq", ("%snmc%", "%snm3c%")),
    Term(r"m3c[-_. ]?seq", ("%m3cseq%", "%m3c_seq%")),
    Term(r"sccrispr", ("%sccrispr%",)),
    Term(r"quartz[-_. ]?seq", ("%quartzseq%", "%quartz_seq%")),
    # inDrop. The boundary rejects vendor and genus collisions: `findrop`,
    # `raindrop`, `Cylindrophis`, `Cylindropuntia`, and `Cylindropsis`.
    Term(r"(?<![a-z])indrop", ("%indrop%",)),
    # Forward-coverage terms retained as named assays.
    Term(r"sugar[-_. ]?seq", ("%sugarseq%", "%sugar_seq%")),
    Term(r"tang ?2009", ("%tang2009%", "%tang_2009%")),
)

PROSE_TERMS: tuple[Term, ...] = (*GENERIC_TERMS, *PLATFORM_TERMS)

# Visium sits in GENERIC_TERMS, which the SRA/ENA prose channel also reads.
# For AE, prose is the main channel because library_source is often NULL.
AE_TERMS: tuple[Term, ...] = PROSE_TERMS

# GEA MAGE-TAB experiments. PLATFORM_TERMS join after the GEA archive file
# participates in the same rebuild, so sc_rule.gate reflects work run.
GEA_TERMS: tuple[Term, ...] = (
    Term(r"single.?cell", ("%singlecell%", "%single_cell%")),
    Term(r"single.?nucle(?!otide)", ("%singlenucle%", "%single_nucle%")),
    Term(r"scrna", ("%scrna%",)),
    Term(r"snrna", ("%snrna%",)),
    Term(r"(?<![A-Za-z0-9])10x", ("%10x%",)),
    Term(r"chromium", ("%chromium%",)),
    Term(r"smart.?seq", ("%smartseq%", "%smart_seq%")),
    Term(r"(?<!nano)drop.?seq", ("%dropseq%", "%drop_seq%")),
)

# Enrichment `assay` vocabulary: normalised model values such as scRNA-Seq,
# 10x 3' v3 and Visium HD. Store it verbatim for future wording.
ENRICHED_TERMS: tuple[Term, ...] = (
    Term(r"single.?cell", ("%singlecell%", "%single_cell%")),
    Term(r"single.?nucle(?!otide)", ("%singlenucle%", "%single_nucle%")),
    Term(r"scrna", ("%scrna%",)),
    Term(r"snrna", ("%snrna%",)),
    Term(r"scatac", ("%scatac%",)),
    Term(r"snatac", ("%snatac%",)),
    Term(r"(?<![A-Za-z0-9])10x", ("%10x%",)),
    Term(r"chromium", ("%chromium%",)),
    Term(r"smart.?seq", ("%smartseq%", "%smart_seq%")),
    Term(r"(?<!nano)drop.?seq", ("%dropseq%", "%drop_seq%")),
    Term(r"cite.?seq", ("%citeseq%", "%cite_seq%")),
    Term(r"visium", ("%visium%",)),
    Term(r"indrop", ("%indrop%",)),
    Term(r"seq.?well", ("%seqwell%", "%seq_well%")),
    Term(r"multiome", ("%multiome%",)),
    Term(r"perturb", ("%perturb%",)),
    Term(r"merfish", ("%merfish%",)),
    Term(r"xenium", ("%xenium%",)),
    Term(r"slide.?seq", ("%slideseq%", "%slide_seq%")),
    Term(r"cosmx", ("%cosmx%",)),
    # Seqspec named protocols in LIKE form. Rule 13 has no extractor, so the SQL
    # gate is the whole gate and `_` is LIKE's any-character wildcard.
    Term(r"cel(?!l)[-_. ]?seq", ("%celseq%", "%cel_seq%")),
    Term(r"mars[-_. ]?seq(?!uenc)", ("%marsseq%", "%mars_seq%")),
    Term(r"(?:mc|ms)?scrb[-_. ]?seq", ("%scrbseq%", "%scrb_seq%")),
    Term(r"microwell[-_. ]?seq", ("%microwellseq%", "%microwell_seq%")),
    Term(r"parse ?bio|evercode", ("%parsebio%", "%parse_bio%", "%evercode%")),
    Term(r"(?<![a-z0-9])sci[-_. ]?rna", ("%scirna%", "%sci_rna%")),
    Term(r"(?<![a-z0-9])sci[-_. ]?atac", ("%sciatac%", "%sci_atac%")),
    Term(r"(?<![a-z0-9])scifi", ("%scifi%",)),
    Term(r"share[-_. ]?seq(?!uenc)", ("%shareseq%", "%share_seq%")),
    Term(r"split[-_. ]?seq(?!uenc)", ("%splitseq%", "%split_seq%")),
    Term(r"(?<![a-z0-9])strt[-_. ]?seq", ("%strtseq%", "%strt_seq%")),
    Term(r"surecell", ("%surecell%",)),
    Term(r"(?<![a-z])ddseq", ("%ddseq%",)),
    Term(r"vasa[-_. ]?seq", ("%vasaseq%", "%vasa_seq%")),
    Term(r"bd[-_. ]?rhapsody", ("%bdrhapsody%", "%bd_rhapsody%")),
    Term(r"issaac", ("%issaac%",)),
    Term(r"dogma[-_. ]?seq", ("%dogmaseq%", "%dogma_seq%")),
    Term(r"snm(?:3c|ct|c)\d?[-_. ]?seq", ("%snmc%", "%snm3c%")),
    Term(r"m3c[-_. ]?seq", ("%m3cseq%", "%m3c_seq%")),
    Term(r"sccrispr", ("%sccrispr%",)),
    Term(r"quartz[-_. ]?seq", ("%quartzseq%", "%quartz_seq%")),
    Term(r"sugar[-_. ]?seq", ("%sugarseq%", "%sugar_seq%")),
    Term(r"tang ?2009", ("%tang2009%", "%tang_2009%")),
    # Solid spelling in this channel. `%pip_seq%` catches unrelated names, and
    # `pip-seq` in archive prose is ambiguous.
    Term(r"pipseq", ("%pipseq%",)),
)

# Controlled enrichment assay_category. Term lists stay because category is sparse.
ENRICHED_SC_CATEGORIES: tuple[str, ...] = (
    "SINGLE-CELL TRANSCRIPTOMICS",
    "SINGLE-CELL EPIGENOMICS",
    "SINGLE-CELL MULTI-OMICS",
    "SINGLE-CELL GENOMICS",
    "SPATIAL TRANSCRIPTOMICS",
)

# Study-prose columns. Assay mentions here are study-title broadcasts, so the
# rule refuses them. Empty guard for future enrichment generations.
ENRICHED_PROSE_COLUMNS: tuple[str, ...] = (
    "title",
    "description",
    "summary",
    "overall design",
    "overall_design",
    "study title",
    "series title",
    "sample title",
    "study_title",
    "series_summary",
    "abstract",
)

# scATAC wording. Few scATAC libraries carry single-cell
# library_source, so strategy plus wording is the rule.
SCATAC_NAME_TERMS: tuple[Term, ...] = (
    Term(r"scatac", ("%scatac%",)),
    Term(r"sn.?atac", ("%snatac%", "%sn_atac%")),
    Term(r"single.?cell", ("%singlecell%", "%single_cell%")),
    Term(r"single.?nucle(?!otide)", ("%singlenucle%", "%single_nucle%")),
    Term(r"(?<![A-Za-z0-9])10x", ("%10x%",)),
    Term(r"chromium", ("%chromium%",)),
)
SCATAC_TITLE_TERMS: tuple[Term, ...] = SCATAC_NAME_TERMS[:4]

# 10x / CellRanger output names from the plan. GEO carries no library_source,
# so files are its controlled signal. .h5ad / .loom prove processed data.
TENX_FILE_TERMS: tuple[Term, ...] = (
    Term(r"barcodes\.tsv", ("%barcodes.tsv%",)),
    Term(r"features\.tsv", ("%features.tsv%",)),
    Term(r"genes\.tsv", ("%genes.tsv%",)),
    Term(r"matrix\.mtx", ("%matrix.mtx%",)),
    Term(r"filtered_feature_bc_matrix", (r"%filtered\_feature\_bc\_matrix%",)),
    Term(r"raw_feature_bc_matrix", (r"%raw\_feature\_bc\_matrix%",)),
    Term(r"molecule_info\.h5", (r"%molecule\_info.h5%",)),
    Term(r"web_summary\.html", (r"%web\_summary.html%",)),
    Term(r"cloupe", ("%cloupe%",)),
    Term(r"cellranger", ("%cellranger%",)),
    Term(r"spaceranger", ("%spaceranger%",)),
    Term(r"fragments\.tsv", ("%fragments.tsv%",)),
    Term(r"peak_bc_matrix", (r"%peak\_bc\_matrix%",)),
    Term(r"singlecell\.csv", ("%singlecell.csv%",)),
    Term(r"per_barcode_metrics", (r"%per\_barcode\_metrics%",)),
)

# Text holding extracted 10x file names: GEO stores supplementary_data as JSON
# objects, so extractor takes URL/file-name span around the match.
TENX_EXTRACT = re.compile(
    r"[^\"/]*(?:" + "|".join(t.regex for t in TENX_FILE_TERMS) + r")[^\"]*",
    re.IGNORECASE,
)


# rules


@dataclass(frozen=True)
class Rule:
    """
    One rule/archive pair and its exact gate.

    `sql` emits evidence columns; `prepare` sets temp objects; `attach` brings related
    archives; `extract` keeps raw match text.
    """

    id: int
    rule: str
    archive: str
    grain: str
    field: str
    tier: str
    gate: str
    sql: str
    prepare: tuple[str, ...] = ()
    attach: tuple[str, ...] = ()
    extract: re.Pattern[str] | None = field(default=None, compare=False)


# ENA uses a base checkpoint plus dated increments; read both for complete coverage.
# The combined ENA DB is corrupt and owned by another user. Do not repair it.
_ENA_DELTA_STUB = """
    CREATE TABLE IF NOT EXISTS ena_delta.ena_experiments (
        experiment_accession TEXT,
        study_accession      TEXT,
        experiment_title     TEXT,
        library_name         TEXT,
        library_source       TEXT,
        library_strategy     TEXT
    )
"""

# Prose rules select from a subquery because SQLite re-evaluates a WHERE-clause
# expression once per LIKE pattern. Concatenate once, then apply the LIKEs.
#
# `LIMIT -1 OFFSET 0` fences optimization by preventing SQLite subquery flattening.
# It streams as a co-routine. Select folded text for the gate and raw text for
# stored evidence.
_SRA_PROSE_TEXT = (
    "coalesce(title,'')||' '||coalesce(design_description,'')"
    "||' '||coalesce(library_name,'')"
)
_ENA_PROSE_TEXT = "coalesce(experiment_title,'')||' '||coalesce(library_name,'')"
_AE_TEXT = (
    "coalesce(title,'')||' '||coalesce(description,'')||' '||coalesce(technology,'')"
    "||' '||coalesce(study_type,'')||' '||coalesce(assay_by_molecule,'')"
    "||' '||coalesce(protocols_description,'')"
)
_GEA_TEXT = (
    "coalesce(title,'')||' '||coalesce(description,'')"
    "||' '||coalesce(protocols_description,'')||' '||coalesce(study_type,'')"
)

RULES: tuple[Rule, ...] = (
    # high tier: the archive's own controlled vocabulary
    Rule(
        id=1,
        rule="insdc_library_source",
        archive="ena",
        grain="experiment",
        field="library_source",
        tier="high",
        gate="upper(library_source) LIKE '%SINGLE CELL%'",
        # ena_experiments is keyed by run; min() gives one deterministic row per
        # experiment and makes full rebuilds byte-stable.
        #
        # The increment is unioned in GSA's two-arm shape with predicates inside each
        # arm. SQLite materialises compound subqueries filtered from outside, which
        # would read every base row before filtering.
        attach=("ena_delta",),
        prepare=(_ENA_DELTA_STUB,),
        sql="""
            SELECT experiment_accession, 'experiment', study_accession,
                   min(library_source), min(library_source), min(library_strategy)
            FROM (
                SELECT experiment_accession, study_accession, library_source,
                       library_strategy
                FROM ena_experiments
                WHERE upper(library_source) LIKE '%SINGLE CELL%'
                UNION ALL
                SELECT experiment_accession, study_accession, library_source,
                       library_strategy
                FROM ena_delta.ena_experiments
                WHERE upper(library_source) LIKE '%SINGLE CELL%'
            )
            GROUP BY experiment_accession, study_accession
        """,
    ),
    Rule(
        id=2,
        rule="insdc_library_source",
        archive="sra",
        grain="experiment",
        field="library_source",
        tier="high",
        gate="upper(library_source) LIKE '%SINGLE CELL%'",
        sql="""
            SELECT accession, 'experiment', study, library_source,
                   library_source, library_strategy
            FROM sra_experiments
            WHERE upper(library_source) LIKE '%SINGLE CELL%'
        """,
    ),
    Rule(
        id=3,
        rule="insdc_library_source",
        archive="gsa",
        grain="experiment",
        field="library_source",
        tier="high",
        gate="upper(library_source) LIKE '%SINGLE CELL%'",
        # GSA has two halves: open CRA and controlled GSA-Human. Both feed the archive
        # audit number, and the controlled half adds to GSA's single-cell
        # studies.
        #
        # The table is keyed (accession, study_accession), using `accession` rather
        # than the Postgres mirror's `experiment_accession`.
        attach=("gsa_human",),
        prepare=(
            # Stand-in for deployments missing the GSA-Human file. The real,
            # immutable database replaces it when attached.
            """
            CREATE TABLE IF NOT EXISTS gsa_human.gsa_human_experiments (
                accession        TEXT,
                study_accession  TEXT,
                library_source   TEXT,
                library_strategy TEXT
            )
            """,
        ),
        sql="""
            SELECT accession, 'experiment', study_accession,
                   min(library_source), min(library_source), min(library_strategy)
            FROM (
                SELECT accession, study_accession, library_source, library_strategy
                FROM gsa_experiments
                WHERE upper(library_source) LIKE '%SINGLE CELL%'
                UNION ALL
                SELECT accession, study_accession, library_source, library_strategy
                FROM gsa_human.gsa_human_experiments
                WHERE upper(library_source) LIKE '%SINGLE CELL%'
            )
            GROUP BY accession, study_accession
        """,
    ),
    Rule(
        id=4,
        rule="insdc_library_source",
        archive="dra",
        grain="experiment",
        field="library_source",
        tier="high",
        gate="upper(library_source) LIKE '%SINGLE CELL%'",
        sql="""
            SELECT accession, 'experiment', study, library_source,
                   library_source, library_strategy
            FROM dra_experiments
            WHERE upper(library_source) LIKE '%SINGLE CELL%'
        """,
    ),
    Rule(
        id=5,
        rule="insdc_library_source",
        archive="ae",
        # ArrayExpress evidence is study-grained: arrayexpress_samples has no sample
        # accession, and an E-MTAB experiment is the study. Grain 'sample' with study
        # accessions would inflate single_cell_study.sc_objects.
        grain="study",
        field="library_source",
        tier="high",
        gate="upper(library_source) LIKE '%SINGLE CELL%'",
        # Reaches a minority of AE's single-cell experiments, because
        # library_source is often NULL. ae_experiment_text
        # carries the rest.
        sql="""
            SELECT experiment_accession, 'study', experiment_accession,
                   min(library_source), min(library_source), min(library_strategy)
            FROM arrayexpress_samples
            WHERE upper(library_source) LIKE '%SINGLE CELL%'
            GROUP BY experiment_accession
        """,
    ),
    Rule(
        id=6,
        rule="gea_study_type",
        archive="gea",
        grain="study",
        field="study_type",
        tier="high",
        gate="upper(study_type) LIKE '%SINGLE CELL%'",
        # A controlled value ('RNA-seq of coding RNA from single cells'), no
        # inference, on a minority of GEA experiments.
        sql="""
            SELECT accession, 'study', accession, study_type, NULL, NULL
            FROM gea_experiments
            WHERE upper(study_type) LIKE '%SINGLE CELL%'
        """,
    ),
    Rule(
        id=7,
        rule="tenx_output_files",
        archive="geo",
        grain="sample",
        field="supplementary_data",
        tier="high",
        gate="supplementary_data matches a 10x/CellRanger output file name",
        # geo_series.samples_ref has string-array and {"@ref": ...} shapes; read
        # both to keep GEO study accessions.
        #
        # Guard json_each inside its argument because table-valued functions run before
        # WHERE json_valid(...).
        #
        # Materialise the Postgres text[] analogue once into an indexed temp table.
        # min() gives a deterministic superseries/subseries alias.
        prepare=(
            """
            CREATE TEMP TABLE IF NOT EXISTS _sce_geo_series_map (
                gsm TEXT PRIMARY KEY,
                gse TEXT
            )
            """,
            "DELETE FROM _sce_geo_series_map",
            """
            INSERT INTO _sce_geo_series_map (gsm, gse)
            SELECT gsm, min(gse) FROM (
                SELECT CASE WHEN j.type = 'object'
                            THEN json_extract(j.value, '$."@ref"')
                            ELSE j.value END       AS gsm,
                       s.accession                 AS gse
                FROM geo_series s,
                     json_each(CASE WHEN s.samples_ref IS NOT NULL
                                     AND json_valid(s.samples_ref)
                                    THEN s.samples_ref ELSE '[]' END) j
            )
            WHERE gsm IS NOT NULL AND gsm LIKE 'GSM%'
            GROUP BY gsm
            """,
        ),
        sql=f"""
            SELECT g.accession, 'sample', m.gse, g.supplementary_data, NULL, NULL
            FROM geo_samples g
            LEFT JOIN _sce_geo_series_map m ON m.gsm = g.accession
            WHERE g.supplementary_data IS NOT NULL
              AND {_like_any("lower(g.supplementary_data)", TENX_FILE_TERMS)}
        """,  # noqa: S608
        extract=TENX_EXTRACT,
    ),
    # medium tier: prose
    Rule(
        id=8,
        rule="ae_experiment_text",
        archive="ae",
        grain="study",
        field="technology,study_type,assay_by_molecule,protocols_description",
        tier="medium",
        gate=(
            "title/description/technology/study_type/assay_by_molecule/"
            "protocols name a generic single-cell term or one of the seqspec "
            "named protocols (PLATFORM_TERMS)"
        ),
        sql=f"""
            SELECT accession, 'study', accession, v, NULL, NULL
            FROM (
                SELECT accession, {_AE_TEXT} AS v, lower({_AE_TEXT}) AS t
                FROM arrayexpress_experiments
                LIMIT -1 OFFSET 0
            )
            WHERE {_like_any("t", AE_TERMS)}
        """,  # noqa: S608
        extract=_context_re(AE_TERMS),
    ),
    Rule(
        id=9,
        rule="gea_text",
        archive="gea",
        grain="study",
        field="title,description,protocols_description,study_type",
        tier="medium",
        gate="title/description/protocols mention a single-cell platform",
        sql=f"""
            SELECT accession, 'study', accession, {_GEA_TEXT}, NULL, NULL
            FROM gea_experiments
            WHERE {_like_any(f"lower({_GEA_TEXT})", GEA_TERMS)}
        """,  # noqa: S608
        extract=_context_re(GEA_TERMS),
    ),
    Rule(
        id=10,
        rule="scatac_library",
        archive="ena",
        grain="experiment",
        field="library_strategy",
        tier="medium",
        gate=(
            "upper(library_strategy)='ATAC-SEQ' AND single-cell wording in "
            "library_name/experiment_title"
        ),
        # The value combines both fields. The gate and extracted value read the
        # same text because either field can carry the matching wording.
        attach=("ena_delta",),
        prepare=(_ENA_DELTA_STUB,),
        sql=f"""
            SELECT experiment_accession, 'experiment', study_accession,
                   min(coalesce(library_name,'')||' '||coalesce(experiment_title,'')),
                   min(library_source), min(library_strategy)
            FROM (
                SELECT experiment_accession, study_accession, library_name,
                       experiment_title, library_source, library_strategy
                FROM ena_experiments
                WHERE upper(library_strategy) = 'ATAC-SEQ'
                  AND ({_like_any("lower(library_name)", SCATAC_NAME_TERMS)}
                    OR {_like_any("lower(experiment_title)", SCATAC_TITLE_TERMS)})
                UNION ALL
                SELECT experiment_accession, study_accession, library_name,
                       experiment_title, library_source, library_strategy
                FROM ena_delta.ena_experiments
                WHERE upper(library_strategy) = 'ATAC-SEQ'
                  AND ({_like_any("lower(library_name)", SCATAC_NAME_TERMS)}
                    OR {_like_any("lower(experiment_title)", SCATAC_TITLE_TERMS)})
            )
            GROUP BY experiment_accession, study_accession
        """,  # noqa: S608
        extract=_context_re(SCATAC_NAME_TERMS),
    ),
    Rule(
        id=11,
        rule="experiment_prose",
        archive="sra",
        grain="experiment",
        field="title,design_description,library_name",
        tier="medium",
        gate=(
            "experiment title/design_description/library_name name a generic "
            "single-cell term or one of the seqspec named protocols "
            "(PLATFORM_TERMS)"
        ),
        sql=f"""
            SELECT accession, 'experiment', study, v,
                   library_source, library_strategy
            FROM (
                SELECT accession, study, library_source, library_strategy,
                       {_SRA_PROSE_TEXT} AS v, lower({_SRA_PROSE_TEXT}) AS t
                FROM sra_experiments
                LIMIT -1 OFFSET 0
            )
            WHERE {_like_any("t", PROSE_TERMS)}
        """,  # noqa: S608
        extract=_context_re(PROSE_TERMS),
    ),
    Rule(
        id=12,
        rule="experiment_prose",
        archive="ena",
        grain="experiment",
        field="experiment_title,library_name",
        tier="medium",
        gate=(
            "experiment_title/library_name name a generic single-cell term or "
            "one of the seqspec named protocols (PLATFORM_TERMS)"
        ),
        # ENA lacks design_description, so this prose channel is narrower than SRA.
        # The plan measured ENA's studies and includes them.
        attach=("ena_delta",),
        prepare=(_ENA_DELTA_STUB,),
        sql=f"""
            SELECT experiment_accession, 'experiment', study_accession,
                   min(v), min(library_source), min(library_strategy)
            FROM (
                SELECT experiment_accession, study_accession, v,
                       library_source, library_strategy
                FROM (
                    SELECT experiment_accession, study_accession,
                           {_ENA_PROSE_TEXT} AS v,
                           lower({_ENA_PROSE_TEXT}) AS t,
                           library_source, library_strategy
                    FROM ena_experiments
                    LIMIT -1 OFFSET 0
                )
                WHERE {_like_any("t", PROSE_TERMS)}
                UNION ALL
                SELECT experiment_accession, study_accession, v,
                       library_source, library_strategy
                FROM (
                    SELECT experiment_accession, study_accession,
                           {_ENA_PROSE_TEXT} AS v,
                           lower({_ENA_PROSE_TEXT}) AS t,
                           library_source, library_strategy
                    FROM ena_delta.ena_experiments
                    LIMIT -1 OFFSET 0
                )
                WHERE {_like_any("t", PROSE_TERMS)}
            )
            GROUP BY experiment_accession, study_accession
        """,  # noqa: S608
        extract=_context_re(PROSE_TERMS),
    ),
    # medium tier: the enrichment's own assay call
    Rule(
        id=13,
        rule="enriched_assay_library_col",
        archive="enrichment",
        grain="sample",
        field="assay,assay_category",
        # Medium tier. Other rules report submitter fields or pipeline outputs;
        # this rule reports a model's metadata reading. Alone, has_high_confidence = 0.
        tier="medium",
        gate=(
            "ontology_samples.assay matches a single-cell platform/method or "
            "one of the seqspec named protocols, or assay_category is one of "
            "the single-cell/spatial categories, AND "
            "provenance_samples.assay_column names a per-sample library or "
            "protocol column rather than study prose"
        ),
        sql=f"""
            SELECT o.sample, 'sample', min(o.study_accession),
                   min(o.assay || ' [assay_column=' || p.assay_column || ']'),
                   NULL, NULL
            FROM ontology_samples o
            JOIN provenance_samples p
              ON p.study_accession = o.study_accession
             AND p.sample = o.sample
            WHERE o.assay IS NOT NULL AND o.assay <> ''
              AND ({_like_any("lower(o.assay)", ENRICHED_TERMS)}
                OR upper(o.assay_category) IN ({_sql_in(ENRICHED_SC_CATEGORIES)}))
              AND p.assay_column IS NOT NULL AND p.assay_column <> ''
              AND lower(p.assay_column) NOT IN ({_sql_in(ENRICHED_PROSE_COLUMNS)})
            GROUP BY o.sample
        """,  # noqa: S608
        # GROUP BY sample and min(study_accession) pin one evidence row per object
        # across superseries/subseries aliases. Widening the gate can re-attribute rows.
        #
        # Controlled-column value is stored with source column appended for provenance.
    ),
)

RULE_SLUGS: frozenset[str] = frozenset(r.rule for r in RULES)


# derived axes

# library_source -> coarse analyte class.
ANALYTE: dict[str, str] = {
    "TRANSCRIPTOMIC SINGLE CELL": "transcriptomic",
    "TRANSCRIPTOMIC": "transcriptomic",
    "METATRANSCRIPTOMIC": "transcriptomic",
    "VIRAL RNA": "transcriptomic",
    "GENOMIC SINGLE CELL": "genomic",
    "GENOMIC": "genomic",
    "METAGENOMIC": "genomic",
    "SYNTHETIC": "synthetic",
    "OTHER": "other",
}

_MODALITY_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "CITE-seq",
        re.compile(
            r"cite.?seq|totalseq|antibody.?derived|\bhto\b|hashtag|cell.?hashing",
            re.IGNORECASE,
        ),
    ),
    (
        "Multiome",
        re.compile(r"multiome|snare.?seq|share.?seq|paired.?tag", re.IGNORECASE),
    ),
    (
        "Spatial Transcriptomics",
        re.compile(
            r"visium|xenium|merfish|seqfish|slide.?seq|cosmx|spatial",
            re.IGNORECASE,
        ),
    ),
    # \bbcr\b can catch BCR-ABL, but tightening it risks genuine BCR-seq recall.
    ("VDJ", re.compile(r"\btcr\b|\bbcr\b|vdj|immune.?profil", re.IGNORECASE)),
    # Trigger only. See _MODALITY_GUARDS: bare `crispr` needs corroboration.
    (
        "Perturb-seq",
        re.compile(
            r"perturb.?seq|crop.?seq|eccite.?seq|mosaic.?seq|crispr.?detect"
            r"|crispr|(?<![A-Za-z0-9])s?grna",
            re.IGNORECASE,
        ),
    ),
    ("snATAC-seq", re.compile(r"sn.?atac|single.?nucle.*atac", re.IGNORECASE)),
    # `atac` is a verified bare substring: its hits are ATAC library names such
    # as `Org67_ATAC`. Keep it broad.
    ("scATAC-seq", re.compile(r"atac", re.IGNORECASE)),
    ("snRNA-seq", re.compile(r"single.?nucle|\bsnrna\b", re.IGNORECASE)),
)

# Modalities needing a corroborating token. The trigger says the word appeared;
# the guard says it appeared in this modality's sense.
#
# Perturb-seq keeps bare `crispr` only with screen/guide wording because edited
# cell lines and depletion kits use it too. Use lookbehind on guide tokens:
# `_` is a word character, so `\bgrna\b` misses names like `MN07_gRNA1_Rep1`.
_MODALITY_GUARDS: dict[str, re.Pattern[str]] = {
    "Perturb-seq": re.compile(
        r"perturb.?seq|crop.?seq|eccite.?seq|mosaic.?seq|crispr.?detect"
        r"|(?<![A-Za-z0-9])s?grna"
        r"|\bscreen|\bpooled\b|perturbation|\bguides?\b|guide.?rna",
        re.IGNORECASE,
    ),
}

_SCRNA_FALLBACK = re.compile(r"scrna|rna-seq", re.IGNORECASE)


def analyte_of(source: str | None) -> str | None:
    """
    Coarse analyte class, or None when library_source is absent/unknown.

    None is an abstention from the archive, distinct from 'other'.
    """
    if not source:
        return None
    return ANALYTE.get(source.strip().upper())


def modality_of(
    value: str | None,
    strategy: str | None,
    source: str | None,
) -> str | None:
    """
    Assay modality from raw library fields.

    None is an abstention. Guard failures fall through to later ladder entries; CITE
    promotion waits for sibling evidence in the rollup.
    """
    text = f"{value or ''} {strategy or ''}"
    for modality, pattern in _MODALITY_RULES:
        if not pattern.search(text):
            continue
        guard = _MODALITY_GUARDS.get(modality)
        if guard is not None and not guard.search(text):
            # Wrong-sense trigger. Fall through: a CRISPR-edited line read by
            # scRNA-seq keeps its evidence and loses only the modality.
            continue
        return modality
    if (source or "").upper().startswith("TRANSCRIPTOMIC") or _SCRNA_FALLBACK.search(
        text,
    ):
        return "scRNA-seq"
    return None
