"""Per-run flags for the frequently-seen viruses and microbes."""

import logging
import re
import sqlite3

logger = logging.getLogger(__name__)

# the knee of the animalPV negative control: 6.7x the sensitivity of 0.50 at the
# same measured false-positive rate
VIRAL_BREADTH = 0.20

# taxa whose sequence is supplied by the host genome or by common lab reagents;
# the negative control cannot see these, so they keep the strict threshold and
# read as contamination or provenance rather than infection
ENDOGENOUS_BREADTH = 0.50

# bacterial and cell-culture references are 0.58-2.2 Mb, so a breadth meaning
# "present" for an 8 kb virus means nothing here; the gate is an absolute
# covered-bp floor plus a breadth far stricter than the 0.005 screening
# heuristic behind is_bacterial_evidence. High-breadth panel evidence only:
# related species map to the nearest of the panel's 27 pathogen genomes.
BACT_BREADTH = 0.30
BACT_COV_BP = 20_000

# below this depth a run cannot support an absence call
MIN_MEASURABLE_READS = 100_000

FLAG_PANEL: tuple[tuple[str, tuple[str, ...], str, float], ...] = (
    ("is_hpv", ("virus",), r"^HPV", VIRAL_BREADTH),
    ("is_ebv", ("virus",), r"^EBV_", VIRAL_BREADTH),
    ("is_cmv", ("virus",), r"^HCMV_", VIRAL_BREADTH),
    ("is_hsv", ("virus",), r"^HSV", VIRAL_BREADTH),
    ("is_hhv6", ("virus",), r"^HHV6", VIRAL_BREADTH),
    ("is_hhv7", ("virus",), r"^HHV7", VIRAL_BREADTH),
    ("is_kshv", ("virus",), r"^KSHV_", VIRAL_BREADTH),
    ("is_polyomavirus", ("virus",), r"polyomavirus|^JC_|^SV40", VIRAL_BREADTH),
    ("is_hbv", ("virus",), r"^HBV", VIRAL_BREADTH),
    ("is_hcv", ("virus",), r"^HCV", VIRAL_BREADTH),
    ("is_hav", ("virus",), r"^Hepatitis_A", VIRAL_BREADTH),
    ("is_sars_cov_2", ("virus",), r"^SARS-CoV-2", VIRAL_BREADTH),
    ("is_influenza_a", ("virus",), r"^InfluenzaA", VIRAL_BREADTH),
    ("is_rsv", ("virus",), r"^RSV", VIRAL_BREADTH),
    ("is_adenovirus", ("virus",), r"^Adenovirus", VIRAL_BREADTH),
    (
        "is_flavivirus",
        ("virus",),
        r"^West_Nile|^Zika|^Dengue|^Yellow_fever",
        VIRAL_BREADTH,
    ),
    ("is_lcmv", ("virus",), r"^LCMV", VIRAL_BREADTH),
    # mouse genomes carry MLV/MMTV-like loci and lentiviral vectors carry HIV1
    # and HTLV1 sequence. At 0.20 these balloon (MLV 244 -> 3,604, HIV1 42 ->
    # 1,120) with no matching rise in the negative control, which is blind to
    # both mechanisms, so a positive here reads as provenance.
    ("is_mlv", ("virus",), r"^Murine_leukemia_virus", ENDOGENOUS_BREADTH),
    ("is_mmtv", ("virus",), r"^MMTV", ENDOGENOUS_BREADTH),
    ("is_hiv", ("virus",), r"^HIV", ENDOGENOUS_BREADTH),
    ("is_htlv", ("virus",), r"^HTLV", ENDOGENOUS_BREADTH),
    # Mycoplasma is invisible to every existing column: class='culture' is
    # excluded from the bacterial screen, so runs carrying >=30% of an 840 kb
    # genome all read as clean
    (
        "is_mycoplasma",
        ("culture", "pathogen"),
        r"^Mycoplasma|^Ureaplasma",
        BACT_BREADTH,
    ),
    ("is_gardnerella", ("pathogen",), r"^Gardnerella", BACT_BREADTH),
    ("is_fusobacterium", ("pathogen",), r"^Fusobacterium", BACT_BREADTH),
)

HPV_AMBIGUITY_MAX_TYPES = 1


def _breadth_col(flag: str) -> str:
    """is_hpv -> hpv_breadth."""
    return f"{flag.removeprefix('is_')}_breadth"


def _gate(classes: tuple[str, ...], breadth: float) -> str:
    if classes == ("virus",):
        return f"class = 'virus' AND breadth_frac >= {breadth}"
    quoted = ",".join(f"'{c}'" for c in classes)
    return (
        f"class IN ({quoted}) AND covered_bp >= {BACT_COV_BP} "
        f"AND breadth_frac >= {breadth}"
    )


def _organism_vocabulary(conn: sqlite3.Connection) -> list[str]:
    """
    Every organism in the panel, fetched ONCE.

    A scan of a large table. Resolving each panel regex with its own query
    repeated the scan for byte-identical results; microbe_quant is never
    written to in this module.
    """
    return [o for (o,) in conn.execute("SELECT DISTINCT organism FROM microbe_quant")]


def _matching_organisms(vocabulary: list[str], pattern: str) -> list[str]:
    """
    Panel organisms matching a flag's regex.

    Matched in Python and emitted as an IN (...) list: SQLite has no REGEXP
    without a user function, and the list keeps the planner on
    idx_microbe_quant_organism.
    """
    rx = re.compile(pattern)
    return [o for o in vocabulary if rx.search(o)]


def _create_table(conn: sqlite3.Connection) -> None:
    """
    Create run_microbe_flags from FLAG_PANEL.

    Not in sql/tables/pentimento.sql because the columns ARE the panel -- writing
    them out twice guarantees they eventually disagree.
    """
    cols = ",\n    ".join(
        f"{flag} INTEGER,\n    {_breadth_col(flag)} REAL"
        for flag, _, _, _ in FLAG_PANEL
    )
    conn.execute("DROP TABLE IF EXISTS run_microbe_flags")
    conn.execute(
        "CREATE TABLE run_microbe_flags (\n"
        "    run_accession TEXT PRIMARY KEY,\n"
        "    status TEXT,\n"
        "    total_reads INTEGER,\n"
        "    measurable INTEGER,\n"
        f"    {cols},\n"
        "    hpv_top_type TEXT,\n"
        "    hpv_n_types INTEGER,\n"
        "    hpv_ambiguous INTEGER,\n"
        "    partial_evidence TEXT,\n"
        "    n_flags INTEGER\n"
        ") WITHOUT ROWID",
    )


def _create_indexes(conn: sqlite3.Connection) -> None:
    """
    Build the per-flag partial indexes AFTER the table is populated.

    Created up front they are maintained incrementally across every write,
    including two whole-table rewrites, instead of built once at the end.
    """
    for flag, _, _, _ in FLAG_PANEL:
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_rmf_{flag} "
            f"ON run_microbe_flags ({flag}) WHERE {flag} = 1",
        )


def build_flags(conn: sqlite3.Connection) -> dict[str, int]:
    """Populate run_microbe_flags and microbe_flag_panel. Idempotent."""
    _create_table(conn)
    conn.execute("DELETE FROM microbe_flag_panel")

    conn.execute(
        "INSERT INTO run_microbe_flags "  # noqa: S608
        "(run_accession, status, total_reads, measurable) "
        "SELECT run_accession, status, total_reads, "
        f"  (status = 'ok' AND COALESCE(total_reads, 0) >= {MIN_MEASURABLE_READS}) "
        "FROM quant_provenance",
    )

    vocabulary = _organism_vocabulary(conn)
    if not vocabulary:
        logger.warning("no microbial quantification loaded; every flag stays 0")
    counts: dict[str, int] = {}
    hpv_organisms: list[str] = []
    for flag, classes, pattern, breadth in FLAG_PANEL:
        organisms = _matching_organisms(vocabulary, pattern)
        if not organisms:
            # no sentinel row: _create_table already defines the column and the
            # COALESCE sweep fills its zeros. A placeholder organism once put a
            # taxon literally named "none" into microbe_flag_panel, which ships
            # to Postgres as the table the UI reads the panel from.
            if vocabulary:
                logger.warning("%s: no organism matches /%s/", flag, pattern)
            counts[flag] = 0
            continue
        if flag == "is_hpv":
            hpv_organisms = organisms
        gate = _gate(classes, breadth)
        conn.executemany(
            "INSERT INTO microbe_flag_panel (flag, organism, gate) VALUES (?,?,?)",
            [(flag, o, gate) for o in organisms],
        )
        holes = ",".join("?" * len(organisms))
        conn.execute(
            f"UPDATE run_microbe_flags SET "  # noqa: S608
            f"  {flag} = CASE WHEN measurable = 1 THEN 1 ELSE {flag} END,"
            f"  {_breadth_col(flag)} = CASE WHEN measurable = 1 THEN ("
            "     SELECT MAX(breadth_frac) FROM microbe_quant"
            f"     WHERE organism IN ({holes}) AND {gate}"
            f"       AND run_accession = run_microbe_flags.run_accession)"
            f"    ELSE {_breadth_col(flag)} END,"
            "  partial_evidence = CASE WHEN measurable = 0"
            f"    THEN COALESCE(partial_evidence || ',', '') || '{flag}'"
            "    ELSE partial_evidence END "
            "WHERE run_accession IN ("
            f"  SELECT run_accession FROM microbe_quant WHERE organism IN ({holes})"
            f"    AND {gate})",
            organisms * 2,
        )
        counts[flag] = conn.execute(
            f"SELECT COUNT(*) FROM run_microbe_flags WHERE {flag} = 1",  # noqa: S608
        ).fetchone()[0]

    # only a measurable run can carry a real negative. n_flags is derived purely
    # from the columns this statement writes, so it is set in the same pass;
    # COALESCE because NULL + 1 is NULL, which would blank n_flags on exactly
    # the rows carrying a positive call.
    sets = ", ".join(f"{f} = COALESCE({f}, 0)" for f, _, _, _ in FLAG_PANEL)
    n_flags = " + ".join(f"COALESCE({f}, 0)" for f, _, _, _ in FLAG_PANEL)
    conn.execute(
        f"UPDATE run_microbe_flags SET {sets}, n_flags = {n_flags}",  # noqa: S608
    )

    _hpv_types(conn, hpv_organisms)
    _create_indexes(conn)

    # SQLite expands f.* at view-creation time, so the view tracks FLAG_PANEL
    # without restating it. The grain is one row per run, so a sample sequenced
    # across several runs appears more than once and those runs can disagree
    conn.execute("DROP VIEW IF EXISTS sample_microbe_flags")
    conn.execute(
        "CREATE VIEW sample_microbe_flags AS SELECT "
        "  p.study_accession,"
        "  COALESCE(p.gsm_accession, p.sample_accession) AS sample_accession,"
        "  f.* "
        "FROM run_preflightx p JOIN run_microbe_flags f USING (run_accession)",
    )

    conn.commit()
    logger.info("run_microbe_flags: %s", counts)
    return counts


def _hpv_types(conn: sqlite3.Connection, hpv: list[str]) -> None:
    """
    Best-supported HPV type plus the ambiguity around it. See docstring.

    Takes the organism list the main loop already resolved for `is_hpv` rather
    than re-deriving it: repeating the regex and the threshold here meant
    editing FLAG_PANEL's HPV row silently left hpv_top_type on the old gate.
    """
    if not hpv:
        return
    holes = ",".join("?" * len(hpv))
    gate = _gate(("virus",), VIRAL_BREADTH)
    conn.execute(
        "UPDATE run_microbe_flags SET"  # noqa: S608
        "  hpv_top_type = (SELECT organism FROM microbe_quant"
        f"   WHERE organism IN ({holes}) AND {gate}"
        "     AND run_accession = run_microbe_flags.run_accession"
        "   ORDER BY breadth_frac DESC LIMIT 1),"
        "  hpv_n_types = (SELECT COUNT(*) FROM microbe_quant"
        f"   WHERE organism IN ({holes}) AND {gate}"
        "     AND run_accession = run_microbe_flags.run_accession) "
        "WHERE is_hpv = 1",
        hpv * 2,
    )
    conn.execute(
        "UPDATE run_microbe_flags SET hpv_ambiguous = "  # noqa: S608
        f"(hpv_n_types > {HPV_AMBIGUITY_MAX_TYPES}) WHERE is_hpv = 1",
    )
