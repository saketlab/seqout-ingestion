"""Sample- and study-grained rollups of Pentimento, for serving on a project page."""

import logging
import sqlite3
from pathlib import Path

from pentimento.flags import FLAG_PANEL

logger = logging.getLogger(__name__)

# (column, sqlite type, confidence column that ranks it). The DDL, the INSERT
# column list and the SELECT expressions all derive from this one list; keeping
# three by hand is how assay (TEXT) once landed in sex_confidence (REAL).
# assay has no confidence column and falls back to run order.
PICKED_COLUMNS: tuple[tuple[str, str, str | None], ...] = (
    ("species_called", "TEXT", "species_confidence"),
    ("species_confidence", "TEXT", "species_confidence"),
    ("species_hit_reads", "INTEGER", "species_confidence"),
    ("species_hit_fraction", "REAL", "species_confidence"),
    ("species_runner_up", "TEXT", "species_confidence"),
    ("species_margin", "REAL", "species_confidence"),
    ("species_ambiguous", "INTEGER", "species_confidence"),
    ("species_mislabel", "INTEGER", "species_confidence"),
    ("sex_verdict", "TEXT", "sex_confidence"),
    ("sex_confidence", "TEXT", "sex_confidence"),
    ("sex_reads_scanned", "INTEGER", "sex_confidence"),
    ("y_hits", "INTEGER", "sex_confidence"),
    ("xist_hits", "INTEGER", "sex_confidence"),
    ("y_xist_ratio", "REAL", "sex_confidence"),
    ("sex_panel_status", "TEXT", "sex_confidence"),
    ("sex_mixed_suspected", "INTEGER", "sex_confidence"),
    ("deep_n_reads", "INTEGER", "sex_confidence"),
    ("assay", "TEXT", None),
    ("assay_display", "TEXT", None),
    ("assay_is_single_cell", "INTEGER", None),
    ("modality", "TEXT", "modality_confidence"),
    ("modality_confidence", "TEXT", "modality_confidence"),
    ("intron_frac", "REAL", "modality_confidence"),
    ("mito_pct", "REAL", "modality_confidence"),
    ("modality_reads_scanned", "INTEGER", "modality_confidence"),
    ("bulk_vs_sc", "TEXT", "bulk_vs_sc_confidence"),
    ("bulk_vs_sc_confidence", "TEXT", "bulk_vs_sc_confidence"),
    ("read_end", "TEXT", "read_end_confidence"),
    ("read_end_confidence", "TEXT", "read_end_confidence"),
)


def _flag_names() -> list[str]:
    return [f for f, _, _, _ in FLAG_PANEL]


# assay/assay_display/assay_is_single_cell are one call. Identical WHERE and
# ORDER BY make one run supply the whole group, avoiding cross-run splices.
_ASSAY_GROUP = ("assay", "assay_display", "assay_is_single_cell")
_ASSAY_PICK = (
    "WHERE r.gsm_accession = s.sample_accession "
    "AND COALESCE(r.assay, r.assay_display) IS NOT NULL "
    "ORDER BY (r.assay_is_single_cell IS NOT NULL) DESC, r.run_accession"
)


def _pick_sql(field: str, confidence: str | None) -> str:
    """Value of field from this sample's best-supported run."""
    if field in _ASSAY_GROUP:
        return f"(SELECT r.{field} FROM run_preflightx r {_ASSAY_PICK} LIMIT 1)"  # noqa: S608
    # preflightx grades confidence 'low'/'medium'/'high'; ordering that column
    # DESC sorts alphabetically and puts 'medium' first, so rank it explicitly
    order = (
        f"CASE {confidence} WHEN 'high' THEN 3 WHEN 'medium' THEN 2 "
        f"WHEN 'low' THEN 1 ELSE 0 END DESC, "
        if confidence
        else ""
    )
    return (
        f"(SELECT r.{field} FROM run_preflightx r "  # noqa: S608
        "WHERE r.gsm_accession = s.sample_accession AND r.{f} IS NOT NULL "
        f"ORDER BY {order}r.run_accession LIMIT 1)".replace("{f}", field)
    )


_SEX_VALUES = ("male", "female", "unknown", "uncertain")
_ASSAY_MAX_LEN = 60
# Length cap for newer verdicts: shifted prose is long, and a closed
# vocabulary would NULL later preflightx values.
_VERDICT_MAX_LEN = 32


def sanitise_calls(conn: sqlite3.Connection) -> dict[str, int]:
    """
    NULL out field-shifted garbage before it can render as species.

    Embedded newlines can shift following columns; only call fields are nulled.
    """
    # SQLite stores embedded NUL happily; Postgres rejects the whole batch, and
    # 15 of them sit in study_accession and 3 in run_accession, i.e. join keys
    nul_cols = (
        "run_accession",
        "study_accession",
        "experiment_accession",
        "sample_accession",
        "gsm_accession",
        "species_called",
        "sex_verdict",
        "assay",
        "fastq_ftp",
    )
    conn.execute(
        "UPDATE run_preflightx SET species_called = REPLACE(species_called, ' ', '_') "
        "WHERE species_called GLOB '[A-Z][a-z]* [a-z]*'",
    )
    field_predicates = {
        "species_called": (
            "species_called <> 'unknown' "
            "AND species_called NOT GLOB '[A-Z][a-z]*_[a-z]*'"
        ),
        "sex_verdict": f"sex_verdict NOT IN {_SEX_VALUES}",
        # Blank strings pass the length test and render as "no assay"; loaders map
        # '' to NULL, and this is the backstop.
        "assay": f"LENGTH(assay) > {_ASSAY_MAX_LEN} OR TRIM(assay) = ''",
        "modality": f"LENGTH(modality) > {_VERDICT_MAX_LEN} OR TRIM(modality) = ''",
        "bulk_vs_sc": (
            f"LENGTH(bulk_vs_sc) > {_VERDICT_MAX_LEN} OR TRIM(bulk_vs_sc) = ''"
        ),
        "read_end": f"LENGTH(read_end) > {_VERDICT_MAX_LEN} OR TRIM(read_end) = ''",
    }
    nul_tests = {c: f"instr({c}, char(0)) > 0" for c in nul_cols}

    row = conn.execute(
        "SELECT "  # noqa: S608
        + ", ".join(f"SUM({t})" for t in field_predicates.values())
        + " FROM run_preflightx",
    ).fetchone()
    counts = {
        name: (value or 0) for name, value in zip(field_predicates, row, strict=True)
    }

    # char(0) is a zero-length string in SQLite: instr() still locates the byte,
    # but replace(X, Y, Z) returns X unchanged when Y is empty, so
    # REPLACE(col, char(0), '') is a silent no-op. Strip in Python.
    nul_where = " OR ".join(nul_tests.values())
    poisoned = conn.execute(
        f"SELECT rowid, {', '.join(nul_cols)} FROM run_preflightx "  # noqa: S608
        f"WHERE {nul_where}",
    ).fetchall()
    if poisoned:
        assignments = ", ".join(f"{c} = ?" for c in nul_cols)
        conn.executemany(
            f"UPDATE run_preflightx SET {assignments} WHERE rowid = ?",  # noqa: S608
            [
                [v.replace("\x00", "") if isinstance(v, str) else v for v in values]
                + [rowid]
                for rowid, *values in poisoned
            ],
        )
    counts["nul_bytes"] = len(poisoned)

    for field, predicate in field_predicates.items():
        conn.execute(
            f"UPDATE run_preflightx SET {field} = NULL "  # noqa: S608
            f"WHERE {field} IS NOT NULL AND ({predicate})",
        )
    conn.commit()
    logger.info("sanitised field-shifted preflightx values: %s", counts)
    return counts


def _warn_study_scope_dropped(conn: sqlite3.Connection) -> None:
    """
    Count scope=study rows discarded by the sample rollup.

    These pooled-matrix studies are real single-cell data; logging lets
    `pentimento study-scope-gaps` recover them by run accession.
    """
    row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(cells), 0) FROM sample_ncount "
        "WHERE scope = 'study'",
    ).fetchone()
    if row and row[0]:
        logger.warning(
            "%d study-scope rows (%d cells) are not sample-grained and were not "
            "written to pentimento_sample; recover them with "
            "`pentimento study-scope-gaps`",
            row[0],
            row[1],
        )


def _matrix_vetoes_not_single_cell(conn: sqlite3.Connection) -> int:
    """
    Let deposited cell x gene matrices outrank absent barcode signal.

    `assay_is_single_cell = 0` means no barcode structure in reads. Plate and
    no-whitelist chemistries can still be single-cell. Return NULL, not 1, to
    preserve the read-derived boundary.
    """
    cur = conn.execute(
        "UPDATE pentimento_sample SET assay_is_single_cell = NULL "
        "WHERE assay_is_single_cell = 0 AND cells > 0 AND genes > 0"
    )
    logger.info("matrix vetoed %d not-single-cell calls", cur.rowcount)
    return cur.rowcount


def build_rollups(
    conn: sqlite3.Connection,
    sra_db_file: Path | None = None,
) -> dict[str, int]:
    """Build pentimento_sample and pentimento_study. Idempotent."""
    sanitise_calls(conn)
    flags = _flag_names()

    conn.execute("DROP TABLE IF EXISTS pentimento_sample")
    conn.execute("DROP TABLE IF EXISTS pentimento_study")

    # Explicit types: a CTAS column built from an expression has an empty
    # declared type, which export.py maps to float64, turning every tri-state
    # flag in the parquet mirror into 1.0/0.0/null. CTAS also drops the PK.
    flag_cols = ",\n            ".join(f"{f} INTEGER" for f in flags)
    picked_cols = ",\n            ".join(f"{c} {t}" for c, t, _ in PICKED_COLUMNS)
    conn.execute(
        f"""
        CREATE TABLE pentimento_sample (
            study_accession       TEXT NOT NULL,
            sample_accession      TEXT NOT NULL,
            cells                 INTEGER,
            genes                 INTEGER,
            nnz                   INTEGER,
            file_format           TEXT,
            unfiltered            INTEGER NOT NULL DEFAULT 0,
            n_runs                INTEGER NOT NULL DEFAULT 0,
            n_runs_measurable     INTEGER NOT NULL DEFAULT 0,
            {picked_cols},
            calls_ambiguous       INTEGER NOT NULL DEFAULT 0,
            {flag_cols},
            n_flags               INTEGER,
            hpv_top_type          TEXT,
            hpv_ambiguous         INTEGER,
            has_viral_reads       INTEGER,
            has_viral_evidence    INTEGER,
            has_bacterial_reads   INTEGER,
            viral_kmer_mass       REAL,
            bacterial_kmer_mass   REAL,
            PRIMARY KEY (study_accession, sample_accession)
        ) WITHOUT ROWID
        """,
    )

    # Pre-filter to validated rows. Grouping every non-background row was
    # slower than the correlated subqueries this replaces.
    conn.execute("DROP TABLE IF EXISTS _run_microbe")
    conn.execute(
        """
        CREATE TABLE _run_microbe AS
        SELECT run_accession,
               MAX(is_validated_viral)     AS any_viral,
               MAX(is_viral_evidence)      AS any_viral_ev,
               MAX(is_validated_bacterial) AS any_bact,
               SUM(CASE WHEN is_validated_viral = 1 OR is_viral_evidence = 1
                        THEN kmer_mass END) AS viral_mass,
               SUM(CASE WHEN is_validated_bacterial = 1
                        THEN kmer_mass END) AS bact_mass
        FROM microbe_quant
        WHERE is_background = 0
          AND (is_validated_viral = 1 OR is_viral_evidence = 1
               OR is_validated_bacterial = 1)
        GROUP BY run_accession
        """,
    )
    conn.execute(
        "CREATE INDEX idx_run_microbe_run ON _run_microbe (run_accession)",
    )

    # MAX over the sample's runs gives the intended tri-state, since it ignores
    # NULLs: any run positive -> 1, measurable but none positive -> 0,
    # nothing measurable -> NULL
    flag_max = ",\n            ".join(f"MAX(f.{f}) AS {f}" for f in flags)
    # counts reconciled flags: run A positive for HPV only and run B for EBV
    # only is a two-flag sample, though the max of the per-run counts is 1
    n_flags = " + ".join(f"COALESCE(MAX(f.{f}), 0)" for f in flags)
    flag_names = ", ".join(flags)
    picked_names = ", ".join(c for c, _, _ in PICKED_COLUMNS)
    picks = ",\n            ".join(
        _pick_sql(field, confidence) for field, _, confidence in PICKED_COLUMNS
    )
    conn.execute(
        f"""
        INSERT INTO pentimento_sample (
            study_accession, sample_accession, cells, genes, nnz, file_format,
            unfiltered, n_runs, n_runs_measurable,
            {picked_names}, calls_ambiguous,
            {flag_names}, n_flags, hpv_top_type, hpv_ambiguous,
            has_viral_reads, has_viral_evidence, has_bacterial_reads,
            viral_kmer_mass, bacterial_kmer_mass
        )
        SELECT
            s.study_accession,
            s.sample_accession,
            s.cells, s.genes, s.nnz, s.file_format, s.unfiltered,
            COUNT(DISTINCT p.run_accession),
            COALESCE(SUM(f.measurable), 0),
            {picks},
            CASE WHEN COUNT(DISTINCT p.run_accession) > 1 THEN COALESCE((
                SELECT COUNT(DISTINCT r.species_called) > 1
                    OR COUNT(DISTINCT r.sex_verdict)    > 1
                    OR COUNT(DISTINCT r.assay)          > 1
                FROM run_preflightx r
                WHERE r.gsm_accession = s.sample_accession), 0) ELSE 0 END,
            {flag_max},
            {n_flags},
            (SELECT f2.hpv_top_type FROM run_preflightx p2
             JOIN run_microbe_flags f2 ON f2.run_accession = p2.run_accession
             WHERE p2.gsm_accession = s.sample_accession
               AND f2.hpv_top_type IS NOT NULL
             ORDER BY f2.hpv_breadth DESC LIMIT 1),
            MAX(f.hpv_ambiguous),
            -- `measurable` gates the ABSENCE claim, not the presence one:
            -- gating positives too hides samples carrying real viral mass
            CASE WHEN MAX(g.any_viral) = 1 THEN 1
                 WHEN COALESCE(MAX(f.measurable), 0) = 0 THEN NULL ELSE 0 END,
            CASE WHEN MAX(g.any_viral_ev) = 1 THEN 1
                 WHEN COALESCE(MAX(f.measurable), 0) = 0 THEN NULL ELSE 0 END,
            CASE WHEN MAX(g.any_bact) = 1 THEN 1
                 WHEN COALESCE(MAX(f.measurable), 0) = 0 THEN NULL ELSE 0 END,
            SUM(g.viral_mass),
            SUM(g.bact_mass)
        FROM sample_ncount s
        LEFT JOIN run_preflightx    p ON p.gsm_accession = s.sample_accession
        LEFT JOIN run_microbe_flags f ON f.run_accession = p.run_accession
        LEFT JOIN _run_microbe      g ON g.run_accession = p.run_accession
        -- scope='study' rows are studies deposited without a per-sample
        -- breakdown, keyed by its own accession. A 'GSE%' prefix test missed the
        -- same case keyed by SRP/E-MTAB/CRA.
        WHERE s.scope <> 'study'
        GROUP BY s.study_accession, s.sample_accession
        """,  # noqa: S608
    )
    _warn_study_scope_dropped(conn)
    _matrix_vetoes_not_single_cell(conn)
    conn.execute(
        "CREATE INDEX idx_pent_sample_sample ON pentimento_sample (sample_accession)",
    )

    # All three routes matter: only GSM names sample-run membership, and each
    # reaches studies the others miss.
    conn.execute("DROP TABLE IF EXISTS _study_run")
    conn.execute(
        """
        CREATE TABLE _study_run (
            study_accession TEXT NOT NULL,
            run_accession   TEXT NOT NULL,
            PRIMARY KEY (study_accession, run_accession)
        ) WITHOUT ROWID
        """,
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO _study_run (study_accession, run_accession)
        SELECT s.study_accession, p.run_accession
        FROM sample_ncount s
        JOIN run_preflightx p ON p.gsm_accession = s.sample_accession
        """,
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO _study_run (study_accession, run_accession)
        SELECT t.study_accession, p.run_accession
        FROM study_ncount t, json_each(t.aliases) j
        JOIN run_preflightx p ON p.study_accession = j.value
        WHERE t.aliases IS NOT NULL AND t.aliases <> ''
        """,
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO _study_run (study_accession, run_accession)
        SELECT t.study_accession, p.run_accession
        FROM study_ncount t
        JOIN run_preflightx p ON p.study_accession = t.study_accession
        """,
    )
    if sra_db_file and Path(sra_db_file).is_file():
        # ATTACH inside the write transaction the INSERTs above leave open fails
        # with "database sra is locked", even opened immutable
        conn.commit()
        conn.execute(
            "ATTACH DATABASE ? AS sra",
            (f"file:{sra_db_file}?immutable=1",),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO _study_run (study_accession, run_accession)
            SELECT s.alias, p.run_accession
            FROM sra.sra_studies s
            JOIN run_preflightx p ON p.study_accession = s.accession
            WHERE s.alias LIKE 'GSE%'
              AND EXISTS (SELECT 1 FROM study_ncount t
                          WHERE t.study_accession = s.alias)
            """,
        )
        conn.commit()
        conn.execute("DETACH DATABASE sra")
    else:
        logger.warning(
            "no sra_db_file: study links fall back to ncount aliases + GSM, "
            "which halves the studies carrying microbial evidence",
        )

    conn.execute(
        f"""
        CREATE TABLE pentimento_study (
            study_accession           TEXT PRIMARY KEY,
            source                    TEXT,
            title                     TEXT,
            aliases                   TEXT,
            study_cells               INTEGER,
            cells_unfiltered          INTEGER,
            single_cell_file_format   TEXT,
            n_samples_reported        INTEGER,
            has_metadata              INTEGER,
            has_celltype              INTEGER,
            has_donor                 INTEGER,
            has_demographics          INTEGER,
            n_samples_detailed        INTEGER,
            sample_cells_total        INTEGER,
            unassigned_cells          INTEGER,
            sample_breakdown_complete INTEGER,
            any_unfiltered            INTEGER,
            -- three coverages; conflating them understates Pentimento sevenfold.
            -- linked: runs attributable to the study at all. preflightx: runs
            -- with a read-derived species, sex or assay call. measurable:
            -- runs quantified against the microbial panel, still a small
            -- subset while Logan quantification runs.
            n_runs_linked             INTEGER,
            n_runs_preflightx         INTEGER,
            n_runs_measurable         INTEGER,
            {",".join(f"{f} INTEGER" for f in flags)}
        ) WITHOUT ROWID
        """,
    )
    flag_any = ", ".join(
        f"(SELECT MAX(f.{f}) FROM _study_run sr JOIN run_microbe_flags f "  # noqa: S608
        f"USING (run_accession) WHERE sr.study_accession = t.study_accession)"
        for f in flags
    )
    conn.execute(
        f"""
        INSERT INTO pentimento_study (
            study_accession, source, title, aliases, study_cells,
            cells_unfiltered, single_cell_file_format, n_samples_reported,
            has_metadata, has_celltype, has_donor, has_demographics,
            n_samples_detailed, sample_cells_total, unassigned_cells,
            sample_breakdown_complete, any_unfiltered,
            n_runs_linked, n_runs_preflightx, n_runs_measurable,
            {flag_names}
        )
        SELECT
            t.study_accession,
            t.source, t.title, t.aliases,
            t.cells,
            t.cells_unfiltered,
            t.single_cell_file_format,
            t.n_samples,
            t.has_metadata, t.has_celltype, t.has_donor, t.has_demographics,
            COUNT(a.sample_accession),
            COALESCE(SUM(a.cells), 0),
            MAX(t.cells - COALESCE(SUM(a.cells), 0), 0),
            (COUNT(a.sample_accession) > 0
              AND COALESCE(SUM(a.cells), 0) >= t.cells),
            MAX(COALESCE(t.cells_unfiltered, 0), COALESCE(MAX(a.unfiltered), 0)),
            -- all three from the link table; the sample rollup sees only
            -- GSM-linked runs, and an alias-linked run still belongs to the
            -- study when no sample can be named
            (SELECT COUNT(*) FROM _study_run sr
             WHERE sr.study_accession = t.study_accession),
            (SELECT COUNT(*) FROM _study_run sr
             JOIN run_preflightx p2 USING (run_accession)
             WHERE sr.study_accession = t.study_accession
               AND (p2.species_called IS NOT NULL
                    OR p2.sex_verdict IS NOT NULL
                    OR p2.assay IS NOT NULL)),
            COALESCE((SELECT SUM(f.measurable) FROM _study_run sr
                      JOIN run_microbe_flags f USING (run_accession)
                      WHERE sr.study_accession = t.study_accession), 0),
            {flag_any}
        FROM study_ncount t
        LEFT JOIN pentimento_sample a ON a.study_accession = t.study_accession
        GROUP BY t.study_accession
        """,  # noqa: S608
    )
    conn.commit()
    counts = {
        t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]  # noqa: S608
        for t in ("pentimento_sample", "pentimento_study")
    }
    logger.info("pentimento rollups: %s", counts)
    return counts
