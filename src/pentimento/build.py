"""Assemble the Pentimento SQLite from cell counts, preflightx, links and quant."""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import TYPE_CHECKING, Any

from helpers import exec_query_from_file, sqlite_connect
from pentimento import benchmark, provenance_json, sources
from pentimento.flags import build_flags
from pentimento.rollup import build_rollups, sanitise_calls

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# a taxon seen in more than this share of all runs is reagent/skin background
# rather than infection; E. coli reaches 0.69 across this corpus
BACKGROUND_PREVALENCE = 0.20
BATCH_SIZE = 20_000

_INSERTS: dict[str, str] = {
    "study_ncount": """
        INSERT OR REPLACE INTO study_ncount (
            study_accession, source, aliases, title, cells, cells_unfiltered,
            single_cell_file_format, n_samples, n_gsm_samples, has_metadata,
            has_celltype, has_donor, has_demographics, metadata_fields
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,
    "sample_ncount": """
        INSERT OR REPLACE INTO sample_ncount (
            sample_accession, study_accession, scope, cells, genes, nnz,
            file_format, unfiltered
        ) VALUES (?,?,?,?,?,?,?,?)
    """,
    "microbe_quant": """
        INSERT OR REPLACE INTO microbe_quant (
            run_accession, class, organism, ref_accession, reads, ref_bp,
            covered_bp, breadth_frac, total_reads, is_validated_viral,
            is_bacterial_evidence
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """,
    "quant_provenance": """
        INSERT OR REPLACE INTO quant_provenance (
            run_accession, status, total_reads, read_length, source_url,
            kmer_size, min_nmatch, min_identity, aligner, panel_ref,
            viral_gate, bacterial_gate, quantified_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,
    "_pfx_run": """
        INSERT OR REPLACE INTO _pfx_run (
            run_accession, experiment_accession, gsm_accession, study_accession,
            pfx_species, pfx_sex, pfx_assay, stated_sex, stated_assay,
            preflightx_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """,
    "preflightx_vote": """
        INSERT OR REPLACE INTO preflightx_vote (
            experiment_accession, study_accession, vote_species, vote_sex,
            vote_assay, n_runs_scanned, n_deep_runs, n_reads, deep_n_reads
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """,
    "preflightx_benchmark": """
        INSERT OR REPLACE INTO preflightx_benchmark (
            accession, experiment, field, gt_raw, pred_raw, gt_norm, pred_norm,
            correct
        ) VALUES (?,?,?,?,?,?,?,?)
    """,
}

# The enrichment DB and the provenance JSONs carry identical columns, so they
# share a staging shape and an overlay UPDATE. They need separate tables only
# because order matters: JSON first, then the DB overwrites it wherever it has a
# graded answer. The route tag arrives as _apply_exp_route's source argument.
_PFX_EXP_COLUMNS = (
    "experiment_accession",
    "study_accession",
    "species",
    "species_called",
    "species_confidence",
    "species_hit_reads",
    "species_hit_fraction",
    "species_runner_up",
    "species_margin",
    "species_ambiguous",
    "species_mislabel",
    "sex",
    "sex_verdict",
    "sex_confidence",
    "sex_reads_scanned",
    "y_hits",
    "xist_hits",
    "y_xist_ratio",
    "sex_panel_status",
    "sex_mixed_suspected",
    "deep_n_reads",
    "assay",
    "assay_display",
    "assay_is_single_cell",
)
# creation and drop order are irrelevant here; authority order is the order
# _apply_exp_route is called in, json then db (see _stage_preflightx)
_PFX_EXP_STAGING = ("_pfx_exp", "_pfx_json")

for _table in _PFX_EXP_STAGING:
    _INSERTS[_table] = (
        f"INSERT OR REPLACE INTO {_table} ({', '.join(_PFX_EXP_COLUMNS)}) "  # noqa: S608
        f"VALUES ({','.join('?' * len(_PFX_EXP_COLUMNS))})"
    )

_PFX_RUN_STAGING = """
CREATE TABLE IF NOT EXISTS _pfx_run (
    run_accession        TEXT PRIMARY KEY,
    experiment_accession TEXT,
    gsm_accession        TEXT,
    study_accession      TEXT,
    pfx_species          TEXT,
    pfx_sex              TEXT,
    pfx_assay            TEXT,
    stated_sex           TEXT,
    stated_assay         TEXT,
    preflightx_source    TEXT
)
"""

_PFX_STAGING = """
CREATE TABLE IF NOT EXISTS {table} (
    experiment_accession TEXT PRIMARY KEY,
    study_accession      TEXT,
    species              TEXT,
    species_called       TEXT,
    -- TEXT, not REAL: preflightx grades 'low'/'medium'/'high'. The declared type
    -- was REAL for both confidence columns; SQLite stored the strings anyway
    -- (affinity only converts losslessly), but the DDL claimed otherwise.
    species_confidence   TEXT,
    species_hit_reads    INTEGER,
    species_hit_fraction REAL,
    species_runner_up    TEXT,
    species_margin       REAL,
    species_ambiguous    INTEGER,
    species_mislabel     INTEGER,
    sex                  TEXT,
    sex_verdict          TEXT,
    sex_confidence       TEXT,
    sex_reads_scanned    INTEGER,
    y_hits               INTEGER,
    xist_hits            INTEGER,
    y_xist_ratio         REAL,
    sex_panel_status     TEXT,
    sex_mixed_suspected  INTEGER,
    deep_n_reads         INTEGER,
    assay                TEXT,
    assay_display        TEXT,
    assay_is_single_cell INTEGER
)
"""


def _executemany(
    conn: sqlite3.Connection, table: str, rows: Any, batch_size: int = BATCH_SIZE
) -> int:
    """Stream rows into one table, committing every batch_size."""
    total = _executemany_multi(conn, ((table, row) for row in rows), batch_size).get(
        table, 0
    )
    if not total:
        logger.info("%s: 0 rows", table)
    return total


def _executemany_multi(
    conn: sqlite3.Connection,
    rows: Any,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int]:
    """
    Stream (table, row) pairs into several tables from ONE iterator.

    The provenance JSONs feed two tables and sit on NFS; iterating them once per
    destination doubled the slowest leg of the build to read the same bytes
    twice.
    """
    batches: dict[str, list[tuple[Any, ...]]] = {}
    totals: dict[str, int] = {}

    def flush(table: str) -> None:
        batch = batches[table]
        conn.executemany(_INSERTS[table], batch)
        conn.commit()
        totals[table] = totals.get(table, 0) + len(batch)
        batch.clear()

    for table, row in rows:
        batches.setdefault(table, []).append(row)
        if len(batches[table]) >= batch_size:
            flush(table)
    for table, batch in batches.items():
        if batch:
            flush(table)
    for table, total in totals.items():
        logger.info("%s: %d rows", table, total)
    return totals


def _stage_preflightx(
    conn: sqlite3.Connection,
    preflightx_db_file: Path,
    preflightx_csv_dir: Path,
    legacy_csv_dirs: list[Path] | None = None,
) -> None:
    """
    Stage all three preflightx routes.

    csv   RUN grain -- the spine. Verdicts only, no evidence.
    json  EXPERIMENT grain -- verdicts, evidence AND confidence grades, from the
          .provenance.json files beside those CSVs. By far the widest source: on
          a 600-file sample, 99.6% of the experiments it reports evidence for
          are absent from the DB's graded set.
    db    EXPERIMENT grain -- the same measurements, curated into the pinned
          enrichment snapshot. A strict, stale subset (9,552 graded).

    db is applied LAST, so it wins on any experiment it covers. Not because it
    knows more -- both routes carry the same 'low'/'medium'/'high' grades, and
    the JSON route grades ~34x more runs -- but because it is the vetted, pinned
    snapshot the previous Pentimento served. Ordering it last means adding the JSON
    route fills gaps without silently changing a call that was already on
    display.
    """
    conn.execute("DROP TABLE IF EXISTS _pfx_run")
    conn.execute(_PFX_RUN_STAGING)
    for table in _PFX_EXP_STAGING:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(_PFX_STAGING.format(table=table))
    conn.commit()
    # legacy dirs first, so the current v4 pass overwrites them on shared runs:
    # _pfx_run is INSERT OR REPLACE keyed on run_accession, so load order is
    # precedence
    n_legacy = sum(
        _executemany(
            conn, "_pfx_run", sources.load_preflightx_csvs(d, source="csv_legacy")
        )
        for d in legacy_csv_dirs or []
    )
    n_csv = _executemany(
        conn, "_pfx_run", sources.load_preflightx_csvs(preflightx_csv_dir)
    )
    n_json = _executemany_multi(conn, provenance_json.load(preflightx_csv_dir))
    n_db = _executemany(
        conn, "_pfx_exp", sources.load_preflightx_db(preflightx_db_file)
    )
    logger.info(
        "preflightx staged: %d runs from csv (+%d legacy), %d experiments from "
        "json, %d experiments from db",
        n_csv,
        n_legacy,
        n_json.get("_pfx_json", 0),
        n_db,
    )


def _expand_to_runs(conn: sqlite3.Connection) -> None:
    """
    Populate run_preflightx from the CSV route, which is already run-grained.

    The annotated CSVs carry run_accession, the GEO sample (GSM) and the GSE
    directly, so they ARE the spine -- no external lookup is needed to reach a
    sample. That matters: the earlier design went through ena_experiments, whose
    on-disk snapshot is a delta overlapping this corpus by zero rows.
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO run_preflightx (
            run_accession, experiment_accession, gsm_accession, study_accession,
            species_called, sex_verdict, assay, sex, preflightx_source
        )
        SELECT run_accession, experiment_accession, gsm_accession,
               study_accession, pfx_species, pfx_sex,
               COALESCE(pfx_assay, stated_assay), stated_sex, preflightx_source
        FROM _pfx_run
        """
    )
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM run_preflightx").fetchone()[0]
    logger.info("run_preflightx from csv route: %d rows", n)


def _apply_exp_route(conn: sqlite3.Connection, staging: str, source: str) -> None:
    """
    Overlay an experiment-grained route onto the run-grained spine.

    Applied as an UPDATE rather than an INSERT so it enriches the existing rows
    without changing the spine. Rows it touches are re-tagged with the route that
    produced them -- the routes are refreshed independently and disagree, so
    which one a value came from is itself a fact worth keeping.

    CALL ORDER IS THE AUTHORITY ORDER (see _stage_preflightx), but every column
    is COALESCEd: a route fills what it knows and NEVER blanks what it does not.
    An unconditional SET
    looked equivalent and was not -- `_pfx_exp` holds all 224,851 experiments
    while only 9,552 of them carry any evidence, so the later DB route wrote
    NULL over the JSON route's real hit counts on the other 215,299.

    The WHERE clause is the other half of the same rule: a staging row with
    nothing in it must not touch the spine at all.

    preflightx_source names the route that produced the READ-DERIVED CALLS, so a
    route only claims it when it supplied one. `species` is excluded from that
    test because it is the DECLARED organism, not a call -- and on 56,203
    experiments a declared species is the DB route's ONLY contribution, which
    would otherwise have it stamping 'db' on rows whose species, sex and assay
    calls all came from the JSON route.
    """
    # generated from the shared column list, so a column added to the routes
    # cannot be left out of the overlay
    skip = {"experiment_accession", "study_accession", "sex", "preflightx_source"}
    payload = [c for c in _PFX_EXP_COLUMNS if c not in skip]
    assignments = ",\n            ".join(
        f"{c} = COALESCE(e.{c}, p.{c})" for c in payload
    )
    carries_value = " OR ".join(f"e.{c} IS NOT NULL" for c in payload)
    carries_call = " OR ".join(f"e.{c} IS NOT NULL" for c in payload if c != "species")
    conn.execute(
        f"""
        UPDATE run_preflightx AS p
        SET {assignments},
            preflightx_source = CASE WHEN {carries_call}
                                     THEN '{source}' ELSE p.preflightx_source END
        FROM {staging} AS e
        WHERE e.experiment_accession = p.experiment_accession
          AND ({carries_value})
        """  # noqa: S608
    )
    conn.commit()
    n = conn.execute(
        "SELECT COUNT(*) FROM run_preflightx WHERE preflightx_source = ?",
        (source,),
    ).fetchone()[0]
    logger.info("%s route enriched %d runs with read-derived evidence", source, n)


def _add_quantified_runs(conn: sqlite3.Connection) -> None:
    """Ensure every quantified run has a row even with no preflightx record."""
    conn.execute(
        """
        INSERT OR IGNORE INTO run_preflightx (run_accession)
        SELECT DISTINCT run_accession FROM microbe_quant
        """
    )
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM run_preflightx").fetchone()[0]
    logger.info("run_preflightx after adding quantified runs: %d rows", n)


def _attach_fastq(conn: sqlite3.Connection, links_db_file: Path) -> None:
    """
    Fill fastq/SRA columns by joining the links DB on run_accession.

    run_accession is indexed in run_download_links; experiment_accession is NOT,
    and joining on that made SQLite rescan all ~34.8 M rows per outer row and
    never finish. Joining on the indexed key keeps this to one pass.
    """
    if not links_db_file.is_file():
        logger.warning("links db missing; fastq columns stay NULL: %s", links_db_file)
        return
    conn.execute("ATTACH DATABASE ? AS links", (f"file:{links_db_file}?immutable=1",))
    try:
        conn.execute(
            """
            UPDATE run_preflightx AS p
            SET library_layout = l.library_layout,
                fastq_ftp      = l.fastq_ftp,
                fastq_bytes    = l.fastq_bytes,
                fastq_md5      = l.fastq_md5,
                sra_ftp        = l.sra_ftp,
                ncbi_sra_url   = l.ncbi_sra_url,
                experiment_accession = COALESCE(p.experiment_accession,
                                                l.experiment_accession),
                study_accession      = COALESCE(p.study_accession,
                                                l.study_accession)
            FROM links.run_download_links AS l
            WHERE l.run_accession = p.run_accession
            """
        )
        conn.commit()
    finally:
        conn.execute("DETACH DATABASE links")
    n = conn.execute(
        "SELECT COUNT(*) FROM run_preflightx WHERE fastq_ftp IS NOT NULL"
    ).fetchone()[0]
    logger.info("fastq links attached to %d runs", n)


def _load_benchmark(conn: sqlite3.Connection, csv_file: Path | None) -> None:
    """Load the accuracy benchmark and log what it says about the calls."""
    if csv_file is None:
        return
    _executemany(conn, "preflightx_benchmark", benchmark.load_benchmark(csv_file))
    conn.commit()
    logger.info("preflightx accuracy: %s", json.dumps(benchmark.summarise(conn)))


def _load_quant(conn: sqlite3.Connection, quant_dir: Path | None) -> None:
    """
    Load the microbial quantification, if there is any.

    quant_dir is optional on purpose: preflightx and the microbial panel are
    independent measurements of the same runs, and a study can carry read-derived
    species/sex/assay calls with nothing ever quantified against the panel. Every
    consumer downstream already reaches the flags through a LEFT JOIN or a
    correlated subquery, so skipping this leaves them NULL -- "not measured" --
    rather than 0, which would claim a clean result nobody measured.
    """
    if quant_dir is None:
        logger.info("no quant dir; building a preflight-only Pentimento")
        return
    logger.info("loading microbial quantification")
    _executemany(conn, "microbe_quant", sources.load_microbe_quant(quant_dir))
    # quant provenance first: _flag_background needs the measured-run count as
    # its denominator, and run_microbe_summary is driven from this table
    _load_quant_provenance(conn, quant_dir)
    _flag_background(conn)


def _load_quant_provenance(conn: sqlite3.Connection, quant_dir: Path) -> None:
    """Load per-run quant provenance, including attempted-but-unmeasured runs."""
    logger.info("loading quant provenance")
    _executemany(conn, "quant_provenance", sources.load_quant_provenance(quant_dir))
    known = {r[0] for r in conn.execute("SELECT run_accession FROM quant_provenance")}
    # runs attempted but never measured (download failed, or absent from Logan);
    # keeping them preserves "not measured" against "measured, nothing found"
    _executemany(conn, "quant_provenance", sources.load_status_only(quant_dir, known))


def _load_provenance(conn: sqlite3.Connection, preflightx_db_file: Path) -> None:
    """Load preflightx and per-field provenance (quant provenance is loaded earlier)."""
    if not preflightx_db_file.is_file():
        logger.warning("no enrichment db; preflightx/field provenance skipped")
        return
    conn.execute(
        "ATTACH DATABASE ? AS enr", (f"file:{preflightx_db_file}?immutable=1",)
    )
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO preflightx_provenance (
                experiment_accession, study_accession, preflightx_version,
                source_type, source_file, deep_n_reads, report_json, updated_at
            )
            SELECT experiment_accession, study_accession, preflightx_version,
                   source_type, source_file, deep_n_reads,
                   CASE WHEN json_valid(report_json) THEN report_json END,
                   updated_at
            FROM enr.preflightx_experiments
            """
        )
        conn.commit()
        logger.info(
            "preflightx_provenance: %d rows",
            conn.execute("SELECT COUNT(*) FROM preflightx_provenance").fetchone()[0],
        )
        _load_field_provenance(conn)
    except sqlite3.OperationalError:
        logger.exception("provenance load failed")
    finally:
        conn.execute("DETACH DATABASE enr")


_PROV_FIELDS = (
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
)


def _load_field_provenance(conn: sqlite3.Connection) -> None:
    """
    Pivot provenance_samples into normalised long form, for ALL samples.

    The source table is ~14 M rows x ~100 sparse columns, one column triplet per
    field. Stored wide it is mostly NULL and cannot be queried by field; stored
    long with raw strings it repeats a 15-value vocabulary tens of millions of
    times. So: long form, with field/source/column interned into prov_dict and
    the fact table WITHOUT ROWID.

    No sample restriction. An earlier version narrowed by study, and a second
    narrowed by linked sample -- which GREW the table (18.1 M -> 25.4 M rows)
    because Pentimento spans ~1.1 M GSMs, far more than the 88 k with cell counts.
    Carrying every sample is both simpler and what makes the provenance usable
    for samples that have no counts yet.
    """
    conn.execute("DELETE FROM prov_dict")
    conn.executemany(
        "INSERT OR IGNORE INTO prov_dict (kind, value) VALUES ('field', ?)",
        [(f,) for f in _PROV_FIELDS],
    )
    conn.commit()
    field_ids = {
        v: i
        for i, v in conn.execute("SELECT id, value FROM prov_dict WHERE kind = 'field'")
    }

    for field in _PROV_FIELDS:
        for kind, col in (
            ("source", f"{field}_source"),
            ("source", f"{field}_original_source"),
            ("column", f"{field}_column"),
            ("column", f"{field}_original_column"),
        ):
            conn.execute(
                f"""
                INSERT OR IGNORE INTO prov_dict (kind, value)
                SELECT DISTINCT '{kind}', {col}
                FROM enr.provenance_samples
                WHERE {col} IS NOT NULL AND {col} != ''
                """  # noqa: S608
            )
        conn.commit()

    total = 0
    for field in _PROV_FIELDS:
        fid = field_ids[field]
        conn.execute(
            f"""
            INSERT OR REPLACE INTO sample_field_provenance (
                sample, field_id, study_accession, source_id, score, column_id,
                original_source_id, original_column_id, original_score
            )
            SELECT ps.sample, {fid}, ps.study_accession,
                   ds.id, ps.{field}_score, dc.id,
                   dos.id, doc.id, ps.{field}_original_score
            FROM enr.provenance_samples ps
            LEFT JOIN prov_dict ds
                   ON ds.kind = 'source' AND ds.value = ps.{field}_source
            LEFT JOIN prov_dict dc
                   ON dc.kind = 'column' AND dc.value = ps.{field}_column
            LEFT JOIN prov_dict dos
                   ON dos.kind = 'source' AND dos.value = ps.{field}_original_source
            LEFT JOIN prov_dict doc
                   ON doc.kind = 'column' AND doc.value = ps.{field}_original_column
            WHERE ps.sample IS NOT NULL AND ps.sample != ''
              AND (ps.{field}_source IS NOT NULL AND ps.{field}_source != '')
            """  # noqa: S608
        )
        conn.commit()
        n = conn.execute(
            "SELECT COUNT(*) FROM sample_field_provenance WHERE field_id = ?", (fid,)
        ).fetchone()[0]
        total += n
        logger.info("field provenance %-20s %d rows", field, n)
    logger.info("sample_field_provenance: %d rows (all samples)", total)


def _link_gsm(
    conn: sqlite3.Connection,
    ena_db_file: Path | None,
    sra_db_file: Path | None,
) -> None:
    """
    Resolve run -> SRS -> GSM.

    This needs an external mapping and cannot come from the annotated CSVs. Those
    are long-format with a record_type column, and a row is EITHER a run
    record or a sample record -- measured co-occurrence of run_accession and
    sample in the same row is exactly zero -- so the file never states which GSM a
    run belongs to.

    SRA carries the full chain (sra_runs -> sra_experiments -> sra_samples, whose
    alias is the GSM). ENA is tried as a fallback but its on-disk snapshot is
    a delta covering almost none of this corpus.
    """
    if sra_db_file and sra_db_file.is_file():
        conn.execute("ATTACH DATABASE ? AS sra", (f"file:{sra_db_file}?immutable=1",))
        try:
            conn.execute(
                """
                UPDATE run_preflightx AS p
                SET experiment_accession =
                    COALESCE(p.experiment_accession, r.experiment)
                FROM sra.sra_runs AS r
                WHERE r.accession = p.run_accession
                """
            )
            conn.commit()
            # sra_experiments.samples is a JSON array, '["SRS6116468"]'; compared
            # against a bare SRS string it matches nothing and fails silently,
            # leaving every GSM NULL
            conn.execute(
                """
                UPDATE run_preflightx AS p
                SET sample_accession = json_extract(e.samples, '$[0]')
                FROM sra.sra_experiments AS e
                WHERE e.accession = p.experiment_accession
                  AND json_valid(e.samples)
                """
            )
            conn.commit()
            conn.execute(
                """
                UPDATE run_preflightx AS p
                SET gsm_accession = s.alias
                FROM sra.sra_samples AS s
                WHERE s.accession = p.sample_accession
                  AND s.alias LIKE 'GSM%'
                """
            )
            conn.commit()
        except sqlite3.OperationalError:
            logger.exception("SRA sample link failed")
        finally:
            conn.execute("DETACH DATABASE sra")
        n = conn.execute(
            "SELECT COUNT(*) FROM run_preflightx WHERE gsm_accession IS NOT NULL"
        ).fetchone()[0]
        logger.info("GSM linked via SRA for %d runs", n)

    if not ena_db_file or not ena_db_file.is_file():
        return
    conn.execute("ATTACH DATABASE ? AS ena", (f"file:{ena_db_file}?immutable=1",))
    try:
        conn.execute(
            """
            UPDATE run_preflightx AS p
            SET sample_accession = e.sample_accession
            FROM ena.ena_experiments AS e
            WHERE e.run_accession = p.run_accession
              AND p.sample_accession IS NULL
            """
        )
        conn.execute(
            """
            UPDATE run_preflightx AS p
            SET gsm_accession = s.sample_alias
            FROM ena.ena_samples AS s
            WHERE s.sample_accession = p.sample_accession
              AND s.sample_alias LIKE 'GSM%'
              AND p.gsm_accession IS NULL
            """
        )
        conn.commit()
    except sqlite3.OperationalError:
        logger.exception("ENA sample link failed")
    finally:
        conn.execute("DETACH DATABASE ena")
    n = conn.execute(
        "SELECT COUNT(*) FROM run_preflightx WHERE gsm_accession IS NOT NULL"
    ).fetchone()[0]
    logger.info("GSM linked (SRA+ENA) for %d runs", n)


def _flag_background(conn: sqlite3.Connection) -> None:
    """Mark taxa that recur across a large share of all runs as background."""
    # denominator is runs measured; hit-positive runs inflate every prevalence
    total_runs = conn.execute(
        "SELECT COUNT(*) FROM quant_provenance WHERE status = 'ok'"
    ).fetchone()[0]
    if not total_runs:
        total_runs = conn.execute(
            "SELECT COUNT(DISTINCT run_accession) FROM microbe_quant"
        ).fetchone()[0]
    if not total_runs:
        return
    conn.execute("DELETE FROM background_taxa")
    conn.execute(
        """
        INSERT INTO background_taxa (organism, runs, prevalence)
        SELECT organism, COUNT(DISTINCT run_accession) AS runs,
               CAST(COUNT(DISTINCT run_accession) AS REAL) / ?
        FROM microbe_quant
        GROUP BY organism
        HAVING CAST(COUNT(DISTINCT run_accession) AS REAL) / ? > ?
        """,
        (total_runs, total_runs, BACKGROUND_PREVALENCE),
    )
    conn.execute(
        """
        UPDATE microbe_quant SET is_background = 1
        WHERE organism IN (SELECT organism FROM background_taxa)
        """
    )
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM background_taxa").fetchone()[0]
    logger.info("background taxa flagged: %d (of %d runs)", n, total_runs)


def build_pentimento(
    output_db_file: Path,
    links_db_file: Path,
    ncount_dirs: list[Path],
    preflightx_db_file: Path,
    preflightx_csv_dir: Path,
    quant_dir: Path | None = None,
    benchmark_file: Path | None = None,
    legacy_csv_dirs: list[Path] | None = None,
    ena_db_file: Path | None = None,
    sra_db_file: Path | None = None,
    geo_db_file: Path | None = None,
) -> None:
    """Build the unified Pentimento SQLite. Idempotent: re-running replaces rows."""
    output_db_file.parent.mkdir(parents=True, exist_ok=True)
    # uri=True lets the ATTACHes pass immutable=1 past the source DBs' live WAL
    # sidecars
    conn = sqlite_connect(output_db_file, uri=True)
    conn.execute("PRAGMA page_size = 16384")
    conn.execute("PRAGMA journal_mode = WAL")
    exec_query_from_file("tables/pentimento.sql", conn=conn)

    logger.info("loading study-level cell counts")
    _executemany(conn, "study_ncount", sources.load_ncount_projects(ncount_dirs))

    logger.info("loading per-sample cell counts")
    _executemany(conn, "sample_ncount", sources.load_ncount_samples(ncount_dirs))

    _load_quant(conn, quant_dir)
    _load_benchmark(conn, benchmark_file)

    logger.info("staging preflightx calls")
    _stage_preflightx(conn, preflightx_db_file, preflightx_csv_dir, legacy_csv_dirs)
    _expand_to_runs(conn)
    _add_quantified_runs(conn)
    # must precede the experiment-grained routes: this backfills
    # experiment_accession from the links DB, the key both routes join on, and
    # run afterwards the evidence silently never lands
    _attach_fastq(conn, links_db_file)
    # json then db: the DB route is the only graded one and must win where it
    # has an answer, while the JSON route covers ~100x more experiments
    _apply_exp_route(conn, "_pfx_json", "json")
    _apply_exp_route(conn, "_pfx_exp", "db")
    _link_gsm(conn, ena_db_file, sra_db_file)

    _load_provenance(conn, preflightx_db_file)

    conn.execute("DROP TABLE IF EXISTS _pfx_run")
    for table in _PFX_EXP_STAGING:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    # index and ANALYZE before the derived stages: every source table is loaded
    # by here, and the pivot and rollup join on microbe_quant.organism and
    # run_preflightx.gsm_accession, which nothing else covers
    exec_query_from_file("indexes/pentimento.sql", conn=conn)
    conn.execute("ANALYZE")
    conn.commit()

    build_flags(conn)
    build_rollups(conn, sra_db_file)
    conn.commit()

    if geo_db_file and geo_db_file.is_file():
        logger.info("geo db present but unused for linkage: %s", geo_db_file)

    summary = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
        for table in (
            "study_ncount",
            "sample_ncount",
            "run_preflightx",
            "microbe_quant",
            "background_taxa",
            "quant_provenance",
            "preflightx_provenance",
            "preflightx_vote",
            "preflightx_benchmark",
            "sample_field_provenance",
            "prov_dict",
        )
    }
    logger.info("pentimento built: %s", json.dumps(summary))
    conn.close()


def ingest_preflight(
    conn: sqlite3.Connection,
    preflightx_csv_dir: Path,
    sra_db_file: Path | None = None,
) -> dict[str, int]:
    """
    Load a preflight run into an existing Pentimento DB without rebuilding it.

    The point of this entry is INDEPENDENCE. A fresh scan reaches Pentimento with
    no microbial quantification, no enrichment DB and no multi-hour rebuild --
    the CSV route plus the provenance JSONs are between them a complete spine
    (run, GSM and study all come off the CSVs) and the only source of evidence.

    Deliberately NOT run here: `_attach_fastq` and `_flag_background`, which
    need the links DB and quant respectively and belong to a full build. GSM
    linkage runs only if an SRA db is offered.
    """
    conn.execute("DROP TABLE IF EXISTS _pfx_run")
    conn.execute(_PFX_RUN_STAGING)
    conn.execute("DROP TABLE IF EXISTS _pfx_json")
    conn.execute(_PFX_STAGING.format(table="_pfx_json"))
    conn.commit()

    n_csv = _executemany(
        conn, "_pfx_run", sources.load_preflightx_csvs(preflightx_csv_dir)
    )
    n_json = _executemany_multi(conn, provenance_json.load(preflightx_csv_dir))
    _expand_to_runs(conn)
    _apply_exp_route(conn, "_pfx_json", "json")
    if sra_db_file:
        _link_gsm(conn, None, sra_db_file)
    # also runs here, not only in build_rollups: this path populates
    # run_preflightx from the CSV route whose embedded-newline corruption the
    # sanitiser exists to clean, and transfer pentimento reads run_preflightx
    # directly, so a preflight ingest then transfer would ship field-shifted
    # species and sex straight to Postgres
    sanitise_calls(conn)
    conn.execute("DROP TABLE IF EXISTS _pfx_run")
    conn.execute("DROP TABLE IF EXISTS _pfx_json")
    conn.commit()
    counts = {
        "runs_from_csv": n_csv,
        "experiments_from_json": n_json.get("_pfx_json", 0),
        "votes": n_json.get("preflightx_vote", 0),
    }
    logger.info("preflight ingested: %s", json.dumps(counts))
    return counts
