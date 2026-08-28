"""Loaders for the four Pentimento inputs, each yielding tuples for executemany."""

from __future__ import annotations

import contextlib
import csv
import json
import logging
import re
import sqlite3
from typing import TYPE_CHECKING, Any

from pentimento.flags import (
    BACT_BREADTH,
    BACT_COV_BP,
    ENDOGENOUS_BREADTH,
    FLAG_PANEL,
    VIRAL_BREADTH,
)
from pentimento.preflight import NO_STRUCTURAL_CALLS, structural_calls

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

SMALL_BP = 50_000

# Genome-size ceiling for a viral reference; breadth_frac is a fraction, so a
# larger genome already demands proportionally more covered bases. class=virus
# bars herpesviruses from the bacterial gate, so a ceiling below their size
# excludes them from both. SMALL_BP stays the bacterial floor.
VIRAL_MAX_BP = 300_000

# Mouse genomes carry MLV/MMTV-like loci and vectors carry HIV1/HTLV1 sequence.
# Excluded from evidence tier; derived from FLAG_PANEL so gate and badge agree.
ENDOGENOUS_RX = re.compile(
    "|".join(rx for _, _, rx, gate in FLAG_PANEL if gate == ENDOGENOUS_BREADTH)
)

# Strict viral validation gate; expression-data HPV follow-up uses the evidence tier.
VIRAL_VALIDATED_BREADTH = ENDOGENOUS_BREADTH

# Loose bacterial screen behind is_bacterial_evidence.
BACT_MIN_COV_BP = 20_000
BACT_MIN_BREADTH = 0.005
BACT_CLASSES = frozenset({"pathogen", "culture"})
PROGRESS_EVERY = 5_000

# quantifier settings, recorded per run so a call can be re-derived later; these
# mirror virus_quant_srr.sh and must change in both places together
QUANT_KMER = 31
QUANT_MIN_NMATCH = 40
QUANT_MIN_IDENTITY = 0.95
QUANT_ALIGNER = "minimap2 -x sr --secondary=no"
QUANT_SOURCE_TMPL = "https://s3.amazonaws.com/logan-pub/u/{acc}/{acc}.unitigs.fa.zst"
# f-strings are stored in quant_provenance as the exact rule record.
VIRAL_GATE_RULE = (
    f"class=virus AND 0 < ref_bp <= {VIRAL_MAX_BP} "
    f"AND breadth_frac >= {VIRAL_VALIDATED_BREADTH}"
)
VIRAL_EVIDENCE_GATE_RULE = (
    f"class=virus AND 0 < ref_bp <= {VIRAL_MAX_BP} "
    f"AND breadth_frac >= {VIRAL_BREADTH} "
    f"AND NOT host-endogenous/vector-derived"
)
BACT_GATE_RULE = (
    f"class=pathogen AND ref_bp > {SMALL_BP} AND covered_bp >= {BACT_MIN_COV_BP} "
    f"AND breadth_frac >= {BACT_MIN_BREADTH}"
)
BACT_VALID_GATE_RULE = (
    f"class in (pathogen,culture) AND ref_bp > {SMALL_BP} "
    f"AND covered_bp >= {BACT_COV_BP} AND breadth_frac >= {BACT_BREADTH}"
)

# quant.csv shape: name,reads,kmer_mass,n_unitigs,ref_bp,covered_bp,breadth_frac,class
QUANT_NCOL = 8
ORGANISM_PART = 1
REFACC_PART = 2

# 10x barcode whitelist sizes; exact multiples indicate unfiltered matrices
# with barcode columns.
WHITELIST_SIZES = (737_280, 6_794_880)

VALID_CLASSES = frozenset(
    {
        "virus",
        "pathogen",
        "kitome",
        "flora",
        "parasite",
        "spike",
        "animalPV",
        "culture",
        "vector",
    }
)


def _as_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _as_text(value: Any) -> str | None:
    """
    Normalise a text column, mapping empty to NULL.

    preflightx confidence grades are text. Empty strings are null calls; letting
    blanks survive would display empty cells and overwrite measured calls.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_unfiltered(cells: int | None) -> bool:
    """Return whether a reported cell count looks like an unfiltered matrix."""
    if not cells or cells <= 0:
        return False
    return any(cells % size == 0 for size in WHITELIST_SIZES)


def load_ncount_projects(
    ncount_dirs: list[Path],
) -> Iterator[tuple[Any, ...]]:
    """
    Yield per-study rows from ncount result JSONs.

    Reads one per-sample JSON dump per study. Missing `unfiltered` values derive
    from the barcode whitelist test.
    """
    for root in ncount_dirs:
        if not root.is_dir():
            logger.warning("ncount dir missing, skipping: %s", root)
            continue
        for path in sorted(root.glob("*.json")):
            try:
                doc = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                logger.warning("unreadable ncount json: %s", path)
                continue
            if not isinstance(doc, dict):
                continue
            acc = doc.get("accession")
            if not acc:
                continue
            cells = _as_int(doc.get("single_cell_count"))
            unfiltered = bool(doc.get("unfiltered")) or is_unfiltered(cells)
            aliases = doc.get("aliases") or ""
            alias_list = [a for a in str(aliases).split("|") if a]
            yield (
                acc,
                doc.get("source"),
                json.dumps(alias_list),
                doc.get("title"),
                cells,
                int(unfiltered),
                doc.get("single_cell_file_format"),
                _as_int(doc.get("n_samples")),
                _as_int(doc.get("n_gsm_samples")),
                int(bool(doc.get("has_metadata"))),
                int(bool(doc.get("has_celltype"))),
                int(bool(doc.get("has_donor"))),
                int(bool(doc.get("has_demographics"))),
                json.dumps(doc.get("metadata_fields") or []),
            )


def load_ncount_samples(ncount_dirs: list[Path]) -> Iterator[tuple[Any, ...]]:
    """Yield per-GSM rows from the per_sample block of the ncount JSONs."""
    for root in ncount_dirs:
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*.json")):
            try:
                doc = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(doc, dict):
                continue
            study = doc.get("accession")
            for entry in doc.get("per_sample") or []:
                if not isinstance(entry, dict):
                    continue
                sample = entry.get("sample")
                if not sample:
                    continue
                cells = _as_int(entry.get("cells"))
                unfiltered = bool(entry.get("unfiltered")) or is_unfiltered(cells)
                yield (
                    sample,
                    study,
                    entry.get("scope"),
                    cells,
                    _as_int(entry.get("genes")),
                    _as_int(entry.get("nnz")),
                    entry.get("file_format"),
                    int(unfiltered),
                )


def _structural_from_report_json(report_json: Any) -> tuple[Any, ...]:
    """structural_calls() from a stored report_json blob; NULLs when unusable."""
    if not report_json:
        return NO_STRUCTURAL_CALLS
    try:
        report = json.loads(report_json)
    except (TypeError, ValueError):
        return NO_STRUCTURAL_CALLS
    if not isinstance(report, dict):
        return NO_STRUCTURAL_CALLS
    return structural_calls(report)


def load_preflightx_db(db_file: Path) -> Iterator[tuple[Any, ...]]:
    """
    Yield experiment-grained preflightx calls from enrichment SQLite.

    Pinned route carrying confidence columns.
    """
    if not db_file.is_file():
        logger.warning("preflightx db missing, skipping: %s", db_file)
        return
    uri = f"file:{db_file}?immutable=1"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        # report_json is where modality and read geometry survive, and older
        # snapshots predate the column; SELECTing it blind would crash the whole
        # build on a DB that is otherwise perfectly usable
        columns = {
            r[1] for r in conn.execute("PRAGMA table_info(preflightx_experiments)")
        }
        report_json = (
            "report_json" if "report_json" in columns else "NULL AS report_json"
        )
        cur = conn.execute(
            f"""
            SELECT study_accession, experiment_accession, species, sex, assay,
                   assay_display, assay_is_single_cell,
                   sex_verdict, sex_confidence, sex_reads_scanned,
                   y_hits, xist_hits, y_xist_ratio, sex_panel_status,
                   sex_mixed_suspected, deep_n_reads,
                   species_called, species_confidence, species_hit_reads,
                   species_hit_fraction, species_runner_up, species_margin,
                   species_ambiguous, species_mislabel, {report_json}
            FROM preflightx_experiments
            """  # noqa: S608
        )
        for row in cur:
            # the table spells "no value" as both NULL and '', and the overlay
            # COALESCEs, so an unnormalised '' blanks a call another route made
            yield (
                row["experiment_accession"],
                row["study_accession"],
                _as_text(row["species"]),
                _as_text(row["species_called"]),
                _as_text(row["species_confidence"]),
                _as_int(row["species_hit_reads"]),
                _as_float(row["species_hit_fraction"]),
                _as_text(row["species_runner_up"]),
                _as_float(row["species_margin"]),
                _as_int(row["species_ambiguous"]),
                _as_int(row["species_mislabel"]),
                _as_text(row["sex"]),
                _as_text(row["sex_verdict"]),
                _as_text(row["sex_confidence"]),
                _as_int(row["sex_reads_scanned"]),
                _as_int(row["y_hits"]),
                _as_int(row["xist_hits"]),
                _as_float(row["y_xist_ratio"]),
                _as_text(row["sex_panel_status"]),
                _as_int(row["sex_mixed_suspected"]),
                _as_int(row["deep_n_reads"]),
                _as_text(row["assay"]),
                _as_text(row["assay_display"]),
                _as_int(row["assay_is_single_cell"]),
                # modality/geometry stay inside stored reports on a minority of rows;
                # other rows yield NULL, meaning unmeasured.
                *_structural_from_report_json(row["report_json"]),
            )


def _col(row: dict[str, str], name: str) -> str | None:
    """One call column; absent or empty values become NULL."""
    return (row.get(name) or "").strip() or None


def load_preflightx_csvs(
    annotated_dir: Path, source: str = "csv"
) -> Iterator[tuple[Any, ...]]:
    """
    Yield run-grained preflightx calls from annotated CSVs.

    Rows carry run_accession, GSM and GSE; header stubs are skipped.
    """
    if not annotated_dir.is_dir():
        logger.warning("preflightx csv dir missing, skipping: %s", annotated_dir)
        return
    # iterdir avoids glob argument-list limits in adjacent tooling.
    n_files = 0
    for path in sorted(annotated_dir.iterdir()):
        if path.suffix != ".csv" or path.name.endswith(".provenance.json"):
            continue
        n_files += 1
        if n_files % PROGRESS_EVERY == 0:
            logger.info("preflightx csv: %d files read", n_files)
        study_from_name = path.stem
        try:
            with path.open(newline="", encoding="utf-8", errors="replace") as fh:
                for row in csv.DictReader(fh):
                    run = (row.get("run_accession") or "").strip()
                    if not run:
                        continue
                    yield (
                        run,
                        (row.get("experiment_accession") or "").strip() or None,
                        (row.get("sample") or "").strip() or None,
                        (row.get("canonical_accession") or "").strip()
                        or study_from_name,
                        _col(row, "preflightx_species"),
                        _col(row, "preflightx_species_confidence"),
                        _col(row, "preflightx_sex"),
                        _col(row, "preflightx_sex_confidence"),
                        _col(row, "preflightx_assay"),
                        _col(row, "preflightx_modality"),
                        _col(row, "preflightx_modality_confidence"),
                        _col(row, "preflightx_bulk_vs_sc"),
                        _col(row, "preflightx_bulk_vs_sc_confidence"),
                        _col(row, "preflightx_end"),
                        _col(row, "preflightx_end_confidence"),
                        (row.get("sex") or "").strip() or None,
                        (row.get("assay") or "").strip() or None,
                        source,
                    )
        except OSError:
            logger.warning("unreadable preflightx csv: %s", path)


def load_microbe_quant(quant_dir: Path) -> Iterator[tuple[Any, ...]]:
    """
    Yield per-(run, organism) rows from quantifier *.quant.csv.

    Read single-threaded; multiple writers to one stream have spliced records above
    PIPE_BUF, e.g. BruceSRR3562463.
    """
    if not quant_dir.is_dir():
        logger.warning("quant dir missing, skipping: %s", quant_dir)
        return
    n_files = 0
    # Drop 6-column files with no covered_bp/breadth_frac; missing gates are
    # counted so the loss is visible.
    n_short = 0
    for path in quant_dir.rglob("*.quant.csv"):
        n_files += 1
        if n_files % PROGRESS_EVERY == 0:
            logger.info("quant: %d files read", n_files)
        run = path.name.removesuffix(".quant.csv")
        total_reads = 0
        rows: list[tuple[Any, ...]] = []
        try:
            with path.open(newline="", encoding="utf-8", errors="replace") as fh:
                for rec in csv.reader(fh):
                    if rec and not rec[0].startswith("__") and rec[0] != "virus":
                        n_short += len(rec) < QUANT_NCOL
                    if len(rec) < QUANT_NCOL or rec[0] == "virus":
                        continue
                    if rec[0] == "__total_reads__":
                        # Empty __total_reads__ keeps total_reads NULL; ENA omitted the answer.
                        total_reads = _as_int(rec[1])
                        continue
                    if rec[0] == "__run__":
                        continue
                    klass = rec[7]
                    if klass not in VALID_CLASSES:
                        continue
                    parts = rec[0].split("|")
                    organism = (
                        parts[ORGANISM_PART] if len(parts) > ORGANISM_PART else rec[0]
                    )
                    ref_acc = parts[REFACC_PART] if len(parts) > REFACC_PART else None
                    ref_bp = _as_int(rec[4]) or 0
                    covered = _as_int(rec[5]) or 0
                    breadth = _as_float(rec[6]) or 0.0
                    small_virus = klass == "virus" and 0 < ref_bp <= VIRAL_MAX_BP
                    viral = small_virus and breadth >= VIRAL_VALIDATED_BREADTH
                    viral_ev = (
                        small_virus
                        and breadth >= VIRAL_BREADTH
                        and not ENDOGENOUS_RX.match(organism)
                    )
                    bact = (
                        klass == "pathogen"
                        and ref_bp > SMALL_BP
                        and covered >= BACT_MIN_COV_BP
                        and breadth >= BACT_MIN_BREADTH
                    )
                    bact_valid = (
                        klass in BACT_CLASSES
                        and ref_bp > SMALL_BP
                        and covered >= BACT_COV_BP
                        and breadth >= BACT_BREADTH
                    )
                    rows.append(
                        (
                            run,
                            klass,
                            organism,
                            ref_acc,
                            _as_int(rec[1]) or 0,
                            _as_float(rec[2]) or 0.0,
                            _as_int(rec[3]) or 0,
                            ref_bp,
                            covered,
                            breadth,
                            int(viral),
                            int(viral_ev),
                            int(bact),
                            int(bact_valid),
                        )
                    )
        except OSError:
            logger.warning("unreadable quant csv: %s", path)
            continue
        # Append new fields by name. A positional patch shifted fields when a column
        # was added.
        for row in rows:
            yield (*row, total_reads)
    if n_short:
        logger.warning(
            "quant: %d rows dropped as pre-breadth 6-column format (of %d files)",
            n_short,
            n_files,
        )


def load_quant_provenance(quant_dir: Path) -> Iterator[tuple[Any, ...]]:
    """
    Yield per-run microbial quantification provenance.

    Depth and read length are per-run because the same breadth means different
    things at different depths. Failed downloads still get rows, preserving
    unmeasured versus measured-empty.
    """
    if not quant_dir.is_dir():
        return
    n_files = 0
    for path in quant_dir.rglob("*.quant.csv"):
        n_files += 1
        if n_files % PROGRESS_EVERY == 0:
            logger.info("quant provenance: %d files read", n_files)
        run = path.name.removesuffix(".quant.csv")
        total_reads = read_length = None
        status = None
        try:
            with path.open(newline="", encoding="utf-8", errors="replace") as fh:
                for rec in csv.reader(fh):
                    if not rec or not rec[0].startswith("__"):
                        continue
                    if rec[0] == "__total_reads__":
                        total_reads = _as_int(rec[1])
                    elif rec[0] == "__read_length__":
                        read_length = _as_int(rec[1])
                    elif rec[0] == "__status__":
                        status = (rec[1] or "").strip() or None
        except OSError:
            logger.warning("unreadable quant csv: %s", path)
            continue
        sidecar = path.with_name(f"{run}.status")
        if sidecar.is_file():
            with contextlib.suppress(OSError):
                status = sidecar.read_text(errors="replace").strip() or status
        try:
            stamp = str(path.stat().st_mtime)
        except OSError:
            stamp = None
        yield (
            run,
            status,
            total_reads,
            read_length,
            QUANT_SOURCE_TMPL.format(acc=run),
            QUANT_KMER,
            QUANT_MIN_NMATCH,
            QUANT_MIN_IDENTITY,
            QUANT_ALIGNER,
            str(quant_dir),
            VIRAL_GATE_RULE,
            BACT_GATE_RULE,
            stamp,
        )


def load_status_only(quant_dir: Path, known: set[str]) -> Iterator[tuple[Any, ...]]:
    """
    Yield provenance rows for .status files whose run lacks a quant CSV.

    These attempted-but-unmeasured runs preserve corpus scope and stay distinct
    from negative results.
    """
    if not quant_dir.is_dir():
        return
    for path in quant_dir.rglob("*.status"):
        run = path.name.removesuffix(".status")
        if run in known:
            continue
        try:
            status = path.read_text(errors="replace").strip() or None
        except OSError:
            continue
        yield (
            run,
            status,
            None,
            None,
            QUANT_SOURCE_TMPL.format(acc=run),
            QUANT_KMER,
            QUANT_MIN_NMATCH,
            QUANT_MIN_IDENTITY,
            QUANT_ALIGNER,
            str(quant_dir),
            VIRAL_GATE_RULE,
            BACT_GATE_RULE,
            None,
        )
