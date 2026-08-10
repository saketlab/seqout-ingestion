"""Loaders for the four Pentimento inputs, each yielding tuples for executemany."""

from __future__ import annotations

import contextlib
import csv
import json
import logging
import sqlite3
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

# the >=0.50 breadth rule is calibrated on this size class
SMALL_BP = 50_000
VIRAL_BREADTH = 0.50
# bacteria: absolute covered bases plus a small relative floor, see pentimento.sql
BACT_MIN_COV_BP = 20_000
BACT_MIN_BREADTH = 0.005
PROGRESS_EVERY = 5_000

# quantifier settings, recorded per run so a call can be re-derived later; these
# mirror virus_quant_srr.sh and must change in both places together
QUANT_KMER = 31
QUANT_MIN_NMATCH = 40
QUANT_MIN_IDENTITY = 0.95
QUANT_ALIGNER = "minimap2 -x sr --secondary=no"
QUANT_SOURCE_TMPL = "https://s3.amazonaws.com/logan-pub/u/{acc}/{acc}.unitigs.fa.zst"
VIRAL_GATE_RULE = "class=virus AND 0 < ref_bp <= 50000 AND breadth_frac >= 0.50"
BACT_GATE_RULE = (
    "class=pathogen AND ref_bp > 50000 AND covered_bp >= 20000 "
    "AND breadth_frac >= 0.005"
)

# quant.csv shape: name,reads,kmer_mass,n_unitigs,ref_bp,covered_bp,breadth_frac,class
QUANT_NCOL = 8
ORGANISM_PART = 1
REFACC_PART = 2

# 10x barcode whitelist sizes; a cell count that is an exact multiple of one of
# these is an unfiltered matrix whose columns are barcodes rather than cells
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

    Two separate failures live here. First, preflightx grades confidence
    'low'/'medium'/'high', NOT numerically: an earlier version ran these through
    _as_float, so float("high") raised and every one of the 9,807 graded calls
    was silently discarded -- the column read as 100% NULL while looking
    correctly typed.

    Second, '' is not a value. The enrichment DB spells "no call" as NULL for
    some columns and as the EMPTY STRING for others, and a blank that survives
    into Pentimento both renders as an empty cell and, because the route overlay
    COALESCEs, overwrites a real call another route measured.
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
    Yield per-study rows from the ncount result JSONs.

    Reads the per-sample JSON dumps (one file per study). unfiltered is
    trusted from the JSON when present, and otherwise derived from the barcode
    whitelist test, because older dumps predate that field.
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


def load_preflightx_db(db_file: Path) -> Iterator[tuple[Any, ...]]:
    """
    Yield experiment-grained preflightx calls from the enrichment SQLite.

    This route is the only one carrying confidence columns, but it is a strict
    and STALE subset of the annotated CSVs (14,539 vs 44,857 studies observed).
    Rows are tagged preflightx_source='db' so a consumer can tell which route
    a call came from when the two disagree.
    """
    if not db_file.is_file():
        logger.warning("preflightx db missing, skipping: %s", db_file)
        return
    uri = f"file:{db_file}?immutable=1"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT study_accession, experiment_accession, species, sex, assay,
                   assay_display, assay_is_single_cell,
                   sex_verdict, sex_confidence, sex_reads_scanned,
                   y_hits, xist_hits, y_xist_ratio, sex_panel_status,
                   sex_mixed_suspected, deep_n_reads,
                   species_called, species_confidence, species_hit_reads,
                   species_hit_fraction, species_runner_up, species_margin,
                   species_ambiguous, species_mislabel
            FROM preflightx_experiments
            """
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
            )


def load_preflightx_csvs(
    annotated_dir: Path, source: str = "csv"
) -> Iterator[tuple[Any, ...]]:
    """
    Yield RUN-grained preflightx calls from the annotated CSVs.

    These CSVs are the richer of the two routes and, importantly, they are
    already run-grained: each row carries run_accession, the GEO sample
    (GSM) and canonical_accession (GSE). That makes them a complete spine on
    their own -- no ENA lookup is needed to reach a GSM, which matters because
    the on-disk ENA snapshot is a delta that covers almost none of this corpus.

    Column names are preflightx_species / preflightx_sex /
    preflightx_assay. Measured population over 4,000 files: run_accession
    83.1%, sample/GSM 16.9%, preflightx_species 34.4%, preflightx_sex 38.4%,
    preflightx_assay 19.9% -- i.e. most rows carry an identifier but no call, so
    rows are emitted whenever a run is present and the call columns are simply
    left NULL.

    ~1,277 files are header-only stubs and a few carry embedded newlines inside a
    quoted description; rows without a run accession are dropped rather than
    trusted.

    source tags the route. The older `mukesh_*_preflight/annotated` dirs have
    this exact 63-column schema and are read by this same function, but they ship
    with ZERO .provenance.json files -- their calls can never be traced back to
    the reads behind them -- so they are tagged 'csv_legacy' and a consumer can
    exclude un-provenanced calls without guessing which dir a row came from.
    """
    if not annotated_dir.is_dir():
        logger.warning("preflightx csv dir missing, skipping: %s", annotated_dir)
        return
    # iterdir() because this directory holds ~83k entries and a glob on it hits
    # the argument-list limit in adjacent tooling. Progress is logged since this
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
                        (row.get("preflightx_species") or "").strip() or None,
                        (row.get("preflightx_sex") or "").strip() or None,
                        (row.get("preflightx_assay") or "").strip() or None,
                        (row.get("sex") or "").strip() or None,
                        (row.get("assay") or "").strip() or None,
                        source,
                    )
        except OSError:
            logger.warning("unreadable preflightx csv: %s", path)


def load_microbe_quant(quant_dir: Path) -> Iterator[tuple[Any, ...]]:
    """
    Yield per-(run, organism) rows from the quantifier's *.quant.csv.

    Read single-threaded on purpose. Fanning this out with N writers into one
    stream splices records together above PIPE_BUF -- observed as fused fields
    like BruceSRR3562463 -- which then survive a naive column check.
    """
    if not quant_dir.is_dir():
        logger.warning("quant dir missing, skipping: %s", quant_dir)
        return
    n_files = 0
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
                    if len(rec) < QUANT_NCOL or rec[0] == "virus":
                        continue
                    if rec[0] == "__total_reads__":
                        total_reads = _as_int(rec[1]) or 0
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
                    viral = (
                        klass == "virus"
                        and 0 < ref_bp <= SMALL_BP
                        and breadth >= VIRAL_BREADTH
                    )
                    bact = (
                        klass == "pathogen"
                        and ref_bp > SMALL_BP
                        and covered >= BACT_MIN_COV_BP
                        and breadth >= BACT_MIN_BREADTH
                    )
                    rows.append(
                        (
                            run,
                            klass,
                            organism,
                            ref_acc,
                            _as_int(rec[1]) or 0,
                            ref_bp,
                            covered,
                            breadth,
                            0,  # total_reads patched below
                            int(viral),
                            int(bact),
                        )
                    )
        except OSError:
            logger.warning("unreadable quant csv: %s", path)
            continue
        # __total_reads__ usually appears before the hits, but patching after
        # the fact keeps the depth correct when a file lists it late
        for row in rows:
            yield (*row[:8], total_reads, *row[9:])


def load_quant_provenance(quant_dir: Path) -> Iterator[tuple[Any, ...]]:
    """
    Yield per-run provenance for the microbial quantification.

    Depth and read length are recorded per run because they change what a breadth
    number MEANS -- breadth 0.5 off 700 k reads is not the evidence that breadth
    0.5 off 700 M reads is -- and the thresholds are stored alongside so a call
    can be re-derived without reading the shell script that produced it.

    status comes from the sidecar <RUN>.status file when present (values
    seen: ok, download_failed <stage>, absent_from_logan), else from the
    __status__ row inside the quant CSV. A run that failed to download still
    gets a row, so "not measured" stays distinguishable from "measured, nothing
    found" -- the distinction the whole bacterial story turns on.
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
    Yield provenance rows for runs that have a .status file but no quant CSV.

    These are the attempted-but-unmeasured runs (download failures, absent from
    Logan). Without them the corpus silently looks smaller than it was, and an
    absence gets read as a negative result.
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
