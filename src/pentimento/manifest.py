"""Build the per-study manifests preflightx scans, from seqout's own databases."""

from __future__ import annotations

import csv
import json
import logging
import re
import sqlite3
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

# Read by pentimento.preflight (record_type, experiment_accession, fastq_ftp) and
# by sources.load_preflightx_csvs (everything else). Keep the two in step.
HEADER = (
    "canonical_accession",
    "record_type",
    "sample",
    "run_accession",
    "experiment_accession",
    "fastq_ftp",
    "sex",
    "assay",
)
CHUNK = 900
# quant fetches Logan by run accession, so a run needs no resolution at all
RUN_RX = re.compile(r"^[SDE]RR\d+$")


def _chunks(items: list[str], size: int = CHUNK) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _readonly(db_file: Path) -> sqlite3.Connection:
    # immutable=1, because these DBs carry live -wal sidecars and mode=ro
    # fails outright against them.
    return sqlite3.connect(f"file:{db_file}?immutable=1", uri=True)


def candidate_keys(canonical: str, aliases_json: str | None) -> set[str]:
    """Return the study accessions download links might be keyed by."""
    # GSEs usually resolve through an SRA alias; include aliases for GEO coverage.
    keys = {canonical}
    try:
        aliases = json.loads(aliases_json) if aliases_json else {}
    except (json.JSONDecodeError, TypeError):
        aliases = {}
    if isinstance(aliases, dict):
        keys.update(v for v in aliases.values() if v)
    elif isinstance(aliases, list):
        keys.update(v for v in aliases if v)
    return keys


def _studies(
    conn: sqlite3.Connection, accessions: list[str] | None
) -> list[tuple[str, str | None]]:
    """(canonical_accession, aliases) for the studies to scan."""
    if accessions:
        found: list[tuple[str, str | None]] = []
        for chunk in _chunks(accessions):
            placeholders = ",".join("?" * len(chunk))
            found.extend(
                conn.execute(
                    "SELECT canonical_accession, aliases FROM unified_metadata "  # noqa: S608
                    f"WHERE canonical_accession IN ({placeholders})",
                    chunk,
                )
            )
        missing = len(accessions) - len(found)
        if missing:
            logger.warning("%d requested accessions absent from unified", missing)
        return found
    return list(
        conn.execute(
            "SELECT canonical_accession, aliases FROM unified_metadata "
            "WHERE is_single_cell = 1"
        )
    )


def _runs_by_study(
    links: sqlite3.Connection, batch: list[tuple[str, str | None]]
) -> dict[str, list[tuple[Any, ...]]]:
    """Resolve one batch of studies to their runs through every candidate key."""
    owner: dict[str, str] = {}
    for canonical, aliases in batch:
        for key in candidate_keys(canonical, aliases):
            owner.setdefault(key, canonical)
    runs: dict[str, list[tuple[Any, ...]]] = {}
    for chunk in _chunks(list(owner)):
        placeholders = ",".join("?" * len(chunk))
        for row in links.execute(
            "SELECT study_accession, run_accession, experiment_accession, "  # noqa: S608
            "fastq_ftp FROM run_download_links "
            f"WHERE study_accession IN ({placeholders})",
            chunk,
        ):
            study = owner.get(row[0])
            if study:
                runs.setdefault(study, []).append(row[1:])
    return runs


def _samples_by_study(
    enrichment: sqlite3.Connection | None, accessions: list[str]
) -> dict[str, list[tuple[Any, ...]]]:
    """Stated sex/assay per sample, for the mismatch comparison. Optional."""
    if enrichment is None:
        return {}
    samples: dict[str, list[tuple[Any, ...]]] = {}
    for chunk in _chunks(accessions):
        placeholders = ",".join("?" * len(chunk))
        for study, sample, sex, assay in enrichment.execute(
            # the column is `sample`; enriched_samples has no sample_accession
            "SELECT study_accession, sample, sex, assay "  # noqa: S608
            f"FROM enriched_samples WHERE study_accession IN ({placeholders})",
            chunk,
        ):
            samples.setdefault(study, []).append((sample, sex, assay))
    return samples


def _write(
    out_dir: Path,
    study: str,
    samples: Iterable[tuple[Any, ...]],
    runs: Iterable[tuple[Any, ...]],
) -> None:
    with (out_dir / f"{study}.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        for sample, sex, assay in samples:
            writer.writerow(
                [study, "sample", sample, "", "", "", sex or "", assay or ""]
            )
        for run, experiment, fastq in runs:
            writer.writerow(
                [study, "run", "", run, experiment or "", fastq or "", "", ""]
            )


def build_manifests(
    out_dir: Path,
    links_db_file: Path,
    unified_db_file: Path,
    enrichment_db_file: Path | None = None,
    accessions: list[str] | None = None,
    batch_size: int = 400,
) -> dict[str, int]:
    """
    Write <out_dir>/<study>.csv for every study to scan.

    Studies lacking fastq links still get manifests with sample rows so
    downstream gap checks can see them.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    unified = _readonly(unified_db_file)
    links = _readonly(links_db_file)
    enrichment = _readonly(enrichment_db_file) if enrichment_db_file else None
    try:
        studies = _studies(unified, accessions)
        written = with_runs = 0
        for i in range(0, len(studies), batch_size):
            batch = studies[i : i + batch_size]
            accs = [s for s, _ in batch]
            runs = _runs_by_study(links, batch)
            samples = _samples_by_study(enrichment, accs)
            for study in accs:
                _write(out_dir, study, samples.get(study, []), runs.get(study, []))
                written += 1
                with_runs += bool(runs.get(study))
            logger.info("manifests written: %d/%d", written, len(studies))
    finally:
        unified.close()
        links.close()
        if enrichment:
            enrichment.close()
    counts = {"studies": written, "with_fastq": with_runs}
    logger.info("manifest: %s", json.dumps(counts))
    return counts


def run_accessions(
    links_db_file: Path,
    unified_db_file: Path,
    accessions: list[str],
    batch_size: int = 400,
    max_per_study: int | None = None,
    exclude_studies: set[str] | None = None,
) -> list[str]:
    """
    Resolve arbitrary accessions to quantifier run accessions.

    Runs pass through; studies resolve by alias-aware lookup. `exclude_studies`
    drops named studies. `max_per_study` caps one contribution: an atlas can
    resolve to orders of magnitude more runs than the median study.
    """
    runs: list[str] = []
    seen: set[str] = set()
    studies = []
    for acc in accessions:
        if RUN_RX.match(acc):
            if acc not in seen:
                seen.add(acc)
                runs.append(acc)
        else:
            studies.append(acc)
    if not studies:
        return runs

    unified = _readonly(unified_db_file)
    links = _readonly(links_db_file)
    try:
        resolved = _studies(unified, studies)
        skip = exclude_studies or set()
        if skip:
            resolved = [r for r in resolved if r[0] not in skip]
            logger.info("excluding %d study/studies: %s", len(skip), sorted(skip))
        dropped: dict[str, int] = {}
        for i in range(0, len(resolved), batch_size):
            by_study = _runs_by_study(links, resolved[i : i + batch_size])
            for study, found in by_study.items():
                kept = found[:max_per_study] if max_per_study else found
                if len(kept) < len(found):
                    dropped[study] = len(found) - len(kept)
                for run, _experiment, _fastq in kept:
                    if run and run not in seen:
                        seen.add(run)
                        runs.append(run)
        for study, n in sorted(dropped.items(), key=lambda kv: -kv[1]):
            logger.warning("capped %s at %d runs, dropped %d", study, max_per_study, n)
    finally:
        unified.close()
        links.close()
    logger.info("quant: %d accessions -> %d runs", len(accessions), len(runs))
    return runs


def studies_with_runs(
    links_db_file: Path,
    unified_db_file: Path,
    studies: list[str],
    batch_size: int = 400,
    *,
    require_fastq: bool = False,
) -> list[str]:
    """
    Keep studies with scan-ready runs, order preserved.

    Work lists drop studies with no runs. `require_fastq` is for preflight;
    quant fetches Logan by run accession.
    """
    unified = _readonly(unified_db_file)
    links = _readonly(links_db_file)
    try:
        resolved = _studies(unified, studies)
        keep: set[str] = set()
        for i in range(0, len(resolved), batch_size):
            batch = resolved[i : i + batch_size]
            for study, runs in _runs_by_study(links, batch).items():
                usable = (
                    [r for r in runs if (r[2] or "").strip()] if require_fastq else runs
                )
                if usable:
                    keep.add(study)
    finally:
        unified.close()
        links.close()
    kept = [s for s in studies if s in keep]
    what = "runs with fastq" if require_fastq else "runs"
    logger.info("run filter: %d/%d studies have %s", len(kept), len(studies), what)
    return kept
