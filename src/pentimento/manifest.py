"""Build the per-study manifests preflightx scans, from seqout's own databases."""

from __future__ import annotations

import csv
import json
import logging
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


def _chunks(items: list[str], size: int = CHUNK) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _readonly(db_file: Path) -> sqlite3.Connection:
    # immutable=1, not mode=ro: these DBs carry live -wal sidecars and a
    # read-only open fails outright against them.
    return sqlite3.connect(f"file:{db_file}?immutable=1", uri=True)


def candidate_keys(canonical: str, aliases_json: str | None) -> set[str]:
    """Return the study accessions download links might be keyed by."""
    # most GSEs resolve only through an SRA alias; the canonical accession
    # alone once left GEO at 0% coverage
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
            "SELECT study_accession, sample_accession, sex, assay "  # noqa: S608
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

    A study with no fastq links still gets a manifest with its sample rows: the
    absence of reads is a fact about the study, and dropping it here would make
    the gap invisible downstream.
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
