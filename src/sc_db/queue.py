"""
Build sc_work_queue: remaining work per single-cell study.

The queue subtracts measured work from data buckets and routes plate protocols
around barcode gates. `preflightx_verdict = "absent"` means unmeasured.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sc_db import facts, plate

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

logger = logging.getLogger(__name__)

# Standing rule: defer studies over 200 samples. This flag protects fleet
# runs; a plate atlas resolves to far more runs than the median, hence the
# backfill_quant cap of 500 runs per study.
MEGA_SAMPLES = 200
MEGA_RUNS = 500

QUEUE_COLUMNS = (
    "study_accession",
    "archive",
    "archives",
    "group_key",
    "group_size",
    "source",
    "evidence_tier",
    "has_high_confidence",
    "n_rules",
    "sc_objects",
    "assay_modality",
    "assay_modalities",
    "analytes",
    "n_samples",
    "n_experiments",
    "organism_stated",
    "n_runs",
    "n_runs_fastq",
    "n_runs_sra_only",
    "fastq_bytes",
    "sra_bytes",
    "has_fastq",
    "is_sra_object_only",
    "has_matrix",
    "has_countable_matrix",
    "matrix_channels",
    "data_bucket",
    "n_samples_ncounted",
    "cells_total",
    "any_unfiltered",
    "ncount_sources",
    "ncount_status",
    "n_runs_preflightx",
    "n_runs_sc_true",
    "n_runs_sc_false",
    "preflightx_status",
    "preflightx_verdict",
    "n_runs_quantified",
    "pentimento_status",
    "is_plate_candidate",
    "plate_terms",
    "next_stage",
    "stage_reason",
    "needs_ncount",
    "needs_preflightx",
    "needs_pentimento",
    "is_mega_study",
    "defer_reason",
    "built_at",
)


def verdict_of(pfx: facts.Preflightx) -> str:
    """
    Turn run-grained calls into the study-level gate input.

    A positive run is enough for mixed studies. A bulk verdict applies after
    positive signal is absent; 5' chemistry can look bulk to geometry.
    """
    if pfx.n_sc_true > 0:
        return "single_cell"
    if pfx.n_measured == 0:
        return "absent"
    if pfx.n_sc_false > 0:
        return "not_single_cell"
    return "uncertain"


def coverage_status(measured: int, target: int | None) -> str:
    """
    Grade coverage as none / partial / done.

    Unknown denominator stays below done because one measured run may be a slice of a
    larger study.
    """
    if measured <= 0:
        return "none"
    if target is None or target <= 0:
        return "partial"
    return "done" if measured >= target else "partial"


@dataclass(frozen=True)
class Decision:
    next_stage: str
    stage_reason: str
    needs_ncount: bool
    needs_preflightx: bool
    needs_pentimento: bool


def classify(
    *,
    has_fastq: bool,
    has_matrix: bool,
    verdict: str,
    ncount_status: str,
    pentimento_status: str,
    is_plate: bool,
) -> Decision:
    """
    Decide next_stage and independent needs flags.

    Order: missing data -> blocked; plate protocol -> plate_route; FASTQ awaiting
    stage 1 -> preflightx; positive stage 1 awaiting quant -> pentimento; matrix
    awaiting count -> ncount; exhausted negative gate -> blocked; otherwise done.
    """
    needs_ncount = has_matrix and ncount_status != "done"
    needs_preflightx = has_fastq and verdict == "absent"
    needs_pentimento = (
        has_fastq and verdict == "single_cell" and pentimento_status != "done"
    )

    if not has_fastq and not has_matrix:
        return Decision(
            next_stage="blocked",
            stage_reason="no_fastq_no_matrix",
            needs_ncount=False,
            needs_preflightx=False,
            needs_pentimento=False,
        )
    if is_plate and has_fastq and verdict != "single_cell":
        # Plate route establishes library structure before stage 2 decisions.
        return Decision(
            next_stage="plate_route",
            stage_reason=f"plate_protocol_verdict_{verdict}",
            needs_ncount=needs_ncount,
            needs_preflightx=needs_preflightx,
            needs_pentimento=False,
        )
    if needs_preflightx:
        return Decision(
            next_stage="preflightx",
            stage_reason="stage1_never_scanned",
            needs_ncount=needs_ncount,
            needs_preflightx=True,
            needs_pentimento=False,
        )
    if needs_pentimento:
        return Decision(
            next_stage="pentimento",
            stage_reason=f"stage1_positive_quant_{pentimento_status}",
            needs_ncount=needs_ncount,
            needs_preflightx=False,
            needs_pentimento=True,
        )
    if needs_ncount:
        return Decision(
            next_stage="ncount",
            stage_reason=f"matrix_{ncount_status}",
            needs_ncount=True,
            needs_preflightx=False,
            needs_pentimento=False,
        )
    is_negative = verdict == "not_single_cell"
    return Decision(
        next_stage="blocked" if is_negative else "done",
        stage_reason="stage1_negative" if is_negative else f"stage1_{verdict}",
        needs_ncount=False,
        needs_preflightx=False,
        needs_pentimento=False,
    )


def _study_rollup(conn: sqlite3.Connection) -> dict[str, tuple]:
    """Read single_cell_study once; it aggregates the whole evidence table."""
    return {
        row[0]: row[1:]
        for row in conn.execute(
            "SELECT study_accession, has_high_confidence, sc_objects, n_rules, "
            "evidence_tier, assay_modalities, analytes FROM single_cell_study",
        )
    }


def _modality(modalities_json: str | None) -> str | None:
    """Collapse modality array to one label, or 'mixed' for multiple values."""
    try:
        values = [m for m in json.loads(modalities_json or "[]") if m]
    except json.JSONDecodeError:
        return None
    if not values:
        return None
    return values[0] if len(values) == 1 else "mixed"


def build_queue(
    conn: sqlite3.Connection,
    csv_file: Path | None = None,
) -> dict[str, int]:
    """Assemble sc_work_queue from the borrowed facts, replacing what is there."""
    built_at = datetime.now(UTC).isoformat()
    rollup = _study_rollup(conn)
    logger.info("  studies in single_cell_study: %s", f"{len(rollup):,}")

    arch = facts.archives(conn)
    groups = facts.alias_groups(conn, rollup)
    unified = facts.unified(conn)
    dl = facts.downloads(conn)
    mx = facts.matrices(conn)
    nc = facts.ncounts(conn)
    pfx = facts.preflightx(conn)
    quant = facts.quantified(conn)
    plate_hits = plate.plate_candidates(conn)
    logger.info(
        "  facts: %s download rollups, %s matrix studies, %s ncounted, "
        "%s preflightx'd, %s plate candidates",
        f"{len(dl):,}",
        f"{len(mx):,}",
        f"{len(nc):,}",
        f"{len(pfx):,}",
        f"{len(plate_hits):,}",
    )

    rows = []
    for study, rolled in rollup.items():
        high, sc_objects, n_rules, tier, modalities, analytes = rolled
        group = groups[study]
        key = facts.group_key(group, unified)

        d = facts.ZERO_DOWNLOADS
        for member in group:
            member_dl = dl.get(member)
            if member_dl:
                d = facts.Downloads(*(a + b for a, b in zip(d, member_dl, strict=True)))

        channels = {ch: c for member in group for ch, c in mx.get(member, {}).items()}
        counted = cells = unfiltered = 0
        nc_sources: set[str] = set()
        for member in group:
            hit = nc.get(member)
            if hit:
                counted += hit[0]
                cells += hit[1]
                unfiltered = max(unfiltered, hit[2])
                nc_sources.update(hit[3].split(","))

        p = facts.ZERO_PREFLIGHTX
        for member in group:
            member_pfx = pfx.get(member)
            if member_pfx:
                p = facts.Preflightx(
                    *(a + b for a, b in zip(p, member_pfx, strict=True)),
                )
        quantified = max((quant.get(member, 0) for member in group), default=0)

        source, n_samples, n_experiments, organism = unified.get(
            key,
            unified.get(study, (None, None, None, None)),
        )
        has_fastq = d.n_runs_fastq > 0
        has_matrix = bool(channels)
        verdict = verdict_of(p)
        ncount_status = coverage_status(counted, n_samples)
        pent_status = coverage_status(quantified, d.n_runs_fastq or None)
        terms = plate_hits.get(study, [])
        decision = classify(
            has_fastq=has_fastq,
            has_matrix=has_matrix,
            verdict=verdict,
            ncount_status=ncount_status,
            pentimento_status=pent_status,
            is_plate=bool(terms),
        )
        bucket = (
            "both"
            if has_fastq and has_matrix
            else "preflightx"
            if has_fastq
            else "ncount"
            if has_matrix
            else "unavailable"
        )
        mega_reasons = []
        if n_samples is not None and n_samples > MEGA_SAMPLES:
            mega_reasons.append(f"n_samples>{MEGA_SAMPLES}")
        if d.n_runs > MEGA_RUNS:
            mega_reasons.append(f"n_runs>{MEGA_RUNS}")

        rows.append(
            (
                study,
                arch.get(study, (None, []))[0],
                json.dumps(arch.get(study, (None, []))[1]),
                key,
                len(group),
                source,
                tier,
                high,
                n_rules,
                sc_objects,
                _modality(modalities),
                modalities,
                analytes,
                n_samples,
                n_experiments,
                organism,
                d.n_runs,
                d.n_runs_fastq,
                d.n_runs_sra_only,
                d.fastq_bytes,
                d.sra_bytes,
                int(has_fastq),
                int(not has_fastq and d.n_runs_sra_only > 0),
                int(has_matrix),
                int(any(channels.values())),
                json.dumps(sorted(channels)),
                bucket,
                counted,
                cells,
                unfiltered,
                ",".join(sorted(nc_sources)) or None,
                ncount_status,
                p.n_measured,
                p.n_sc_true,
                p.n_sc_false,
                coverage_status(p.n_measured, d.n_runs_fastq or None),
                verdict,
                quantified,
                pent_status,
                int(bool(terms)),
                json.dumps(terms) if terms else None,
                decision.next_stage,
                decision.stage_reason,
                int(decision.needs_ncount),
                int(decision.needs_preflightx),
                int(decision.needs_pentimento),
                int(bool(mega_reasons)),
                ",".join(mega_reasons) or None,
                built_at,
            )
        )

    conn.execute("DELETE FROM sc_work_queue")
    conn.executemany(
        f"INSERT INTO sc_work_queue ({','.join(QUEUE_COLUMNS)}) "  # noqa: S608
        f"VALUES ({','.join('?' * len(QUEUE_COLUMNS))})",
        rows,
    )
    conn.commit()
    logger.info("  sc_work_queue: %s rows", f"{len(rows):,}")

    if csv_file is not None:
        export_csv(conn, csv_file)
    return {"rows": len(rows)}


def export_csv(conn: sqlite3.Connection, csv_file: Path) -> int:
    """Write the queue out verbatim; the CSV is the fleet's input format."""
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    with csv_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(QUEUE_COLUMNS)
        rows = conn.execute(
            f"SELECT {','.join(QUEUE_COLUMNS)} FROM sc_work_queue "  # noqa: S608
            "ORDER BY next_stage, n_runs, study_accession",
        )
        count = 0
        for row in rows:
            writer.writerow(row)
            count += 1
    logger.info("  wrote %s rows to %s", f"{count:,}", csv_file)
    return count
