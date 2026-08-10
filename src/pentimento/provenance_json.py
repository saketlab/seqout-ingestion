"""Read the per-study .provenance.json files preflightx writes beside the CSVs."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

PROGRESS_EVERY = 5_000


def _best_run(experiment: dict[str, Any]) -> dict[str, Any] | None:
    """
    Return the run whose report backs the experiment's evidence: the deepest one.

    deep_runs are listed separately but are not automatically deeper than every
    entry in runs, so both are considered and reads_scanned decides.
    """
    best: dict[str, Any] | None = None
    best_depth = -1
    for key in ("runs", "deep_runs"):
        for run in experiment.get(key) or []:
            report = run.get("report") or {}
            sex = report.get("sex") or {}
            if not sex:
                continue
            depth = sex.get("reads_scanned") or 0
            if depth > best_depth:
                best, best_depth = report, depth
    return best


def _clean(value: Any) -> Any:
    """Empty string -> NULL. preflightx writes '' for "did not call"."""
    if isinstance(value, str):
        return value.strip() or None
    return value


def _as_bool_int(value: Any) -> int | None:
    return None if value is None else int(bool(value))


def _call_row(
    accession: str,
    study: Any,
    experiment: dict[str, Any],
    deep_n_reads: Any,
) -> tuple[Any, ...] | None:
    """
    One experiment's calls, shaped exactly like load_preflightx_db's rows.

    Identical column order on purpose: the JSON route then reuses the `_pfx_exp`
    staging table and the same overlay UPDATE instead of duplicating either.
    """
    vote = experiment.get("vote") or {}
    report = _best_run(experiment) or {}
    sex = report.get("sex") or {}
    species = report.get("species") or {}
    chemistry = report.get("chemistry") or {}
    # a row of NULLs here would overwrite the CSV route's calls with blanks
    if not any(_clean(v) for v in vote.values()) and not sex:
        return None
    return (
        accession,
        study,
        _clean(species.get("declared")),
        _clean(vote.get("species")) or _clean(species.get("called")),
        _clean(species.get("confidence")),
        species.get("called_hit_reads"),
        species.get("called_hit_fraction"),
        _clean(species.get("runner_up")),
        species.get("margin"),
        None,  # species_ambiguous: not in the JSON; the DB route is its source
        _as_bool_int(species.get("mislabel")),
        None,  # stated sex belongs to the metadata, not to a read scan
        _clean(vote.get("sex")) or _clean(sex.get("verdict")),
        _clean(sex.get("confidence")),
        sex.get("reads_scanned"),
        sex.get("y_hit_reads"),
        sex.get("xist_hit_reads"),
        sex.get("y_xist_ratio"),
        _clean(sex.get("panel_status")),
        _as_bool_int(sex.get("mixed_sex_suspected")),
        deep_n_reads,
        _clean(vote.get("assay")) or _clean(chemistry.get("assay_display")),
        _clean(chemistry.get("assay_display")),
        _as_bool_int(chemistry.get("is_single_cell")),
    )


def _vote_row(
    accession: str,
    study: Any,
    experiment: dict[str, Any],
    params: dict[str, Any],
) -> tuple[Any, ...]:
    """
    Return the vote and HOW MUCH was looked at to reach it.

    Runs scanned, reads per run, whether the deep pass ran: this is what
    separates "abstained after looking hard" from "barely looked".
    `preflightx_provenance` was meant to carry it and does not -- its
    params_json is 100% NULL across all 193,748 rows.
    """
    vote = experiment.get("vote") or {}
    return (
        accession,
        study,
        _clean(vote.get("species")),
        _clean(vote.get("sex")),
        _clean(vote.get("assay")),
        len(experiment.get("runs") or []),
        len(experiment.get("deep_runs") or []),
        params.get("n_reads"),
        params.get("deep_n_reads"),
    )


def load(annotated_dir: Path) -> Iterator[tuple[str, tuple[Any, ...]]]:
    """
    Yield (table, row) for both destinations in ONE pass over the files.

    One pass, not two: this is 38,108 files on NFS, and reading them once per
    destination table doubled the slowest leg of the build for nothing.
    """
    if not annotated_dir.is_dir():
        logger.warning("provenance dir missing, skipping: %s", annotated_dir)
        return
    seen = 0
    malformed = 0
    # iterdir() because this directory holds ~83k entries and a glob on it hits
    # the argument-list limit in adjacent tooling
    for path in annotated_dir.iterdir():
        if not path.name.endswith(".provenance.json"):
            continue
        seen += 1
        if seen % PROGRESS_EVERY == 0:
            logger.info("provenance json: %d files read", seen)
        try:
            doc = json.loads(path.read_text(errors="replace"))
        except (OSError, json.JSONDecodeError):
            # some are truncated mid-write by the fleet and unrecoverable
            malformed += 1
            logger.debug("unreadable provenance json: %s", path)
            continue
        if not isinstance(doc, dict):
            continue
        study = doc.get("accession")
        params = doc.get("params") or {}
        for accession, experiment in (doc.get("experiments") or {}).items():
            if not isinstance(experiment, dict):
                continue
            yield ("preflightx_vote", _vote_row(accession, study, experiment, params))
            row = _call_row(accession, study, experiment, params.get("deep_n_reads"))
            if row is not None:
                # _pfx_json and _pfx_exp share a shape but must stay separate:
                # staged together, the DB load's INSERT OR REPLACE overwrites the
                # JSON rows and re-tags the survivors 'db'
                yield ("_pfx_json", row)
    logger.info(
        "provenance json: %d files read, %d skipped as truncated (%.1f%%)",
        seen,
        malformed,
        100 * malformed / seen if seen else 0,
    )
