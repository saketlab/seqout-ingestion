"""Read the per-study .provenance.json files preflightx writes beside the CSVs."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

from pentimento.preflight import structural_calls

logger = logging.getLogger(__name__)

PROGRESS_EVERY = 5_000


def _best_run(experiment: dict[str, Any]) -> dict[str, Any] | None:
    """
    Return the deepest run whose report backs the experiment's evidence.

    deep_runs and runs are both considered; reads_scanned decides.
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
    One experiment's calls in load_preflightx_db row shape.

    Column order matches `_pfx_exp`, so both routes reuse one overlay UPDATE.
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
        None,  # species_ambiguous comes from the DB route
        _as_bool_int(species.get("mislabel")),
        None,  # stated sex belongs to metadata
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
        *structural_calls(report),
    )


def _vote_row(
    accession: str,
    study: Any,
    experiment: dict[str, Any],
    params: dict[str, Any],
) -> tuple[Any, ...]:
    """
    Return the vote and scan depth behind it.

    Runs scanned, reads per run and deep-pass status separate shallow abstention
    from deep abstention. params_json is NULL across all 193,748 rows.
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
    Yield rows for both destinations in one pass over NFS files.

    The corpus is 38,108 files; single-pass loading avoids duplicate reads.
    """
    if not annotated_dir.is_dir():
        logger.warning("provenance dir missing, skipping: %s", annotated_dir)
        return
    seen = 0
    malformed = 0
    # iterdir() because this directory holds many entries and a glob on it hits
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
