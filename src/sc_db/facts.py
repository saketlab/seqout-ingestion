"""
Load per-accession facts and sum them over alias groups.

Evidence, downloads, counts and read scans use different archive keys. Loading
both sides here avoids an unindexed SQL OR over unified_metadata aliases.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable

logger = logging.getLogger(__name__)


class Downloads(NamedTuple):
    n_runs: int
    n_runs_fastq: int
    n_runs_sra_only: int
    fastq_bytes: int
    sra_bytes: int


class Preflightx(NamedTuple):
    n_measured: int
    n_sc_true: int
    n_sc_false: int


ZERO_DOWNLOADS = Downloads(0, 0, 0, 0, 0)
ZERO_PREFLIGHTX = Preflightx(0, 0, 0)


def alias_groups(
    conn: sqlite3.Connection,
    studies: Iterable[str],
) -> dict[str, set[str]]:
    """
    Map each study to accessions naming the same physical study.

    Studies absent from unified_metadata are singleton unlinked groups.
    """
    canon_to_aliases: dict[str, set[str]] = {}
    alias_to_canon: dict[str, set[str]] = defaultdict(set)
    for canonical, aliases_json in conn.execute(
        "SELECT canonical_accession, aliases FROM unified_metadata",
    ):
        values: set[str] = set()
        for value in json.loads(aliases_json or "{}").values():
            if isinstance(value, str):
                values.add(value)
            elif isinstance(value, list):
                values.update(v for v in value if isinstance(v, str))
        canon_to_aliases[canonical] = values
        for value in values:
            alias_to_canon[value].add(canonical)

    groups: dict[str, set[str]] = {}
    for study in studies:
        group = {study} | canon_to_aliases.get(study, set())
        for canonical in alias_to_canon.get(study, ()):
            group.add(canonical)
            group |= canon_to_aliases.get(canonical, set())
        groups[study] = group
    return groups


def group_key(group: set[str], sources: dict[str, tuple]) -> str:
    """
    Pick one stable name per alias group for scattering work.

    Prefer the unified canonical accession, then the lexically smallest member.
    """
    canonical = sorted(a for a in group if a in sources)
    return canonical[0] if canonical else min(group)


def downloads(conn: sqlite3.Connection) -> dict[str, Downloads]:
    """Map accession -> its per-study download-link rollup."""
    return {
        row[0]: Downloads(row[1], row[2], row[3], row[4] or 0, row[5] or 0)
        for row in conn.execute(
            "SELECT study_accession, n_runs, n_runs_fastq, n_runs_sra_only, "
            "fastq_bytes, sra_bytes FROM sc_study_downloads",
        )
    }


def matrices(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    """Map accession -> {channel: has_countable}, one entry per channel that hit."""
    out: dict[str, dict[str, int]] = defaultdict(dict)
    for study, channel, countable in conn.execute(
        "SELECT study_accession, channel, has_countable FROM sc_study_matrix",
    ):
        out[study][channel] = countable
    return out


def ncounts(conn: sqlite3.Connection) -> dict[str, tuple[int, int, int, str]]:
    """
    Map accession -> counted samples, cells, unfiltered flag and sources.

    sample_ncount is the Postgres snapshot; ncount_extra is the local gap run.
    Source labels prevent a later re-borrow from looking like a loss. Carry
    any_unfiltered beside summed cells because barcode columns are not cells.
    """
    out: dict[str, tuple[int, int, int, str]] = {}
    seen: dict[str, set[str]] = defaultdict(set)
    cells: dict[str, int] = defaultdict(int)
    unfiltered: dict[str, int] = defaultdict(int)
    sources: dict[str, set[str]] = defaultdict(set)
    queries = (
        (
            "sample_ncount",
            "SELECT study_accession, sample_accession, cells, unfiltered "
            "FROM sample_ncount",
        ),
        (
            "ncount_extra",
            "SELECT study_accession, sample_accession, cells, unfiltered "
            "FROM ncount_extra",
        ),
    )
    for label, sql in queries:
        for study, sample, cell_count, is_unfiltered in conn.execute(sql):
            if not study or sample in seen[study]:
                continue
            seen[study].add(sample)
            sources[study].add(label)
            if cell_count is not None:
                cells[study] += cell_count
            if is_unfiltered:
                unfiltered[study] = 1
    for study, samples in seen.items():
        out[study] = (
            len(samples),
            cells[study],
            unfiltered[study],
            ",".join(sorted(sources[study])),
        )
    return out


def preflightx(conn: sqlite3.Connection) -> dict[str, Preflightx]:
    """
    Map accession -> measured-run counts and study-level calls.

    A run counts as measured when any read-derived column is populated, including
    a species call with an abstained assay. `bulk_or_5prime_broken` is weak bulk
    geometry from 5' chemistry, so the false arm keys on assay_is_single_cell = 0
    and excludes single_cell geometry.
    """
    out: dict[str, Preflightx] = {}
    for study, measured, sc_true, sc_false in conn.execute(
        """
        SELECT study_accession,
               count(*) FILTER (WHERE preflightx_source IS NOT NULL
                                   OR species_called IS NOT NULL
                                   OR assay_is_single_cell IS NOT NULL
                                   OR bulk_vs_sc IS NOT NULL),
               count(*) FILTER (WHERE assay_is_single_cell = 1
                                   OR bulk_vs_sc = 'single_cell'),
               count(*) FILTER (WHERE assay_is_single_cell = 0
                                  AND coalesce(bulk_vs_sc, '') <> 'single_cell')
        FROM run_preflightx
        WHERE study_accession IS NOT NULL
        GROUP BY study_accession
        """,
    ):
        out[study] = Preflightx(measured, sc_true, sc_false)
    return out


def quantified(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Map accession -> runs with a measurable quantification result.

    NULL and 0 both mean nothing measurable landed, which is the gap. A measured
    negative is a non-zero count with empty flags.
    """
    out: dict[str, int] = defaultdict(int)
    for study, runs in conn.execute(
        "SELECT study_accession, coalesce(n_runs_measurable, 0) FROM pentimento_study",
    ):
        out[study] = max(out[study], runs or 0)
    for study, runs in conn.execute(
        "SELECT study_accession, sum(coalesce(n_runs_measurable, 0)) "
        "FROM pentimento_sample GROUP BY study_accession",
    ):
        out[study] = max(out[study], runs or 0)
    return dict(out)


def unified(conn: sqlite3.Connection) -> dict[str, tuple]:
    """Map accession -> (source, n_samples, n_experiments, organism)."""
    return {
        row[0]: (row[1], row[2], row[3], row[4])
        for row in conn.execute(
            "SELECT canonical_accession, source, n_samples, n_experiments, "
            "dominant_scientific_name FROM unified_metadata",
        )
    }


def archives(conn: sqlite3.Connection) -> dict[str, tuple[str, list[str]]]:
    """
    Map study -> strongest-rule archive and all contributing archives.

    `enrichment` is model-derived archive prose and stays named separately.
    """
    order = {"high": 0, "medium": 1, "low": 2}
    best: dict[str, tuple[int, str]] = {}
    seen: dict[str, set[str]] = defaultdict(set)
    for study, archive, tier in conn.execute(
        "SELECT DISTINCT e.study_accession, r.archive, r.tier "
        "FROM single_cell_evidence e JOIN sc_rule r ON r.id = e.rule_id "
        "WHERE e.study_accession IS NOT NULL",
    ):
        seen[study].add(archive)
        rank = order.get(tier, 3)
        if study not in best or rank < best[study][0]:
            best[study] = (rank, archive)
    return {s: (best[s][1], sorted(seen[s])) for s in seen}
