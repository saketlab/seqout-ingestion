"""
Find counted studies missing read-derived calls or quantification.

Samples with cells/genes are known single-cell data. The unit of work is study
because download_links resolves runs more completely than Postgres sample joins.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)

# Repeat the static gate literally. f-string construction makes every caller
# look like an injection site to ruff and readers.
GAP_SQL: dict[str, str] = {
    # no read-derived sex or assay call -> `pentimento preflight`
    "sex-assay": """
        SELECT DISTINCT s.study_accession
        FROM pentimento_sample s
        WHERE (s.cells IS NOT NULL OR s.genes IS NOT NULL)
          AND (s.sex_verdict IS NULL OR s.assay IS NULL)
    """,
    # Runs with no usable microbial-panel measurement. NULL and 0 both mean the
    # quantifier produced no result; measured-empty has a non-zero count and no flags.
    "quant": """
        SELECT DISTINCT s.study_accession
        FROM pentimento_sample s
        WHERE (s.cells IS NOT NULL OR s.genes IS NOT NULL)
          AND (s.n_runs_measurable IS NULL OR s.n_runs_measurable = 0)
    """,
}


# Sample-count cap. Plate studies can sit below it and still resolve to six
# figures of runs; use run_accessions(max_per_study=...) for run caps.
_MAX_SAMPLES = """
    AND s.study_accession IN (
        SELECT study_accession FROM pentimento_sample
        WHERE cells IS NOT NULL OR genes IS NOT NULL
        GROUP BY study_accession HAVING count(*) <= %s
    )
"""


def gap_studies(
    pg_url: str,
    need: str,
    limit: int | None = None,
    max_samples: int | None = None,
) -> list[str]:
    """Study accessions with the named gap, oldest first."""
    if need not in GAP_SQL:
        msg = f"unknown gap {need!r}; expected one of {sorted(GAP_SQL)}"
        raise ValueError(msg)
    sql = GAP_SQL[need]
    params: tuple[int, ...] = ()
    if max_samples:
        sql += _MAX_SAMPLES
        params = (max_samples,)
    sql += " ORDER BY 1"
    if limit:
        sql += f" LIMIT {int(limit)}"  # int() cast; not caller text
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute(sql, params)  # type: ignore[arg-type]
        studies = [row[0] for row in cur.fetchall()]
    logger.info("gap %s: %d studies", need, len(studies))
    return studies


def gap_counts(pg_url: str) -> Iterator[tuple[str, int]]:
    """(gap, study count) for every gap, for a quick sizing before committing."""
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        for need, sql in GAP_SQL.items():
            cur.execute(f"SELECT count(*) FROM ({sql}) t")  # type: ignore[arg-type]  # noqa: S608
            row = cur.fetchone()
            yield need, (row[0] if row else 0)


# Studies outside the sample-grained rollup. Pooled matrices are counted at
# study scope, then dropped from pentimento_sample because a study-keyed row has
# no GSM join key. Select them by study-grained cell count and linkage.
_STUDY_SCOPE_SQL = """
    SELECT study_accession, study_cells, cells_unfiltered
    FROM pentimento_study
    WHERE COALESCE(n_samples_detailed, 0) = 0
      AND COALESCE(study_cells, 0) > 0
      AND COALESCE(n_runs_linked, 0) > 0
"""
# an unfiltered matrix's columns are 10x barcodes, not cells; keep them from
# dominating size ordering
_FILTERED_ONLY = " AND NOT cells_unfiltered"
_STUDY_SCOPE_NEED = {
    # no read-derived call on any run
    "preflight": " AND COALESCE(n_runs_preflightx, 0) = 0",
    # no usable quantifier result
    "quant": " AND COALESCE(n_runs_measurable, 0) = 0",
}


def study_scope_gaps(
    pg_url: str,
    need: str | None = None,
    limit: int | None = None,
    min_cells: int = 1,
    *,
    include_unfiltered: bool = False,
) -> list[tuple[str, int, bool]]:
    """
    Study-scope cell counts invisible to sample-grained probes.

    Pooled matrices have no pentimento_sample row; order biggest filtered
    matrices first.
    """
    if need and need not in _STUDY_SCOPE_NEED:
        msg = f"unknown need {need!r}; expected one of {sorted(_STUDY_SCOPE_NEED)}"
        raise ValueError(msg)
    sql = _STUDY_SCOPE_SQL
    if not include_unfiltered:
        sql += _FILTERED_ONLY
    if need:
        sql += _STUDY_SCOPE_NEED[need]
    sql += f" AND COALESCE(study_cells, 0) >= {int(min_cells)}"
    sql += " ORDER BY cells_unfiltered, study_cells DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"  # int() cast; not caller text
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute(sql)  # type: ignore[arg-type]
        rows = [(r[0], r[1], bool(r[2])) for r in cur.fetchall()]
    logger.info("study-scope gap (need=%s): %d studies", need or "any", len(rows))
    return rows
