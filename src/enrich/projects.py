"""Fetch and cache project metadata."""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://seqout.org/api/bulk/project-samples-metadata"
API_URL = os.environ.get("SEQOUT_API_URL", DEFAULT_API_URL)

_SRA_DB_ENV = os.environ.get("SRA_DB")
SRA_DB: Path | None = Path(_SRA_DB_ENV) if _SRA_DB_ENV else None


@dataclass
class Project:
    """One project (GSE / SRP / ERP / DRP) with its sample-level metadata."""

    accession: str
    title: str | None = None
    description: str | None = None
    source: str | None = None
    overall_design: str | None = None
    samples: pd.DataFrame = field(default_factory=pd.DataFrame)

    @classmethod
    def from_dict(cls, d: dict) -> Project:
        """Build a Project from one bulk-endpoint record."""
        raw_samples = d.get("samples", [])
        return cls(
            accession=d["accession"],
            title=d.get("title"),
            description=d.get("description"),
            source=d.get("source"),
            overall_design=d.get("overall_design"),
            samples=pd.DataFrame(raw_samples) if raw_samples else pd.DataFrame(),
        )


def fetch_projects(
    accessions: list[str],
    timeout: int = 120,
    num_retries: int = 3,
    max_wait: int = 30,
) -> list[Project]:
    """
    POST the bulk project-samples-metadata endpoint and type the response.

    Args:
        accessions: GSE / SRP / ERP / DRP accession strings.
        timeout: per-request timeout, seconds.
        num_retries: retries on a retryable transport error or 5xx.
        max_wait: cap on the exponential backoff between retries, seconds.

    Returns:
        One Project per accession the endpoint knew about; unknown accessions
        are simply absent, so the caller compares lengths to find the gaps.

    """
    from helpers import send_req

    resp = send_req(
        "POST",
        API_URL,
        json={"accessions": accessions},
        timeout=timeout,
        num_retries=num_retries,
        max_wait=max_wait,
    )
    return [Project.from_dict(item) for item in resp.json()]


def fetch_projects_batched(
    accessions: list[str],
    batch_size: int = 25,
    on_batch: Callable[[list[Project], int, int], None] | None = None,
) -> dict[str, Project]:
    """
    Fetch projects in batches, retrying each failed batch once after five seconds.

    Log and skip batches that fail again. Call
    `on_batch(projects, done_count, total_count)` after each successful batch.
    """
    got: dict[str, Project] = {}
    for i in range(0, len(accessions), batch_size):
        chunk = accessions[i : i + batch_size]
        try:
            projects = fetch_projects(chunk)
        except Exception as exc:
            logger.warning(
                "batch %d-%d failed (%s), retrying once after 5s",
                i,
                i + len(chunk),
                exc,
            )
            time.sleep(5)
            try:
                projects = fetch_projects(chunk)
            except Exception as retry_exc:
                logger.warning(
                    "batch %d-%d failed again, SKIPPING %d accessions: %s",
                    i,
                    i + len(chunk),
                    len(chunk),
                    retry_exc,
                )
                continue
        for project in projects:
            got[project.accession] = project
        if on_batch:
            on_batch(projects, len(got), len(accessions))
    return got


def write_projects_sqlite(projects: list[Project], db_file: Path) -> None:
    """
    Cache projects and sample DataFrames in SQLite for offline fleet workers.

    Alternative to the default `enrich prefetch` pickle cache. Store sample rows
    as JSON to preserve study-specific columns; `load_projects_sqlite` rebuilds
    the DataFrames.
    """
    import json

    db_file = Path(db_file)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS projects ("
        "accession TEXT PRIMARY KEY, title TEXT, description TEXT, "
        "source TEXT, overall_design TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS project_samples ("
        "accession TEXT NOT NULL, sample_index INTEGER NOT NULL, "
        "attributes_json TEXT NOT NULL, "
        "PRIMARY KEY (accession, sample_index))"
    )
    for p in projects:
        conn.execute(
            "INSERT OR REPLACE INTO projects VALUES (?, ?, ?, ?, ?)",
            (p.accession, p.title, p.description, p.source, p.overall_design),
        )
        conn.execute("DELETE FROM project_samples WHERE accession = ?", (p.accession,))
        records = p.samples.to_dict("records") if not p.samples.empty else []
        conn.executemany(
            "INSERT INTO project_samples VALUES (?, ?, ?)",
            [
                (p.accession, i, json.dumps(row, default=str))
                for i, row in enumerate(records)
            ],
        )
    conn.commit()
    conn.close()


def load_projects_sqlite(accessions: list[str], db_file: Path) -> dict[str, Project]:
    """Read projects back from a cache `write_projects_sqlite` built."""
    import json

    conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
    out: dict[str, Project] = {}
    for acc in accessions:
        row = conn.execute(
            "SELECT title, description, source, overall_design "
            "FROM projects WHERE accession = ?",
            (acc,),
        ).fetchone()
        if row is None:
            continue
        title, description, source, overall_design = row
        sample_rows = conn.execute(
            "SELECT attributes_json FROM project_samples "
            "WHERE accession = ? ORDER BY sample_index",
            (acc,),
        ).fetchall()
        samples = (
            pd.DataFrame([json.loads(r[0]) for r in sample_rows])
            if sample_rows
            else pd.DataFrame()
        )
        out[acc] = Project(
            accession=acc,
            title=title,
            description=description,
            source=source,
            overall_design=overall_design,
            samples=samples,
        )
    conn.close()
    return out


def _sra_alias_query(query: str, param: str, db_path: Path | None) -> str | None:
    db_path = db_path or SRA_DB
    if db_path is None or not Path(db_path).exists():
        return None  # unset or absent means unavailable, never an error
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            row = conn.execute(query, (param,)).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        logger.warning(
            "SRA_DB at %s is not readable; skipping counterpart lookup", db_path
        )
        return None
    return row[0] if row else None


def gse_to_srp(gse: str, db_path: Path | None = None) -> str | None:
    """Resolve the SRP behind a GSE, via sra_studies.alias.  None if unavailable."""
    return _sra_alias_query(
        "SELECT accession FROM sra_studies WHERE alias = ? LIMIT 1", gse, db_path
    )


def srp_to_gse(srp: str, db_path: Path | None = None) -> str | None:
    """Resolve the GSE behind an SRP, via sra_studies.alias.  None if it is not one."""
    alias = _sra_alias_query(
        "SELECT alias FROM sra_studies WHERE accession = ? LIMIT 1", srp, db_path
    )
    return alias if alias and alias.startswith("GSE") else None


def find_counterpart(accession: str, db_path: Path | None = None) -> str | None:
    """For a GSE return its SRP; for an SRP return its GSE; otherwise None."""
    if accession.startswith("GSE"):
        return gse_to_srp(accession, db_path)
    if accession.startswith("SRP"):
        return srp_to_gse(accession, db_path)
    return None
