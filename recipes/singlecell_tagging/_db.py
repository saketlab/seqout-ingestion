"""Shared helpers: read-only Postgres and the local SQLite copy.

Postgres is only ever read: the session is forced read-only, so no statement
issued through `pg()` can write production. Every write in this recipe goes to
the LOCAL copy at LOCAL_DB, never to the pipeline's staging SQLite.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)
REPO = HERE.parent.parent
sys.path.insert(0, str(REPO / "src"))

LOCAL_DIR = Path("/NAS/saket/github/seqout-ingestion-sctag-local")
LOCAL_DB = LOCAL_DIR / "sctag_local.db"
PAPER = Path("/NAS/saket/github/seqout-paper")
GAPS = PAPER / "scripts" / "singlecell_coverage_gaps" / "out"
EXT = PAPER / "figures" / "data"
SEED = 20260929

# The pipeline's real staging file. Opened read-only (mode=ro) and only to copy
# rows out of it; never written.
PIPELINE_STAGING = Path("/data/saket/sqlites/recall_fix.db")


def pg():
    """Read-only Postgres connection through the ingestion repo's tunnel."""
    sys.path.insert(0, "/NAS/saket/github/seqout-ingestion/recipes")
    import psycopg
    from pg_connect import ensure_tunnel

    conn = psycopg.connect(ensure_tunnel())
    conn.execute("set session characteristics as transaction read only")
    conn.execute("set statement_timeout = '3h'")
    conn.commit()
    return conn


def q(conn, sql: str, params=None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    conn.commit()
    return pd.DataFrame(rows, columns=cols)


def local(readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        return sqlite3.connect(f"file:{LOCAL_DB}?mode=ro", uri=True)
    assert str(LOCAL_DB).startswith(str(LOCAL_DIR)), "refusing to write outside the local copy"
    return sqlite3.connect(LOCAL_DB)
