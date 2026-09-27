"""
Incremental {accession, distance} neighbor lists for "Similar projects".

`pysradb-server`'s SIMILAR_STUDIES_SQL reads only `geo_series.neighbors` /
`sra_studies.neighbors` (jsonb[] of {accession, distance}); it needs no
x_3d/y_3d/z_3d and no coords_2d/coords_3d, so this skips the UMAP/cuML
coords step and the neighbors.sqlite3 intermediate that `embed neighbors` +
`transfer embed` require. arrayexpress rows are never written: an AE
project's "Similar projects" is served from its linked GEO/SRA row, not its
own `neighbors` column.

Scope is incremental, not a full recompute: only geo/sra accessions with
`neighbors IS NULL` are queried, run as GPU matmul rows against the full
combined matrix (all three collections) as candidates, the same brute-force
cosine approach as `embed neighbors`, just query-subset x full-matrix instead
of full x full, so it stays cheap enough to run after every `embed run`
rather than only quarterly. Existing rows' neighbor sets are left untouched;
they get refreshed by the next full `embed neighbors` + `transfer embed`
pass.

Writes are additive only (`WHERE neighbors IS NULL`), batched as multi-row
VALUES updates: one row-at-a-time UPDATE per accession over an SSH-tunneled
connection is orders of magnitude slower for tens of thousands of rows.
"""

import json
import logging
from pathlib import Path

import numpy as np
import psycopg
import torch

from embed.common import load_combined_matrix, open_chroma

logger = logging.getLogger(__name__)

DEFAULT_K = 300
DEFAULT_QUERY_CHUNK = 1024
WRITE_BATCH = 2000

_TARGET_TABLES = ("geo_series", "sra_studies")


def _pending_accessions(conn: psycopg.Connection) -> dict[str, str]:
    """Map each geo_series/sra_studies accession missing neighbors to its table."""
    pending: dict[str, str] = {}
    with conn.cursor() as cur:
        for table in _TARGET_TABLES:
            cur.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS neighbors jsonb[]",
            )
            cur.execute(
                f"SELECT accession FROM {table} WHERE neighbors IS NULL",  # noqa: S608
            )
            for (acc,) in cur.fetchall():
                pending[acc] = table
    conn.commit()
    return pending


def _write_batch(
    conn: psycopg.Connection,
    table: str,
    rows: list[tuple[str, list[dict]]],
) -> int:
    total = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), WRITE_BATCH):
            chunk = rows[start : start + WRITE_BATCH]
            values_sql = ",".join(["(%s,%s::jsonb)"] * len(chunk))
            params: list[str] = []
            for acc, neighbors in chunk:
                params.append(acc)
                params.append(json.dumps(neighbors))
            cur.execute(
                f"WITH data(accession, neighbors) AS (VALUES {values_sql}) "  # noqa: S608
                f"UPDATE {table} t "
                "SET neighbors = "
                "(SELECT array_agg(e) FROM jsonb_array_elements(d.neighbors) e) "
                "FROM data d "
                "WHERE t.accession = d.accession AND t.neighbors IS NULL",
                params,
            )
            total += cur.rowcount
        conn.commit()
    return total


def push_new_neighbors(
    chroma_db_dir: Path,
    pg_url: str,
    k: int = DEFAULT_K,
    query_chunk: int = DEFAULT_QUERY_CHUNK,
) -> None:
    with psycopg.connect(pg_url) as conn:
        pending = _pending_accessions(conn)
        logger.info("%d geo/sra accessions missing neighbors", len(pending))
        if not pending:
            return

        db = open_chroma(chroma_db_dir)
        combined = load_combined_matrix(db)
        ids, x_np = combined[0], combined[1]
        id_index = {acc: i for i, acc in enumerate(ids)}

        query_idx = [id_index[acc] for acc in pending if acc in id_index]
        missing_from_chroma = len(pending) - len(query_idx)
        if missing_from_chroma:
            logger.warning(
                "%d pending accession(s) have no chroma embedding yet -- "
                "run `embed run` first",
                missing_from_chroma,
            )
        if not query_idx:
            return

        x_np = x_np / np.linalg.norm(x_np, axis=1, keepdims=True)
        x = torch.from_numpy(x_np).half().cuda()
        del x_np

        by_table: dict[str, list[tuple[str, list[dict]]]] = {
            t: [] for t in _TARGET_TABLES
        }
        for start in range(0, len(query_idx), query_chunk):
            chunk_idx = query_idx[start : start + query_chunk]
            q = x[chunk_idx]
            sim = (q @ x.T).float()
            topk = sim.topk(min(k + 1, x.shape[0]), dim=1)
            top_vals = topk.values.cpu().tolist()
            top_idx = topk.indices.cpu().tolist()
            for row_pos, self_idx in enumerate(chunk_idx):
                acc = ids[self_idx]
                neighbors = [
                    {"accession": ids[j], "distance": round(1.0 - v, 6)}
                    for j, v in zip(top_idx[row_pos], top_vals[row_pos], strict=True)
                    if j != self_idx
                ][:k]
                by_table[pending[acc]].append((acc, neighbors))

        for table, rows in by_table.items():
            if not rows:
                continue
            n = _write_batch(conn, table, rows)
            logger.info("%s: wrote neighbors for %d rows", table, n)
