"""
Push embedding-derived columns from chroma/sqlite into Postgres.

Per accession (geo_series / sra_studies / arrayexpress_experiments):
  coords_2d  double precision[]   -- [x, y]         from chroma umap_2d_*
  coords_3d  double precision[]   -- [x, y, z]      from chroma umap_3d_*
  clusters   jsonb                -- {"cluster_lN": id, ...}, noise (-1) dropped,
                                     cluster_id deliberately excluded (layers only)
  neighbors  jsonb[]              -- [{x_3d, y_3d, z_3d, source, accession}, ...]
                                     accession lists from neighbors.sqlite3

Per cluster (one shared table ``cluster_labels``): loaded from the labels CSVs;
``centroid_x/y`` is the label anchor. All writes are upserts (accession PK /
(level, cluster_id) PK), columns/table created if absent, so this is re-runnable.
"""

import csv
import json
import logging
import sqlite3
from pathlib import Path

import chromadb
import psycopg
from psycopg.types.json import Jsonb
from tqdm import tqdm

from embed.embed_constants import (
    COLLECTION_TABLES,
    FETCH_BATCH,
    TRANSFER_STEP_ORDER,
    TransferStep,
)

logger = logging.getLogger(__name__)

WRITE_BATCH = 1000


def _ensure_columns(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        for table in COLLECTION_TABLES.values():
            cur.execute(
                f"ALTER TABLE {table} "
                "ADD COLUMN IF NOT EXISTS coords_2d double precision[]",
            )
            cur.execute(
                f"ALTER TABLE {table} "
                "ADD COLUMN IF NOT EXISTS coords_3d double precision[]",
            )
            cur.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS clusters jsonb",
            )
            cur.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS neighbors jsonb[]",
            )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS cluster_labels ("
            "level text, cluster_id int, label text, size int, "
            "centroid_x double precision, centroid_y double precision, "
            "PRIMARY KEY (level, cluster_id))",
        )
    conn.commit()


def _cluster_map(meta: dict) -> dict[str, int]:
    """Layer-wise membership only: {cluster_lN: id}; noise (-1)/cluster_id dropped."""
    return {
        k: int(v) for k, v in meta.items() if k.startswith("cluster_l") and int(v) != -1
    }


def _load_coords(db: chromadb.ClientAPI) -> dict[str, tuple]:
    """Acc -> (x3, y3, z3, source) for every embedding across all collections."""
    coords: dict[str, tuple] = {}
    for source in COLLECTION_TABLES:
        coll = db.get_collection(source)
        total = coll.count()
        for off in tqdm(range(0, total, FETCH_BATCH), desc=f"coords {source}"):
            d = coll.get(limit=FETCH_BATCH, offset=off, include=["metadatas"])
            for acc, m in zip(d["ids"], d["metadatas"], strict=True):
                coords[acc] = (
                    round(m["umap_3d_x"], 4),
                    round(m["umap_3d_y"], 4),
                    round(m["umap_3d_z"], 4),
                    source,
                )
    return coords


def _neighbor_objs(acc_list: list[str], coords: dict[str, tuple]) -> list[dict]:
    objs = []
    for a in acc_list:
        c = coords.get(a)
        if c is None:
            continue
        objs.append(
            {"x_3d": c[0], "y_3d": c[1], "z_3d": c[2], "source": c[3], "accession": a},
        )
    return objs


def _transfer_coords_neighbors(
    db: chromadb.ClientAPI,
    conn: psycopg.Connection,
    neighbors_db_file: Path,
) -> None:
    coords = _load_coords(db)
    logger.info("%d coords loaded", len(coords))

    nb = sqlite3.connect(f"file:{neighbors_db_file}?mode=ro", uri=True)
    cur = conn.cursor()
    try:
        for source, table in COLLECTION_TABLES.items():
            accs = [a for a, c in coords.items() if c[3] == source]
            sql = (
                f"INSERT INTO {table} (accession, coords_3d, neighbors) "  # noqa: S608
                "VALUES (%s, %s, "
                "(SELECT array_agg(e) FROM jsonb_array_elements(%s) e)) "
                "ON CONFLICT (accession) DO UPDATE "
                "SET coords_3d = EXCLUDED.coords_3d, neighbors = EXCLUDED.neighbors"
            )
            batch = []
            for a in tqdm(accs, desc=f"write coords/neighbors {table}"):
                x, y, z, _ = coords[a]
                row = nb.execute(
                    "SELECT neighbors FROM neighbors WHERE accession=?",
                    (a,),
                ).fetchone()
                nlist = _neighbor_objs(json.loads(row[0]), coords) if row else []
                batch.append((a, [x, y, z], Jsonb(nlist)))
                if len(batch) >= WRITE_BATCH:
                    cur.executemany(sql, batch)
                    conn.commit()
                    batch = []
            if batch:
                cur.executemany(sql, batch)
                conn.commit()
    finally:
        nb.close()


def _transfer_coords2d_clusters(
    db: chromadb.ClientAPI,
    conn: psycopg.Connection,
) -> None:
    cur = conn.cursor()
    for source, table in COLLECTION_TABLES.items():
        coll = db.get_collection(source)
        total = coll.count()
        sql = (
            f"INSERT INTO {table} (accession, coords_2d, clusters) "  # noqa: S608
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (accession) DO UPDATE "
            "SET coords_2d = EXCLUDED.coords_2d, clusters = EXCLUDED.clusters"
        )
        batch = []
        for off in tqdm(
            range(0, total, FETCH_BATCH), desc=f"write 2d/clusters {table}"
        ):
            d = coll.get(limit=FETCH_BATCH, offset=off, include=["metadatas"])
            for acc, m in zip(d["ids"], d["metadatas"], strict=True):
                coords = [round(m["umap_2d_x"], 4), round(m["umap_2d_y"], 4)]
                batch.append((acc, coords, Jsonb(_cluster_map(m))))
                if len(batch) >= WRITE_BATCH:
                    cur.executemany(sql, batch)
                    conn.commit()
                    batch = []
        if batch:
            cur.executemany(sql, batch)
            conn.commit()


def transfer_labels(conn: psycopg.Connection, labels_dir: Path) -> None:
    """(Re)load the cluster_labels table from labels_cluster_l*.csv."""
    cur = conn.cursor()
    sql = (
        "INSERT INTO cluster_labels "
        "(level, cluster_id, label, size, centroid_x, centroid_y) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (level, cluster_id) DO UPDATE SET "
        "label = EXCLUDED.label, size = EXCLUDED.size, "
        "centroid_x = EXCLUDED.centroid_x, centroid_y = EXCLUDED.centroid_y"
    )
    for path in sorted(labels_dir.glob("labels_cluster_l*.csv")):
        level = path.stem[len("labels_") :]
        rows = []
        skipped = 0
        with path.open() as f:
            for r in csv.DictReader(f):
                try:
                    rows.append(
                        (
                            level,
                            int(r["cluster"]),
                            r["label"],
                            int(r["size"]),
                            float(r["centroid_x"]),
                            float(r["centroid_y"]),
                        )
                    )
                except (ValueError, TypeError, KeyError):
                    skipped += 1
        cur.executemany(sql, rows)
        conn.commit()
        if skipped:
            logger.warning("%s: skipped %d malformed row(s)", level, skipped)
        logger.info("%s: %d labels", level, len(rows))


def embed_transfer(
    pg_url: str,
    chroma_db_dir: Path,
    neighbors_db_file: Path,
    labels_dir: Path,
    *,
    start_from: TransferStep = TransferStep.coords_neighbors,
) -> None:
    start_idx = TRANSFER_STEP_ORDER.index(start_from)
    do_coords_neighbors = start_idx <= TRANSFER_STEP_ORDER.index(
        TransferStep.coords_neighbors,
    )
    do_coords2d = start_idx <= TRANSFER_STEP_ORDER.index(TransferStep.coords2d)

    with psycopg.connect(pg_url) as conn:
        _ensure_columns(conn)
        db = (
            chromadb.PersistentClient(path=str(chroma_db_dir))
            if do_coords_neighbors or do_coords2d
            else None
        )
        if do_coords_neighbors:
            _transfer_coords_neighbors(db, conn, neighbors_db_file)
        if do_coords2d:
            _transfer_coords2d_clusters(db, conn)
        transfer_labels(conn, labels_dir)
    logger.info("transfer done")
