"""
One-time swap: make Leiden the clustering the apps read (overwrites EVoC in PG).

The frontend/backend read EVoC's ``cluster_l*`` names (jsonb column ``clusters``
and table ``cluster_labels``). This rewrites those in place with Leiden data, so
both apps adopt Leiden with zero code changes. Leiden's 5 labeled levels are
remapped onto contiguous ``cluster_l*`` names (finest -> coarsest):

    leiden_l0 -> cluster_l0    leiden_l3 -> cluster_l2    leiden_l5 -> cluster_l4
    leiden_l2 -> cluster_l1    leiden_l4 -> cluster_l3
    leiden_l1 is dropped (near-duplicate of leiden_l0).

Reversible: EVoC is still in chroma (``cluster_l*``) and ``labels_cluster_l*.csv``,
so ``transfer embed`` regenerates it. Existing ``cluster_labels`` rows are
snapshotted to a CSV before deletion. After running, purge the server cache
(/cache/purge) or restart so it re-reads the new levels.
"""

import csv
import logging
from pathlib import Path

import chromadb
import psycopg
from psycopg.types.json import Jsonb
from tqdm import tqdm

from embed.embed_constants import COLLECTION_TABLES, FETCH_BATCH

logger = logging.getLogger(__name__)

# leiden level (chroma key / label CSV stem) -> app level (what the apps read).
# l0..l5 come from the recursive top-down split (cluster_leiden); l6..l8 are the
# agglomerated levels above its root (cluster_leiden_coarse).
LEIDEN_TO_APP = {
    "leiden_l0": "cluster_l0",  # finest   (70348 clusters)
    "leiden_l2": "cluster_l1",  #          (42473)
    "leiden_l3": "cluster_l2",  #          (15497)
    "leiden_l4": "cluster_l3",  #          ( 3826)
    "leiden_l5": "cluster_l4",  # split root (626)
    "leiden_l6": "cluster_l5",  #          (  209)
    "leiden_l7": "cluster_l6",  #          (   70)
    "leiden_l8": "cluster_l7",  # coarsest (   23)
}
WRITE_BATCH = 1000


def _remap_clusters(meta: dict) -> dict[str, int]:
    """Per-accession {cluster_lN: id} built from Leiden layers (no noise)."""
    return {
        app: int(meta[leiden])
        for leiden, app in LEIDEN_TO_APP.items()
        if meta.get(leiden) is not None
    }


def _swap_memberships(db: chromadb.ClientAPI, conn: psycopg.Connection) -> None:
    cur = conn.cursor()
    for source, table in COLLECTION_TABLES.items():
        coll = db.get_collection(source)
        total = coll.count()
        sql = f"UPDATE {table} SET clusters = %s WHERE accession = %s"  # noqa: S608
        batch: list = []
        for off in tqdm(range(0, total, FETCH_BATCH), desc=f"clusters {table}"):
            d = coll.get(limit=FETCH_BATCH, offset=off, include=["metadatas"])
            for acc, m in zip(d["ids"], d["metadatas"], strict=True):
                batch.append((Jsonb(_remap_clusters(m)), acc))
                if len(batch) >= WRITE_BATCH:
                    cur.executemany(sql, batch)
                    conn.commit()
                    batch = []
        if batch:
            cur.executemany(sql, batch)
            conn.commit()


def _snapshot_labels(conn: psycopg.Connection, labels_dir: Path) -> None:
    out = labels_dir / "cluster_labels_backup_pre_leiden.csv"
    cur = conn.cursor()
    cur.execute(
        "SELECT level, cluster_id, label, size, centroid_x, centroid_y "
        "FROM cluster_labels ORDER BY level, cluster_id",
    )
    rows = cur.fetchall()
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["level", "cluster_id", "label", "size", "centroid_x", "centroid_y"])
        w.writerows(rows)
    logger.info("snapshotted %d cluster_labels rows -> %s", len(rows), out)


def _swap_labels(conn: psycopg.Connection, labels_dir: Path) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM cluster_labels WHERE level LIKE %s", ("cluster_l%",))
    logger.info("deleted old cluster_l* label rows")
    sql = (
        "INSERT INTO cluster_labels "
        "(level, cluster_id, label, size, centroid_x, centroid_y) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (level, cluster_id) DO UPDATE SET "
        "label = EXCLUDED.label, size = EXCLUDED.size, "
        "centroid_x = EXCLUDED.centroid_x, centroid_y = EXCLUDED.centroid_y"
    )
    for leiden_level, app_level in LEIDEN_TO_APP.items():
        path = labels_dir / f"labels_{leiden_level}.csv"
        rows: list = []
        skipped = 0
        with path.open() as f:
            for r in csv.DictReader(f):
                try:
                    rows.append(
                        (
                            app_level,
                            int(r["cluster"]),
                            r["label"],
                            int(r["size"]),
                            float(r["centroid_x"]),
                            float(r["centroid_y"]),
                        ),
                    )
                except (ValueError, TypeError, KeyError):
                    skipped += 1
        cur.executemany(sql, rows)
        conn.commit()
        logger.info(
            "%s -> %s: %d labels%s",
            leiden_level,
            app_level,
            len(rows),
            f" ({skipped} skipped)" if skipped else "",
        )


def leiden_swap(pg_url: str, chroma_db_dir: Path, labels_dir: Path) -> None:
    # fail before mutating anything if any label CSV is missing
    missing = [
        lv for lv in LEIDEN_TO_APP if not (labels_dir / f"labels_{lv}.csv").exists()
    ]
    if missing:
        msg = f"missing leiden label CSVs for {missing}; finish labeling first"
        raise FileNotFoundError(msg)

    db = chromadb.PersistentClient(path=str(chroma_db_dir))
    with psycopg.connect(pg_url) as conn:
        _snapshot_labels(conn, labels_dir)
        _swap_memberships(db, conn)
        _swap_labels(conn, labels_dir)
    logger.info("leiden swap done -- purge server cache (/cache/purge) or restart")
