"""
Nearest neighbors (cosine, K=300) over the combined embedding matrix.

Brute-force on GPU: cosine == dot product of L2-normalized vectors, so the whole
matrix lives on the GPU as fp16 and each query chunk is one matmul + topk. No
approximate index needed at this scale. Results are stored in a sqlite table
``neighbors(accession, neighbors)`` where neighbors is a JSON accession list.
"""

import json
import logging
import sqlite3
from pathlib import Path

import chromadb
import numpy as np
import torch
from tqdm import tqdm

from embed.common import fetch_collection, open_chroma
from embed.embed_constants import COLLECTIONS

logger = logging.getLogger(__name__)

DEFAULT_K = 300
DEFAULT_QUERY_CHUNK = 1024  # rows per matmul


def _load_embeddings(db: chromadb.ClientAPI) -> tuple[list[str], list]:
    ids: list[str] = []
    embeds: list = []
    for name in COLLECTIONS:
        coll = db.get_collection(name)
        coll_ids, coll_embeds, _ = fetch_collection(coll, ["embeddings"])
        ids.extend(coll_ids)
        embeds.extend(coll_embeds)
    return ids, embeds


def compute_neighbors(
    chroma_db_dir: Path,
    neighbors_db_file: Path,
    k: int = DEFAULT_K,
    query_chunk: int = DEFAULT_QUERY_CHUNK,
) -> None:
    db = open_chroma(chroma_db_dir)
    ids, embeds = _load_embeddings(db)
    n = len(ids)
    logger.info("%d embeddings loaded", n)

    # Build + normalize on CPU, ship a single fp16 copy to the GPU, and matmul
    # against its (non-contiguous) transpose to avoid a second transposed copy.
    x_np = np.asarray(embeds, dtype=np.float32)
    del embeds
    x_np /= np.linalg.norm(x_np, axis=1, keepdims=True)
    x = torch.from_numpy(x_np).half().cuda()
    del x_np
    logger.info(
        "matrix %s %.1fGB on gpu",
        tuple(x.shape),
        x.element_size() * x.nelement() / 1e9,
    )

    neighbors_db_file.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(neighbors_db_file))
    con.execute(
        "CREATE TABLE IF NOT EXISTS neighbors "
        "(accession TEXT PRIMARY KEY, neighbors TEXT)",
    )
    con.execute("PRAGMA journal_mode=WAL")

    for start in tqdm(range(0, n, query_chunk), desc="knn"):
        q = x[start : start + query_chunk]
        sim = q @ x.T
        idx = sim.topk(k + 1, dim=1).indices.cpu().tolist()  # +1 to drop self
        rows = []
        for i, neigh in enumerate(idx):
            self_id = start + i
            neigh = [j for j in neigh if j != self_id][:k]  # noqa: PLW2901
            rows.append((ids[self_id], json.dumps([ids[j] for j in neigh])))
        con.executemany("INSERT OR REPLACE INTO neighbors VALUES (?, ?)", rows)
        con.commit()

    con.close()
    logger.info("wrote %s", neighbors_db_file)
