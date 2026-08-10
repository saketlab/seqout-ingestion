"""
Levels COARSER than the hierarchical-Leiden root (extends cluster_leiden.py).

cluster_leiden builds its hierarchy by recursively SPLITTING the full graph, so
its coarsest level is the root partition -- there is nothing above it. This module
adds levels on top by AGGLOMERATING that root: cluster centroids -> cosine kNN graph
-> Leiden -> super-clusters, repeated. Each new level is a union of the level below,
so the hierarchy stays nested and labels already generated for finer levels stay valid.

Resolution is binary-searched per level to hit a target cluster count (~SHRINK x
coarser each step) rather than fixed, because a fixed resolution collapses straight
into the corpus's dominant blob.

Writes leiden_l<root+1> .. leiden_l<root+N> into chroma metadata; existing
keys are left untouched.
"""

import logging
import time
from collections import defaultdict

import igraph as ig
import leidenalg as la
import numpy as np

from embed.common import fetch_collection, open_chroma, scatter_write
from embed.embed_constants import COLLECTIONS

logger = logging.getLogger(__name__)

K = 5  # neighbours per node in the centroid graph
N_ITER = 10
SEED = 42
SHRINK = 3.0  # each new level ~3x coarser than the one below
MIN_CLUSTERS = 20  # stop before a level degenerates into one mega-cluster
RES_LO, RES_HI = 0.01, 200.0
RES_STEPS = 40


def _root_level(sample_meta: dict) -> str:
    """The coarsest existing leiden_l* key (the recursive split's root)."""
    keys = [k for k in sample_meta if k.startswith("leiden_l")]
    if not keys:
        msg = "no leiden_l* keys in chroma; run `embed cluster-leiden` first"
        raise ValueError(msg)
    return max(keys, key=lambda k: int(k[len("leiden_l") :]))


def _centroids(db, level: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Mean embedding per cluster at `level` (streamed -- never holds the full matrix)."""
    sums: dict[int, np.ndarray] = {}
    counts: dict[int, int] = defaultdict(int)
    for name in COLLECTIONS:
        coll = db.get_collection(name)
        _ids, embs, metas = fetch_collection(coll, ["embeddings", "metadatas"])
        for e, m in zip(np.asarray(embs, dtype=np.float32), metas, strict=True):
            c = m.get(level)
            if c is None:
                continue
            c = int(c)
            sums[c] = e.copy() if c not in sums else sums[c] + e
            counts[c] += 1
    ids = np.array(sorted(sums), dtype=np.int64)
    cent = np.stack([sums[c] / counts[c] for c in ids])
    cent /= np.linalg.norm(cent, axis=1, keepdims=True).clip(1e-9)
    sizes = np.array([counts[c] for c in ids], dtype=np.int64)
    logger.info("  %s: %d centroids over %d records", level, len(ids), sizes.sum())
    return ids, cent.astype(np.float32), sizes


def _graph(cent: np.ndarray, k: int = K) -> ig.Graph:
    """Cosine-kNN graph over L2-normalized centroids, edges weighted by similarity."""
    n = len(cent)
    k = min(k, n - 1)
    sim = cent @ cent.T
    np.fill_diagonal(sim, -np.inf)
    nbr = np.argpartition(-sim, kth=k - 1, axis=1)[:, :k]
    seen: dict[tuple[int, int], float] = {}
    for i in range(n):
        for j in nbr[i]:
            j = int(j)
            a, b = (i, j) if i < j else (j, i)
            if a != b:
                seen[(a, b)] = float(max(sim[a, b], 0.0))
    el = list(seen)
    g = ig.Graph(n=n, edges=el, directed=False)
    g.es["weight"] = [seen[e] for e in el]
    return g


def _partition(g: ig.Graph, res: float) -> np.ndarray:
    p = la.find_partition(
        g,
        la.RBConfigurationVertexPartition,
        weights="weight",
        resolution_parameter=res,
        n_iterations=N_ITER,
        seed=SEED,
    )
    return np.asarray(p.membership)


def _fit_target(g: ig.Graph, target: int) -> tuple[np.ndarray, int]:
    """Binary-search resolution for a partition with ~`target` communities."""
    lo, hi = RES_LO, RES_HI
    best: tuple[np.ndarray, int] | None = None
    for _ in range(RES_STEPS):
        mid = (lo + hi) / 2
        m = _partition(g, mid)
        n = len(np.unique(m))
        if best is None or abs(n - target) < abs(best[1] - target):
            best = (m, n)
        if n < target:
            lo = mid
        elif n > target:
            hi = mid
        else:
            break
    return best  # type: ignore[return-value]


def _roll_up(cent: np.ndarray, sizes: np.ndarray, memb: np.ndarray):
    """Size-weighted centroid + record count for each new super-cluster."""
    n = len(np.unique(memb))
    nc, ns = [], []
    for c in range(n):
        idx = np.where(memb == c)[0]
        w = sizes[idx].astype(np.float64)
        nc.append((cent[idx] * w[:, None]).sum(0) / w.sum())
        ns.append(int(w.sum()))
    out = np.stack(nc).astype(np.float32)
    out /= np.linalg.norm(out, axis=1, keepdims=True).clip(1e-9)
    return out, np.array(ns, dtype=np.int64)


def compute_coarse_leiden_clusters(chroma_db_dir, levels: int = 3) -> None:
    db = open_chroma(chroma_db_dir)
    sample = db.get_collection(COLLECTIONS[0]).get(limit=1, include=["metadatas"])
    root = _root_level(sample["metadatas"][0])
    root_n = int(root[len("leiden_l") :])
    logger.info("agglomerating above root level %s", root)

    base_ids, cent, sizes = _centroids(db, root)
    total = int(sizes.sum())

    chain = np.arange(len(base_ids), dtype=np.int64)
    new_levels: dict[str, np.ndarray] = {}  # leiden_lN -> per-root-cluster label

    t0 = time.time()
    cur_c, cur_s = cent, sizes
    for step in range(1, levels + 1):
        target = max(MIN_CLUSTERS, int(round(len(cur_c) / SHRINK)))
        if len(cur_c) <= target:
            logger.info("  stopping: level would not coarsen further")
            break
        memb, n = _fit_target(_graph(cur_c), target)
        if n >= len(cur_c) or n < 2:
            logger.info("  stopping: no further agglomeration possible")
            break
        chain = memb[chain]  # map each ROOT cluster to its id at this new level
        name = f"leiden_l{root_n + step}"
        new_levels[name] = chain.copy()
        cur_c, cur_s = _roll_up(cur_c, cur_s, memb)
        srt = np.sort(cur_s)[::-1]
        logger.info(
            "  %s: %d clusters | largest %.1f%% of corpus | median %d records",
            name,
            n,
            100 * srt[0] / total,
            int(np.median(cur_s)),
        )
    logger.info("  agglomeration %.1fs", time.time() - t0)

    if not new_levels:
        logger.warning("no coarse levels produced; nothing written")
        return

    pos = {int(c): i for i, c in enumerate(base_ids)}

    def build_meta(_i: int, meta: dict) -> dict:
        c = meta.get(root)
        if c is None:
            return meta
        row = pos.get(int(c))
        if row is None:
            return meta
        for name, labels in new_levels.items():
            meta[name] = int(labels[row])
        return meta

    all_ids: list[str] = []
    all_metas: list[dict] = []
    all_sources: list[str] = []
    for name in COLLECTIONS:
        coll = db.get_collection(name)
        ids, _primary, metas = fetch_collection(coll, ["metadatas"])
        all_ids.extend(ids)
        all_metas.extend(metas)
        all_sources.extend([name] * len(ids))

    scatter_write(db, all_ids, all_metas, all_sources, build_meta)
    logger.info("coarse leiden levels written: %s", ", ".join(new_levels))
