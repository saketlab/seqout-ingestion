"""
Hierarchical Leiden clustering over the shared space (alternative to EVoC).

Pipeline: raw 4096-d embeddings -> GPU cuML PCA (400-d, L2-normalized) -> approx
kNN graph -> recursive top-down Leiden (vtraag/leidenalg). Unlike EVoC, Leiden
assigns EVERY point to a community, so the output has ZERO noise. Results are
merged into each accession's chroma metadata under leiden_l0 (finest) ..
leiden_lN (coarsest); EVoC's cluster_l* keys are left untouched.

Needs a CUDA GPU (cuML) plus python-igraph + leidenalg.
"""

import logging
import time

import igraph as ig
import leidenalg as la
import numpy as np

from embed.common import load_combined_matrix, open_chroma, scatter_write

logger = logging.getLogger(__name__)

N_COMPONENTS = 400
K = 15  # neighbors per node in the kNN graph
RESOLUTION = 1.0  # Leiden resolution (RBConfiguration ~ modularity at 1.0)
N_ITER = 5  # Leiden iterations per split
SEED = 42
MIN_SPLIT_SIZE = 25  # don't try to split communities smaller than this
MAX_DEPTH = 12  # hard cap on hierarchy depth


def _pca_normalize(x: np.ndarray) -> np.ndarray:
    import cupy as cp
    from cuml.decomposition import PCA

    logger.info("cuML PCA -> %d-d on GPU", N_COMPONENTS)
    t0 = time.time()
    xg = cp.asarray(x)
    pca = PCA(n_components=N_COMPONENTS)
    xr = pca.fit_transform(xg)
    xr = xr / cp.linalg.norm(xr, axis=1, keepdims=True).clip(1e-9)
    evr = float(pca.explained_variance_ratio_.sum())
    out = cp.asnumpy(xr).astype(np.float32)
    del xg, xr
    cp.get_default_memory_pool().free_all_blocks()
    logger.info(
        "  PCA %.1fs | %d comps = %.1f%% variance",
        time.time() - t0,
        N_COMPONENTS,
        evr * 100,
    )
    return out


def _knn_edges(xr: np.ndarray) -> np.ndarray:
    from cuml.neighbors import NearestNeighbors

    n = xr.shape[0]
    logger.info("cuML kNN (k=%d) on GPU", K)
    t0 = time.time()
    nn = NearestNeighbors(n_neighbors=K + 1, algorithm="ivfflat", metric="euclidean")
    nn.fit(xr)
    _, idx = nn.kneighbors(xr)
    idx = np.asarray(idx)[:, 1:]  # drop self
    src = np.repeat(np.arange(n, dtype=np.int64), idx.shape[1])
    dst = idx.reshape(-1).astype(np.int64)
    good = (dst >= 0) & (dst != src)
    src, dst = src[good], dst[good]
    a = np.minimum(src, dst)
    b = np.maximum(src, dst)
    key = np.unique(a * n + b)  # dedup undirected
    edges = np.stack([key // n, key % n], axis=1)
    logger.info(
        "  kNN %.1fs | %d nodes, %d undirected edges", time.time() - t0, n, len(edges)
    )
    return edges


def _hierarchical_leiden(n: int, edges: np.ndarray) -> list[np.ndarray]:
    """
    Recursive top-down Leiden.

    Returns per-node label arrays, level 0 finest to level L coarsest. Every
    node is always assigned, so there is no noise label.
    """
    g = ig.Graph(n=n, edges=edges.tolist(), directed=False)
    levels: list[np.ndarray] = []
    counter = [0]

    def ensure(depth: int) -> None:
        while len(levels) <= depth:
            levels.append(np.full(n, -1, dtype=np.int64))

    def recurse(global_idx: np.ndarray, sub: ig.Graph, depth: int) -> int:
        ensure(depth)
        part = la.find_partition(
            sub,
            la.RBConfigurationVertexPartition,
            resolution_parameter=RESOLUTION,
            n_iterations=N_ITER,
            seed=SEED,
        )
        membership = np.asarray(part.membership)
        deepest = depth
        for comm in range(len(part)):
            local = np.where(membership == comm)[0]
            gidx = global_idx[local]
            uid = counter[0]
            counter[0] += 1
            levels[depth][gidx] = uid
            if len(gidx) >= MIN_SPLIT_SIZE and depth + 1 < MAX_DEPTH and len(part) > 1:
                child = sub.induced_subgraph(local.tolist())
                deepest = max(deepest, recurse(gidx, child, depth + 1))
            else:
                deepest = max(deepest, depth)
        return deepest

    logger.info("running recursive hierarchical Leiden")
    t0 = time.time()
    recurse(np.arange(n, dtype=np.int64), g, 0)
    logger.info(
        "  Leiden %.1f min, %d raw levels", (time.time() - t0) / 60, len(levels)
    )

    m = np.vstack(levels)
    for d in range(1, m.shape[0]):
        mask = m[d] == -1
        m[d][mask] = m[d - 1][mask]

    out = [
        np.unique(m[d], return_inverse=True)[1].astype(np.int64)
        for d in range(m.shape[0])
    ]
    out.reverse()
    return out


def compute_leiden_clusters(chroma_db_dir) -> None:
    db = open_chroma(chroma_db_dir)
    all_ids, x, all_metas, all_sources = load_combined_matrix(db)
    n = x.shape[0]

    xr = _pca_normalize(x)
    del x
    edges = _knn_edges(xr)
    del xr
    layers = _hierarchical_leiden(n, edges)

    for i, layer in enumerate(layers):
        logger.info("  leiden_l%d: %d clusters (0 noise)", i, len(np.unique(layer)))

    def build_meta(i: int, meta: dict) -> dict:
        for layer_idx, layer in enumerate(layers):
            meta[f"leiden_l{layer_idx}"] = int(layer[i])
        return meta

    scatter_write(db, all_ids, all_metas, all_sources, build_meta)
    logger.info("leiden clusters done")
