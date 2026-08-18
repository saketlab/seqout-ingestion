"""
UMAP coordinates over the shared 4096-d space (cuML / RAPIDS).

Runs cuML UMAP directly on the raw embeddings (no PCA) for both 2D and 3D, then
writes umap_2d_* / umap_3d_* back into each accession's chroma metadata.
Needs a CUDA GPU.
"""

import logging
from pathlib import Path

from cuml.manifold import UMAP

from embed.common import load_combined_matrix, open_chroma, scatter_write

logger = logging.getLogger(__name__)

RANDOM_STATE = 42


def compute_coords(chroma_db_dir: Path) -> None:
    db = open_chroma(chroma_db_dir)
    all_ids, x, all_metas, all_sources = load_combined_matrix(db)

    logger.info("cuML UMAP (n_components=2)")
    coords_2d = UMAP(
        n_components=2,
        random_state=RANDOM_STATE,
        output_type="numpy",
    ).fit_transform(x)

    logger.info("cuML UMAP (n_components=3)")
    coords_3d = UMAP(
        n_components=3,
        random_state=RANDOM_STATE,
        output_type="numpy",
    ).fit_transform(x)
    del x

    def build_meta(i: int, meta: dict) -> dict:
        meta["umap_2d_x"] = float(coords_2d[i, 0])
        meta["umap_2d_y"] = float(coords_2d[i, 1])
        meta["umap_3d_x"] = float(coords_3d[i, 0])
        meta["umap_3d_y"] = float(coords_3d[i, 1])
        meta["umap_3d_z"] = float(coords_3d[i, 2])
        return meta

    scatter_write(db, all_ids, all_metas, all_sources, build_meta)
    logger.info("coords done")
