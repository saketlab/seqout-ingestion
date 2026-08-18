"""
Hierarchical clustering over the shared space with EVoC.

EVoC (Embedding Vector Oriented Clustering) runs on the raw 4096-d embeddings.
cluster_layers_ gives the hierarchical layers (l0 finest -> lN coarsest, -1 =
noise); the flat cluster_id is written too but is deliberately not transferred
downstream. Results are merged into each accession's chroma metadata.
"""

import logging
from pathlib import Path

import evoc

from embed.common import load_combined_matrix, open_chroma, scatter_write

logger = logging.getLogger(__name__)


def compute_clusters(chroma_db_dir: Path) -> None:
    db = open_chroma(chroma_db_dir)
    all_ids, x, all_metas, all_sources = load_combined_matrix(db)

    logger.info("running EVoC")
    clusterer = evoc.EVoC()
    labels = clusterer.fit_predict(x)
    layers = clusterer.cluster_layers_
    del x

    logger.info("flat clusters: %d (incl noise if present)", len(set(labels)))
    for i, layer in enumerate(layers):
        logger.info("  layer %d: %d clusters", i, len(set(layer)))

    def build_meta(i: int, meta: dict) -> dict:
        meta["cluster_id"] = int(labels[i])
        for layer_idx, layer in enumerate(layers):
            meta[f"cluster_l{layer_idx}"] = int(layer[i])
        return meta

    scatter_write(db, all_ids, all_metas, all_sources, build_meta)
    logger.info("clusters done")
