"""Shared chroma read/write helpers for the coords/cluster/neighbor stages."""

import logging
from collections.abc import Callable
from pathlib import Path

import chromadb
import numpy as np
from tqdm import tqdm

from embed.embed_constants import CHROMA_MAX_BATCH, COLLECTIONS, FETCH_BATCH

logger = logging.getLogger(__name__)


def open_chroma(chroma_db_dir: Path) -> chromadb.ClientAPI:
    return chromadb.PersistentClient(path=str(chroma_db_dir))


def fetch_collection(
    collection: chromadb.Collection,
    include: list[str],
) -> tuple[list[str], list, list]:
    """Page (ids, <first-include>, metadatas) out of a collection."""
    total = collection.count()
    ids: list[str] = []
    primary: list = []
    metas: list = []
    want_meta = "metadatas" in include
    for offset in tqdm(
        range(0, total, FETCH_BATCH),
        desc=f"fetch {collection.name}",
    ):
        data = collection.get(limit=FETCH_BATCH, offset=offset, include=include)
        ids.extend(data["ids"])
        if include and include[0] != "metadatas":
            primary.extend(data[include[0]])
        if want_meta:
            metas.extend(data["metadatas"] or [{} for _ in data["ids"]])
    return ids, primary, metas


def load_combined_matrix(
    db: chromadb.ClientAPI,
) -> tuple[list[str], np.ndarray, list[dict], list[str]]:
    """
    Concatenate embeddings+metadata across all source collections.

    Returns (ids, X, metadatas, sources) aligned row-for-row so results can be
    scattered back per source afterwards.
    """
    all_ids: list[str] = []
    all_embeds: list = []
    all_metas: list[dict] = []
    all_sources: list[str] = []
    for name in COLLECTIONS:
        coll = db.get_collection(name)
        ids, embeds, metas = fetch_collection(coll, ["embeddings", "metadatas"])
        logger.info("  %s: %d entries", name, len(ids))
        all_ids.extend(ids)
        all_embeds.extend(embeds)
        all_metas.extend(metas)
        all_sources.extend([name] * len(ids))

    x = np.asarray(all_embeds, dtype=np.float32)
    del all_embeds
    logger.info("matrix %s, %.2f GB", x.shape, x.nbytes / 1e9)
    return all_ids, x, all_metas, all_sources


def write_back(
    collection: chromadb.Collection,
    ids: list[str],
    metas: list[dict],
) -> None:
    """Merge-update metadata in chunks (chroma update merges keys, cap 5461)."""
    for k in tqdm(
        range(0, len(ids), CHROMA_MAX_BATCH),
        desc=f"update {collection.name}",
    ):
        collection.update(
            ids=ids[k : k + CHROMA_MAX_BATCH],
            metadatas=metas[k : k + CHROMA_MAX_BATCH],
        )


def scatter_write(
    db: chromadb.ClientAPI,
    all_ids: list[str],
    all_metas: list[dict],
    all_sources: list[str],
    build_meta: Callable[[int, dict], dict],
) -> None:
    """Regroup rows by source and write updated metadata back per collection."""
    for name in COLLECTIONS:
        mask = [i for i, s in enumerate(all_sources) if s == name]
        coll = db.get_collection(name)
        upd_ids = [all_ids[i] for i in mask]
        upd_metas = [build_meta(i, dict(all_metas[i] or {})) for i in mask]
        write_back(coll, upd_ids, upd_metas)
