"""
Embed new accessions into the shared chroma space.

Diffs each source's studies against the corresponding chroma collection, builds
cleaned text for the missing ones, and embeds them with the shared MedEmbed model.
Existing accessions are left untouched (their metadata is assumed unchanged).

Encoding reuses the adaptive length-sorted batching from the original sra embed:
batch size is chosen per-batch from a token budget and auto-shrinks on CUDA OOM,
so a few very long abstracts don't blow up memory. Texts longer than the model's
max sequence length are chunked and mean-pooled instead.
"""

import gc
import logging
from collections.abc import Iterator
from pathlib import Path

import chromadb
import numpy as np
import torch
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from transformers.cache_utils import DynamicCache

from embed.embed_constants import (
    CHROMA_MAX_BATCH,
    DEFAULT_MODEL_NAME,
    FETCH_BATCH,
)
from embed.text import (
    iter_new_ae_texts,
    iter_new_geo_texts,
    iter_new_sra_texts,
)

logger = logging.getLogger(__name__)

# Patch for the custom F2LLM model code that still calls the old
# DynamicCache.get_usable_length (renamed to get_seq_length upstream).
if not hasattr(DynamicCache, "get_usable_length"):
    DynamicCache.get_usable_length = lambda self, *_args, **_kwargs: (  # type: ignore[attr-defined]
        self.get_seq_length()
    )

TOKEN_BUDGET = 16384
CHARS_PER_TOKEN = 3  # conservative proxy for length estimation


def _existing_ids(collection: chromadb.Collection) -> set[str]:
    """All accessions already in a collection, paged (a full get() OOMs sqlite)."""
    ids: set[str] = set()
    total = collection.count()
    for offset in tqdm(
        range(0, total, FETCH_BATCH),
        desc=f"scan {collection.name}",
        leave=False,
    ):
        data = collection.get(limit=FETCH_BATCH, offset=offset, include=[])
        ids.update(data["ids"])
    return ids


def _flush(
    collection: chromadb.Collection,
    buf_ids: list[str],
    buf_embeds: list[list[float]],
    buf_texts: list[str],
) -> None:
    for k in range(0, len(buf_ids), CHROMA_MAX_BATCH):
        collection.upsert(
            ids=buf_ids[k : k + CHROMA_MAX_BATCH],
            embeddings=buf_embeds[k : k + CHROMA_MAX_BATCH],
            documents=buf_texts[k : k + CHROMA_MAX_BATCH],
        )


def _encode_adaptive(
    model: SentenceTransformer,
    ids: list[str],
    texts: list[str],
    max_seq_length: int,
    collection: chromadb.Collection,
) -> None:
    """Length-sorted adaptive batching with OOM auto-shrink."""
    order = sorted(range(len(texts)), key=lambda i: len(texts[i]), reverse=True)

    token_budget = TOKEN_BUDGET
    buf_ids: list[str] = []
    buf_embeds: list[list[float]] = []
    buf_texts: list[str] = []
    i = 0
    pbar = tqdm(total=len(texts), desc=collection.name)

    while i < len(texts):
        longest = order[i]
        approx_tok = min(
            max(len(texts[longest]) // CHARS_PER_TOKEN, 1),
            max_seq_length,
        )
        batch_size = max(1, token_budget // approx_tok)
        batch_indices = order[i : i + batch_size]
        batch_texts = [texts[j] for j in batch_indices]

        try:
            embeds = model.encode(batch_texts, show_progress_bar=False)
        except torch.cuda.OutOfMemoryError:
            torch.cuda.empty_cache()
            gc.collect()
            if batch_size == 1:
                pbar.write(f"OOM on single item of ~{approx_tok} tokens, skipping")
                i += 1
                pbar.update(1)
                continue
            token_budget = max(approx_tok, token_budget // 2)
            pbar.write(f"OOM at batch={batch_size}, token_budget -> {token_budget}")
            continue

        for j, e in zip(batch_indices, embeds, strict=True):
            buf_ids.append(ids[j])
            buf_embeds.append(e.tolist())
            buf_texts.append(texts[j])

        i += len(batch_indices)
        pbar.update(len(batch_indices))
        pbar.set_postfix(batch=batch_size, tok=approx_tok, budget=token_budget)

        if len(buf_ids) >= CHROMA_MAX_BATCH:
            _flush(collection, buf_ids, buf_embeds, buf_texts)
            buf_ids, buf_embeds, buf_texts = [], [], []

    if buf_ids:
        _flush(collection, buf_ids, buf_embeds, buf_texts)
    pbar.close()


def _encode_chunked(
    model: SentenceTransformer,
    ids: list[str],
    texts: list[str],
    max_seq_length: int,
    collection: chromadb.Collection,
) -> None:
    """Texts exceed max_seq_length: chunk each, encode chunks, mean-pool."""
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=max_seq_length,
        chunk_overlap=20,
    )
    buf_ids: list[str] = []
    buf_embeds: list[list[float]] = []
    buf_texts: list[str] = []
    for sid, text in zip(tqdm(ids, desc=collection.name), texts, strict=True):
        chunk_embeds = model.encode(splitter.split_text(text), show_progress_bar=False)
        buf_ids.append(sid)
        buf_embeds.append(np.mean(chunk_embeds, axis=0).tolist())
        buf_texts.append(text)
        if len(buf_ids) >= CHROMA_MAX_BATCH:
            _flush(collection, buf_ids, buf_embeds, buf_texts)
            buf_ids, buf_embeds, buf_texts = [], [], []
    if buf_ids:
        _flush(collection, buf_ids, buf_embeds, buf_texts)


def _embed_collection(
    model: SentenceTransformer,
    collection: chromadb.Collection,
    pairs: Iterator[tuple[str, str]],
) -> None:
    ids: list[str] = []
    texts: list[str] = []
    for acc, text in pairs:
        ids.append(acc)
        texts.append(text)

    if not ids:
        logger.info("%s: nothing new to embed", collection.name)
        return

    logger.info("%s: embedding %d new accessions", collection.name, len(ids))
    max_len = model.max_seq_length
    needs_chunking = any(len(t) > max_len * CHARS_PER_TOKEN for t in texts)
    if needs_chunking:
        _encode_chunked(model, ids, texts, max_len, collection)
    else:
        _encode_adaptive(model, ids, texts, max_len, collection)


def embed_new(
    geo_db_file: Path,
    sra_db_file: Path,
    ae_db_file: Path,
    unified_metadata_db_file: Path,
    chroma_db_dir: Path,
    model_name: str = DEFAULT_MODEL_NAME,
) -> None:
    """Embed accessions missing from chroma across geo, sra and arrayexpress."""
    db = chromadb.PersistentClient(path=str(chroma_db_dir))
    geo = db.get_or_create_collection("geo")
    sra = db.get_or_create_collection("sra")
    ae = db.get_or_create_collection("arrayexpress")

    logger.info("loading model %s", model_name)
    model = SentenceTransformer(model_name, trust_remote_code=True)

    try:
        _embed_collection(
            model, geo, iter_new_geo_texts(geo_db_file, _existing_ids(geo))
        )
        _embed_collection(
            model, sra, iter_new_sra_texts(sra_db_file, _existing_ids(sra))
        )
        _embed_collection(
            model,
            ae,
            iter_new_ae_texts(ae_db_file, unified_metadata_db_file, _existing_ids(ae)),
        )
    finally:
        del model
        gc.collect()
        torch.cuda.empty_cache()

    for coll in (geo, sra, ae):
        logger.info("%s now has %d entries", coll.name, coll.count())
