"""
Generate human-readable cluster labels (TF-IDF keywords + local LLM).

For each clustering level, groups documents by cluster, extracts the top TF-IDF
keywords, and asks a local OpenAI-compatible LLM (e.g. Qwen3 via llama.cpp/vLLM)
for a short 2-5 word label. Writes one CSV per level with the label plus the 2D
centroid (the anchor where the frontend draws the label). Coarsest levels are
processed first and each CSV resumes, so a re-run picks up where it left off.
"""

import csv
import logging
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from openai import OpenAI
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm

from embed.common import fetch_collection, open_chroma
from embed.embed_constants import COLLECTIONS

logger = logging.getLogger(__name__)

DEFAULT_VLLM_BASE = "http://localhost:1729/v1"
DEFAULT_MODEL = "unsloth/Qwen3.6-27B-MTP-GGUF:UD-Q8_K_XL"

SYSTEM_PROMPT = (
    "You are a domain expert in genomics and biomedical research who names "
    "clusters of research datasets. You are given the most distinctive keywords "
    "from one cluster (ranked by TF-IDF over the datasets' metadata). Reply with "
    "ONLY a specific 2-5 word label that names the organism, disease, tissue, "
    "assay/technique, pathway, or biological process distinguishing the cluster. "
    "Never use generic terms like 'Genomic Research', 'Biomedical Analysis', or "
    "'Sequencing Studies'. Good labels look like: 'E. coli Antibiotic Resistance', "
    "'Arabidopsis Epigenomics', 'Single-Cell RNA-seq Tumors', 'Gut Microbiome 16S'. "
    "Output the label text only -- no quotes, punctuation, explanation, or reasoning."
)

MAX_DOCS_PER_CLUSTER = 500  # sample cap for TF-IDF
TOP_K_KEYWORDS = 20
_MIN_DF_DOC_THRESHOLD = 5  # below this, don't require a term in >=2 docs
# Only purely-alphabetic tokens (>=2 chars): drops accession ids, years, and
# other numeric/alphanumeric noise (e.g. erp142156, 2022, covid19) that make
# for useless label keywords.
_TOKEN_PATTERN = r"(?u)\b[a-zA-Z]{2,}\b"  # noqa: S105
CONCURRENCY = 8  # in-flight LLM requests; match server --parallel slots
MAX_RETRIES = 5
INITIAL_BACKOFF = 2


def _load_all_data(chroma_db_dir: Path) -> list[tuple[str, dict]]:
    db = open_chroma(chroma_db_dir)
    records: list[tuple[str, dict]] = []
    for name in COLLECTIONS:
        coll = db.get_collection(name)
        _ids, docs, metas = fetch_collection(coll, ["documents", "metadatas"])
        for doc, meta in zip(docs, metas, strict=True):
            records.append((doc or "", meta))
    return records


def _detect_levels(sample_meta: dict, prefix: str = "cluster_l") -> list[str]:
    """Clustering keys for `prefix` ordered coarsest -> finest.

    For EVoC (``cluster_l``) the flat ``cluster_id`` is prepended; other
    clusterings (e.g. ``leiden_l``) expose hierarchical layers only.
    """
    layer_keys = sorted(
        (k for k in sample_meta if k.startswith(prefix)),
        key=lambda k: int(k[len(prefix) :]),
    )
    levels = list(reversed(layer_keys))
    if prefix == "cluster_l":
        levels = ["cluster_id", *levels]
    return levels


def _extract_keywords(docs: list[str], top_k: int = TOP_K_KEYWORDS) -> list[str]:
    if not docs:
        return []
    if len(docs) > MAX_DOCS_PER_CLUSTER:
        rng = np.random.default_rng(42)
        idx = rng.choice(len(docs), MAX_DOCS_PER_CLUSTER, replace=False)
        docs = [docs[i] for i in idx]
    # Tight clusters have near-identical docs, so max_df=0.9 can prune the whole
    # vocab -> retry with no pruning before giving up.
    for max_df, min_df in (
        (0.9, 2 if len(docs) > _MIN_DF_DOC_THRESHOLD else 1),
        (1.0, 1),
    ):
        try:
            vec = TfidfVectorizer(
                max_features=5000,
                stop_words="english",
                token_pattern=_TOKEN_PATTERN,
                max_df=max_df,
                min_df=min_df,
            )
            mat = vec.fit_transform(docs)
            names = vec.get_feature_names_out()
            scores = np.asarray(mat.mean(axis=0)).flatten()
            top = scores.argsort()[-top_k:][::-1]
            return [names[i] for i in top]
        except ValueError:
            continue
    return []


def _label_cluster(client: OpenAI, model: str, keywords: list[str], size: int) -> str:
    if not keywords:
        return "unknown"

    prompt = (
        f"Top TF-IDF keywords from a cluster of {size} biomedical / genomics "
        f"research datasets:\n\n{', '.join(keywords)}\n\nLabel:"
    )

    backoff = INITIAL_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            r = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7,
                top_p=0.80,
                presence_penalty=1.5,
                max_tokens=30,
                extra_body={
                    "chat_template_kwargs": {"enable_thinking": False},
                    "top_k": 20,
                    "min_p": 0.0,
                    "repetition_penalty": 1.0,
                },
            )
            out = r.choices[0].message.content or ""
            out = re.sub(r"<think>.*?</think>", "", out, flags=re.DOTALL)
            return out.strip().strip('"').strip("'")
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            tqdm.write(f"  error after {MAX_RETRIES} retries: {exc}")
            return ", ".join(keywords[:3])
    return ", ".join(keywords[:3])


def _process_level(
    level: str,
    all_data: list[tuple[str, dict]],
    client: OpenAI,
    model: str,
    out_dir: Path,
) -> None:
    out_path = out_dir / f"labels_{level}.csv"

    existing: set[int] = set()
    if out_path.exists():
        with out_path.open() as f:
            existing = {int(row["cluster"]) for row in csv.DictReader(f)}
        if existing:
            logger.info("  resuming -- %d already labelled", len(existing))

    clusters: dict[int, dict] = defaultdict(lambda: {"docs": [], "xs": [], "ys": []})
    for doc, meta in all_data:
        cid = meta.get(level)
        if cid is None:
            continue
        cid = int(cid)
        clusters[cid]["docs"].append(doc)
        clusters[cid]["xs"].append(meta["umap_2d_x"])
        clusters[cid]["ys"].append(meta["umap_2d_y"])

    todo = sorted(c for c in clusters if c not in existing)
    logger.info("  %s: %d clusters, %d to label", level, len(clusters), len(todo))
    if not todo:
        return

    def build_row(cid: int) -> list:
        info = clusters[cid]
        size = len(info["docs"])
        cx = float(np.mean(info["xs"]))
        cy = float(np.mean(info["ys"]))
        kw = _extract_keywords(info["docs"])
        label = _label_cluster(client, model, kw, size)
        return [cid, label, "|".join(kw), size, f"{cx:.4f}", f"{cy:.4f}"]

    # Header only when the file is truly empty. Keying on `existing` would
    # re-emit the header if a prior run wrote the header but no data rows yet.
    write_header = not out_path.exists() or out_path.stat().st_size == 0
    lock = threading.Lock()
    with out_path.open("a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                ["cluster", "label", "keywords", "size", "centroid_x", "centroid_y"],
            )
            f.flush()
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            futures = [pool.submit(build_row, cid) for cid in todo]
            for fut in tqdm(as_completed(futures), total=len(todo), desc=f"  {level}"):
                row = fut.result()
                with lock:
                    writer.writerow(row)
                    f.flush()


def generate_labels(
    chroma_db_dir: Path,
    labels_dir: Path,
    vllm_base_url: str = DEFAULT_VLLM_BASE,
    model: str = DEFAULT_MODEL,
    prefix: str = "cluster_l",
    skip_levels: list[str] | None = None,
) -> None:
    client = OpenAI(base_url=vllm_base_url, api_key="unused")

    logger.info("loading data from chroma")
    all_data = _load_all_data(chroma_db_dir)
    logger.info("loaded %d documents", len(all_data))
    if not all_data:
        logger.warning("no documents found; nothing to label")
        return

    levels = _detect_levels(all_data[0][1], prefix)
    skip = set(skip_levels or ())
    if skip:
        levels = [lv for lv in levels if lv not in skip]
    logger.info("cluster levels (coarsest -> finest): %s", levels)

    labels_dir.mkdir(parents=True, exist_ok=True)
    for level in levels:
        _process_level(level, all_data, client, model, labels_dir)

    logger.info("labels written to %s", labels_dir)
