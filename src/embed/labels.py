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
import multiprocessing
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from openai import OpenAI
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm

from embed.common import fetch_collection, open_chroma
from embed.embed_constants import COLLECTIONS

logger = logging.getLogger(__name__)

DEFAULT_VLLM_BASE = "http://localhost:1729/v1"
DEFAULT_MODEL = "unsloth/Qwen3.6-27B-GGUF:UD-Q8_K_XL"

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
# Raise -c on the server before raising this.
BATCH_SIZE = 12
_MIN_DF_DOC_THRESHOLD = 5  # below this, don't require a term in >=2 docs
_TOKEN_PATTERN = r"(?u)\b[a-zA-Z]{2,}\b"  # noqa: S105
CONCURRENCY = 12
KW_WORKERS = 8
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
    """
    Clustering keys for prefix, ordered coarsest to finest.

    For EVoC (cluster_l) the flat cluster_id is prepended; other clusterings
    such as leiden_l expose hierarchical layers only.
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
    # tight clusters have near-identical docs, so max_df=0.9 can prune the whole
    # vocabulary; retry with no pruning before giving up
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


# llama.cpp and vLLM suppress this model's thinking via chat_template_kwargs;
# ollama's OpenAI layer ignores those and needs reasoning_effort=none, or
# content comes back empty
LLAMACPP_EXTRA = {
    "chat_template_kwargs": {"enable_thinking": False},
    "top_k": 20,
    "min_p": 0.0,
    "repetition_penalty": 1.0,
}
OLLAMA_EXTRA = {"reasoning_effort": "none"}


def _label_cluster(
    client: OpenAI, model: str, extra_body: dict, keywords: list[str], size: int
) -> str | None:
    """
    Return the cluster's label, or None if the LLM could not be reached.

    None means "unknown, try again": callers must skip the row rather than write
    a placeholder, since the resume logic treats a written row as done and would
    bake a transient 503 in as permanent bad data.
    """
    if not keywords:
        return "unknown"  # nothing cached to label from; a retry can't do better

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
                extra_body=extra_body,
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
            return None
    return None


def _label_row(
    client: OpenAI,
    model: str,
    extra_body: dict,
    cid: int,
    kw: list[str],
    size: int,
    cx: float,
    cy: float,
) -> list | None:
    label = _label_cluster(client, model, extra_body, kw, size)
    if label is None:
        return None  # unreachable LLM -> leave unwritten so a re-run retries it
    return [cid, label, "|".join(kw), size, f"{cx:.4f}", f"{cy:.4f}"]


BATCH_SYSTEM_PROMPT = (
    "You are a domain expert in genomics and biomedical research who names "
    "clusters of research datasets. For EACH numbered cluster you are given its "
    "most distinctive keywords (ranked by TF-IDF over the datasets' metadata). "
    "Reply with a specific 2-5 word label that names the organism, disease, "
    "tissue, assay/technique, pathway, or biological process distinguishing that "
    "cluster. Never use generic terms like 'Genomic Research', 'Biomedical "
    "Analysis', or 'Sequencing Studies'. Good labels look like: 'E. coli "
    "Antibiotic Resistance', 'Arabidopsis Epigenomics', 'Single-Cell RNA-seq "
    "Tumors', 'Gut Microbiome 16S'. Output EXACTLY one line per cluster as "
    "'<number>. <label>' using the same numbering as the input -- no skipped or "
    "merged clusters, no quotes, punctuation, explanation, or reasoning."
)

_BATCH_LINE = re.compile(r"\s*(\d+)[.)]\s*(.+)")


def _label_batch(
    client: OpenAI, model: str, extra_body: dict, batch: list[tuple]
) -> list[list]:
    """
    Label a whole batch in one request, one CSV row per item in input order.

    batch items are (cid, kw, size, cx, cy). Any cluster missing from the reply
    is re-labelled individually.
    """
    lines = []
    for i, (_cid, kw, size, _cx, _cy) in enumerate(batch):
        kwtext = ", ".join(kw) if kw else "(no keywords)"
        lines.append(f"{i + 1}. cluster of {size} datasets; keywords: {kwtext}")
    prompt = f"Label these {len(batch)} clusters:\n\n" + "\n".join(lines)

    parsed: dict[int, str] = {}
    backoff = INITIAL_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            r = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": BATCH_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.7,
                top_p=0.80,
                presence_penalty=1.5,
                max_tokens=24 * len(batch),
                extra_body=extra_body,
            )
            out = r.choices[0].message.content or ""
            out = re.sub(r"<think>.*?</think>", "", out, flags=re.DOTALL)
            for line in out.splitlines():
                m = _BATCH_LINE.match(line)
                if m:
                    n = int(m.group(1))
                    if 1 <= n <= len(batch):
                        parsed[n] = m.group(2).strip().strip('"').strip("'")
            break
        except Exception as exc:  # noqa: BLE001
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            tqdm.write(f"  batch error after {MAX_RETRIES} retries: {exc}")

    rows = []
    for i, (cid, kw, size, cx, cy) in enumerate(batch):
        label = parsed.get(i + 1)
        if not label:  # dropped/misaligned -> label this one on its own
            label = _label_cluster(client, model, extra_body, kw, size)
        if label is None:  # LLM unreachable; skip so a re-run retries this cluster
            continue
        rows.append([cid, label, "|".join(kw), size, f"{cx:.4f}", f"{cy:.4f}"])
    return rows


def _process_level(
    level: str,
    all_data: list[tuple[str, dict]],
    workers: list[tuple],
    out_dir: Path,
) -> None:
    out_path = out_dir / f"labels_{level}.csv"

    existing: set[int] = set()
    if out_path.exists():
        with out_path.open() as fh:
            existing = {int(row["cluster"]) for row in csv.DictReader(fh)}
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

    write_header = not out_path.exists() or out_path.stat().st_size == 0
    lock = threading.Lock()
    total_conc = sum(w[3] for w in workers)
    rotation = [i for i, w in enumerate(workers) for _ in range(w[3])]
    ctx = multiprocessing.get_context("spawn")
    with (
        out_path.open("a", newline="") as f,
        ProcessPoolExecutor(max_workers=KW_WORKERS, mp_context=ctx) as kpool,
        ThreadPoolExecutor(max_workers=total_conc) as lpool,
    ):
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                ["cluster", "label", "keywords", "size", "centroid_x", "centroid_y"],
            )
            f.flush()

        kw_futures = {
            kpool.submit(_extract_keywords, clusters[c]["docs"]): c for c in todo
        }
        llm_futures = []
        for rr, kf in enumerate(
            tqdm(as_completed(kw_futures), total=len(todo), desc=f"  {level} kw")
        ):
            cid = kw_futures[kf]
            try:
                kw = kf.result()
            except Exception:  # noqa: BLE001
                kw = []
            info = clusters[cid]
            client, model, extra_body, _ = workers[rotation[rr % len(rotation)]]
            llm_futures.append(
                lpool.submit(
                    _label_row,
                    client,
                    model,
                    extra_body,
                    cid,
                    kw,
                    len(info["docs"]),
                    float(np.mean(info["xs"])),
                    float(np.mean(info["ys"])),
                ),
            )

        for lf in tqdm(
            as_completed(llm_futures), total=len(llm_futures), desc=f"  {level} llm"
        ):
            row = lf.result()
            if row is None:  # LLM unreachable; leave unwritten so a re-run retries it
                continue
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
    endpoints: list[dict] | None = None,
) -> None:
    if endpoints is None:
        endpoints = [
            {
                "base_url": vllm_base_url,
                "model": model,
                "concurrency": CONCURRENCY,
                "extra_body": LLAMACPP_EXTRA,
            },
        ]
    # max_retries=0: the SDK's fast auto-retries stack on top of _label_cluster's
    # retry loop and snowball into a request storm under load, so it handles
    # backoff alone. The generous timeout keeps a slow generation from being
    # cancelled and retried, which only doubles server load.
    workers = [
        (
            OpenAI(
                base_url=e["base_url"], api_key="unused", max_retries=0, timeout=180.0
            ),
            e["model"],
            e.get("extra_body", {}),
            e["concurrency"],
        )
        for e in endpoints
    ]

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
        _process_level(level, all_data, workers, labels_dir)

    logger.info("labels written to %s", labels_dir)


def _precompute_level(
    level: str, all_data: list[tuple[str, dict]], out_dir: Path
) -> None:
    out_path = out_dir / f"keywords_{level}.csv"
    existing: set[int] = set()
    if out_path.exists():
        with out_path.open() as fh:
            existing = {int(r["cluster"]) for r in csv.DictReader(fh)}

    clusters: dict[int, dict] = defaultdict(lambda: {"docs": [], "xs": [], "ys": []})
    for doc, meta in all_data:
        cid = meta.get(level)
        if cid is None:
            continue
        cid = int(cid)
        clusters[cid]["docs"].append(doc)
        clusters[cid]["xs"].append(meta["umap_2d_x"])
        clusters[cid]["ys"].append(meta["umap_2d_y"])

    todo = [c for c in clusters if c not in existing]
    logger.info("  %s: %d clusters, %d to extract", level, len(clusters), len(todo))
    if not todo:
        return

    write_header = not out_path.exists() or out_path.stat().st_size == 0
    ctx = multiprocessing.get_context("spawn")
    with (
        out_path.open("a", newline="") as f,
        ProcessPoolExecutor(max_workers=KW_WORKERS, mp_context=ctx) as kpool,
    ):
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["cluster", "keywords", "size", "centroid_x", "centroid_y"])
            f.flush()
        kw_futures = {
            kpool.submit(_extract_keywords, clusters[c]["docs"]): c for c in todo
        }
        for kf in tqdm(as_completed(kw_futures), total=len(todo), desc=f"  kw {level}"):
            cid = kw_futures[kf]
            try:
                kw = kf.result()
            except Exception:  # noqa: BLE001
                kw = []
            info = clusters[cid]
            writer.writerow(
                [
                    cid,
                    "|".join(kw),
                    len(info["docs"]),
                    f"{float(np.mean(info['xs'])):.4f}",
                    f"{float(np.mean(info['ys'])):.4f}",
                ],
            )
            f.flush()


def precompute_keywords(
    chroma_db_dir: Path,
    keywords_dir: Path,
    prefix: str = "cluster_l",
    skip_levels: list[str] | None = None,
) -> None:
    """
    Cache TF-IDF keywords and centroids per cluster to keywords_<level>.csv.

    Run once; label_from_keywords() then needs no chroma, TF-IDF or doc reload.
    """
    logger.info("loading data from chroma")
    all_data = _load_all_data(chroma_db_dir)
    logger.info("loaded %d documents", len(all_data))
    if not all_data:
        logger.warning("no documents found; nothing to do")
        return
    levels = _detect_levels(all_data[0][1], prefix)
    skip = set(skip_levels or ())
    levels = [lv for lv in levels if lv not in skip]
    keywords_dir.mkdir(parents=True, exist_ok=True)
    for level in levels:
        _precompute_level(level, all_data, keywords_dir)
    logger.info("keywords cached to %s", keywords_dir)


def _label_level_from_cache(
    kw_path: Path, out_path: Path, workers: list[tuple]
) -> None:
    existing: set[int] = set()
    if out_path.exists():
        with out_path.open() as fh:
            existing = {int(r["cluster"]) for r in csv.DictReader(fh)}

    rows: list[tuple] = []
    with kw_path.open() as fh:
        for r in csv.DictReader(fh):
            cid = int(r["cluster"])
            if cid in existing:
                continue
            kw = r["keywords"].split("|") if r["keywords"] else []
            rows.append(
                (
                    cid,
                    kw,
                    int(r["size"]),
                    float(r["centroid_x"]),
                    float(r["centroid_y"]),
                )
            )
    logger.info("  %s: %d to label", out_path.stem, len(rows))
    if not rows:
        return

    total_conc = sum(w[3] for w in workers)
    rotation = [i for i, w in enumerate(workers) for _ in range(w[3])]
    batches = [rows[i : i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
    write_header = not out_path.exists() or out_path.stat().st_size == 0
    lock = threading.Lock()
    with (
        out_path.open("a", newline="") as f,
        ThreadPoolExecutor(max_workers=total_conc) as lpool,
    ):
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                ["cluster", "label", "keywords", "size", "centroid_x", "centroid_y"],
            )
            f.flush()
        futs = []
        for bi, batch in enumerate(batches):
            client, model, extra_body, _ = workers[rotation[bi % len(rotation)]]
            futs.append(lpool.submit(_label_batch, client, model, extra_body, batch))
        written = 0
        for lf in tqdm(
            as_completed(futs),
            total=len(futs),
            desc=f"  llm {out_path.stem} (x{BATCH_SIZE})",
        ):
            batch_rows = lf.result()
            if not batch_rows:
                continue
            written += len(batch_rows)
            with lock:
                writer.writerows(batch_rows)
                f.flush()
    skipped = len(rows) - written
    if skipped:
        logger.warning(
            "  %s: %d cluster(s) unreachable and left unlabelled -- re-run to retry",
            out_path.stem,
            skipped,
        )


def label_from_keywords(
    keywords_dir: Path,
    labels_dir: Path,
    endpoints: list[dict] | None = None,
    skip_levels: list[str] | None = None,
    vllm_base_url: str = DEFAULT_VLLM_BASE,
    model: str = DEFAULT_MODEL,
    concurrency: int = CONCURRENCY,
) -> None:
    """
    LLM-label from cached keywords_<level>.csv.

    Needs no chroma, TF-IDF or doc reload, so it starts in seconds and resumes
    cleanly.

    Defaults to the single llama.cpp server; pass endpoints, each
    {base_url, model, concurrency, extra_body}, to fan out across several
    backends or GPUs, weighted by concurrency.
    """
    if endpoints is None:
        endpoints = [
            {
                "base_url": vllm_base_url,
                "model": model,
                "concurrency": concurrency,
                "extra_body": LLAMACPP_EXTRA,
            },
        ]
    workers = [
        (
            OpenAI(
                base_url=e["base_url"], api_key="unused", max_retries=0, timeout=180.0
            ),
            e["model"],
            e.get("extra_body", {}),
            e["concurrency"],
        )
        for e in endpoints
    ]
    skip = set(skip_levels or ())
    labels_dir.mkdir(parents=True, exist_ok=True)

    def lvl_num(p: Path) -> int:
        digits = "".join(ch for ch in p.stem if ch.isdigit())
        return int(digits) if digits else 0

    caches = sorted(keywords_dir.glob("keywords_*.csv"), key=lvl_num, reverse=True)
    for kw_path in caches:
        level = kw_path.stem[len("keywords_") :]
        if level in skip:
            continue
        _label_level_from_cache(kw_path, labels_dir / f"labels_{level}.csv", workers)
    logger.info("labels written to %s", labels_dir)
