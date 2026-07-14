# Leiden clustering migration (replaced EVoC)

_Last updated 2026-07-15._

The map clustering was migrated from **EVoC** to **hierarchical Leiden**. This doc
records why, how the pipeline fits together, the operational gotchas, and what is
still open. It is written for whoever next touches clustering/labelling.

## Why we switched

EVoC (HDBSCAN-lineage, density-based) left **~60% of points as noise (-1)**. Noise
points belong to no cluster, so on the map they were unlabelled and, in Postgres, the
noise levels were dropped from each record's `clusters` jsonb entirely (ragged keys —
a record could carry only `cluster_l8`). The noise is *structural* to density
clustering, not a tuning problem; PCA / `noise_level` / normalization did not fix it.

**Leiden (graph community detection) assigns every point to a community → zero noise.**
Every accession now carries all levels, and cohesion/label quality validated well.

## Pipeline

### 1. Fine clustering — `src/embed/cluster_leiden.py` (`embed cluster-leiden`)

4096-d embeddings → cuML GPU PCA to 400-d (L2-normalized, ~80% variance) → cuML approx
kNN graph (k=15) → **recursive top-down Leiden** (`leidenalg`, RBConfiguration,
resolution 1.0). Needs a CUDA GPU + `igraph` + `leidenalg`.

Writes `leiden_l0` (finest) .. `leiden_l5` (coarsest = the split's **root**) into chroma
metadata. `leiden_l1` is a near-duplicate of `leiden_l0` and is dropped downstream.

Cluster counts: l0=70348, l2=42473, l3=15497, l4=3826, l5=626.

### 2. Coarser levels — `src/embed/cluster_leiden_coarse.py` (`embed cluster-leiden-coarse`)

The recursive split builds the hierarchy by *splitting the full graph*, so its coarsest
level **is** the root — there is nothing above it. To get coarser levels we
**agglomerate**: one centroid per root cluster → cosine kNN graph → Leiden → super
clusters, repeated. Resolution is **binary-searched per level** to hit a target count
(~3× coarser each step) because a fixed resolution collapses straight into the corpus's
dominant blob. Each new level is a union of the level below, so the hierarchy stays
nested and **labels already generated for finer levels stay valid**.

Adds `leiden_l6` (209), `leiden_l7` (70), `leiden_l8` (23).

### 3. Labelling — `src/embed/labels.py`

TF-IDF keywords (sklearn, alphabetic tokens only) + a local LLM (Qwen3.6-27B via
llama.cpp, OpenAI-compatible, `http://localhost:1729/v1`). **Two-phase**, so a restart
is instant:

- `embed precompute-keywords` → `keywords_<level>.csv` (reads chroma once, the slow part)
- `embed label-from-keywords` → `labels_<level>.csv` (no chroma / no TF-IDF / no doc
  reload; resumes from the CSV; fans out across endpoints if given several)

**Batched** (`BATCH_SIZE=12`): one request labels 12 clusters via a numbered prompt,
parsed back by number, with a per-cluster fallback for any the reply drops. ~7× faster
than one-at-a-time. Concurrency only needs to exceed the server's slot count; past that
the GPU is compute-bound and more concurrency/bigger batches buy nothing.

**Skip-don't-fabricate (important correctness fix):** on LLM failure `_label_cluster`
returns `None` and the writers **skip the row** so a re-run retries it. It used to write
`", ".join(keywords[:3])` as the label — which resume then treated as done, permanently
baking in garbage. A transient `503 Loading model` (server reloading mid-run) corrupted
**802 rows** this way before the fix. If you see labels like `'cells, ovarian, cancer'`
(lowercase, comma-joined keywords) in a CSV, those are the old fabricated fallbacks.

### 4. Postgres swap — `src/transfer/leiden_swap.py` (`transfer leiden-swap`)

Makes the apps read Leiden **without any frontend/backend code change**, by overwriting
what sits behind EVoC's `cluster_l*` names. `LEIDEN_TO_APP` remaps (finest→coarsest):

```
leiden_l0 -> cluster_l0 (70348)   leiden_l5 -> cluster_l4 (626, split root)
leiden_l2 -> cluster_l1 (42473)   leiden_l6 -> cluster_l5 (209)
leiden_l3 -> cluster_l2 (15497)   leiden_l7 -> cluster_l6 (70)
leiden_l4 -> cluster_l3 (3826)    leiden_l8 -> cluster_l7 (23)
                                  (leiden_l1 dropped)
```

Steps: preflight (all label CSVs exist, else raise before mutating) → snapshot
`cluster_labels` to `cluster_labels_backup_pre_leiden.csv` → `UPDATE ... SET clusters`
per accession (batched) across `geo_series` / `sra_studies` / `arrayexpress_experiments`
→ `DELETE cluster_l% FROM cluster_labels` and reinsert Leiden labels. Chroma is
read-only here.

Final state: **8 levels, 133,072 labels, 860,333 accessions, zero noise.**

### 5. Backend / frontend

- `seqout-server/main.py`: **`_COLOR_LAYER_COUNT` must equal the number of levels (now
  8).** It controls how many levels get baked as tile columns; `bake_levels =
  levels[:_COLOR_LAYER_COUNT]`. Set too low, the coarse levels appear in `/map/meta` but
  are never baked, and the frontend silently falls back to the coarsest baked level.
- Frontend is dynamic — `seqout-map.tsx` picks the coarsest level present in the tile
  columns (`[...meta.levels].reverse().find(l => tileCols.has(l))`). No hardcoded label
  constant. (There is **no** `LABEL_LAYER_MAX` in the map engine, despite old comments.)
- **After any swap: restart `seqout-server` (or `/cache/purge`)** — it caches
  `cluster_labels`, and a restart rebakes tiles with the new layer count.

## Operational gotchas

- **Chroma metadata write-back is pathological.** Adding 3 int keys to 860k records took
  **49 min and 244 GB of write I/O** (Chroma rewrites whole HNSW segments per metadata
  batch). The clustering math itself was **1.5 s**. If you add levels again, prefer
  writing them **straight to Postgres** and skip the chroma round-trip.
- **Shared box.** saket runs ollama on this machine; it spreads across GPU 0/1/2
  regardless of `CUDA_VISIBLE_DEVICES`, contends for GPU 0 with our llama server, and has
  periodically **restarted our llama server mid-run** (→ `503`, stalled labelling; the
  skip-don't-fabricate fix makes that safe to just re-run). Always scope process kills to
  our uid; never touch saket's processes.
- **Only one labelling writer at a time.** Two concurrent `run_leiden_labels.py`
  appending to the same CSV produced **4,152 duplicate rows** once. `pgrep ... | head -1`
  is unreliable for finding the job — it matches bash wrappers; check `comm == python`.
- **llama server** for labelling: `llama serve -hf
  unsloth/Qwen3.6-27B-GGUF:UD-Q8_K_XL --flash-attn on -ngl 999 --parallel 8 -c 16384
  --port 1729` (the **non**-MTP build; `-c/--parallel` split → keep ≥2048 tok/slot for
  batch-12). This is the only labelling model — do not swap it for a smaller one.
- **`.env`** (in `seqout-ingestion/`, gitignored) needs `PG_URL=` for the swap. The DB
  tunnel runs on `localhost:5433`, db `pysradb`. (An earlier `.env` had a `DB_HOST` typo
  written as `ST=`.)

## Known issue: lopsided coarse taxonomy

The coarse levels split at wildly inconsistent resolution because of embedding geometry.
At `cluster_l7` (23 clusters), **"Bulk RNA-seq Expression Profiles" = 405,258 datasets
(47% of the corpus)** — all of human/mouse biology (cancer, single-cell, ChIP-seq,
clinical, immunology) collapsed into one node — while environmental microbiology gets
~9 separate top-level nodes, some under 1,000 datasets. Individual labels are clean but
the tree is unbalanced, and one cluster ("Nasopharyngeal and Soil Microbiomes") is an
incoherent mashup. This is correct clustering of a genuinely dense RNA-seq mass, not a
bug.

**Possible fix (not done):** re-split *within* the RNA-seq blob only, at a much higher
resolution, so it breaks into human/mouse/cancer/single-cell/etc. while leaving the rest
alone. Cheap now — root-cluster centroids are cached, so it is minutes of compute.

## Recovery

EVoC is untouched in chroma (`cluster_l*`) and `labels_cluster_l*.csv`, so `just
transfer_embed` regenerates it. The pre-swap `cluster_labels` snapshot is at
`databases/labels/cluster_labels_backup_pre_leiden.csv`. Chroma metadata was backed up
to `databases/prod_metadata_backup_pre_leiden.pkl` before the first Leiden write.

## Status (2026-07-15)

Done: all 8 levels clustered, labelled, and swapped into Postgres; verified (0 missing /
dupes / fabricated). Repo changes are **uncommitted**. **Open:** restart
`seqout-server` to rebake 8-layer tiles; optionally re-split the RNA-seq blob.
