# seqout-ingestion

Ingestion toolkit for the Seqout platform — ingests metadata from SRA, GEO, ENA, ArrayExpress, and GSA (CNCB-NGDC, China) into SQLite, unifies it, enriches it, and transfers it to Postgres/Parquet.

## Pipeline

```
                     ┌─ backfill ──┐
SRA ───────────────┐ │  pm-ids     │
GEO ───────────────┤ │  pm-meta    ├──► unify ──► sc_enrich ──► transfer ──► Postgres
ENA ───────────────┤ │  download-  │    build-     classify     postgres      Parquet
AE  ───────────────┤ │  links      │    catalog    counter      parquet
GSA ───────────────┤ └─────────────┘    build-                  embed
GSA-Human ─────────┘                    metadata
                           │
                           └──► geocode
```

GSA has two portals: open (`gsa`, CRA studies) and human (`gsa-human`, HRA studies
from which only open-access studies are fetched; controlled-access ones keep a study row but no
detail). Both are ENA-style: one metadata workbook downloaded per study to disk
(resumable), then bulk-loaded into a per-source SQLite DB.

Embed pipeline (requires CUDA tox env):
```
embed run ──► coords / cluster / neighbors / labels ──► transfer embed ──► Postgres
```

## Quick start

```bash
# Ingest metadata from all sources
just ingest_sra --start-date 2026-06-01 --end-date 2026-06-30
just ingest_geo --start-date 2026-06-01 --end-date 2026-06-30
just ingest_ena --start-date 2026-06-01 --end-date 2026-06-30
just ingest_ae  --start-date 2026-06-01 --end-date 2026-06-30
just ingest_gsa                                    # open GSA (CRA); no dates = full archive
just ingest_gsa_human                              # GSA-Human (HRA), open-access studies

# Backfill supplementary data
just backfill_pm_ids
just backfill_pm_meta
just backfill_download_links

# Build unified catalog and metadata
just unify_build_catalog
just unify_build_metadata

# Geocode center names
just geocode

# Classify and count single-cell studies
just sc_enrich_classify
just sc_enrich_counter

# Export to Postgres or Parquet
just transfer_postgres
just transfer_parquet
```

Or run directly without `just`:

```bash
uv run python src/main.py --log-level info ingest sra \
  --output-dir /data/share/seqout \
  --start-date 2026-06-01 --end-date 2026-06-30
```

## Setup

```bash
# Clone
git clone https://github.com/0xmukesh/seqout-ingestion-v2.git
cd seqout-ingestion-v2

# Install with uv (Python 3.13+)
uv sync

# Install dev dependencies (linting, pre-commit)
uv sync --group dev

# Install git hooks (runs ruff on every commit)
uv run pre-commit install

# Validate
uv run ruff check src/
```

### just

Recipes in the [`justfile`](./justfile) provide convenient shortcuts with sensible defaults.
Install `just` if you don't have it:

```bash
# Linux (x86_64)
curl -fsSL https://just.systems/install.sh | bash

# macOS
brew install just
# Other methods: https://github.com/casey/just#installation
```

List available recipes:

```bash
just
```

### Environment variables

Create a `.env` file in the project root to configure defaults. The `justfile` loads it automatically on every run.

```bash
DATA_DIR=/data/share/seqout
PG_URL=postgresql://user:pass@host:5432/seqout
LOG_LEVEL=info
RETRIES=20
WORKERS=8
NCBI_KEYS_FILE=/data/share/seqout/keys/ncbi.txt
OPENALEX_KEYS_FILE=/data/share/seqout/keys/openalex.txt
CHROMA_DB_DIR=/data/share/seqout/embed/chroma_db
NEIGHBORS_DB_FILE=/data/share/seqout/embed/neighbors.sqlite3
LABELS_DIR=/data/share/seqout/embed/labels
```

| Variable | Default | Description |
|---|---|---|
| `DATA_DIR` | `/data/share/seqout` | Base directory for all data |
| `PG_URL` | — | PostgreSQL connection string |
| `LOG_LEVEL` | `info` | Log verbosity |
| `START_DATE` / `END_DATE` | — | Date range for ingest |
| `RETRIES` | `20` | HTTP retries |
| `WORKERS` | `8` | Parallel workers |
| `BATCH_SIZE` | `500` | DB batch size |
| `TIMEOUT` | `30` | HTTP timeout (seconds) |
| `MAX_TIMEOUT` | `120` | Max HTTP timeout (seconds) |
| `NCBI_KEYS_FILE` | `$DATA_DIR/keys/ncbi.txt` | NCBI API keys |
| `OPENALEX_KEYS_FILE` | `$DATA_DIR/keys/openalex.txt` | OpenAlex API key |
| `CHROMA_DB_DIR` | `$DATA_DIR/embed/chroma_db` | Chroma db directory (embed pipeline) |
| `NEIGHBORS_DB_FILE` | `$DATA_DIR/embed/neighbors.sqlite3` | Neighbors database (embed pipeline) |
| `LABELS_DIR` | `$DATA_DIR/embed/labels` | Cluster labels directory (embed pipeline) |

### CUDA environment (optional, for Embed pipeline)

```bash
tox run -e cuda
```

Creates a Python 3.12 venv with `torch`, `cuml-cu13`, `chromadb`, `sentence-transformers`, and other ML dependencies.

## Commands

### Global options

| Option | Default | Description |
|---|---|---|
| `--log-level` | `info` | debug, info, warning, error |
| `--log-file` | — | Redirect logs to file |

### `ingest`

Fetch study metadata from public archives.

| Command | Source | Key args |
|---|---|---|
| `ingest sra` | NCBI SRA | `--start-date`, `--end-date`, `--write-batch-size` |
| `ingest geo` | NCBI GEO | `--start-date`, `--end-date`, `--series-limit` |
| `ingest ena` | EBI ENA | `--start-date`, `--end-date`, `--skip-experiments`, `--skip-samples` |
| `ingest ae` | EBI ArrayExpress | `--start-date`, `--end-date` |

All accept `--workers`, `--chunk-size`, `--retries`, `--timeout`, `--max-timeout`.

### `backfill`

Add supplementary data to ingested databases.

| Command | Description | Key args |
|---|---|---|
| `backfill pm-ids` | Backfill PubMed IDs from NCBI | `--ncbi-keys-file`, `--process-all` |
| `backfill pm-meta` | Backfill PubMed metadata (citations, journals) | `--ncbi-keys-file`, `--openalex-keys-file` |
| `backfill download-links` | Backfill file download links | `--source` (ncbi, ena, both) |

### `unify`

Merge source databases into unified catalogs.

| Command | Description |
|---|---|
| `unify build-catalog` | Build cross-source accession alias catalog |
| `unify build-metadata` | Build unified metadata with deduplication and enrichment |

### `geocode`

| Command | Description | Key args |
|---|---|---|
| `geocode` | Geocode center names via ROR / ArcGIS / Nominatim | `--ror-data-file`, `--cache-file`, `--rate-limit` |

### `sc_enrich`

Identify and quantify single-cell studies.

| Command | Description | Key args |
|---|---|---|
| `sc_enrich classify` | Classify studies as single-cell by title/description/assay | `--is-dry-run` |
| `sc_enrich counter` | Download files and count cells per study | `--check` (dry-run mode) |

### `embed`

Requires the CUDA tox environment. Embed accessions, compute coordinates/clusters/neighbors, and generate labels.

| Command | Description | Key args |
|---|---|---|
| `embed run` | Embed new accessions into chroma (SentenceTransformer F2LLM) | `--chroma-db-dir`, `--model-name` |
| `embed coords` | Compute UMAP 2D/3D coordinates (cuML) | `--chroma-db-dir` |
| `embed cluster` | Hierarchical clustering (EVoC) | `--chroma-db-dir` |
| `embed neighbors` | KNN cosine neighbors | `--chroma-db-dir`, `--neighbors-db-file` |
| `embed labels` | LLM-based cluster labeling via vLLM | `--chroma-db-dir`, `--labels-dir`, `--vllm-base-url` |

### `transfer`

Export to downstream formats.

| Command | Description | Key args |
|---|---|---|
| `transfer postgres` | SQLite → PostgreSQL (idempotent upserts) | `--pg-url` |
| `transfer parquet` | SQLite → Parquet files | `--output-dir` |
| `transfer embed` | Push embedding columns from chroma to Postgres | `--chroma-db-dir`, `--neighbors-db-file`, `--labels-dir`, `--start-from` |

## Embed / Embed pipeline

Requires the CUDA tox environment. All embed commands are wired into the main CLI and have `just` recipes:

```bash
# Ingest new accessions, compute coordinates, cluster, neighbors, labels
just embed_run
just embed_coords
just embed_cluster
just embed_neighbors
just embed_labels

# Push results to Postgres
just transfer_embed
```

Or run directly:

```bash
tox run -e cuda -- uv run python src/main.py embed run \
  --geo-db-file <path> --sra-db-file <path> \
  --ae-db-file <path> --unified-metadata-db-file <path> \
  --chroma-db-dir <path>
```

Pipeline steps:

1. **`embed run`** — Embed new accessions into chroma
2. **`embed coords`** — Compute UMAP 2D/3D coordinates
3. **`embed cluster`** — Hierarchical clustering
4. **`embed neighbors`** — KNN cosine neighbors
5. **`embed labels`** — LLM-based cluster labeling
6. **`transfer embed`** — Push results to Postgres
