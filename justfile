set dotenv-load

export PYTHON_GIL := "0"
export DATA_DIR := env("DATA_DIR", "/data/share/seqout")

LOG_LEVEL := env("LOG_LEVEL", "info")
START_DATE := env("START_DATE", "2026-04-28")
END_DATE := env("END_DATE", "2026-05-31")
RETRIES := env("RETRIES", "20")
WORKERS := env("WORKERS", "8")
BATCH_SIZE := env("BATCH_SIZE", "500")
TIMEOUT := env("TIMEOUT", "30")
MAX_TIMEOUT := env("MAX_TIMEOUT", "120")

SRA_DB_FILE := env("SRA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/sra/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/sra/db/$latest/sra_metadata_$latest.db\""))
GEO_DB_FILE := env("GEO_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/geo/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/geo/db/$latest/geo_metadata_$latest.db\""))
ENA_DB_FILE := env("ENA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/ena/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/ena/db/$latest/ena_metadata_$latest.db\""))
AE_DB_FILE := env("AE_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/ae/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/ae/db/$latest/ae_metadata_$latest.db\""))
GSA_DB_FILE := env("GSA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/gsa/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/gsa/db/$latest/gsa_metadata_$latest.db\""))
GSA_HUMAN_DB_FILE := env("GSA_HUMAN_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/gsa_human/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/gsa_human/db/$latest/gsa_human_metadata_$latest.db\""))
# tolerant of a missing dir: these do not exist until the first ingest
DRA_DB_FILE := env("DRA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/dra/db/ 2>/dev/null | sort -r | head -1); if [ -n \"$latest\" ]; then echo \"" + DATA_DIR + "/dra/db/$latest/dra_metadata_$latest.db\"; fi"))
GEA_DB_FILE := env("GEA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/gea/db/ 2>/dev/null | sort -r | head -1); if [ -n \"$latest\" ]; then echo \"" + DATA_DIR + "/gea/db/$latest/gea_metadata_$latest.db\"; fi"))

AGE_SOURCE_DB_FILE := env("AGE_SOURCE_DB_FILE", "/data/share/saket_dump/enrichment/enriched_metadata_v4_combined.sqlite")
AGE_NORM_DB_FILE := env("AGE_NORM_DB_FILE", DATA_DIR + "/age/age_norm.sqlite3")

UNIFIED_CATALOG_DB_FILE := env("UNIFIED_CATALOG_DB_FILE", DATA_DIR + "/unified_catalog/unified_catalog.db")
UNIFIED_METADATA_DB_FILE := env("UNIFIED_METADATA_DB_FILE", DATA_DIR + "/unified_metadata/unified_metadata.db")
DOWNLOAD_LINKS_DB_FILE := env("DOWNLOAD_LINKS_DB_FILE", DATA_DIR + "/download_links/download_links.db")
PUBMED_METADATA_DB_FILE := env("PUBMED_METADATA_DB_FILE", DATA_DIR + "/pubmed_metadata/pubmed_metadata.db")
CHROMA_DB_DIR := env("CHROMA_DB_DIR", DATA_DIR + "/embed/chroma_db")
NEIGHBORS_DB_FILE := env("NEIGHBORS_DB_FILE", DATA_DIR + "/embed/neighbors.sqlite3")
LABELS_DIR := env("LABELS_DIR", DATA_DIR + "/embed/labels")
KEYWORDS_DIR := env("KEYWORDS_DIR", DATA_DIR + "/embed/keywords")

NCBI_KEYS_FILE := env("NCBI_KEYS_FILE", DATA_DIR + "/keys/ncbi.txt")
OPENALEX_KEYS_FILE := env("OPENALEX_KEYS_FILE", DATA_DIR + "/keys/openalex.txt")
ASSAY_MAPPING_FILE := env("ASSAY_MAPPING_FILE", DATA_DIR + "/data/assay_mapping.csv")
TAXONOMY_MAPPING_FILE := env("TAXONOMY_MAPPING_FILE", DATA_DIR + "/data/ncbi_taxonomy_common_names.csv")
ROR_DATA := env("ROR_DATA", "/data/ror_data.json")
GEOCODING_CACHE_FILE := env("GEOCODING_CACHE_FILE", DATA_DIR + "/geocoding/geocoding_cache.json")

PYTHON := ".venv/bin/python"
ENTRYPOINT := "./src/main.py --log-level " + LOG_LEVEL

default:
    @just --list

[private]
_ingest_args := " --output-dir " + DATA_DIR + " --start-date " + START_DATE + " --end-date " + END_DATE + " --workers " + WORKERS + " --retries " + RETRIES
[private]
_all_db_args := "--sra-db-file " + SRA_DB_FILE + " --geo-db-file " + GEO_DB_FILE + " --ena-db-file " + ENA_DB_FILE + " --ae-db-file " + AE_DB_FILE + " "
[private]
_gsa_db_args := "--gsa-db-file " + GSA_DB_FILE + " --gsa-human-db-file " + GSA_HUMAN_DB_FILE + " "
[private]
_ddbj_db_args := "--dra-db-file " + DRA_DB_FILE + " --gea-db-file " + GEA_DB_FILE + " "
[private]
_transfer_args := _all_db_args + " --pubmed-metadata-db-file " + PUBMED_METADATA_DB_FILE + " --unified-metadata-db-file " + UNIFIED_METADATA_DB_FILE + " --download-links-db-file " + DOWNLOAD_LINKS_DB_FILE + " --read-batch-size " + BATCH_SIZE + " --workers " + WORKERS + " "
[private]
_embed_db_args := "--geo-db-file " + GEO_DB_FILE + " --sra-db-file " + SRA_DB_FILE + " --ae-db-file " + AE_DB_FILE + " --unified-metadata-db-file " + UNIFIED_METADATA_DB_FILE + " --chroma-db-dir " + CHROMA_DB_DIR + " "
[private]
_tox_cuda := "tox run -e cuda -- "

[group("ingest")]
ingest_sra *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest sra \
        {{ _ingest_args }} \
        --write-batch-size {{ BATCH_SIZE }} \
        {{ flags }}

# DDBJ DRA. Ingests every public study in the window and stamps is_only_ddbj=1 on those
# SRA/ENA hold no mirror of (checked against the sqlite dumps; --pg-url uses postgres).
#   just ingest_dra --novelty-filter   # ingest only the DDBJ-exclusive set
#   just ingest_dra --dry-run          # show the split, download nothing
ingest_dra *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest dra \
        {{ _ingest_args }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        --ena-db-file {{ ENA_DB_FILE }} \
        {{ flags }}

# Full DDBJ DRA history -- every public study ever released (~18.8k). Long-running.
ingest_dra_all *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest dra \
        --output-dir {{ DATA_DIR }} \
        --start-date 2007-01-01 \
        --end-date $(date -u +%F) \
        --workers {{ WORKERS }} \
        --retries {{ RETRIES }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        --ena-db-file {{ ENA_DB_FILE }} \
        {{ flags }}

# DDBJ GEA (expression). No novelty filter -- nothing in GEA is mirrored into
# GEO or ArrayExpress, so all ~735 public experiments are ingested.
ingest_gea *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gea \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_geo *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest geo \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_ena *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest ena \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_ae *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest ae \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_gsa *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gsa \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_gsa_human *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gsa-human \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_gsa_bioproject *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gsa-bioproject \
        --output-dir {{ DATA_DIR }} --workers {{ WORKERS }} --retries {{ RETRIES }} \
        {{ _gsa_db_args }} \
        {{ flags }}

[group("backfill")]
backfill_download_links *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill download-links \
        --output-dir {{ DATA_DIR }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        --ena-db-file {{ ENA_DB_FILE }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --workers {{ WORKERS }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

# fastq links for the novel DRA runs, from DDBJ's own fastqlist manifest
# (ena has never seen them, so backfill_download_links cannot find their fastqs)
[group("backfill")]
backfill_dra_download_links *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill dra-download-links \
        --output-dir {{ DATA_DIR }} \
        --dra-db-file {{ DRA_DB_FILE }} \
        --download-links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --workers {{ WORKERS }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

# Everything DDBJ needs between ingest and transfer, in order:
#   1. pm-ids              -- DRP -> pmid, via the bioproject (no SRA/ENA mirror to elink)
#   2. pm-meta             -- those pmids -> pubmed_metadata + citation counts
#   3. dra-download-links  -- fastq urls from DDBJ's own manifest
[group("backfill")]
backfill_ddbj *flags:
    just backfill_pm_ids {{ flags }}
    just backfill_pm_meta --fill-metrics {{ flags }}
    just backfill_dra_download_links {{ flags }}

[group("backfill")]
backfill_pm_ids *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-ids \
        {{ _all_db_args }} \
        {{ _ddbj_db_args }} \
        {{ _gsa_db_args }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

[group("backfill")]
backfill_citation_pmids *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill citation-pmids \
        --geo-db-file {{ GEO_DB_FILE }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

[group("backfill")]
backfill_pm_meta *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-meta \
        --output-dir {{ DATA_DIR }} \
        {{ _all_db_args }} \
        {{ _ddbj_db_args }} \
        {{ _gsa_db_args }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --openalex-keys-file {{ OPENALEX_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

[group("unify")]
unify_build_catalog *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} unify build-catalog \
        --output-dir {{ DATA_DIR }} \
        {{ _all_db_args }} \
        {{ _gsa_db_args }} \
        {{ _ddbj_db_args }} \
        {{ flags }}

[group("unify")]
unify_build_metadata *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} unify build-metadata \
        --output-dir {{ DATA_DIR }} \
        {{ _all_db_args }} \
        {{ _gsa_db_args }} \
        {{ _ddbj_db_args }} \
        --unified-catalog-db-file {{ UNIFIED_CATALOG_DB_FILE }} \
        --pubmed-metadata-db-file {{ PUBMED_METADATA_DB_FILE }} \
        --assay-mapping-csv-file {{ ASSAY_MAPPING_FILE }} \
        --taxonomy-mapping-csv-file {{ TAXONOMY_MAPPING_FILE }} \
        {{ flags }}

[group("geocode")]
geocode *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} geocode \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --geo-db-file {{ GEO_DB_FILE }} \
        --ror-data-file {{ ROR_DATA }} \
        --cache-file {{ GEOCODING_CACHE_FILE }} \
        --save-every {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        {{ flags }}

[group("sc_enrich")]
sc_enrich_classify *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich classify \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        {{ flags }}

[group("sc_enrich")]
sc_enrich_yuj_setup *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich counter-setup \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --geo-db-file {{ GEO_DB_FILE }} \
        --ae-db-file {{ AE_DB_FILE }} \
        --output-dir {{ DATA_DIR }}/yuj_setup \
        --machines-csv-file {{ DATA_DIR }}/yuj_machines.csv \
        {{ flags }}

[group("transfer")]
transfer_postgres *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer postgres \
        --pg-url "$PG_URL" \
        {{ _transfer_args }} \
        {{ _gsa_db_args }} \
        {{ flags }}

# Project the pentimento SQLite into the pentimento_* pg tables the API serves.
# Idempotent -- a rerun over unchanged data is a no-op.
[group("pentimento")]
transfer_pentimento *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer pentimento \
        --pg-url "$PG_URL" \
        --pentimento-db-file {{ PENTIMENTO_DB_FILE }} \
        {{ flags }}

[group("transfer")]
transfer_parquet *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer parquet \
        {{ _transfer_args }} \
        {{ flags }}

# Rebuild the Neo4j ontology graph from scratch. Rarely run -- see docs/ontology_graph.md.
# Needs $NEO4J_PASSWORD, and $NEO4J_URI pointing at the server (prod is behind an SSH tunnel).
[group("ontology")]
ontology_build *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology build \
        --output-dir {{ DATA_DIR }} \
        {{ flags }}

# The stages of ontology_build, for resuming a run that died partway.
[group("ontology")]
ontology_download *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology download --output-dir {{ DATA_DIR }} {{ flags }}

[group("ontology")]
ontology_extract *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology extract --output-dir {{ DATA_DIR }} {{ flags }}

[group("ontology")]
ontology_load *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology load --output-dir {{ DATA_DIR }} {{ flags }}

# Refresh the has_children flag the deep-dive explorer reads. Run after any load.
[group("ontology")]
ontology_has_children *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology has-children {{ flags }}

[group("ontology")]
ontology_stats *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology stats {{ flags }}

[group("age")]
age_normalize *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} age normalize \
        --source-db-file {{ AGE_SOURCE_DB_FILE }} \
        --output-dir {{ DATA_DIR }} \
        --batch-size {{ BATCH_SIZE }} \
        {{ flags }}

[group("transfer")]
transfer_age *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer age \
        --pg-url "$PG_URL" \
        --age-norm-db-file {{ AGE_NORM_DB_FILE }} \
        {{ flags }}

[group("embed")]
embed_run *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed run \
        {{ _embed_db_args }} \
        {{ flags }}

[group("embed")]
embed_coords *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed coords \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

[group("embed")]
embed_cluster *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed cluster \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

[group("embed")]
embed_cluster_leiden *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed cluster-leiden \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

[group("embed")]
embed_neighbors *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed neighbors \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --neighbors-db-file {{ NEIGHBORS_DB_FILE }} \
        {{ flags }}

[group("embed")]
embed_labels *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed labels \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

# Add levels coarser than the Leiden root (agglomerates it; nests over existing levels)
[group("embed")]
embed_cluster_leiden_coarse *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed cluster-leiden-coarse \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

# Phase 1 of labelling (run once): cache TF-IDF keywords. Reads chroma; the slow part.
[group("embed")]
embed_precompute_keywords *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed precompute-keywords \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --keywords-dir {{ KEYWORDS_DIR }} \
        {{ flags }}

# Phase 2 of labelling (repeatable): LLM-label from the cache. No chroma, resumes cleanly.
[group("embed")]
embed_label_from_keywords *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} embed label-from-keywords \
        --keywords-dir {{ KEYWORDS_DIR }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

[group("transfer")]
transfer_embed *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer embed \
        --pg-url "$PG_URL" \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --neighbors-db-file {{ NEIGHBORS_DB_FILE }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

# Overwrite EVoC with Leiden in Postgres so the apps adopt it (needs $PG_URL tunnel)
[group("transfer")]
transfer_leiden_swap *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer leiden-swap \
        --pg-url "$PG_URL" \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

# Pentimento: one SQLite joining cell counts + preflightx + fastq links + microbes
PENTIMENTO_DB_FILE := env("PENTIMENTO_DB_FILE", DATA_DIR + "/pentimento/pentimento.db")
NCOUNT_PERSAMPLE_DIR := env("NCOUNT_PERSAMPLE_DIR", "/data/share/ncount_persample_July14")
PREFLIGHTX_DB_FILE := env("PREFLIGHTX_DB_FILE", "/home/saket/enrichment/enriched_metadata_v4_combined_July14.sqlite")
PREFLIGHTX_CSV_DIR := env("PREFLIGHTX_CSV_DIR", "/NAS/saket/github/pysradb-ingestion/sc_preflight_v4/annotated")
VIRUS_QUANT_DIR := env("VIRUS_QUANT_DIR", "/home/saket/tissue_pull")
PREFLIGHTX_BENCHMARK_FILE := env("PREFLIGHTX_BENCHMARK_FILE", "/NAS/saket/github/pysradb-ingestion/mukesh_single_cell_preflightx/preflightx_results/preflight_per_experiment.csv")

[group("pentimento")]
pentimento_build *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento build \
        --output-db-file {{ PENTIMENTO_DB_FILE }} \
        --links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --ncount-dir {{ NCOUNT_PERSAMPLE_DIR }} \
        --preflightx-db-file {{ PREFLIGHTX_DB_FILE }} \
        --preflightx-csv-dir {{ PREFLIGHTX_CSV_DIR }} \
        --quant-dir {{ VIRUS_QUANT_DIR }} \
        --benchmark-file {{ PREFLIGHTX_BENCHMARK_FILE }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        {{ flags }}

# Running preflight ourselves. Three stages, each separately resumable: pick the
# studies, scan the reads (network-bound, scatter this one across the fleet with
# yuj), load the result. `preflight_ingest` needs NO quant and NO enrichment DB --
# that is what makes a fresh scan usable on its own.
PREFLIGHT_MANIFEST_DIR := env("PREFLIGHT_MANIFEST_DIR", DATA_DIR + "/pentimento/preflight/manifests")
PREFLIGHT_ANNOTATED_DIR := env("PREFLIGHT_ANNOTATED_DIR", DATA_DIR + "/pentimento/preflight/annotated")

[group("pentimento")]
preflight_manifest *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight manifest \
        --out-dir {{ PREFLIGHT_MANIFEST_DIR }} \
        --links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --unified-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        {{ flags }}

# Streams remote FASTQ; hours, not minutes. Pass --in-dir to run one host's slice.
[group("pentimento")]
preflight_run *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight run \
        --in-dir {{ PREFLIGHT_MANIFEST_DIR }} \
        --out-dir {{ PREFLIGHT_ANNOTATED_DIR }} \
        {{ flags }}

[group("pentimento")]
preflight_ingest *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight ingest \
        --db-file {{ PENTIMENTO_DB_FILE }} \
        --preflightx-csv-dir {{ PREFLIGHT_ANNOTATED_DIR }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        {{ flags }}

PENTIMENTO_PARQUET_DIR := env("PENTIMENTO_PARQUET_DIR", DATA_DIR + "/pentimento/parquet")

# Mirror the Pentimento tables to parquet (one file per table). Pass --resume to
# continue a killed export; finished files are complete by construction.
[group("pentimento")]
pentimento_parquet *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento export-parquet \
        --db-file {{ PENTIMENTO_DB_FILE }} \
        --output-dir {{ PENTIMENTO_PARQUET_DIR }} \
        {{ flags }}
