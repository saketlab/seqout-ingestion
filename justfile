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

UNIFIED_CATALOG_DB_FILE := env("UNIFIED_CATALOG_DB_FILE", DATA_DIR + "/unified_catalog/unified_catalog.db")
UNIFIED_METADATA_DB_FILE := env("UNIFIED_METADATA_DB_FILE", DATA_DIR + "/unified_metadata/unified_metadata.db")
DOWNLOAD_LINKS_DB_FILE := env("DOWNLOAD_LINKS_DB_FILE", DATA_DIR + "/download_links/download_links.db")
PUBMED_METADATA_DB_FILE := env("PUBMED_METADATA_DB_FILE", DATA_DIR + "/pubmed_metadata/pubmed_metadata.db")
CHROMA_DB_DIR := env("CHROMA_DB_DIR", DATA_DIR + "/embed/chroma_db")
NEIGHBORS_DB_FILE := env("NEIGHBORS_DB_FILE", DATA_DIR + "/embed/neighbors.sqlite3")
LABELS_DIR := env("LABELS_DIR", DATA_DIR + "/embed/labels")

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

[group("backfill")]
backfill_pm_ids *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-ids \
        {{ _all_db_args }} \
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
        {{ flags }}

[group("unify")]
unify_build_metadata *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} unify build-metadata \
        --output-dir {{ DATA_DIR }} \
        {{ _all_db_args }} \
        {{ _gsa_db_args }} \
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

[group("transfer")]
transfer_parquet *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer parquet \
        {{ _transfer_args }} \
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

[group("transfer")]
transfer_embed *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer embed \
        --pg-url "$PG_URL" \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --neighbors-db-file {{ NEIGHBORS_DB_FILE }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}
