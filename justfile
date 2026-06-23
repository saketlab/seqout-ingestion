set dotenv-load

export PYTHON_GIL := "0"
export DATA_DIR := env("DATA_DIR", "/data/share/seqout_may")

LOG_LEVEL := env("LOG_LEVEL", "info")
START_DATE := env("START_DATE", "2026-04-28")
END_DATE := env("END_DATE", "2026-05-31")
RETRIES := env("RETRIES", "20")
WORKERS := env("WORKERS", "10")
BATCH_SIZE := env("BATCH_SIZE", "500")
RATELIMIT := env("RATELIMIT", "3")

PYTHON := ".venv/bin/python"
ENTRYPOINT := "./src/main.py --log-level " + LOG_LEVEL

NCBI_KEYS_FILE := DATA_DIR + "/_keys/ncbi.txt"
OPENALEX_KEYS_FILE := DATA_DIR + "/_keys/openalex.txt"
ASSAY_MAPPING_FILE := DATA_DIR + "/_csv/assay_mapping.csv"
TAXONOMY_MAPPING_FILE := DATA_DIR + "/_csv/ncbi_taxonomy_common_names.csv"
GEOCODING_CACHE_FILE := DATA_DIR + "/geocoding/geocode_cache.json"
UNIFIED_METADATA_DB := DATA_DIR + "/unified_metadata/unified_metadata.db"
UNIFIED_CATALOG_DB := DATA_DIR + "/unified_catalog/unified_catalog.db"
PUBMED_METADATA_DB := DATA_DIR + "/pubmed_meta/pubmed_meta.db"
DOWNLOAD_LINKS_DB := DATA_DIR + "/download_links/download_links.db"

_latest_db source:
    @latest=$(ls {{ DATA_DIR }}/{{ source }}/db/ | sort -r | head -1) && \
    echo "{{ DATA_DIR }}/{{ source }}/db/$latest/{{ source }}_metadata_$latest.db"

ingest_sra:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest sra \
        --output-dir {{ DATA_DIR }} \
        --start-date {{ START_DATE }} \
        --end-date {{ END_DATE }} \
        --workers {{ WORKERS }} \
        --write-batch-size {{ BATCH_SIZE }} \

ingest_geo:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest geo \
        --output-dir {{ DATA_DIR }} \
        --start-date {{ START_DATE }} \
        --end-date {{ END_DATE }} \
        --workers {{ WORKERS }} \
        --retries {{ RETRIES }}

ingest_ena:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest ena \
        --output-dir {{ DATA_DIR }} \
        --start-date {{ START_DATE }} \
        --end-date {{ END_DATE }} \
        --workers {{ WORKERS }} \
        --retries {{ RETRIES }}

ingest_ae:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest ae \
        --output-dir {{ DATA_DIR }} \
        --start-date {{ START_DATE }} \
        --end-date {{ END_DATE }} \
        --workers {{ WORKERS }} \
        --retries {{ RETRIES }}

backfill_download_links:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill download-links \
        --output-dir {{ DATA_DIR }} \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --source ncbi \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --workers {{ WORKERS }}

backfill_pm_ids:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-ids \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \

backfill_pm_meta:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-meta \
        --output-dir {{ DATA_DIR }} \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --openalex-keys-file {{ OPENALEX_KEYS_FILE }} \
        --update-journal-metrics \
        --refresh-all-journals-metrics \
        --batch-size {{ BATCH_SIZE }} \

unify_build_catalog:
    {{ PYTHON }} {{ ENTRYPOINT }} unify build-catalog \
        --output-dir {{ DATA_DIR }} \
        --sra-db-file $(just _latest_db sra) \
        --geo-db-file $(just _latest_db geo) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \

unify_build_metadata:
    {{ PYTHON }} {{ ENTRYPOINT }} unify build-metadata \
        --output-dir {{ DATA_DIR }} \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --unified-catalog-db-file {{ UNIFIED_CATALOG_DB }} \
        --pubmed-metadata-db-file {{ PUBMED_METADATA_DB }} \
        --assay-mapping-csv-file {{ ASSAY_MAPPING_FILE }} \
        --taxonomy-mapping-csv-file {{ TAXONOMY_MAPPING_FILE }}

geocode_missing:
    {{ PYTHON }} {{ ENTRYPOINT }} geocode \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB }} \
        --geo-db-file $(just _latest_db geo) \
        --cache-file {{ GEOCODING_CACHE_FILE }} \
        --save-every {{ BATCH_SIZE }} \
        --rate-limit {{ RATELIMIT }} \
        --retries {{ RETRIES }}

sc_enrich_regex:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich regex \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB }}

sc_enrich_detector:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich detector \
        --output-dir {{ DATA_DIR }} \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB }} \
        --geo-db-file $(just _latest_db geo) \
        --ae-db-file $(just _latest_db ae) \
        --workers {{ WORKERS }}

transfer_pg:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer pg \
        --pg-url "$PG_URL" \
        --sra-db-file $(just _latest_db sra) \
        --geo-db-file $(just _latest_db geo) \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB }} \
        --download-links-db-file {{ DOWNLOAD_LINKS_DB }} \
        --batch-size {{ BATCH_SIZE }} \
        --workers {{ WORKERS }}
