set dotenv-load

export PYTHON_GIL := "0"
export DATA_DIR := env("DATA_DIR", "/data/share/seqout_may")

LOG_LEVEL := env("LOG_LEVEL", "info")
START_DATE := env("START_DATE", "2026-04-28")
END_DATE := env("END_DATE", "2026-05-31")
RETRIES := env("RETRIES", "20")
WORKERS := env("WORKERS", "10")

PYTHON := ".venv/bin/python"
ENTRYPOINT := "./src/main.py --log-level " + LOG_LEVEL

_latest_db source:
    @latest=$(ls {{ DATA_DIR }}/{{ source }}/db/ | sort -r | head -1) && \
    echo "{{ DATA_DIR }}/{{ source }}/db/$latest/{{ source }}_metadata_$latest.db"

ingest_sra:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest sra \
        --output-dir {{ DATA_DIR }} \
        --start-date {{ START_DATE }} \
        --end-date {{ END_DATE }} \
        --workers {{ WORKERS }} \
        --write-batch-size 1000

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
        --ncbi-keys-file {{ DATA_DIR }}/_keys/ncbi.txt \
        --workers {{ WORKERS }}

backfill_pm_ids:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-ids \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file {{ DATA_DIR }}/_keys/ncbi.txt \
        --batch-size 100 \

backfill_pm_meta:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-meta \
        --output-dir {{ DATA_DIR }} \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file {{ DATA_DIR }}/_keys/ncbi.txt \
        --openalex-keys-file {{ DATA_DIR }}/_keys/openalex.txt \
        --update-journal-metrics \
        --refresh-all-journals-metrics \
        --batch-size 100 \

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
        --unified-catalog-db-file {{ DATA_DIR }}/unified_catalog/unified_catalog.db \
        --pubmed-metadata-db-file {{ DATA_DIR }}/pubmed_meta/pubmed_meta.db \
        --assay-mapping-csv-file {{ DATA_DIR }}/_csv/assay_mapping.csv \
        --taxonomy-mapping-csv-file {{ DATA_DIR }}/_csv/ncbi_taxonomy_common_names.csv

geocode_missing:
    {{ PYTHON }} {{ ENTRYPOINT }} geocode \
        --unified-metadata-db-file {{ DATA_DIR }}/unified_metadata/unified_metadata.db \
        --geo-db-file $(just _latest_db geo) \
        --cache-file {{ DATA_DIR }}/geocoding/geocode_cache.json \
        --save-every 100 \
        --rate-limit 3 \
        --retries {{ RETRIES }}

sc_enrich_regex:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich regex \
        --unified-metadata-db-file {{ DATA_DIR }}/unified_metadata/unified_metadata.db

sc_enrich_detector:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich detector \
        --output-dir {{ DATA_DIR }} \
        --unified-metadata-db-file {{ DATA_DIR }}/unified_metadata/unified_metadata.db \
        --geo-db-file $(just _latest_db geo) \
        --ae-db-file $(just _latest_db ae) \
        --workers {{ WORKERS }}

transfer_pg:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer pg \
        --pg-url "$PG_URL" \
        --geo-db-file $(just _latest_db geo) \
        --batch-size 500 \
