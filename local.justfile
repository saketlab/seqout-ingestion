export PYTHON_GIL := "0"

PYTHON := ".venv/bin/python"
DATA_DIR := "./data"

_latest_db source:
    @latest=$(ls {{ DATA_DIR }}/{{ source }}/db/ | sort -r | head -1) && \
    echo "{{ DATA_DIR }}/{{ source }}/db/$latest/{{ source }}_metadata_$latest.db"

ingest_sra:
    {{ PYTHON }} ./src/main.py ingest sra \
        --output-dir {{ DATA_DIR }} \
        --start-date 2026-01-01 \
        --end-date 2026-01-20 \
        --workers 10 \
        --read-batch-size 200 \
        --write-batch-size 50000

ingest_geo:
    {{ PYTHON }} ./src/main.py ingest geo \
        --output-dir {{ DATA_DIR }} \
        --workers 10

ingest_ena:
    {{ PYTHON }} ./src/main.py ingest ena \
        --output-dir {{ DATA_DIR }} \
        --start-date 2026-01-01 \
        --end-date 2026-01-10 \
        --workers 10

ingest_ae:
    {{ PYTHON }} ./src/main.py ingest ae \
        --output-dir {{ DATA_DIR }} \
        --start-date 2025-01-01 \
        --end-date 2026-01-01 \
        --workers 10

backfill_pm_ids:
    {{ PYTHON }} ./src/main.py backfill pm-ids \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file ./data/_keys/ncbi.txt \
        --batch-size 100

backfill_pm_meta:
    {{ PYTHON }} ./src/main.py backfill pm-meta \
        --output-dir {{ DATA_DIR }} \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file ./data/_keys/ncbi.txt \
        --openalex-keys-file ./data/_keys/openalex.txt \
        --update-journal-metrics \
        --refresh-all-journals-metrics \
        --batch-size 100 \

unify_build_catalog:
    {{ PYTHON }} ./src/main.py unify build-catalog \
        --output-dir {{ DATA_DIR }} \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \

unify_build_metadata:
    {{ PYTHON }} ./src/main.py unify build-metadata \
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
    {{ PYTHON }} ./src/main.py geocode \
        --unified-metadata-db-file {{ DATA_DIR }}/unified_metadata/unified_metadata.db \
        --geo-db-file $(just _latest_db geo) \
        --cache-file ./data/geocoding/geocode_cache.json \
        --save-every 1 \
        --rate-limit 3.0

sc_enrich_regex:
    {{ PYTHON }} ./src/main.py sc_enrich regex \
        --unified-metadata-db-file {{ DATA_DIR }}/unified_metadata/unified_metadata.db \

sc_enrich_fastcounter:
    {{ PYTHON }} ./src/main.py --log-level debug \
        sc_enrich fastcounter \
            --output-dir {{ DATA_DIR }} \
            --unified-metadata-db-file {{ DATA_DIR }}/unified_metadata/unified_metadata.db \
            --geo-db-file $(just _latest_db geo) \
            --ae-db-file $(just _latest_db ae) \
            --workers 5 \
