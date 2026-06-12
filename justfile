export PYTHON_GIL := "0"

PYTHON := ".venv/bin/python"
DATA_DIR := "./data"

_latest_db source:
    @latest=$(ls {{ DATA_DIR }}/{{ source }}/db/ | sort -r | head -1) && \
    echo "{{ DATA_DIR }}/{{ source }}/db/$latest/{{ source }}_metadata_$latest.db"

ingest_sra:
    {{ PYTHON }} ./src/main.py ingest sra --output-dir {{ DATA_DIR }} --start-date 2026-01-15 --end-date 2026-01-20 --workers 6 --read-batch-size 500 --write-batch-size 5000

ingest_geo:
    {{ PYTHON }} ./src/main.py ingest geo --output-dir {{ DATA_DIR }} --series-limit 1

ingest_ena:
    {{ PYTHON }} ./src/main.py ingest ena --output-dir {{ DATA_DIR }} --start-date 2026-01-01 --end-date 2026-01-10 --workers 10

ingest_ae:
    {{ PYTHON }} ./src/main.py ingest ae --output-dir {{ DATA_DIR }} --start-date 2026-01-01 --end-date 2026-01-10 --workers 10

backfill_pm_ids:
    {{ PYTHON }} ./src/main.py backfill pm-ids \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file ./data/_keys/ncbi.txt \
        --batch-size 100 \

backfill_pm_meta:
    {{ PYTHON }} ./src/main.py backfill pm-meta \
        --output-dir {{ DATA_DIR }} \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
        --ncbi-keys-file ./data/_keys/ncbi.txt \
        --openalex-keys-file ./data/_keys/openalex.txt \
        # --update-journal-metrics \
        # --refresh-all-journals-metrics \
        --batch-size 100 \

unify_build_catalog:
    {{ PYTHON }} ./src/main.py unify build-catalog \
        --output-dir {{ DATA_DIR }} \
        --geo-db-file $(just _latest_db geo) \
        --sra-db-file $(just _latest_db sra) \
        --ena-db-file $(just _latest_db ena) \
        --ae-db-file $(just _latest_db ae) \
