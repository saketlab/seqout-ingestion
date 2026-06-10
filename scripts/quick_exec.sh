#!/usr/bin/bash

# --backfill_pubmed_ids
# PYTHON_GIL=1 python ./src/main.py --stage=backfill_pubmed_ids \
#     --sra-db-file=./data/sra/db/2026-06-08/sra_metadata_2026-06-05.db \
#     --workers=4 --batch-size=4 \
#     --ncbi-keys-file=./data/keys/ncbi.txt --openalex-keys-file=./data/keys/openalex.txt

# -- backfill_pubmed_meta
# PYTHON_GIL=1 python ./src/main.py --stage=backfill_pubmed_meta \
#     --db-file=./data/pubmed_meta/pubmed_meta.db \
#     --sra-db-file=./data/sra/db/2026-06-08/sra_metadata_2026-06-05.db \
#     --workers=10 --batch-size=100 \
#     --ncbi-keys-file=./data/keys/ncbi.txt --openalex-keys-file=./data/keys/openalex.txt \
#     --pm-meta-update-journal-metrics --pm-meta-refresh-all-journals

# -- build_unified_catalog
PYTHON_GIL=1 python ./src/main.py --stage=build_unified_catalog \
    --sra-db-file=./data/sra/db/2026-06-08/sra_metadata_2026-06-05.db \
    --geo-db-file=./data/geo/db/2026-06-08/geo_metadata_2026-06-08.db \
    --ena-db-file=./data/ena/db/2026-06-08/ena_metadata_2026-06-08.db \
    --ae-db-file=./data/arrayexpress/db/2026-06-08/ae_metadata_2026-06-08.db \
    --output-dir=./data --workers=10 --batch-size=100 --chunk-size="10 MiB" \
    --dump-parquet
