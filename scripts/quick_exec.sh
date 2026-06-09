#!/usr/bin/bash

PYTHON_GIL=1 python ./src/main.py --stage=backfill_pubmed_meta --datasource=sra --db-file=./data/sra/db/2026-06-08/sra_metadata_2026-06-05.db --output-dir=./data --workers=4 --batch-size=15 --ncbi-keys-file=./data/keys/ncbi.txt --openalex-keys-file=./data/keys/openalex.txt
