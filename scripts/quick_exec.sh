#!/usr/bin/bash

PYTHON_GIL=1 python ./src/main.py --stage=backfill_pubmed --datasource=sra --output-dir=./data --workers=10 --batch-size=10 --db-file=./data/sra/db/2026-06-08/sra_metadata_2026-06-05.db --pubmed-ncbi-api-keys=./data/pubmed/keys.txt
