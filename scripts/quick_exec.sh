#!/usr/bin/bash

python ./src/main.py --stage=ingest --datasource=geo --output-dir=./data --workers=8 --chunk-size="3 MiB" --start-date="2025-10-17" --end-date="2025-12-18" --sra-accessions-ttl="7 days" --log-level="debug" --geo-series-limit=1
