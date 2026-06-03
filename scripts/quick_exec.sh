#!/usr/bin/bash

python ./src/main.py --stage=ingest --datasource=ae --output-dir=./data --workers=5 --chunk-size="3 MiB" --start-date="2025-10-17" --end-date="2025-12-18" --sra-accessions-ttl="7 days" --log-level="debug"
