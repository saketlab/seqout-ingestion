#!/usr/bin/bash

python ./src/main.py --stage=ingest --datasource=ena --output-dir=./data --workers=3 --chunk-size="3 MiB" --start-date="2025-10-17" --end-date="2025-10-18" --sra-accessions-ttl="7 days" --log-level="debug"
