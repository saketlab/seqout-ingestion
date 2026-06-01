#!/usr/bin/bash

python ./src/main.py --stage=ingest --datasource=sra --output-dir=./data --workers=8 --chunk-size="10 MiB" --start-date="2025-10-17" --end-date="2025-10-18" --sra-accessions-ttl="7 days" --log-level="debug"
