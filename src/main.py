import logging
import sys
from datetime import datetime

import config
from backfill_pubmed import backfill_pubmed
from ingest.ingest import IngestionConfig, run_ingest


def main() -> None:
    args = config.parse_config()
    handlers = []

    if args.stream == "stdout":
        handlers.append(logging.StreamHandler())
    else:
        handlers.append(logging.FileHandler(args.stream))

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )

    print(f"GIL enabled: {sys._is_gil_enabled()}")

    match args.stage:
        case "ingest":
            run_ingest(
                cfg=IngestionConfig(
                    datasource=config.Datasources(args.datasource),
                    output_dir=args.output_dir,
                    db_file=args.db_file,
                    start_date=datetime(*args.start_date).date(),
                    end_date=datetime(*args.end_date).date(),
                    num_workers=args.workers,
                    chunk_size=args.chunk_size,
                    batch_size=args.batch_size,
                    sra_accessions_ttl=args.sra_accessions_ttl,
                    ena_use_last_updated=args.ena_use_last_updated,
                    ena_skip_experiments=args.ena_skip_experiments,
                    ena_skip_samples=args.ena_skip_samples,
                    geo_series_pickle=args.geo_series_pickle,
                    geo_series_limit=args.geo_series_limit,
                )
            )
        case "backfill_pubmed":
            backfill_pubmed(
                datasource=config.Datasources(args.datasource),
                db_file=args.db_file,
                keys_file=args.pubmed_ncbi_api_keys,
                num_workers=args.workers,
                batch_size=args.batch_size,
                is_dry_run=args.pubmed_dry_run,
            )
        case _:
            raise NotImplementedError(f"{args.stage} is not yet implemented")


if __name__ == "__main__":
    main()
