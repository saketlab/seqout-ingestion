import logging
from datetime import datetime

import config
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

    if args.stage == "ingest":
        run_ingest(
            cfg=IngestionConfig(
                datasource=args.datasource,
                output_dir=args.output_dir,
                start_date=datetime(*args.start_date).date(),
                end_date=datetime(*args.end_date).date(),
                num_workers=args.workers,
                chunk_size=args.chunk_size,
                sra_accessions_ttl=args.sra_accessions_ttl,
                ena_use_last_updated=args.ena_use_last_updated,
                ena_skip_experiments=args.ena_skip_experiments,
                ena_skip_samples=args.ena_skip_samples,
            )
        )
    else:
        raise NotImplementedError(f"{args.stage} is not yet implemented")


if __name__ == "__main__":
    main()
