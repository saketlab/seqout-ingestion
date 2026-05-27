from datetime import datetime

import config
from ingest.ingest import IngestionConfig, run_ingest


def main() -> None:
    args = config.parse_config()

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
            )
        )
    else:
        raise NotImplementedError(f"{args.stage} is not yet implemented")


if __name__ == "__main__":
    main()
