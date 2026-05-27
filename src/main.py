import config
from ingest.ingest import IngestionConfig, run_ingest


def main() -> None:
    args = config.parse_config()

    if args.stage == "ingest":
        run_ingest(
            cfg=IngestionConfig(
                datasource=args.datasource,
                output_dir=args.output_dir,
                start_date=args.start_date,
                end_date=args.end_date,
                sra_accessions_ttl=args.sra_accessions_ttl,
                num_download_workers=args.download_workers,
                download_chunk_size=args.download_chunk_size,
            )
        )
    else:
        raise NotImplementedError(f"{args.stage} is not yet implemented")


if __name__ == "__main__":
    main()
