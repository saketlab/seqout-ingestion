import logging
import sys
from datetime import datetime

import config
from backfill.backfill_config import BackfillConfig
from backfill.pubmed_ids import backfill_pubmed_ids
from backfill.pubmed_meta import backfill_pubmed_meta
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

    ingestion_cfg = IngestionConfig(
        datasource=config.Datasource(args.datasource),
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

    backfill_cfg = backfill_cfg = BackfillConfig(
        datasource=args.datasource,
        base_output_dir=args.output_dir,
        db_file=args.db_file,
        ncbi_keys_file=args.ncbi_keys_file,
        openalex_keys_file=args.openalex_keys_file,
        num_workers=args.workers,
        chunk_size=args.chunk_size,
        batch_size=args.batch_size,
        update_metrics=args.pm_meta_update_metrics,
        fill_metrics=args.pm_meta_fill_metrics,
        skip_journal_metrics=args.pm_meta_skip_journal_metrics,
        update_journal_metrics=args.pm_meta_update_journal_metrics,
        refresh_all_journals_metrics=args.pm_meta_refresh_all_journals,
        is_dry_run=args.dry_run,
    )

    match args.stage:
        case "ingest":
            run_ingest(ingestion_cfg)
        case "backfill_pubmed_ids":
            backfill_pubmed_ids(backfill_cfg)
        case "backfill_pubmed_meta":
            backfill_pubmed_meta(backfill_cfg)
        case _:
            raise NotImplementedError(f"{args.stage} is not yet implemented")


if __name__ == "__main__":
    main()
