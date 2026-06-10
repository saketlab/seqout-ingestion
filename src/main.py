import logging
import sys
from datetime import datetime

import config
from backfill.backfill_config import BackfillConfig
from backfill.pubmed_ids import backfill_pubmed_ids
from backfill.pubmed_meta import backfill_pubmed_meta
from config import Stage
from ingest.ingest import IngestionConfig, run_ingest
from unifier.build_unified_catalog import build_unified_catalog
from unifier.unifier_config import UnifierConfig


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
    stage = Stage(args.stage)

    match stage:
        case Stage.Ingest:
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

            run_ingest(ingestion_cfg)
        case Stage.BackfillPubmedIds | Stage.BackfillPubmedMeta:
            backfill_cfg = BackfillConfig(
                datasource=args.datasource,
                base_output_dir=args.output_dir,
                db_file=args.db_file,
                geo_db_file=args.geo_db_file,
                sra_db_file=args.sra_db_file,
                ena_db_file=args.ena_db_file,
                ae_db_file=args.ae_db_file,
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
            fn = (
                backfill_pubmed_ids
                if args.stage == Stage.BackfillPubmedIds
                else backfill_pubmed_meta
            )

            fn(backfill_cfg)
        case Stage.BuildUnifiedCatalog:
            unifier_cfg = UnifierConfig(
                geo_db_file=args.geo_db_file,
                sra_db_file=args.sra_db_file,
                ena_db_file=args.ena_db_file,
                ae_db_file=args.ae_db_file,
                output_dir=args.output_dir,
                db_file=args.db_file,
                num_workers=args.workers,
                batch_size=args.batch_size,
                chunk_size=args.chunk_size,
            )

            build_unified_catalog(unifier_cfg)
        case _:
            raise NotImplementedError(f"{args.stage} is not yet implemented")


if __name__ == "__main__":
    main()
