import argparse
import datetime
import enum
import sys

import humanfriendly


class Stage(enum.StrEnum):
    Ingest = "ingest"
    BackfillPubmedIds = "backfill_pubmed_ids"
    BackfillPubmedMeta = "backfill_pubmed_meta"
    BuildUnifiedCatalog = "build_unified_catalog"


class Datasource(enum.StrEnum):
    Geo = "geo"
    Sra = "sra"
    Ena = "ena"
    ArrayExpress = "ae"
    _None = "none"


def parse_config() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="seqout ingestion toolkit")
    parser.add_argument(
        "--stage",
        type=Stage,
        required=True,
        choices=list(Stage),
        help="stage of the pipeline to run",
    )
    parser.add_argument(
        "--datasource",
        type=str,
        default=Datasource._None,
        choices=list(Datasource),
        help="name of the Datasource from where to pull data for ingest stage",
    )
    parser.add_argument(
        "--db-file",
        type=str,
        help="path from where the db file would be used (or) where the db file would be created",
    )
    parser.add_argument("--geo-db-file", type=str, help="path of geo db file")
    parser.add_argument("--sra-db-file", type=str, help="path of sra db file")
    parser.add_argument("--ena-db-file", type=str, help="path of ena db file")
    parser.add_argument("--ae-db-file", type=str, help="path of arrayexpress db file")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/NAS/pysraweb",
        help="directory where all the dump files would be stored",
    )
    parser.add_argument(
        "--start-date",
        type=humanfriendly.parse_date,
        default=datetime.datetime.now().timetuple()[:6],
        help="start date of data dump range for ingest stage",
    )
    parser.add_argument(
        "--end-date",
        type=humanfriendly.parse_date,
        default=datetime.datetime.now().timetuple()[:6],
        help="end date of data dump range for ingest stage",
    )
    parser.add_argument(
        "--workers",
        type=int,
        required=True,
        help="total worker budget for the stage. the stage distributes these across various tasks automatically",
    )
    parser.add_argument(
        "--chunk-size",
        type=humanfriendly.parse_size,
        default="20 MiB",
        help="amount of data which is to be held in RAM per worker at once during any read/write phase",
    )
    parser.add_argument("--batch-size", type=int, default=2000, help="batch size")
    parser.add_argument(
        "--ncbi-keys-file",
        type=str,
        help="path to .txt file which contains ncbi api keys, one key per line",
    )
    parser.add_argument(
        "--openalex-keys-file",
        type=str,
        help="path to .txt file which contains openalex api keys, one key per line",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="whether to only count and display the information or update the existing records also",
    )
    parser.add_argument(
        "--stream",
        type=str,
        default="stdout",
        help="name of the stream where the logs would be sent out to. it can be either stdout or path of a file",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="info",
        help="minimum log level above which (and equal to) the logs are streamed",
    )
    parser.add_argument(
        "--sra-accessions-ttl",
        type=humanfriendly.parse_timespan,
        default=2 * 24 * 60 * 60,
        help="time-to-live for `SRA_Accessions.tab` file for sra ingest stage",
    )
    parser.add_argument(
        "--geo-series-pickle",
        type=str,
        help="path to pickle file which contains geo series dict",
    )
    parser.add_argument(
        "--geo-series-limit",
        type=int,
        default=None,
        help="limit the total number of series groups which would be indexed",
    )
    parser.add_argument(
        "--ena-use-last-updated",
        action="store_true",
        default=False,
        help="filter on last_updated instead of first_public while fetching ena accessions",
    )
    parser.add_argument(
        "--ena-skip-experiments",
        action="store_true",
        default=False,
        help="whether to skip download phase for per-study experiments for ena ingest stage",
    )
    parser.add_argument(
        "--ena-skip-samples",
        action="store_true",
        default=False,
        help="whether to skip download phase for per-study samples for ena ingest stage",
    )
    parser.add_argument(
        "--pm-meta-update-metrics",
        action="store_true",
        default=False,
        help="update citation counts and journal metrics for existing records (instead of fetching new records)",
    )
    parser.add_argument(
        "--pm-meta-fill-metrics",
        action="store_true",
        default=False,
        help="only fetch metrics for records where citation_count or journal metrics are null",
    )
    parser.add_argument(
        "--pm-meta-skip-journal-metrics",
        action="store_true",
        default=False,
        help="skip openalex journal metrics (h-index, i10, etc.) and only fetch citation counts",
    )
    parser.add_argument(
        "--pm-meta-update-journal-metrics",
        action="store_true",
        default=False,
        help="looks up unique journals in the db via openalex and bulk-update all rows for each journal",
    )
    parser.add_argument(
        "--pm-meta-refresh-all-journals",
        action="store_true",
        default=False,
        help="with --pm-meta-update-journal-metrics: re-fetch all journals, not just those missing metrics.",
    )

    args = parser.parse_args(args=None if sys.argv[1:] else ["--help"])
    if not args.start_date <= args.end_date:
        raise ValueError("start date needs to be less than end date")

    return args
