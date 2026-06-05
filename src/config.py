import argparse
import datetime
import sys

import humanfriendly


def parse_config() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="seqout ingestion toolkit")
    parser.add_argument(
        "--stage",
        type=str,
        required=True,
        choices=["ingest", "sc_enrich"],
        help="stage of the pipeline to run",
    )
    parser.add_argument(
        "--datasource",
        type=str,
        required=True,
        choices=["geo", "sra", "ena", "ae"],
        help="name of the datasources from where to pull data for ingest stage",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/NAS/pysraweb",
        help="directory where all the dump files would be stored",
    )
    parser.add_argument(
        "--start-date",
        type=humanfriendly.parse_date,
        default=datetime.datetime.now(),
        help="start date of data dump range for ingest stage",
    )
    parser.add_argument(
        "--end-date",
        type=humanfriendly.parse_date,
        default=datetime.datetime.now(),
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

    args = parser.parse_args(args=None if sys.argv[1:] else ["--help"])
    if not args.start_date < args.end_date:
        raise ValueError("start date needs to be less than end date")

    return args
