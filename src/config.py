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
        choices=["ingest", "all"],
        help="stage of the pipeline to run",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/NAS/pysraweb",
        help="directory where all the dump files would be stored",
    )
    parser.add_argument(
        "--datasource",
        type=str,
        default="all",
        choices=["geo", "sra", "ena", "ae", "all"],
        help="name of the datasources from where to pull data for ingest stage",
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
        "--sra-accessions-ttl",
        type=humanfriendly.parse_timespan,
        default=2 * 24 * 60 * 60,  # 2 days
        help="TTL for SRA_Accessions.tab file for SRA ingest stage",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        required=True,
        help="number of parallel workers which would be running during any download phase. need to be manually finetuned based on rate limits",
    )
    parser.add_argument(
        "--download-chunk-size",
        type=int,
        default=20 * 1024,  # 20 KiB
        help="size of the chunk which sits in RAM at once while writing to output content",
    )

    args = parser.parse_args(args=None if sys.argv[1:] else ["--help"])
    if not args.start_date < args.end_date:
        raise ValueError("start date needs to be less than end date")

    return args
