import datetime
import logging
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import send_req

NUM_WORKERS = 10
LIMIT: int | None = 5
NUM_RETRIES = 10
TIMEOUT = 5
MAX_WAIT = 30
START_DATE = datetime.date(year=2026, month=4, day=28)
END_DATE = datetime.date(year=2026, month=5, day=31)


def _fetch_accessions_per_series_worker(series: str) -> list[str] | None:
    dir_resp = send_req(
        method="GET",
        url=f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series}",
        num_retries=NUM_RETRIES,
        timeout=TIMEOUT,
        max_wait=MAX_WAIT,
    )

    if dir_resp is None:
        logging.error("failed to fetch geo accession ids for %s series", series)
        return None

    soup = BeautifulSoup(dir_resp.text, "html.parser")
    anchors = soup.find_all("a")
    accessions_list = [
        str(a["href"]).rstrip("/") for a in anchors if str(a["href"]).startswith("GSE")
    ]

    return accessions_list


def _filter_accession_worker(accession: str, series: str) -> bool:
    dir_resp = send_req(
        method="GET",
        url=f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series}/{accession}",
        num_retries=NUM_RETRIES,
        timeout=TIMEOUT,
        max_wait=MAX_WAIT,
    )

    if dir_resp is None:
        logging.error("failed to fetch %s geo accession dir", accession)
        return False

    soup = BeautifulSoup(dir_resp.text, "html.parser")

    for a in soup.find_all("a"):
        href = a.get("href", "")
        if not href:
            continue

        next = a.next_sibling
        if next is None:
            continue

        date_match = re.match(r"\s*(\d{4}-\d{2}-\d{2})", str(next))
        if date_match is None:
            continue

        modified_date = datetime.date.fromisoformat(date_match.group(1))
        if modified_date < START_DATE:
            continue
        if modified_date > END_DATE:
            continue

        return True
    return False


def main():
    series_resp = send_req(
        method="GET",
        url="https://ftp.ncbi.nlm.nih.gov/geo/series/",
        num_retries=NUM_RETRIES,
        timeout=TIMEOUT,
        max_wait=MAX_WAIT,
    )
    assert series_resp is not None, "failed to fetch geo series"

    soup = BeautifulSoup(series_resp.text, "html.parser")
    anchors = soup.find_all("a")
    series_list = [
        str(a["href"]).rstrip("/") for a in anchors if str(a["href"]).startswith("GSE")
    ]

    if LIMIT:
        series_list = series_list[:LIMIT]

    # build a mapping between all the series and their list of accessions
    all_series_accession_mapping: dict[str, list[str]] = {}

    with (
        ThreadPoolExecutor(NUM_WORKERS) as pool,
        tqdm(total=len(series_list), desc="building geo series dict") as pbar,
    ):
        futures = {
            pool.submit(_fetch_accessions_per_series_worker, s): s for s in series_list
        }

        for f in as_completed(futures):
            accessions = f.result()
            series = futures[f]
            if not accessions:
                pbar.update(1)
                continue

            all_series_accession_mapping[series] = accessions
            pbar.update(1)

    total_accessions = sum([len(x) for x in all_series_accession_mapping.values()])

    to_process_series_accession_mapping: dict[str, list[str]] = defaultdict(list[str])

    with (
        ThreadPoolExecutor(NUM_WORKERS) as pool,
        tqdm(total=total_accessions, desc="filtering accessions to process") as pbar,
    ):
        futures = {}

        for series, accessions in all_series_accession_mapping.items():
            for accession in accessions:
                futures[pool.submit(_filter_accession_worker, accession, series)] = (
                    series,
                    accession,
                )

        for f in as_completed(futures):
            to_process = f.result()
            if not to_process:
                continue

            print(series, accession)
            (series, accession) = futures[f]
            to_process_series_accession_mapping[series].append(accession)


if __name__ == "__main__":
    main()
