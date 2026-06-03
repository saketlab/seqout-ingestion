import datetime
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from helpers import send_req
from ingest.ingest_config import IngestionConfig

_AE_OUTPUT_DIR = "arrayexpress"
_BIOSTUDIES_API_BASE_URL = "https://www.ebi.ac.uk/biostudies/api/v1"


def ingest_ae(cfg: IngestionConfig):
    output_dir = Path(cfg.output_dir) / _AE_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    accessions = _fetch_accessions(cfg.start_date, cfg.end_date, cfg.num_workers)
    _download_study_metadatas(accessions, cfg.num_workers, output_dir / "studies")


def _send_search_req(date: datetime.date) -> list[str] | None:
    r = send_req(
        method="GET",
        url=f"{_BIOSTUDIES_API_BASE_URL}/search",
        params={
            "collection": "arrayexpress",
            "release_date": date,
            "page_size": 100,
        },
    )
    if r is None:
        return []

    data = r.json()
    hits = data.get("hits")
    if hits is None or not isinstance(hits, list):
        return None

    return [hit["accession"] for hit in hits]


def _fetch_accessions(
    start_date: datetime.date, end_date: datetime.date, num_workers: int
) -> list[str]:
    accessions: list[str] = []
    num_days = (end_date - start_date).days + 1
    num_workers = min(num_days, num_workers)

    chunk_size = math.ceil(num_days / num_workers)
    batches = [
        (
            start_date + datetime.timedelta(days=i),
            min(start_date + datetime.timedelta(days=i + chunk_size - 1), end_date),
        )
        for i in range(0, num_days, chunk_size)
    ]

    def _worker(batch: tuple[datetime.date, datetime.date]) -> tuple[list[str], int]:
        accessions: list[str] = []
        days_processed = 0
        start, end = batch
        current_date = start

        while current_date <= end:
            per_page_accessions = _send_search_req(current_date)
            if per_page_accessions is None or len(per_page_accessions) == 0:
                current_date += datetime.timedelta(1)
                days_processed += 1
                continue

            for hit in per_page_accessions:
                accessions.append(hit)

            if len(per_page_accessions) < 100:
                current_date += datetime.timedelta(1)
                days_processed += 1
            else:
                curr_accessions = len(per_page_accessions)
                while curr_accessions >= 100:
                    per_page_accessions = _send_search_req(current_date)
                    if per_page_accessions is None:
                        break
                    if len(per_page_accessions) == 0:
                        break
                    for hit in per_page_accessions:
                        accessions.append(hit)
                    curr_accessions = len(per_page_accessions)

                current_date += datetime.timedelta(1)
                days_processed += 1

        return accessions, days_processed

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=num_days, unit=" day", desc="fetching accessions in range") as pbar,
    ):
        futures = [pool.submit(_worker, b) for b in batches]

        for f in as_completed(futures):
            batch_accessions, days_processed = f.result()
            accessions.extend(batch_accessions)
            pbar.update(days_processed)

    return accessions


def _study_metadata_worker(accession: str, output_dir: Path):
    r = send_req(method="GET", url=f"{_BIOSTUDIES_API_BASE_URL}/studies/{accession}")
    if r is None:
        return

    data = r.json()
    output_file = output_dir / f"{accession}.json"

    with open(output_file, "w") as f:
        json.dump(data, f)


def _download_study_metadatas(
    accessions: list[str], num_workers: int, output_dir: Path
):
    output_dir.mkdir(exist_ok=True, parents=True)

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(
            total=len(accessions), unit=" accession", desc="downloading study metadata"
        ) as pbar,
    ):
        futures = [
            pool.submit(_study_metadata_worker, acc, output_dir) for acc in accessions
        ]

        for f in as_completed(futures):
            f.result()
            pbar.update(1)
