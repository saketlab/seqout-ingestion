import logging
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import send_req, simple_file_download
from ingest.ingest_config import IngestionConfig

_GEO_OUTPUT_DIR = "geo"


def ingest_geo(cfg: IngestionConfig):
    output_dir = Path(cfg.output_dir) / _GEO_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    geo_series_pkl_file = (
        Path(cfg.geo_series_pickle)
        if cfg.geo_series_pickle
        else output_dir / "geo_series.pkl"
    )

    geo_dict = _build_geo_dict(cfg.num_workers, geo_series_pkl_file)
    num_accessions = sum(len(v) for v in geo_dict.values())
    logging.info("loaded %d groups and %d accessions", len(geo_dict), num_accessions)

    _download_miniml_tars(
        geo_dict, num_accessions, cfg.num_workers, data_dir, cfg.chunk_size
    )


def _build_geo_dict(num_workers: int, pkl_file: Path) -> dict[str, list[str]]:
    if pkl_file.exists():
        with open(pkl_file, "rb") as f:
            return pickle.load(f)

    geo_dict: dict[str, list[str]] = {}

    series_resp = send_req("GET", "https://ftp.ncbi.nlm.nih.gov/geo/series/")
    assert series_resp is not None, "failed to fetch geo series"

    soup = BeautifulSoup(series_resp.text, "html.parser")
    anchors = soup.find_all("a")
    series = [str(a["href"][:-1]) for a in anchors if str(a["href"]).startswith("GSE")]

    def _worker(s: str) -> list[str] | None:
        dir_resp = send_req("GET", f"https://ftp.ncbi.nlm.nih.gov/geo/series/{s}")
        if dir_resp is None:
            logging.error("failed to fetch geo accession ids (series=%s)", s)
            return None

        soup = BeautifulSoup(dir_resp.text, "html.parser")
        anchors = soup.find_all("a")
        accessions = [
            str(a["href"][:-1]) for a in anchors if str(a["href"]).startswith("GSE")
        ]

        return accessions

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=len(series), desc="building geo series dict") as pbar,
    ):
        futures = {pool.submit(_worker, s): s for s in series[:3]}

        for future in as_completed(futures):
            series = futures[future]
            accessions = future.result()
            if not accessions:
                continue

            geo_dict[series] = accessions
            pbar.update(1)

    with open(pkl_file, "wb") as f:
        pickle.dump(geo_dict, f)

    return geo_dict


def _download_miniml_tars(
    geo_dict: dict[str, list[str]],
    num_accessions: int,
    num_workers: int,
    output_dir: Path,
    chunk_size: int,
):
    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(
            total=num_accessions, unit=" accession", desc="downloading miniml file"
        ) as pbar,
    ):
        futures = {}

        for series_group, accs in geo_dict.items():
            for acc in accs:
                url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series_group}/{acc}/miniml/{acc}_family.xml.tgz"
                dest_path = output_dir / f"{acc}_family.xml.tgz"
                if dest_path.exists():
                    continue

                futures[
                    pool.submit(
                        simple_file_download,
                        url,
                        dest_path,
                        chunk_size,
                        (10, 60),
                        False,
                    )
                ] = acc

        for future in as_completed(futures):
            future.result()
            pbar.update(1)
