from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import BinaryIO

import requests
from tqdm import tqdm


def better_downloader(
    url: str, dest_path: Path, num_workers: int, chunk_size: int
) -> None:
    dest_path.parent.mkdir(exist_ok=True)

    with requests.head(url) as r:
        r.raise_for_status()
        content_length = int(r.headers.get("Content-Length", -1))
        accepts_range = r.headers.get("Accept-Ranges", "")

    if content_length == -1 or accepts_range == "":
        simple_file_download(url, dest_path, chunk_size)
        return

    worker_chunk_size = content_length // num_workers

    with open(dest_path, "wb") as f:
        f.truncate(content_length)

        with (
            tqdm(
                total=content_length,
                unit="B",
                unit_scale=True,
                desc=f"downloading {url}",
            ) as pbar,
            ThreadPoolExecutor(max_workers=num_workers) as pool,
        ):

            def _download_chunk(url: str, start: int, end: int, f: BinaryIO) -> None:
                with requests.get(
                    url, headers={"Range": f"bytes={start}-{end}"}, stream=True
                ) as r:
                    r.raise_for_status()
                    f.seek(start)
                    for chunk in r.iter_content(chunk_size):
                        f.write(chunk)
                        pbar.update(len(chunk))

            futures: list[Future[None]] = []
            for i in range(num_workers):
                start = i * worker_chunk_size
                end = start + worker_chunk_size - 1
                if i == num_workers - 1:
                    end = content_length - 1
                futures.append(pool.submit(_download_chunk, url, start, end, f))

            for future in as_completed(futures):
                future.result()


def simple_file_download(url: str, dest_path: Path, chunk_size: int) -> None:
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        content_length = int(r.headers.get("Content-Length", -1))
        if content_length == -1:
            raise ValueError(
                f"invalid content length while trying to download {url}: {content_length}"
            )

        with (
            open(dest_path, "wb") as f,
            tqdm(
                total=content_length,
                unit="B",
                unit_scale=True,
                desc=f"downloading {url}",
            ) as pbar,
        ):
            for chunk in r.iter_content(chunk_size):
                f.write(chunk)
                pbar.update(len(chunk))
