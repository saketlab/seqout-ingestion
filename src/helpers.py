import sqlite3
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from importlib.resources import files
from pathlib import Path

import requests
from tqdm import tqdm


def better_downloader(
    url: str, dest_path: Path, num_workers: int, chunk_size: int
) -> None:
    dest_path.parent.mkdir(exist_ok=True)
    timeout = (10, 60)

    with requests.head(url, timeout=timeout) as r:
        r.raise_for_status()
        content_length = int(r.headers.get("Content-Length", -1))
        accepts_range = r.headers.get("Accept-Ranges", "")

    if content_length == -1 or accepts_range == "":
        simple_file_download(url, dest_path, chunk_size, timeout)
        return

    worker_chunk_size = content_length // num_workers
    with open(dest_path, "wb") as f:
        f.truncate(content_length)

    with tqdm(
        total=content_length,
        unit="B",
        unit_scale=True,
        desc=f"downloading {url}",
        delay=5,
    ) as pbar:

        def _download_chunk(start: int, end: int, retries: int = 5) -> None:
            bytes_written = 0
            for attempt in range(retries):
                try:
                    with (
                        open(dest_path, "r+b") as f,
                        requests.get(
                            url,
                            headers={"Range": f"bytes={start + bytes_written}-{end}"},
                            stream=True,
                            timeout=(10, 60),
                        ) as r,
                    ):
                        r.raise_for_status()
                        f.seek(start + bytes_written)
                        for data in r.iter_content(chunk_size):
                            if not data:
                                break

                            f.write(data)
                            bytes_written += len(data)
                            pbar.update(len(data))
                    return
                except Exception as e:
                    if attempt == retries - 1:
                        raise e

                    pbar.update(-bytes_written)
                    time.sleep(2**attempt)

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures: list[Future[None]] = []
            for i in range(num_workers):
                start = i * worker_chunk_size
                end = (
                    (start + worker_chunk_size - 1)
                    if i < num_workers - 1
                    else content_length - 1
                )

                futures.append(pool.submit(_download_chunk, start, end))

            for future in as_completed(futures):
                future.result()


def simple_file_download(
    url: str,
    dest_path: Path,
    chunk_size: int,
    timeout: int | tuple[int, int],
    with_pbar: bool = True,
) -> None:
    r = send_req("GET", url, stream=True, timeout=timeout)
    if r is None:
        return

    content_length = int(r.headers.get("Content-Length", -1))
    if content_length == -1:
        raise ValueError(
            f"invalid content length while trying to download {url}: {content_length}"
        )

    if with_pbar:
        pbar = tqdm(
            total=content_length,
            unit="B",
            unit_scale=True,
            desc=f"downloading {url}",
        )

    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size):
            f.write(chunk)
            if with_pbar:
                pbar.update(len(chunk))

    if with_pbar:
        pbar.close()
    r.close()


def send_req(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    json: dict | None = None,
    timeout: int | tuple[int, int] = 300,
    num_retries: int = 5,
    max_wait: int = 60,
    stream: bool = False,
    **kwargs,
) -> requests.Response | None:
    for attempt in range(num_retries):
        try:
            r = requests.request(
                method,
                url,
                params=params,
                headers=headers,
                json=json,
                timeout=timeout,
                stream=stream,
                **kwargs,
            )
            r.raise_for_status()

            return r
        except requests.RequestException:
            if attempt == num_retries - 1:
                raise
            wait = min(2**attempt, max_wait)
            time.sleep(wait)

    raise RuntimeError(f"failed after {num_retries} retries")


def exec_query_from_file(
    query_file: str | Path,
    db_file: str | Path | None = None,
    conn: sqlite3.Connection | None = None,
):
    if not conn:
        if not db_file:
            raise ValueError("missing db file")
        conn = sqlite3.connect(db_file)

    cursor = conn.cursor()
    query = files("sql").joinpath(query_file).read_text()
    cursor.executescript(query)
    conn.close()
