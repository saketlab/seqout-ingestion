import logging
import sqlite3
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import LiteralString, cast

import psycopg
import requests
from tqdm import tqdm

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        return (
            exc.response is not None
            and exc.response.status_code in _RETRYABLE_STATUS_CODES
        )

    return isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    )


def send_req(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    json: dict | None = None,
    timeout: int,
    num_retries: int,
    max_wait: int,
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
        except requests.RequestException as exc:
            if not _is_retryable(exc) or attempt == num_retries - 1:
                raise

            wait = min(2**attempt, max_wait)
            logging.warning(
                "failed to send %s req to %s (attempt %d/%d, retrying in %.0fs) - %s",
                method,
                url,
                attempt + 1,
                num_retries,
                wait,
                exc,
            )

            time.sleep(wait)

    raise RuntimeError(f"failed after {num_retries} retries")


def simple_file_download(
    url: str,
    dest_path: Path,
    chunk_size: int,
    num_retries: int,
    timeout: int,
    max_wait: int,
    with_pbar: bool = True,
) -> Path:
    dest_path.parent.mkdir(exist_ok=True, parents=True)
    already_downloaded = dest_path.stat().st_size if dest_path.exists() else 0

    for attempt in range(num_retries):
        bytes_this_attempt = 0
        try:
            headers = {}
            if already_downloaded > 0:
                headers["Range"] = f"bytes={already_downloaded}-"

            r = requests.get(url, headers=headers, stream=True, timeout=timeout)
            if r.status_code == 416:
                return dest_path

            r.raise_for_status()

            content_length = int(r.headers.get("Content-Length", -1))
            if content_length == -1:
                raise ValueError(f"missing Content-Length for {url}")

            if r.status_code == 206:
                open_mode = "ab"
                resumed_from = already_downloaded
            else:
                open_mode = "wb"
                resumed_from = 0
            total = resumed_from + content_length

            pbar = (
                tqdm(
                    total=total,
                    initial=resumed_from,
                    unit="B",
                    unit_scale=True,
                    desc=f"downloading {url[:100]}",
                )
                if with_pbar
                else None
            )

            with open(dest_path, open_mode) as f:
                for chunk in r.iter_content(chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    n = len(chunk)
                    bytes_this_attempt += n
                    already_downloaded += n
                    if pbar:
                        pbar.update(n)

            actual_size = dest_path.stat().st_size
            if actual_size != total:
                raise IOError(
                    f"downloaded {actual_size} bytes, expected {total} for {url}"
                )

            if pbar:
                pbar.close()
            r.close()

            return dest_path

        except Exception as exc:
            if not _is_retryable(exc) or attempt == num_retries - 1:
                raise

            already_downloaded -= bytes_this_attempt

            wait = min(2**attempt, max_wait)
            logging.warning(
                "failed to download %s (attempt %d/%d): %s. retrying in %.0fs",
                url,
                attempt + 1,
                num_retries,
                exc,
                wait,
            )
            time.sleep(wait)

    raise RuntimeError(f"failed after {num_retries} retries")


def better_downloader(
    url: str,
    dest_path: Path,
    num_workers: int,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    dest_path.parent.mkdir(exist_ok=True, parents=True)
    content_length = -1
    accepts_range = ""

    for attempt in range(retries):
        try:
            with requests.head(url, timeout=timeout) as r:
                r.raise_for_status()
                content_length = int(r.headers.get("Content-Length", -1))
                accepts_range = r.headers.get("Accept-Ranges", "")
            break
        except Exception as exc:
            if not _is_retryable(exc) or attempt == retries - 1:
                raise

            wait = min(2**attempt, max_timeout)
            logging.warning(
                "failed to send head request to %s (attempt %d/%d): %s. retrying in %.2fs",
                url,
                attempt + 1,
                retries,
                exc,
                wait,
            )
            time.sleep(wait)

    if content_length == -1 or accepts_range == "":
        simple_file_download(url, dest_path, chunk_size, retries, timeout, max_timeout)
        return

    already_downloaded = 0
    if dest_path.exists():
        already_downloaded = dest_path.stat().st_size
        if already_downloaded >= content_length:
            logging.info(
                "skipping %s as it is already fully downloaded", dest_path.name
            )
            return

    if already_downloaded == 0:
        with open(dest_path, "wb") as f:
            f.truncate(content_length)

    with tqdm(
        total=content_length,
        initial=already_downloaded,
        unit="B",
        unit_scale=True,
        desc=f"downloading {url[:100]}",
        mininterval=1.0,
    ) as pbar:

        def _download_chunk(chunk_start: int, chunk_end: int) -> None:
            if chunk_start > chunk_end:
                return

            cursor = chunk_start
            pbar_counted_up_to = chunk_start

            for attempt in range(retries):
                bytes_this_attempt = 0
                try:
                    with (
                        open(dest_path, "r+b") as f,
                        requests.get(
                            url,
                            headers={"Range": f"bytes={cursor}-{chunk_end}"},
                            stream=True,
                            timeout=(10, 60),
                        ) as r,
                    ):
                        r.raise_for_status()
                        f.seek(cursor)

                        for data in r.iter_content(chunk_size):
                            if not data:
                                break

                            remaining = chunk_end - cursor + 1
                            if remaining <= 0:
                                break

                            if len(data) > remaining:
                                data = data[:remaining]

                            f.write(data)
                            n = len(data)
                            cursor += n
                            bytes_this_attempt += n

                            if cursor > pbar_counted_up_to:
                                new_bytes = cursor - pbar_counted_up_to
                                pbar.update(new_bytes)
                                pbar_counted_up_to = cursor

                    if cursor <= chunk_end:
                        raise requests.exceptions.ChunkedEncodingError(
                            f"chunk {chunk_start}-{chunk_end}: "
                            f"expected bytes up to {chunk_end}, got to {cursor - 1}"
                        )
                    return

                except Exception as exc:
                    if not _is_retryable(exc) or attempt == retries - 1:
                        raise

                    cursor -= bytes_this_attempt

                    wait = min(2**attempt, max_timeout)
                    logging.warning(
                        "chunk %d-%d failed (attempt %d/%d): %s. retrying in %.0fs",
                        chunk_start,
                        chunk_end,
                        attempt + 1,
                        retries,
                        exc,
                        wait,
                    )
                    time.sleep(wait)

        worker_chunk_size = content_length // num_workers

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures: list[Future[None]] = []
            for i in range(num_workers):
                c_start = i * worker_chunk_size
                c_end = (
                    c_start + worker_chunk_size - 1
                    if i < num_workers - 1
                    else content_length - 1
                )

                effective_start = max(c_start, already_downloaded)
                futures.append(pool.submit(_download_chunk, effective_start, c_end))

            for future in as_completed(futures):
                future.result()

        actual_size = dest_path.stat().st_size
        if actual_size != content_length:
            raise RuntimeError(
                f"downloaded file size {actual_size} does not match expected content length {content_length} for {url}"
            )


def exec_query_from_file(
    query_file: str | Path,
    db_file: str | Path | None = None,
    pg_dsn: str | None = None,
    conn: sqlite3.Connection | psycopg.Connection | None = None,
):
    is_custom_conn = conn is not None
    if not is_custom_conn:
        if pg_dsn:
            conn = psycopg.connect(pg_dsn)
        elif db_file:
            conn = sqlite3.connect(db_file)
        else:
            raise ValueError("provide either db_file (sqlite) or pg_dsn (postgres)")

    sql_dir = Path(__file__).resolve().parent.parent / "sql"
    query = (sql_dir / query_file).read_text()

    if isinstance(conn, psycopg.Connection):
        conn.cursor().execute(cast(LiteralString, query))
        conn.commit()
    elif isinstance(conn, sqlite3.Connection):
        conn.cursor().executescript(query)
        conn.commit()

    if not is_custom_conn:
        conn.close()
