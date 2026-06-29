import http
import sqlite3
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, LiteralString, cast

import httpx
import psycopg
from tqdm import tqdm

from main import logger

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUS_CODES
    return isinstance(
        exc,
        (httpx.ConnectError, httpx.TimeoutException, httpx.TransportError),
    )


def _with_retry[T](
    fn: Callable[[], T],
    num_retries: int,
    max_wait: int,
    description: str,
) -> T:
    for attempt in range(num_retries):
        try:
            return fn()
        except Exception as exc:
            if not _is_retryable(exc) or attempt == num_retries - 1:
                raise

            wait = min(2**attempt, max_wait)
            logger.warning(
                "%s (attempt %d/%d, retrying in %.0fs) - %s",
                description,
                attempt + 1,
                num_retries,
                wait,
                exc,
            )
            time.sleep(wait)

    raise RuntimeError(f"{description}: failed after {num_retries} retries")


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
    client: httpx.Client | None = None,
    **kwargs: Any,
) -> httpx.Response:
    close_client = client is None
    if client is None:
        client = httpx.Client()

    try:

        def _do() -> httpx.Response:
            r = client.request(
                method,
                url,
                params=params,
                headers=headers,
                json=json,
                timeout=timeout,
                **kwargs,
            )
            r.raise_for_status()
            return r

        return _with_retry(_do, num_retries, max_wait, f"send {method} {url}")
    finally:
        if close_client:
            client.close()


def download_file(
    url: str,
    dest_path: Path,
    *,
    chunk_size: int,
    num_workers: int,
    num_retries: int,
    timeout: int,
    max_wait: int,
    with_pbar: bool,
    validate_content_length: bool,
) -> Path:
    dest_path.parent.mkdir(exist_ok=True, parents=True)

    if num_workers <= 1:
        _download_simple(
            url,
            dest_path,
            chunk_size=chunk_size,
            num_retries=num_retries,
            timeout=timeout,
            max_wait=max_wait,
            with_pbar=with_pbar,
            validate_content_length=validate_content_length,
        )
    else:
        _download_multipart(
            url,
            dest_path,
            num_workers,
            chunk_size,
            num_retries,
            timeout,
            max_wait,
            with_pbar,
        )

    return dest_path


def _download_simple(
    url: str,
    dest_path: Path,
    *,
    chunk_size: int,
    num_retries: int,
    timeout: int,
    max_wait: int,
    with_pbar: bool = False,
    validate_content_length: bool = True,
) -> None:
    already_downloaded = dest_path.stat().st_size if dest_path.exists() else 0

    for attempt in range(num_retries):
        bytes_this_attempt = 0
        try:
            headers = {}
            if already_downloaded > 0:
                headers["Range"] = f"bytes={already_downloaded}-"

            with (
                httpx.Client() as client,
                client.stream("GET", url, headers=headers, timeout=timeout) as r,
            ):
                if r.status_code == http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                    return

                r.raise_for_status()

                content_length = int(r.headers.get("Content-Length", -1))
                if validate_content_length and content_length == -1:
                    raise ValueError(f"missing Content-Length header for {url}")

                if r.status_code == http.HTTPStatus.PARTIAL_CONTENT:
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

                with dest_path.open(open_mode) as f:
                    for chunk in r.iter_bytes(chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        n = len(chunk)
                        bytes_this_attempt += n
                        already_downloaded += n
                        if pbar:
                            pbar.update(n)

                actual_size = dest_path.stat().st_size
                if validate_content_length and actual_size != total:
                    raise OSError(
                        f"downloaded {actual_size} bytes, expected {total} for {url}",
                    )

                if pbar:
                    pbar.close()
        except Exception as exc:
            if not _is_retryable(exc) or attempt == num_retries - 1:
                raise

            already_downloaded -= bytes_this_attempt

            wait = min(2**attempt, max_wait)
            logger.warning(
                "failed to download %s (attempt %d/%d): %s. retrying in %.0fs",
                url,
                attempt + 1,
                num_retries,
                exc,
                wait,
            )
            time.sleep(wait)

    raise RuntimeError(f"failed after {num_retries} retries")


def _download_multipart(
    url: str,
    dest_path: Path,
    num_workers: int,
    chunk_size: int,
    num_retries: int,
    timeout: int,
    max_wait: int,
    with_pbar: bool,
) -> None:
    with httpx.Client() as client:

        def _head() -> httpx.Response:
            r = client.get(url, timeout=timeout)
            r.raise_for_status()
            return r

        r = _with_retry(_head, num_retries, max_wait, f"head {url}")
        content_length = int(r.headers.get("Content-Length", -1))
        accepts_range = r.headers.get("Accept-Ranges", "")

    if content_length == -1 or accepts_range == "":
        _download_simple(
            url,
            dest_path,
            chunk_size=chunk_size,
            num_retries=num_retries,
            timeout=timeout,
            max_wait=max_wait,
            with_pbar=with_pbar,
        )
        return

    already_downloaded = 0
    if dest_path.exists():
        already_downloaded = dest_path.stat().st_size
        if already_downloaded >= content_length:
            logger.info("skipping %s as it is already fully downloaded", dest_path.name)
            return

    if already_downloaded == 0:
        with dest_path.open("wb") as f:
            f.truncate(content_length)

    pbar = (
        tqdm(
            total=content_length,
            initial=already_downloaded,
            unit="B",
            unit_scale=True,
            desc=f"downloading {url[:100]}",
        )
        if with_pbar
        else None
    )

    def _download_chunk(chunk_start: int, chunk_end: int) -> None:
        if chunk_start > chunk_end:
            return

        cursor = chunk_start
        pbar_counted_up_to = chunk_start

        for attempt in range(num_retries):
            bytes_this_attempt = 0
            try:
                with (
                    httpx.Client() as chunk_client,
                    chunk_client.stream(
                        "GET",
                        url,
                        headers={"Range": f"bytes={cursor}-{chunk_end}"},
                        timeout=timeout,
                    ) as r,
                ):
                    r.raise_for_status()

                    with dest_path.open("r+b") as f:
                        f.seek(cursor)
                        for data in r.iter_bytes(chunk_size):
                            if not data:
                                break

                            remaining = chunk_end - cursor + 1
                            if remaining <= 0:
                                break

                            if len(data) > remaining:
                                chunk = data[:remaining]
                                data = chunk  # noqa: PLW2901

                            f.write(data)
                            n = len(data)
                            cursor += n
                            bytes_this_attempt += n

                            if pbar and cursor > pbar_counted_up_to:
                                new_bytes = cursor - pbar_counted_up_to
                                pbar.update(new_bytes)
                                pbar_counted_up_to = cursor

                if cursor <= chunk_end:
                    raise httpx.TransportError(
                        f"chunk {chunk_start}-{chunk_end}: expected bytes up to {chunk_end}, got to {cursor - 1}",  # noqa: E501
                    )
            except Exception as exc:
                if not _is_retryable(exc) or attempt == num_retries - 1:
                    raise

                cursor -= bytes_this_attempt

                wait = min(2**attempt, max_wait)
                logger.warning(
                    "chunk %d-%d failed (attempt %d/%d): %s. retrying in %.0fs",
                    chunk_start,
                    chunk_end,
                    attempt + 1,
                    num_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)

    worker_chunk_size = content_length // num_workers

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures: list[Any] = []
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
            f"downloaded file size mismatch: expected {content_length}, got {actual_size} for {url}",  # noqa: E501
        )

    if pbar:
        pbar.close()


def exec_query_from_file(
    query_file: str | Path,
    db_file: str | Path | None = None,
    pg_dsn: str | None = None,
    conn: Any | None = None,
) -> None:
    is_custom_conn = conn is not None
    if not is_custom_conn:
        if pg_dsn:
            conn = psycopg.connect(pg_dsn)
        elif db_file:
            conn = sqlite3.connect(db_file)
        else:
            msg = "provide either db_file (sqlite) or pg_dsn (postgres)"
            raise ValueError(msg)

    sql_dir = Path(__file__).resolve().parent.parent / "sql"
    query = (sql_dir / query_file).read_text()

    if isinstance(conn, sqlite3.Connection):
        conn.cursor().executescript(query)
    else:
        conn.cursor().execute(cast("LiteralString", query))
    conn.commit()

    if not is_custom_conn:
        conn.close()
