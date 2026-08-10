import contextlib
import ftplib
import http
import io
import logging
import socket
import sqlite3
import time
import zlib
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any, LiteralString, cast
from urllib.parse import urlparse

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

import httpx
import psycopg
import socks
from tqdm import tqdm

logger = logging.getLogger(__name__)

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
                httpx.Client(follow_redirects=True) as client,
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


class FileTooLargeError(Exception):
    """File exceeds max_bytes, skipped without downloading (not a matrix)."""


def download_file_ftp(
    url: str,
    dest_path: Path,
    *,
    chunk_size: int,
    num_retries: int,
    timeout: int,
    max_wait: int,
    with_pbar: bool = False,
    socks_proxy: str | None = None,
    max_bytes: int | None = None,
) -> Path:
    dest_path.parent.mkdir(exist_ok=True, parents=True)

    parsed = urlparse(url)
    if not parsed.hostname or not parsed.path:
        raise ValueError(f"invalid FTP URL: {url}")

    orig_socket = socket.socket
    if socks_proxy:
        phost, _, pport = socks_proxy.partition(":")
        socks.set_default_proxy(socks.SOCKS5, phost, int(pport or "1080"))
        socket.socket = socks.socksocket

    try:
        for attempt in range(num_retries):
            try:
                ftp = ftplib.FTP(parsed.hostname, timeout=timeout)  # noqa: S321
                ftp.login("anonymous", "guest@")
                ftp.set_pasv(True)
                ftp.voidcmd("TYPE I")

                total = ftp.size(parsed.path)
                if max_bytes is not None and total is not None and total > max_bytes:
                    ftp.quit()
                    msg = f"{url} is {total} bytes (> {max_bytes} cap)"
                    raise FileTooLargeError(msg)
                pbar = (
                    tqdm(
                        total=total,
                        unit="B",
                        unit_scale=True,
                        desc=f"downloading {url[:100]}",
                    )
                    if with_pbar
                    else None
                )

                with dest_path.open("wb") as f:
                    written = 0

                    def write_cb(data: bytes, pbar: tqdm | None = pbar) -> None:
                        nonlocal written
                        f.write(data)
                        written += len(data)
                        # cap mid-stream too; some servers don't answer SIZE
                        if max_bytes is not None and written > max_bytes:
                            msg = f"{url} exceeds {max_bytes} cap mid-download"
                            raise FileTooLargeError(msg)
                        if pbar:
                            pbar.update(len(data))

                    ftp.retrbinary(
                        f"RETR {parsed.path}", write_cb, blocksize=chunk_size
                    )

                if pbar:
                    pbar.close()

                ftp.quit()
            except FileTooLargeError:
                if dest_path.exists():
                    dest_path.unlink()
                raise
            except ftplib.error_perm:
                if dest_path.exists():
                    dest_path.unlink()
                raise
            except Exception as exc:
                if dest_path.exists():
                    dest_path.unlink()
                if attempt == num_retries - 1:
                    raise
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
            else:
                return dest_path

        raise RuntimeError(f"failed after {num_retries} retries")
    finally:
        socket.socket = orig_socket


class _HeadComplete(Exception):  # noqa: N818
    """Internal: enough decompressed bytes read; abort the transfer."""


def _make_head_sink(
    f: Any, state: dict[str, Any], max_decompressed: int
) -> Callable[[bytes], None]:
    """Gunzip a .gz stream and abort once max_decompressed bytes are written."""

    def sink(data: bytes) -> None:
        if not state["checked"]:
            state["checked"] = True
            if data[:2] == b"\x1f\x8b":  # gzip magic
                state["dec"] = zlib.decompressobj(zlib.MAX_WBITS | 16)
        out = state["dec"].decompress(data) if state["dec"] else data
        if out:
            f.write(out)
            state["written"] += len(out)
        if state["written"] >= max_decompressed:
            raise _HeadComplete

    return sink


def download_http_head(
    url: str,
    dest_path: Path,
    *,
    max_decompressed: int,
    num_retries: int,
    timeout: int,
    max_wait: int,
) -> Path:
    """
    HTTP twin of download_ftp_head: first max_decompressed bytes over HTTPS.

    EBI/ArrayExpress serves supplementary files over HTTPS only.
    """
    dest_path.parent.mkdir(exist_ok=True, parents=True)
    for attempt in range(num_retries):
        state: dict[str, Any] = {"written": 0, "dec": None, "checked": False}
        try:
            with (
                dest_path.open("wb") as f,
                httpx.Client(timeout=timeout, follow_redirects=True) as client,
                client.stream("GET", url) as resp,
            ):
                resp.raise_for_status()
                sink = _make_head_sink(f, state, max_decompressed)
                with contextlib.suppress(_HeadComplete):
                    for chunk in resp.iter_bytes(1 << 20):
                        sink(chunk)
        except httpx.HTTPStatusError:
            dest_path.unlink(missing_ok=True)
            raise
        except Exception as exc:
            dest_path.unlink(missing_ok=True)
            if attempt == num_retries - 1:
                raise
            wait = min(2**attempt, max_wait)
            logger.warning(
                "http head fetch failed %s (attempt %d/%d): %s. retrying in %.0fs",
                url,
                attempt + 1,
                num_retries,
                exc,
                wait,
            )
            time.sleep(wait)
        else:
            return dest_path
    raise RuntimeError(f"http head fetch failed after {num_retries} retries")


@contextlib.contextmanager
def ftp_data_stream(
    url: str,
    *,
    timeout: int,
    socks_proxy: str | None = None,
) -> "Iterator[Any]":
    """
    Yield a readable binary stream of an FTP file's bytes.

    Closing early aborts the transfer; reading a tar's first members pulls only those.
    """
    parsed = urlparse(url)
    if not parsed.hostname or not parsed.path:
        raise ValueError(f"invalid FTP URL: {url}")
    orig_socket = socket.socket
    if socks_proxy:
        phost, _, pport = socks_proxy.partition(":")
        socks.set_default_proxy(socks.SOCKS5, phost, int(pport or "1080"))
        socket.socket = socks.socksocket
    ftp = None
    try:
        ftp = ftplib.FTP(parsed.hostname, timeout=timeout)  # noqa: S321
        ftp.login("anonymous", "guest@")
        ftp.voidcmd("TYPE I")
        conn = ftp.transfercmd(f"RETR {parsed.path}")
        stream = conn.makefile("rb")
        try:
            yield stream
        finally:
            with contextlib.suppress(Exception):
                stream.close()
            with contextlib.suppress(Exception):
                conn.close()
    finally:
        socket.socket = orig_socket
        if ftp is not None:
            with contextlib.suppress(Exception):
                ftp.close()


def download_ftp_head(
    url: str,
    dest_path: Path,
    *,
    max_decompressed: int,
    num_retries: int,
    timeout: int,
    max_wait: int,
    socks_proxy: str | None = None,
) -> Path:
    """
    Fetch only the first max_decompressed bytes of a file's content.

    Gunzips a .gz stream and aborts once enough is written; dest_path gets the
    decompressed head, so the sniff/count path is unchanged.
    """
    dest_path.parent.mkdir(exist_ok=True, parents=True)
    parsed = urlparse(url)
    if not parsed.hostname or not parsed.path:
        raise ValueError(f"invalid FTP URL: {url}")

    orig_socket = socket.socket
    if socks_proxy:
        phost, _, pport = socks_proxy.partition(":")
        socks.set_default_proxy(socks.SOCKS5, phost, int(pport or "1080"))
        socket.socket = socks.socksocket

    try:
        for attempt in range(num_retries):
            state: dict[str, Any] = {"written": 0, "dec": None, "checked": False}
            try:
                ftp = ftplib.FTP(parsed.hostname, timeout=timeout)  # noqa: S321
                ftp.login("anonymous", "guest@")
                ftp.set_pasv(True)
                ftp.voidcmd("TYPE I")

                with dest_path.open("wb") as f:
                    sink = _make_head_sink(f, state, max_decompressed)
                    with contextlib.suppress(_HeadComplete):
                        ftp.retrbinary(f"RETR {parsed.path}", sink, blocksize=1 << 20)
                with contextlib.suppress(Exception):
                    ftp.close()
            except ftplib.error_perm:
                dest_path.unlink(missing_ok=True)
                raise
            except Exception as exc:
                dest_path.unlink(missing_ok=True)
                if attempt == num_retries - 1:
                    raise
                wait = min(2**attempt, max_wait)
                logger.warning(
                    "head fetch failed %s (attempt %d/%d): %s. retrying in %.0fs",
                    url,
                    attempt + 1,
                    num_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)
            else:
                return dest_path

        raise RuntimeError(f"head fetch failed after {num_retries} retries")
    finally:
        socket.socket = orig_socket


class HttpRangeReader(io.RawIOBase):
    """
    A seekable read-only file over HTTP range requests.

    Lets h5py read an .h5/.h5ad shape while fetching only the byte ranges HDF5
    touches, instead of downloading the whole file.
    """

    def __init__(self, client: httpx.Client, url: str, size: int) -> None:  # noqa: D107
        self._client = client
        self._url = url
        self._size = size
        self._pos = 0

    def seekable(self) -> bool:  # noqa: D102
        return True

    def readable(self) -> bool:  # noqa: D102
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:  # noqa: D102
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        return self._pos

    def tell(self) -> int:  # noqa: D102
        return self._pos

    def readinto(self, b: "WriteableBuffer") -> int:  # noqa: D102
        # h5py's fileobj driver calls readinto, not read.
        mv = memoryview(b).cast("B")
        if self._pos >= self._size or len(mv) == 0:
            return 0
        end = min(self._pos + len(mv), self._size) - 1
        resp = self._client.get(
            self._url, headers={"Range": f"bytes={self._pos}-{end}"}
        )
        resp.raise_for_status()
        data = resp.content
        n = len(data)
        mv[:n] = data
        self._pos += n
        return n


def open_http_range(url: str, *, timeout: int = 30) -> HttpRangeReader | None:
    """Open url for random access via HTTP range, or None if unsupported."""
    client = httpx.Client(timeout=timeout, follow_redirects=True)
    try:
        head = client.head(url)
        head.raise_for_status()
        length = head.headers.get("Content-Length")
        accepts = head.headers.get("Accept-Ranges", "")
        if length is None or accepts.lower() == "none":
            client.close()
            return None
        return HttpRangeReader(client, url, int(length))
    except (httpx.HTTPError, ValueError):
        client.close()
        return None


def sqlite_connect(
    db_file: str | Path,
    busy_timeout: int = 120000,
    **kwargs: Any,
) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_file), **kwargs)
    conn.execute(f"PRAGMA busy_timeout={busy_timeout}")
    return conn


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
            conn = sqlite_connect(db_file)
        else:
            msg = "provide either db_file (sqlite) or pg_dsn (postgres)"
            raise ValueError(msg)

    sql_dir = Path(__file__).resolve().parent.parent / "sql"
    query = (sql_dir / query_file).read_text()

    if not isinstance(conn, sqlite3.Connection | psycopg.Connection):
        msg = "conn must be a sqlite3 or psycopg connection"
        raise TypeError(msg)

    if isinstance(conn, sqlite3.Connection):
        conn.cursor().executescript(query)
    else:
        conn.cursor().execute(cast("LiteralString", query))
    conn.commit()

    if not is_custom_conn:
        conn.close()


def batch_query_in(
    conn: sqlite3.Connection,
    query_prefix: str,
    items: list[str],
    batch_size: int,
    query_suffix: str = "",
) -> list[tuple]:
    results = []
    chunk_size = min(batch_size, 999)  # sqlite's max variable number

    for i in range(0, len(items), chunk_size):
        chunk = items[i : i + chunk_size]
        ph = ", ".join(["?"] * len(chunk))
        results.extend(
            conn.execute(f"{query_prefix} IN ({ph}) {query_suffix}", chunk).fetchall(),
        )
    return results
