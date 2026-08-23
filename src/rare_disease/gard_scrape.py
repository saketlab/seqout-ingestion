"""
Scrape GARD mode-of-inheritance labels from rendered disease pages.

The trimmed JSON has empty tagsCause values; per-disease pages expose rendered
inheritance buttons. Empty scrape results leave existing values intact.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

from helpers import send_req, sqlite_connect

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

logger = logging.getLogger(__name__)

PAGE_URL = "https://rarediseases.info.nih.gov/diseases/{gard_id}/{slug}"

#: Rendered component and labels inside it. Element name is stable; _ngcontent
#: class hashes change every build.
_SECTION = re.compile(r"<inheritance[^>]*>(.*?)</inheritance>", re.DOTALL)
_LABEL = re.compile(r"<button[^>]*>\s*([^<]+?)\s*</button>", re.DOTALL)

#: GARD button labels mapped to the shared HPO/Orphanet vocabulary. FAQ
#: buttons share the same shape; unmapped labels are logged and dropped.
GARD_MODES = {
    "autosomal dominant": "Autosomal dominant",
    "autosomal recessive": "Autosomal recessive",
    "x-linked": "X-linked",
    "x-linked dominant": "X-linked dominant",
    "x-linked recessive": "X-linked recessive",
    "y-linked": "Y-linked",
    "mitochondrial": "Mitochondrial",
    "multifactorial": "Multifactorial",
    "not applicable": "Not applicable",
    "isolated cases": "Sporadic",
    "sporadic": "Sporadic",
}

#: Rows per commit. Small enough that a kill loses little, large enough to
#: keep the write off the critical path.
_BATCH = 200

_UPDATE = """
UPDATE gard_disease SET inheritance = ?, inheritance_fetched_at = ?
WHERE gard_id = ?
"""


def modes_from_page(html: str) -> tuple[list[str], set[str]]:
    """Pull the inheritance labels out of one page, plus anything unrecognised."""
    section = _SECTION.search(html)
    if not section:
        return [], set()
    modes: set[str] = set()
    unmapped: set[str] = set()
    for raw in _LABEL.findall(section.group(1)):
        label = " ".join(raw.split())
        mapped = GARD_MODES.get(label.lower())
        if mapped:
            modes.add(mapped)
        else:
            unmapped.add(label)
    return sorted(modes), unmapped


def _slug(name: str) -> str:
    """GARD's url slug: lowercase, non-alphanumerics collapsed to hyphens."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _has_column(conn: sqlite3.Connection, name: str) -> bool:
    return any(
        row[1] == name for row in conn.execute("PRAGMA table_info(gard_disease)")
    )


def _scrape_one(
    client: httpx.Client, gard_id: str, slug: str, timeout: int, num_retries: int
) -> tuple[str, list[str], str]:
    """Return (gard_id, modes, outcome). The outcome separates the empty cases."""
    # gard_id is stored as GARD:5528; the url wants the bare number.
    # Passing the curie 404s every page and reports zero hits.
    url = PAGE_URL.format(gard_id=gard_id.removeprefix("GARD:"), slug=slug)
    try:
        response = send_req(
            "GET",
            url,
            headers={"User-Agent": "seqout-ingestion (+https://seqout.org)"},
            timeout=timeout,
            num_retries=num_retries,
            max_wait=60,
            client=client,
            follow_redirects=True,
        )
    except Exception:  # one dead page must not end the run
        # One dead page records a failure and the run continues.
        logger.debug("gard inheritance: %s unreachable", gard_id)
        # A network failure must not be recorded as an absent inheritance.
        return gard_id, [], "unreachable"
    modes, unmapped = modes_from_page(response.text)
    if unmapped:
        logger.debug("gard inheritance: %s unmapped labels %s", gard_id, unmapped)
    return gard_id, modes, "ok" if modes else "no_section"


def scrape_inheritance(
    gard_db_file: Path,
    workers: int = 8,
    timeout: int = 60,
    num_retries: int = 3,
    *,
    refresh: bool = False,
) -> None:
    """
    Fill gard_disease.inheritance from GARD's per-disease pages.

    Rows with a value are skipped unless `refresh`; workers stay low for 6,265 NIH
    page requests.
    """
    with sqlite_connect(gard_db_file) as conn:
        # Add the column in place: it arrived after the table shipped, and a rebuild
        # would refetch the whole catalogue to gain one field.
        for column in ("inheritance", "inheritance_fetched_at"):
            if not _has_column(conn, column):
                conn.execute(f"ALTER TABLE gard_disease ADD COLUMN {column} TEXT")
        conn.commit()
        # Resume on the timestamp. `inheritance IS NULL` re-scraped the empty
        # pages on every refresh. Empty string means found none, NULL means
        # unvisited.
        where = "" if refresh else " WHERE inheritance_fetched_at IS NULL"
        todo = conn.execute(
            "SELECT gard_id, gard_name FROM gard_disease" + where  # noqa: S608
        ).fetchall()

    if not todo:
        logger.info("gard inheritance: nothing to do")
        return

    logger.info("gard inheritance: %d diseases to scrape", len(todo))
    fetched_at = datetime.now(UTC).isoformat()
    outcomes: Counter[str] = Counter()
    batch: list[tuple[str, str, str]] = []
    written = 0

    # Batch writes keep completed pages on disk during a long run; the
    # resume clause can use them after a kill.
    with (
        sqlite_connect(gard_db_file) as conn,
        httpx.Client() as client,
        ThreadPoolExecutor(max_workers=workers) as pool,
    ):
        futures = [
            pool.submit(_scrape_one, client, gid, _slug(name), timeout, num_retries)
            for gid, name in todo
        ]
        for done, future in enumerate(as_completed(futures), start=1):
            gard_id, modes, outcome = future.result()
            outcomes[outcome] += 1
            # "unreachable" stays unwritten for retry; rendered no-mode pages are
            # recorded.
            if outcome != "unreachable":
                batch.append((";".join(modes), fetched_at, gard_id))
            if len(batch) >= _BATCH:
                conn.executemany(_UPDATE, batch)
                conn.commit()
                written += len(batch)
                batch = []
            if done % 1000 == 0:
                logger.info("gard inheritance: %d/%d scraped", done, len(todo))
        if batch:
            conn.executemany(_UPDATE, batch)
            conn.commit()
            written += len(batch)

    logger.info(
        "gard inheritance: %d rows written (%d with a mode, %d rendered none); "
        "%d unreachable and left for the next run",
        written,
        outcomes["ok"],
        outcomes["no_section"],
        outcomes["unreachable"],
    )
    if outcomes["unreachable"] > len(todo) // 10:
        logger.warning(
            "over 10%% of pages were unreachable -- check the url shape before "
            "trusting this run"
        )
