"""
Put each claimed PMID to the archive that owns the accession.

pysradb falls back to a PubMed title search and keeps the smallest PMID, which
is the oldest paper. An unvouched claim is usually the wrong one.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from helpers import exec_query_from_file, send_req, sqlite_connect

logger = logging.getLogger(__name__)

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
EPMC = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

PROMOTABLE = ("archive_link", "epmc_cites")

_BIOPROJECT_PREFIXES = ("PRJ",)


def api_keys() -> list[str]:
    """Every key available. NCBI meters per key, so the pool sets the ceiling."""
    path = os.environ.get("NCBI_KEYS_FILE")
    if path and Path(path).is_file():
        keys = [k.strip() for k in Path(path).read_text().splitlines() if k.strip()]
        if keys:
            return keys
    single = os.environ.get("NCBI_API_KEY")
    return [single] if single else []


class KeyRing:
    """Hand out keys round-robin so one key does not carry the whole pool."""

    def __init__(self, keys: list[str]) -> None:
        """Build the ring from the keys available."""
        self._keys = keys
        self._next = itertools.cycle(keys) if keys else None
        self._lock = threading.Lock()

    def take(self) -> str | None:
        """Return the next key, or None when running unauthenticated."""
        if self._next is None:
            return None
        with self._lock:
            return next(self._next)

    def __len__(self) -> int:
        """Count the keys in the ring."""
        return len(self._keys)


def _get_json(client: httpx.Client, url: str, params: dict, key: str | None) -> Any:
    if key:
        params = {**params, "api_key": key}
    resp = send_req(
        "GET", url, params=params, timeout=30, num_retries=4, max_wait=30, client=client
    )
    return resp.json()


def _geo_pmids(client: httpx.Client, accession: str, key: str | None) -> list[str]:
    """GEO's own publication field, authoritative for a GSE."""
    found = _get_json(
        client,
        f"{EUTILS}/esearch.fcgi",
        {"db": "gds", "term": f"{accession}[ACCN]", "retmode": "json"},
        key,
    )
    uids = found.get("esearchresult", {}).get("idlist", [])
    if not uids:
        return []
    summary = _get_json(
        client,
        f"{EUTILS}/esummary.fcgi",
        {"db": "gds", "id": uids[0], "retmode": "json"},
        key,
    )
    record = summary.get("result", {}).get(uids[0], {})
    return [str(p) for p in record.get("pubmedids", []) if str(p).isdigit()]


def _elink_pmids(
    client: httpx.Client, db: str, uids: list[str], key: str | None
) -> list[str]:
    if not uids:
        return []
    linked = _get_json(
        client,
        f"{EUTILS}/elink.fcgi",
        {"dbfrom": db, "db": "pubmed", "id": ",".join(uids), "retmode": "json"},
        key,
    )
    pmids: list[str] = []
    for linkset in linked.get("linksets", []):
        for db_link in linkset.get("linksetdbs", []):
            if db_link.get("dbto") == "pubmed":
                pmids.extend(str(i) for i in db_link.get("links", []))
    return pmids


def _sra_pmids(
    client: httpx.Client, accession: str, key: str | None
) -> tuple[list[str], str]:
    """BioProject is the archive's own link between a study and its paper."""
    db = "bioproject" if accession.startswith(_BIOPROJECT_PREFIXES) else "sra"
    found = _get_json(
        client,
        f"{EUTILS}/esearch.fcgi",
        {"db": db, "term": accession, "retmode": "json"},
        key,
    )
    uids = found.get("esearchresult", {}).get("idlist", [])[:5]
    return _elink_pmids(client, db, uids, key), db


def _epmc_pmids(client: httpx.Client, accession: str) -> list[str]:
    """Papers whose indexed text prints the accession."""
    resp = send_req(
        "GET",
        EPMC,
        params={
            "query": f'"{accession}"',
            "resultType": "lite",
            "format": "json",
            "pageSize": 25,
        },
        timeout=30,
        num_retries=3,
        max_wait=20,
        client=client,
    )
    hits = resp.json().get("resultList", {}).get("result", [])
    return [h["pmid"] for h in hits if str(h.get("pmid", "")).isdigit()]


def _judge(
    client: httpx.Client, accession: str, source: str, claimed: str, key: str | None
) -> dict:
    archive: list[str] = []
    route = ""
    try:
        if source == "geo":
            archive, route = _geo_pmids(client, accession, key), "geo_summary"
        elif source in {"sra", "ena"}:
            archive, route = _sra_pmids(client, accession, key)
    except (httpx.HTTPError, RuntimeError, ValueError, KeyError) as exc:
        logger.debug("%s archive lookup failed: %s", accession, exc)
        return {
            "verdict": "lookup_failed",
            "archive": None,
            "route": route,
            "epmc": None,
        }

    if archive:
        verdict = "archive_link" if claimed in archive else "archive_other"
        return {"verdict": verdict, "archive": archive, "route": route, "epmc": None}

    try:
        epmc = _epmc_pmids(client, accession)
    except (httpx.HTTPError, RuntimeError, ValueError) as exc:
        logger.debug("%s epmc lookup failed: %s", accession, exc)
        return {
            "verdict": "lookup_failed",
            "archive": archive,
            "route": route,
            "epmc": None,
        }

    verdict = "epmc_cites" if claimed in epmc else "unconfirmed"
    return {"verdict": verdict, "archive": archive, "route": route, "epmc": epmc}


def pmid_retry_verify(
    db_file: Path,
    num_workers: int = 6,
    source: str | None = None,
    *,
    recheck: bool = False,
) -> dict[str, int]:
    """Judge every unjudged claim in db_file and record the verdict."""
    exec_query_from_file("tables/pmid_retry.sql", db_file=db_file)
    # writes are serialised by `lock` below, so one connection serves the pool
    conn = sqlite_connect(db_file, check_same_thread=False)

    query = (
        "SELECT r.accession, r.source, r.pmid FROM pmid_retry_results r "
        "WHERE r.n_pmids > 0"
    )
    params: list[str] = []
    if not recheck:
        query += (
            " AND NOT EXISTS (SELECT 1 FROM pmid_retry_verification v"
            " WHERE v.accession = r.accession AND v.verdict != 'lookup_failed')"
        )
    if source:
        query += " AND r.source = ?"
        params.append(source)
    todo = conn.execute(query, params).fetchall()
    logger.info("verifying %d claims with %d workers", len(todo), num_workers)

    keys = KeyRing(api_keys())
    logger.info("using %d NCBI key(s)", len(keys))
    lock = threading.Lock()
    counts: dict[str, int] = {}
    client = httpx.Client(http2=False)

    def work(row: tuple[str, str, str]) -> None:
        accession, src, claimed = row
        result = _judge(client, accession, src, claimed, keys.take())
        with lock:
            conn.execute(
                "INSERT INTO pmid_retry_verification"
                " (accession, claimed_pmid, verdict, archive_pmids, archive_route,"
                "  epmc_pmids, verified_at)"
                " VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(accession) DO UPDATE SET"
                "  claimed_pmid=excluded.claimed_pmid, verdict=excluded.verdict,"
                "  archive_pmids=excluded.archive_pmids,"
                "  archive_route=excluded.archive_route,"
                "  epmc_pmids=excluded.epmc_pmids, verified_at=excluded.verified_at",
                (
                    accession,
                    claimed,
                    result["verdict"],
                    json.dumps(result["archive"])
                    if result["archive"] is not None
                    else None,
                    result["route"] or None,
                    json.dumps(result["epmc"]) if result["epmc"] is not None else None,
                    datetime.now(UTC).isoformat(),
                ),
            )
            counts[result["verdict"]] = counts.get(result["verdict"], 0) + 1
            total = sum(counts.values())
            if total % 500 == 0:
                conn.commit()
                logger.info("%d/%d verified", total, len(todo))

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            list(pool.map(work, todo))
    finally:
        conn.commit()
        client.close()
        conn.close()
    return counts
