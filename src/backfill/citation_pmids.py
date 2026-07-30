"""Resolve the DOI in a GEO series' <Citation> to a PubMed PMID.

GEO records some papers only as a <Citation> DOI, never a <Pubmed-ID> (ingest
captures that raw text in geo_series.citation). This backfill pulls the DOI out
of that free text and resolves it to a PMID via NCBI esearch `<doi>[DOI]` — the
authoritative index lookup, NOT the SRA->PubMed elink used for pubmed_id_pysradb
(which returns *citing* papers). The result lands in geo_series.pubmed_id_citation
(json array), carried to Postgres as text[].

GEO-only: citation lives on geo_series alone. Resumable: every processed row is
stamped citation_pmid_checked, so a normal run skips it; --process-all re-checks
(useful when a DOI later gets a PubMed record). Idempotent.

Usage:
  ... backfill citation-pmids --geo-db-file geo.db --ncbi-keys-file keys.txt
"""

import json
import logging
import re
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

from tqdm import tqdm

from backfill.backfill_utils import _chunked, _parse_pmid
from helpers import sqlite_connect

logger = logging.getLogger(__name__)

ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
# DOI in free text; trailing sentence punctuation is trimmed after the match.
_DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"<>]+")
_TRAIL = ".,;:)]}>'\""
# ~10 req/s per NCBI api_key; one distinct key per worker, so throttle per worker.
_MIN_INTERVAL = 0.11

# GEO series that carry a citation but no resolved pmid_citation yet.
_MISSING_SQL = (
    "SELECT accession, citation FROM geo_series"
    " WHERE citation IS NOT NULL"
    " AND (pubmed_id_citation IS NULL"
    "      OR pubmed_id_citation = '' OR pubmed_id_citation = '[]')"
)


def _dois_from_citation(citation_json: str) -> list[str]:
    try:
        cites = json.loads(citation_json)
    except (json.JSONDecodeError, TypeError):
        return []
    out: list[str] = []
    for c in cites or []:
        for m in _DOI_RE.findall(c or ""):
            d = m.rstrip(_TRAIL)
            if d and d not in out:
                out.append(d)
    return out


def _esearch_doi(doi: str, api_key: str, retries: int, timeout: int) -> list[str]:
    q = urllib.parse.urlencode(
        {
            "db": "pubmed",
            "term": f"{doi}[DOI]",
            "retmode": "json",
            "tool": "seqout",
            "email": "seqout@gmail.com",
            "api_key": api_key,
        }
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                f"{ESEARCH}?{q}", headers={"User-Agent": "seqout-citation-pmids"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read())
            time.sleep(_MIN_INTERVAL)
            return data.get("esearchresult", {}).get("idlist", []) or []
        except Exception:  # noqa: BLE001 - transient net/parse: back off and retry
            time.sleep(min(2**attempt, 30))
    logger.warning("esearch gave up on doi %s", doi)
    return []


def _resolve_citation(
    citation_json: str, api_key: str, retries: int, timeout: int
) -> list[str]:
    pmids: list[str] = []
    for doi in _dois_from_citation(citation_json):
        for pid in _esearch_doi(doi, api_key, retries, timeout):
            p = _parse_pmid(pid)
            if p and p not in pmids:
                pmids.append(p)
    return pmids


def _worker(
    api_key: str,
    rows: list[tuple[str, str]],
    db_file: Path,
    batch_size: int,
    pbar: tqdm,
    retries: int,
    timeout: int,
) -> None:
    now = datetime.now(UTC).strftime("%Y-%m-%d")
    for batch in _chunked(rows, batch_size):
        checked: list[str] = []
        pmid_updates: list[tuple[str, str]] = []
        for acc, citation_json in batch:
            checked.append(acc)
            pmids = _resolve_citation(citation_json, api_key, retries, timeout)
            if pmids:
                pmid_updates.append((json.dumps(pmids), acc))

        with sqlite_connect(db_file, busy_timeout=5000) as conn:
            placeholders = ",".join("?" for _ in checked)
            conn.execute(
                f"UPDATE geo_series SET citation_pmid_checked = ?"  # noqa: S608
                f" WHERE accession IN ({placeholders})",
                (now, *checked),
            )
            if pmid_updates:
                conn.executemany(
                    "UPDATE geo_series SET pubmed_id_citation = ? WHERE accession = ?",
                    pmid_updates,
                )
            conn.commit()
        pbar.update(len(batch))


def backfill_citation_pmids(
    geo_db_file: Path,
    ncbi_keys_file: Path,
    num_workers: int,
    batch_size: int,
    is_dry_run: bool,  # noqa: FBT001
    retries: int,
    timeout: int,
    process_all: bool = False,  # noqa: FBT001, FBT002
) -> None:
    query = _MISSING_SQL
    if not process_all:
        query += " AND citation_pmid_checked IS NULL"

    with sqlite_connect(geo_db_file) as conn:
        rows: list[tuple[str, str]] = conn.execute(query).fetchall()

    logger.info("citation_pmids: %d GEO series with a citation to resolve", len(rows))
    if is_dry_run or not rows:
        return

    keys = [k.strip() for k in ncbi_keys_file.read_text().splitlines() if k.strip()]
    if not keys:
        msg = "ncbi_keys_file is empty"
        raise ValueError(msg)
    num_workers = len(keys)

    buckets: list[list[tuple[str, str]]] = [[] for _ in range(num_workers)]
    for i, row in enumerate(rows):
        buckets[i % num_workers].append(row)

    pbar = tqdm(total=len(rows), unit="acc", desc="citation_pmids")
    with ThreadPoolExecutor(num_workers) as pool:
        futures = [
            pool.submit(
                _worker,
                keys[i],
                buckets[i],
                geo_db_file,
                batch_size,
                pbar,
                retries,
                timeout,
            )
            for i in range(num_workers)
        ]
        for f in as_completed(futures):
            f.result()
    pbar.close()
