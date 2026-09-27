"""
Resolve BioProject -> SRA study for PRJ rows that look empty.

Some submissions get two BioProject accessions; only the SRA XML one reaches
sra_studies. Use elink bioproject_sra_all; esearch db=sra [BioProject] indexes
only the submitting accession and misses these.
"""

import argparse
import itertools
import logging
import os
import re
import sys
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import psycopg
from psycopg.rows import dict_row

from helpers import exec_query_from_file, send_req

logger = logging.getLogger(__name__)

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
_COMMON = {"tool": "seqout", "email": "seqout@gmail.com"}
# NCBI allows 10 req/s per key, 3 without.
_INTERVAL = 0.11
_BATCH = 150
_STUDY_RE = re.compile(r'<Study[^>]*\bacc="([SED]RP\d+)"')

_keys: list[str] = []
_key_slot = threading.local()
_slot_lock = threading.Lock()
_next_slot = itertools.count()


def load_keys(path: str | None) -> list[str]:
    """One API key per line; empty means anonymous."""
    if not path or not os.path.exists(path):
        return []
    with open(path) as fh:
        return [ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")]


def _my_key() -> str | None:
    """Pin one key per thread so a worker never shares another's rate budget."""
    if not _keys:
        return None
    if not hasattr(_key_slot, "key"):
        with _slot_lock:
            _key_slot.key = _keys[next(_next_slot) % len(_keys)]
    return _key_slot.key


CANDIDATES_SQL = """
SELECT es.study_accession
FROM ena_studies es
LEFT JOIN bioproject_sra_links l ON l.bioproject = es.study_accession
WHERE es.study_accession LIKE 'PRJN%%'
  AND NOT EXISTS (
      SELECT 1 FROM ena_experiments e
      WHERE e.study_accession = es.study_accession)
  AND (l.bioproject IS NULL OR l.checked_at < now() - %(stale)s::interval)
ORDER BY es.study_accession
"""


def _post(path: str, params: list[tuple[str, str]]) -> bytes:
    if key := _my_key():
        params = [*params, ("api_key", key)]
    params = [*params, *_COMMON.items()]
    data = urllib.parse.urlencode(params, doseq=True).encode()
    r = send_req(
        "POST",
        f"{EUTILS}/{path}",
        data=data,
        timeout=120,
        num_retries=4,
        max_wait=30,
    )
    time.sleep(_INTERVAL)
    return r.content


def _uid(accession: str) -> str:
    """BioProject UID is the numeric part of the accession."""
    return accession[5:]


def _linked_sra_uids(bioprojects: list[str]) -> dict[str, list[str]]:
    """BioProject accession -> SRA UIDs NCBI links to it."""
    uid_to_acc = {_uid(a): a for a in bioprojects}
    params = [
        ("dbfrom", "bioproject"),
        ("db", "sra"),
        ("linkname", "bioproject_sra_all"),
        ("retmode", "xml"),
    ]
    params += [("id", u) for u in uid_to_acc]
    xml = ET.fromstring(_post("elink.fcgi", params))
    out: dict[str, list[str]] = {}
    for ls in xml.findall("./LinkSet"):
        uid = ls.findtext("./IdList/Id") or ""
        acc = uid_to_acc.get(uid)
        if not acc:
            continue
        out[acc] = [e.text or "" for e in ls.findall("./LinkSetDb/Link/Id")]
    return out


def _studies_of_sra_uids(sra_uids: list[str]) -> dict[str, str]:
    """SRA UID -> study accession (SRP/ERP/DRP), from esummary ExpXml."""
    out: dict[str, str] = {}
    for i in range(0, len(sra_uids), 200):
        chunk = sra_uids[i : i + 200]
        xml = ET.fromstring(
            _post(
                "esummary.fcgi",
                [("db", "sra"), ("retmode", "xml"), ("id", ",".join(chunk))],
            )
        )
        for d in xml.iter("DocSum"):
            uid = d.findtext("./Id") or ""
            exp = ""
            for item in d.findall("./Item"):
                if item.get("Name") == "ExpXml":
                    exp = item.text or ""
            # ExpXml is escaped XML; regex the study accession out of it
            if m := _STUDY_RE.search(exp):
                out[uid] = m.group(1)
    return out


def _chunks(seq: list[str], n: int) -> Iterator[list[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def run(
    dsn: str, *, limit: int | None, stale: str, only: list[str] | None, workers: int = 6
) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        exec_query_from_file("tables/bioproject_sra_links_pg.sql", conn=conn)
        if only:
            candidates = only
        else:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(CANDIDATES_SQL, {"stale": stale})
                candidates = [r["study_accession"] for r in cur.fetchall()]
            if limit:
                candidates = candidates[:limit]
        logger.info("%d candidate BioProjects", len(candidates))

        found = 0
        done = 0

        def _resolve(batch: list[str]) -> list[tuple]:
            """One batch -> upsert rows. Network only; shares no DB handle."""
            links = _linked_sra_uids(batch)
            head_uids = [uids[0] for uids in links.values() if uids]
            uid_study = _studies_of_sra_uids(head_uids) if head_uids else {}
            rows = []
            for acc in batch:
                uids = links.get(acc, [])
                study = uid_study.get(uids[0]) if uids else None
                rows.append((acc, study, [study] if study else None, len(uids)))
            return rows

        batches = list(_chunks(candidates, _BATCH))
        # latency-bound; keep workers <= key count
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for rows in pool.map(_resolve, batches):
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO bioproject_sra_links AS l"
                        " (bioproject, canonical_study, resolved_studies,"
                        "  sra_record_count, checked_at)"
                        " VALUES (%s, %s, %s, %s, now())"
                        " ON CONFLICT (bioproject) DO UPDATE SET"
                        "  canonical_study ="
                        "      COALESCE(EXCLUDED.canonical_study, l.canonical_study),"
                        "  resolved_studies ="
                        "      COALESCE(EXCLUDED.resolved_studies, l.resolved_studies),"
                        "  sra_record_count ="
                        "      GREATEST(EXCLUDED.sra_record_count, l.sra_record_count),"
                        "  checked_at = now()",
                        rows,
                    )
                done += len(rows)
                found += sum(1 for r in rows if r[3])
                logger.info(
                    "  %d/%d checked, %d with SRA data", done, len(candidates), found
                )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    ap.add_argument("--limit", type=int)
    ap.add_argument("--stale", default="180 days")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument(
        "--ncbi-keys-file",
        default=os.environ.get(
            "NCBI_KEYS_FILE",
            os.path.join(
                os.environ.get("DATA_DIR", "/data/share/seqout"), "keys/ncbi.txt"
            ),
        ),
    )
    ap.add_argument("--only", nargs="*", help="specific BioProject accessions")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if not a.dsn:
        sys.exit("set DATABASE_URL or pass --dsn")
    _keys.extend(load_keys(a.ncbi_keys_file))
    logger.info("%d NCBI keys, %d workers", len(_keys), a.workers)
    run(a.dsn, limit=a.limit, stale=a.stale, only=a.only, workers=a.workers)


if __name__ == "__main__":
    main()
