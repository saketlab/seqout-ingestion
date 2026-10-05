#!/usr/bin/env python
"""
Merge fleet centre geocodes into a rebuild, through the plausibility gate.

The fleet worker returns whatever ArcGIS said. ArcGIS answers every query, so an
ungated merge is how "Guangdong Agriculture Industry Business Polytechnic" becomes
Nigeria (it matched the word "Polytechnic") and "Fuwai Central China
Cardiovascular Hospital" becomes the United States (it matched "Cardiovascular").
Those are the same failure that put the Chinese Academy of Agricultural Science in
Tbilisi, and the gate is the only thing standing between them and the corpus.

Three checks, all from src/geocode so there is one definition of each:

  * the centre name must carry a distinctive token at all; a name that is only
    generic words cannot be verified by any route
  * the answer must share a distinctive token with the query, or it is a
    coincidence rather than a match
  * the answer must not contradict a country named in the query itself

Transient failures are recorded separately rather than counted as answers. 3,059
of the errors here are HTTP 503s: "Hiroshima Univeristy" did not fail to geocode,
it failed to be asked, and treating that as unresolvable would retire the centre
from every future pass.

  geocode_centres_merge.py            # report only
  geocode_centres_merge.py --apply    # write into the rebuild (never Postgres)
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from geocode.geocode import (
    _canonical_country,
    _contradicts_named_country,
    _distinctive_tokens,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

#: Worth another pass; not evidence that the centre is ungeocodable.
_TRANSIENT = re.compile(
    r"HTTPError|TimeoutError|URLError|50\d|Temporarily", re.IGNORECASE
)


def gate(query: str, rec: dict) -> tuple[bool, str]:
    """Return (accept, reason)."""
    q_tokens = _distinctive_tokens(query)
    if not q_tokens:
        return False, "centre name has no distinctive token"
    place = rec.get("place_name") or ""
    if not _distinctive_tokens(place) & q_tokens:
        return False, "answer shares no distinctive token with the centre"
    if _contradicts_named_country(query, rec):
        return False, "answer contradicts a country named in the centre"
    if not _canonical_country(rec.get("country_code"), rec.get("country"))[1]:
        return False, "country will not canonicalise"
    return True, "accepted"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="yuj/geocode_centres/results")
    ap.add_argument(
        "--staged", default="/data/saket/rebuild/unified_metadata/unified_metadata.db"
    )
    ap.add_argument("--retry-out", default="/data/saket/geocode/centres_retry.txt")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    accepted: dict[str, tuple] = {}
    retry: list[str] = []
    reasons: collections.Counter = collections.Counter()

    for f in Path(args.results).glob("*.json"):
        try:
            rec = json.loads(f.read_text())
        except (json.JSONDecodeError, OSError):
            reasons["unreadable"] += 1
            continue
        query = rec.get("address") or ""
        err = rec.get("error")
        if err:
            if _TRANSIENT.search(str(err)):
                retry.append(query)
                reasons["transient, queued for retry"] += 1
            else:
                reasons["no result"] += 1
            continue
        ok, why = gate(query, rec)
        reasons[why] += 1
        if ok:
            code, name = _canonical_country(rec.get("country_code"), rec.get("country"))
            accepted[query] = (
                name,
                code,
                rec.get("place_name"),
                rec.get("city"),
                rec.get("state"),
                rec.get("latitude"),
                rec.get("longitude"),
                rec.get("geocode_score"),
            )

    logger.info("results read: %d", sum(reasons.values()))
    for why, n in reasons.most_common():
        logger.info("    %-46s %d", why, n)
    logger.info("")
    logger.info("centres ACCEPTED  : %d", len(accepted))
    logger.info("centres to RETRY  : %d", len(retry))

    # Postgres already holds a vetted country for most of the corpus, and a row
    # can be NULL here only because an earlier raw pass blanked it. Filling those
    # from a fresh lookup replaces a checked answer with an unchecked one: it put
    # Kyoto University in the United States and RIKEN in South Africa, because
    # "Kyoto" is a distinctive token and there are places called Kyoto in the US,
    # so the token gate passes on a coincidence. Only fill what Postgres cannot.
    with psycopg.connect(os.environ["PG_URL"]) as pg:
        vetted = {
            r[0]
            for r in pg.execute(
                "SELECT canonical_accession FROM unified_metadata "
                "WHERE country IS NOT NULL AND country <> ''"
            )
        }
    con = sqlite3.connect(args.staged)
    candidates = [
        (acc, centre)
        for acc, centre in con.execute(
            "SELECT canonical_accession, center_name FROM unified_metadata "
            "WHERE country IS NULL AND center_name IS NOT NULL AND center_name <> ''"
        )
        if centre in accepted
    ]
    todo = [(a, c) for a, c in candidates if a not in vetted]
    logger.info(
        "candidates %d; postgres already answers for %d, so they are left alone",
        len(candidates),
        len(candidates) - len(todo),
    )
    logger.info("studies it would fill: %d", len(todo))

    if not args.apply:
        logger.info("")
        logger.info("DRY RUN -- nothing written. Re-run with --apply.")
        return 0

    Path(args.retry_out).write_text("\n".join(sorted(set(retry))) + "\n")
    con.executemany(
        "UPDATE unified_metadata SET country = ?, country_code = ?, place_name = ?, "
        "city = ?, state = ?, latitude = ?, longitude = ?, geocode_score = ? "
        "WHERE canonical_accession = ?",
        [(*accepted[c], a) for a, c in todo],
    )
    con.commit()
    filled = con.execute(
        "SELECT count(*) FROM unified_metadata WHERE country IS NOT NULL"
    ).fetchone()[0]
    logger.info("applied; rebuild rows with a country: %d", filled)
    logger.info("retry list written to %s", args.retry_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
