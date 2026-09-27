#!/usr/bin/env python
"""
Geocode the postal address the depositor actually submitted.

`geo_contributors` carries organization, street, city, state, postcode and
country per study, and `arrayexpress_experiments.organizations` carries the same
as free text. The pipeline used only `address_country`, and only to veto a
result, throwing away the sharpest evidence available and geocoding an
ambiguous institution name instead: "AstraZeneca" resolved to Bengaluru when
every address on those studies says Cambridge, Moelndal or Gaithersburg.

An address needs none of the machinery a bare name does: it names the place
outright. The depositor also states a country in a separate field, but that
field is not authoritative; where the two disagree it is usually the field
that is wrong, so the geocoder's answer is kept and the disagreement is
recorded in declared_cc rather than used to discard the row.

Keyed on the distinct address, not the study, so one lookup serves every study
sharing it. Resumable by output.

  geocode_addresses.py [--db FILE] [--limit N] [--delay S]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import psycopg
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import ArcGIS

from geocode.geocode import (
    _COMPATIBLE,
    GeocodingProviders,
    _canonical_country,
    _geocode,
    _parse_arcgis,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

#: Below this an "address" is a fragment, not a place.
_MIN_ADDRESS_LEN = 12

DDL = """
CREATE TABLE IF NOT EXISTS address_geocode (
    address_key   TEXT PRIMARY KEY,
    query         TEXT,
    declared_cc   TEXT,
    country       TEXT,
    country_code  TEXT,
    place_name    TEXT,
    city          TEXT,
    state         TEXT,
    latitude      REAL,
    longitude     REAL,
    geocode_score REAL,
    source        TEXT,
    error         TEXT,
    checked_at    TEXT NOT NULL
);
"""

# ArrayExpress stores the same evidence as free text on the experiment rather
# than as parsed fields. No declared country to check against, so these are
# geocoded without that guard, still far better than geocoding a bare name.
AE_SQL = """
SELECT organizations FROM arrayexpress_experiments WHERE organizations IS NOT NULL
"""

ADDRESS_SQL = """
SELECT COALESCE(organization,''), COALESCE(address_line,''), COALESCE(address_city,''),
       COALESCE(address_state,''), COALESCE(address_zipcode,''),
       COALESCE(address_country,''), COUNT(*)
FROM geo_contributors
WHERE address_city IS NOT NULL AND address_city <> ''
GROUP BY 1,2,3,4,5,6
ORDER BY COUNT(*) DESC
"""


def compose(
    org: str, line: str, city: str, state: str, zipcode: str, country: str
) -> str:
    """One search string from the parts, most specific first."""
    parts = [p.strip(" ,") for p in (org, line, city, state, zipcode, country)]
    return ", ".join(p for p in parts if p)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/data/saket/geocode/address_geocode.db")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--delay", type=float, default=0.15)
    ap.add_argument("--include-ae", action="store_true")
    # Concurrency helps only up to the provider's own ceiling: measured,
    # twelve workers returned about 2.6x one worker and six about 2.0x, so
    # the throttle is per client, not per connection.
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.executescript(DDL)
    done = {r[0] for r in conn.execute("SELECT address_key FROM address_geocode")}

    with psycopg.connect(os.environ["PG_URL"]) as pg:
        rows = pg.execute(ADDRESS_SQL).fetchall()
        ae_rows = pg.execute(AE_SQL).fetchall() if args.include_ae else []

    # AE addresses arrive as free text with no parsed country, so they enter as
    # a query with an empty declared country: geocoded, but not country-checked.
    seen_ae: set[str] = set()
    for (orgs,) in ae_rows:
        payload = orgs if isinstance(orgs, list) else json.loads(orgs or "[]")
        for entry in payload or ():
            addr = ((entry or {}).get("address") or "").strip()
            if len(addr) > _MIN_ADDRESS_LEN and addr not in seen_ae:
                seen_ae.add(addr)
                rows.append(("", addr, "", "", "", "", 1))
    logger.info(
        "distinct submitted addresses: %d (%d already done)", len(rows), len(done)
    )

    todo = [r for r in rows if compose(*r[:6]) not in done]
    if args.limit:
        todo = todo[: args.limit]
    logger.info("to geocode: %d", len(todo))

    geocoders = [
        RateLimiter(
            ArcGIS(timeout=30).geocode,
            min_delay_seconds=args.delay,
            max_retries=3,
        )
        for _ in range(max(1, args.workers))
    ]
    t0, n, ok, mismatched = time.time(), 0, 0, 0

    def fetch(item: tuple) -> tuple:
        idx, row = item
        query = compose(*row[:6])
        geocoder = geocoders[idx % len(geocoders)]
        try:
            r = _geocode(query, geocoder, _parse_arcgis, GeocodingProviders.Arcgis)
        except Exception as exc:
            r = {"error": f"{type(exc).__name__}: {exc}"}
        return query, _canonical_country(None, row[5])[0], r or {"error": "no result"}

    # Only this thread touches SQLite; the pool does nothing but fetch.
    pool = ThreadPoolExecutor(max(1, args.workers))
    for query, declared, r in pool.map(fetch, enumerate(todo)):
        err = r.get("error")
        got_cc = (r.get("country_code") or "").lower()
        # The address states its country. An answer that disagrees is wrong by
        # construction: no gate, no heuristic, no judgement call.
        # Hong Kong, Macau and Taiwan have their own codes but a depositor
        # writing "China" is not contradicted by an answer of "hk". Reuse the
        # same compatibility map the name gate uses rather than inventing a
        # second notion of which countries count as the same.
        # The declared country is recorded, not enforced: it is a separate
        # field the depositor selects, and a disagreement usually means that
        # field is wrong rather than the geocoder ("Heidelberg, Deutschland"
        # declared as us, "Oak Ridge, Tennessee" declared as nz). The geocoder
        # reads the whole address, the richer evidence, so its answer is kept
        # and the disagreement stays visible in declared_cc for review.
        allowed = _COMPATIBLE.get(declared, {declared}) if declared else set()
        if not err and declared and got_cc and got_cc not in allowed:
            mismatched += 1
        if not err:
            ok += 1
        conn.execute(
            "INSERT OR REPLACE INTO address_geocode "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                query,
                query,
                declared,
                None if err else r.get("country"),
                None if err else r.get("country_code"),
                None if err else r.get("place_name"),
                None if err else r.get("city"),
                None if err else r.get("state"),
                None if err else r.get("latitude"),
                None if err else r.get("longitude"),
                None if err else r.get("geocode_score"),
                None if err else r.get("source"),
                err,
                time.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        )
        n += 1
        if n % 200 == 0:
            conn.commit()
            logger.info(
                "%d/%d  %.2f/s  resolved=%d declared-mismatch=%d",
                n,
                len(todo),
                n / (time.time() - t0),
                ok,
                mismatched,
            )
    pool.shutdown()
    conn.commit()
    logger.info(
        "done: %d processed, %d resolved, %d disagreeing with declared",
        n,
        ok,
        mismatched,
    )
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
