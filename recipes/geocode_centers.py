#!/usr/bin/env python
"""
Geocode centre names into a standalone, resumable SQLite.

The canonical unified_metadata.db is read-only here and is months stale, so
this keys on the centre name, the unit the geocoder actually works on, and
leaves joining it back to studies to the transfer step. Resumable by output:
a centre already in the table is skipped, so a killed run is restarted by
rerunning it.

  geocode_centers.py <work.json> [--db FILE] [--no-osm]

--no-osm restricts the run to ArcGIS. Nominatim's usage policy caps bulk use at
one request per second from a single client, so the OSM fallback is what bounds
throughput; dropping it trades recall for rate.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import ArcGIS, Nominatim

from geocode.geocode import _geocode_item

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS centre_geocode (
    center_name   TEXT PRIMARY KEY,
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
CREATE INDEX IF NOT EXISTS centre_geocode_country ON centre_geocode(country_code);
"""

COLS = [
    "center_name",
    "country",
    "country_code",
    "place_name",
    "city",
    "state",
    "latitude",
    "longitude",
    "geocode_score",
    "source",
    "error",
    "checked_at",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("work")
    ap.add_argument("--db", default="/data/saket/geocode/centre_geocode.db")
    ap.add_argument("--no-osm", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    # 1s was inherited from the Nominatim policy and applied to ArcGIS too,
    # which is what actually bounded the run. ArcGIS is a commercial endpoint
    # with its own limits; raise the rate only as far as it stays error-free.
    ap.add_argument("--delay", type=float, default=1.0)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.executescript(DDL)
    conn.commit()
    done = {r[0] for r in conn.execute("SELECT center_name FROM centre_geocode")}

    centres = [c for c in json.loads(Path(args.work).read_text()) if c not in done]
    if args.limit:
        centres = centres[: args.limit]
    logger.info("%d centres to do (%d already stored)", len(centres), len(done))

    arc = RateLimiter(
        ArcGIS(timeout=30).geocode,
        min_delay_seconds=args.delay,
        max_retries=3,
    )
    osm = (
        None
        if args.no_osm
        else RateLimiter(
            Nominatim(user_agent="seqout-ingestion-v2", timeout=30).geocode,
            min_delay_seconds=1,
            max_retries=3,
        )
    )

    t0, n = time.time(), 0
    for centre in centres:
        try:
            r = _geocode_item(centre, [], {}, osm, arc)
        except Exception as exc:
            r = {"error": f"{type(exc).__name__}: {exc}"}
        r = r or {"error": "no result"}
        conn.execute(
            f"INSERT OR REPLACE INTO centre_geocode ({','.join(COLS)}) "  # noqa: S608
            f"VALUES ({','.join('?' * len(COLS))})",
            (
                centre,
                r.get("country"),
                r.get("country_code"),
                r.get("place_name"),
                r.get("city"),
                r.get("state"),
                r.get("latitude"),
                r.get("longitude"),
                r.get("geocode_score"),
                r.get("source"),
                r.get("error"),
                time.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        )
        n += 1
        if n % 200 == 0:
            conn.commit()
            el = time.time() - t0
            logger.info("%d/%d  %.2f/s", n, len(centres), n / el)
    conn.commit()
    resolved = conn.execute(
        "SELECT COUNT(*) FROM centre_geocode WHERE error IS NULL"
    ).fetchone()[0]
    logger.info("done: %d stored, %d resolved", n, resolved)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
