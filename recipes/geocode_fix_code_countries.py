#!/usr/bin/env python
"""
Withdraw rows whose `country` holds a code or an empty string. DRY RUN BY DEFAULT.

These survived the geocode correction because the staged build had nothing to
say about them and their centre name is verifiable enough to escape its narrow
withdrawal, so neither the update nor the clear touched them. They are legacy
name-pass answers, and the country field holding "cn" rather than "China" is
the tell that exposes them, not the defect itself.

Reformatting "cn" to "China" would launder a wrong coordinate into a
well-formatted wrong country, so this reverse geocodes the stored point and
reports what country it actually lands in. That report is evidence, not a
gate: agreement establishes only the country, and a point can agree on the
country while naming the wrong institution in the wrong city, which is what
several of these do. Nothing in the row is independently verified, so the row
goes. A wrong geocode is worse than an absent one, and any of these that a
depositor address later covers will be resolved properly by the normal build.

  geocode_fix_code_countries.py            # report only
  geocode_fix_code_countries.py --apply    # write
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg
from geopy.geocoders import ArcGIS

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from geocode.geocode import _canonical_country

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

GEO_COLS = (
    "country",
    "country_code",
    "country_code_iso2",
    "place_name",
    "city",
    "state",
    "latitude",
    "longitude",
)

# A country name this short is a code. The empty string is caught by the same
# test on purpose: it is the third state the column should never hold.
SUSPECT_SQL = """
SELECT canonical_accession, center_name, country, country_code,
       place_name, latitude, longitude
FROM unified_metadata
WHERE country IS NOT NULL AND length(country) <= 3
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    with psycopg.connect(os.environ["PG_URL"]) as pg:
        rows = pg.execute(SUSPECT_SQL).fetchall()
    logger.info("rows with a code-shaped country: %d", len(rows))

    # One lookup per distinct point, not per row: a shared bad match is one
    # place however many studies inherited it.
    points = {(r[5], r[6]) for r in rows if r[5] is not None and r[6] is not None}
    logger.info("distinct coordinates to reverse geocode: %d", len(points))

    reverse = ArcGIS(timeout=30).reverse
    where: dict[tuple, str | None] = {}
    for lat, lon in sorted(points):
        try:
            loc = reverse((lat, lon), exactly_one=True)
            # ArcGIS reverse puts the address fields at the top level of raw,
            # not under an "address" key, and names the country in alpha-3,
            # which is why both fields are handed to the canonicaliser rather
            # than the code alone.
            raw = (loc.raw or {}) if loc else {}
            where[lat, lon] = _canonical_country(
                raw.get("CountryCode"), raw.get("CntryName")
            )[0]
        except Exception as exc:
            logger.warning("reverse %s,%s failed: %s", lat, lon, exc)
            where[lat, lon] = None
        time.sleep(0.2)

    # Reported per centre rather than per study: one bad match is one mistake,
    # however many studies inherited it.
    seen: set[str] = set()
    for _acc, centre, country, code, place, lat, lon in rows:
        if centre in seen:
            continue
        seen.add(centre)
        claimed = _canonical_country(code or country, None)[0]
        logger.info(
            "%-46s claims %-4s point is in %-4s and names %s",
            (centre or "")[:46],
            claimed or "''",
            where.get((lat, lon)) or "?",
            (place or "")[:46],
        )

    logger.info("")
    logger.info("would CLEAR: %d rows across %d centres", len(rows), len(seen))

    if not args.apply:
        logger.info("DRY RUN -- nothing written. Re-run with --apply to write.")
        return 0

    nulls = ", ".join(f"{c} = NULL" for c in GEO_COLS)
    with psycopg.connect(os.environ["PG_URL"]) as pg, pg.cursor() as cur:
        cur.execute(
            f"UPDATE unified_metadata SET {nulls} "  # noqa: S608
            "WHERE canonical_accession = ANY(%s)",
            ([r[0] for r in rows],),
        )
        cleared = cur.rowcount
        pg.commit()
    logger.info("cleared %d rows", cleared)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
