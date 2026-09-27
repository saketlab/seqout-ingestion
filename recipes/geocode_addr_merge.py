#!/usr/bin/env python
"""
Merge fleet address-geocode results into the local address store.

The fleet worker writes one JSON per address, named by a hash of the address,
because a postal address is not a filename. The address itself is inside the
file, which is what lets this merge key on it without a lookup table.

Idempotent: re-running merges the same rows to the same values. Rows already
present are left alone unless the incoming one resolved and the stored one did
not, so a later successful retry can replace an earlier failure but never the
reverse.
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

from geocode.geocode import _canonical_country

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH = 5000

COLS = [
    "address_key",
    "query",
    "declared_cc",
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
    ap.add_argument("--results", default="/data/saket/geocode/fleet_results")
    ap.add_argument("--db", default="/data/saket/geocode/address_geocode.db")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    existing = {
        r[0]: r[1]
        for r in conn.execute("SELECT address_key, error IS NULL FROM address_geocode")
    }

    added = replaced = skipped = unparseable = 0
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    rows = []
    for path in Path(args.results).rglob("*.json"):
        try:
            rec = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            unparseable += 1
            continue
        address = rec.get("address")
        if not address:
            unparseable += 1
            continue
        resolved = rec.get("error") is None
        was = existing.get(address)
        if was is not None:
            # Only a success may displace a stored failure.
            if was or not resolved:
                skipped += 1
                continue
            replaced += 1
        else:
            added += 1
        # The fleet worker is stdlib-only, so it stores whatever ArcGIS returns,
        # which is alpha-3. Canonicalise here, at the boundary where fleet
        # data enters the store, or one country lives under both "us" and
        # "usa": centres then look multi-country, the majority rule stops
        # settling them, and alpha-3 codes reach the transfer.
        code, name = _canonical_country(rec.get("country_code"), rec.get("country"))
        rows.append(
            (
                address,
                address,
                None,
                name,
                code,
                rec.get("place_name"),
                rec.get("city"),
                rec.get("state"),
                rec.get("latitude"),
                rec.get("longitude"),
                rec.get("geocode_score"),
                rec.get("source"),
                rec.get("error"),
                now,
            )
        )
        if len(rows) >= BATCH:
            conn.executemany(
                f"INSERT OR REPLACE INTO address_geocode ({','.join(COLS)}) "  # noqa: S608
                f"VALUES ({','.join('?' * len(COLS))})",
                rows,
            )
            conn.commit()
            rows.clear()
    if rows:
        conn.executemany(
            f"INSERT OR REPLACE INTO address_geocode ({','.join(COLS)}) "  # noqa: S608
            f"VALUES ({','.join('?' * len(COLS))})",
            rows,
        )
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM address_geocode").fetchone()[0]
    ok = conn.execute(
        "SELECT COUNT(*) FROM address_geocode WHERE error IS NULL"
    ).fetchone()[0]
    logger.info(
        "merged: %d added, %d replaced a failure, %d already known, %d unreadable",
        added,
        replaced,
        skipped,
        unparseable,
    )
    logger.info("store now: %d addresses, %d resolved", total, ok)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
