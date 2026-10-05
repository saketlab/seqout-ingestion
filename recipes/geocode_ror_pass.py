#!/usr/bin/env python
"""
Resolve centre names against the local ROR dump into a SQLite.

ROR is a large offline institution registry, so this pass costs nothing but
CPU and should always run before the rate-limited API pass: whatever ROR
answers never needs fetching. It writes its own database rather than sharing
centre_geocode.db, because the API run holds that one open and commits
continuously, and two writers on one SQLite is a lock waiting to happen.

Answers are held to the same plausibility gate as the API path. ROR rewards an
acronym match in its own scoring, and an acronym is the weakest evidence of
identity: ungated, "BI" resolved to Bank Indonesia and moved the Broad
Institute's studies to Jakarta.

  geocode_ror_pass.py <work.json> [--db FILE] [--ror-file FILE]
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

from geocode.geocode import _geocode_from_ror, _load_ror_index

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
    ap.add_argument("--db", default="/data/saket/geocode/ror_geocode.db")
    ap.add_argument("--ror-file", default="/data/saket/ror/ror_data.json")
    args = ap.parse_args()

    centres = json.loads(Path(args.work).read_text())
    logger.info("centres to resolve: %d", len(centres))

    index = _load_ror_index(Path(args.ror_file))
    if not index:
        logger.error("no ROR index at %s; nothing to do", args.ror_file)
        return 1

    conn = sqlite3.connect(args.db)
    conn.executescript(DDL)
    stored = 0
    for i, centre in enumerate(centres):
        result = _geocode_from_ror(centre, index, [], {})
        if result is None:
            continue
        conn.execute(
            f"INSERT OR REPLACE INTO centre_geocode ({','.join(COLS)}) "  # noqa: S608
            f"VALUES ({','.join('?' * len(COLS))})",
            (
                centre,
                result.get("country"),
                result.get("country_code"),
                result.get("place_name"),
                result.get("city"),
                result.get("state"),
                result.get("latitude"),
                result.get("longitude"),
                result.get("geocode_score"),
                result.get("source"),
                None,
                time.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        )
        stored += 1
        if stored % 1000 == 0:
            conn.commit()
            logger.info("  %d resolved of %d seen", stored, i + 1)
    conn.commit()
    logger.info("ROR resolved %d of %d centres", stored, len(centres))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
