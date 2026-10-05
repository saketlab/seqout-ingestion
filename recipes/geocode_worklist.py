#!/usr/bin/env python
"""
Classify every centre in Postgres into the geocoding work it needs.

Three buckets, written as JSON lists:

  centers_missing   no country anywhere for this centre
  centers_suspect   has a country, but it fails the current plausibility gate
  centers_need_api  the two above, minus what the local ROR dump can answer

Reads Postgres because unified_metadata.db is read-only on this host and months
stale. Keying on the centre NAME rather than the study is what makes the work
tractable: 1.2M studies reduce to ~105k distinct centres, and one lookup
answers every study that shares a name.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import psycopg

from geocode.geocode import _distinctive_tokens, _geocode_from_ror, _load_ror_index

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="/data/saket/geocode/worklists")
    ap.add_argument("--ror-file", default="/data/saket/ror/ror_data.json")
    args = ap.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(os.environ["PG_URL"]) as pg:
        rows = pg.execute(
            "SELECT center_name, country, place_name, COUNT(*) "
            "FROM unified_metadata "
            "WHERE center_name IS NOT NULL AND center_name <> '' "
            "GROUP BY 1,2,3"
        ).fetchall()

    per: dict[str, list] = {}
    for centre, country, place, n in rows:
        per.setdefault(centre, []).append((country, place, n))

    missing, suspect = [], []
    for centre, vals in per.items():
        if not any(c for c, _, _ in vals):
            missing.append(centre)
            continue
        wanted = _distinctive_tokens(centre)
        ok = any(
            country and place and wanted and (wanted & _distinctive_tokens(str(place)))
            for country, place, _ in vals
        )
        if not ok:
            suspect.append(centre)

    logger.info(
        "centres: %d  missing: %d  suspect: %d",
        len(per),
        len(missing),
        len(suspect),
    )

    index = _load_ror_index(Path(args.ror_file))
    ror_hit, need_api = [], []
    for centre in missing + suspect:
        hit = _geocode_from_ror(centre, index, [], {})
        (ror_hit if hit else need_api).append(centre)
    logger.info("ROR can answer %d; %d need the API", len(ror_hit), len(need_api))

    for name, data in (
        ("centers_missing", missing),
        ("centers_suspect", suspect),
        ("centers_ror_hit", ror_hit),
        ("centers_need_api", need_api),
    ):
        (out / f"{name}.json").write_text(json.dumps(data))
        logger.info("wrote %s (%d)", out / f"{name}.json", len(data))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
