#!/usr/bin/env python3
"""
Fleet worker: geocode a slice of submitted postal addresses.

Deployed flat and run as bare python3. It imports stdlib only, not geopy,
which is on no host, and nothing from this repo, because the hosts get this
file and a work list and nothing else. The ArcGIS call is a plain HTTP GET and
the parsing is a small local copy of what src/geocode does with the response.

Batch, not item: yuj hands the host a slice file and this loops over it,
because one process per address would pay interpreter startup per lookup.
Resumable by output: an address already in results/ is skipped, so a killed
run is restarted by rerunning it.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

WORKERS = 6
RETRIES = 3
DELAY = 0.15


def key(address: str) -> str:
    """Stable filename for an address of any length or character set."""
    return hashlib.sha1(address.encode("utf-8")).hexdigest()[:20]  # noqa: S324


ENDPOINT = (
    "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/"
    "findAddressCandidates"
)


def lookup(address: str, timeout: int = 30) -> dict:
    """One ArcGIS candidate for an address, as the fields this job stores."""
    url = (
        ENDPOINT
        + "?"
        + urllib.parse.urlencode(
            {"SingleLine": address, "f": "json", "outFields": "*", "maxLocations": 1}
        )
    )
    with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310
        payload = json.load(r)
    if payload.get("error"):
        return {"address": address, "error": str(payload["error"])[:200]}
    candidates = payload.get("candidates") or []
    if not candidates:
        return {"address": address, "error": "no result"}
    top = candidates[0]
    attrs = top.get("attributes", {}) or {}
    loc = top.get("location", {}) or {}
    score = top.get("score")
    return {
        "address": address,
        "country": attrs.get("CntryName"),
        "country_code": (attrs.get("Country") or "").lower() or None,
        "place_name": attrs.get("PlaceName") or top.get("address"),
        "city": attrs.get("City") or attrs.get("Subregion"),
        "state": attrs.get("Region"),
        "latitude": loc.get("y"),
        "longitude": loc.get("x"),
        "geocode_score": (score / 100) if score else None,
        "source": "arcgis",
        "error": None,
    }


def main() -> int:
    work = Path(sys.argv[1] if len(sys.argv) > 1 else "addresses.txt")
    out = Path("results")
    out.mkdir(exist_ok=True)
    # The work list is base64, one address per line. yuj splits a slice file on
    # ordinary delimiters and a postal address is full of commas and quotes, so
    # the encoding is what lets an address survive being scattered at all.
    todo = [
        base64.b64decode(line.strip()).decode("utf-8", "replace")
        for line in work.read_text().splitlines()
        if line.strip()
    ]
    todo = [a for a in todo if not (out / f"{key(a)}.json").exists()]
    logger.info("%d addresses to geocode", len(todo))

    def fetch(item: tuple) -> tuple:
        _idx, address = item
        for attempt in range(RETRIES):
            try:
                return address, lookup(address)
            except Exception as exc:
                if attempt == RETRIES - 1:
                    err = f"{type(exc).__name__}: {exc}"
                    return address, {"address": address, "error": err}
                time.sleep(DELAY * (attempt + 1) * 4)
        return address, {"address": address, "error": "unreachable"}

    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(WORKERS) as pool:
        for address, result in pool.map(fetch, enumerate(todo)):
            (out / f"{key(address)}.json").write_text(json.dumps(result))
            done += 1
            if done % 200 == 0:
                logger.info("%d/%d  %.2f/s", done, len(todo), done / (time.time() - t0))
    logger.info("done: %d", done)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
