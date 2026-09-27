#!/usr/bin/env python
"""
Fetch every rare disease NORD publishes and map it onto MONDO.

Two public inputs:

  - rarediseases.org. The /rare-diseases/?starts_with=... listing sits behind a
    Cloudflare managed challenge, so it cannot be scraped; its WordPress REST API
    serves the same rows as paginated JSON and is not challenged. Three post
    types hold diseases: `rare-diseases` (NORD's own curated reports),
    `mondo-disease` (NORD's MONDO mirror) and `gard-rare-disease` (NIH GARD).

  - MONDO's OBO-JSON release, the same URL `src/ontology/sources.py` downloads.
    Cached under DATA_DIR so the ontology stage and this script share the file.

NORD's mondo-disease posts carry the MONDO name but no MONDO id, so the join is
by name: primary label first, then exact synonym, then broad/related synonym.
Unmatched diseases are kept with an empty mondo_id because
find_rare_disease_datasets.py can still search by name.

    uv run python rare_disease/fetch_rare_diseases.py
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import unicodedata
from html import unescape
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from helpers import send_req
from ontology.extract import download as download_ontology
from ontology.sources import SOURCES

logger = logging.getLogger("rare_disease.fetch")

NORD_API = "https://rarediseases.org/wp-json/wp/v2"
NORD_TYPES = ("rare-diseases", "mondo-disease", "gard-rare-disease")
MONDO = next(s for s in SOURCES if s.name == "mondo")
RARE_SUBSET = "http://purl.obolibrary.org/obo/mondo#rare"

# Ranked worst-to-best; a later hit overwrites an earlier one in the index, so a
# name that is one term's label and another's broad synonym resolves to the label.
SYNONYM_RANK = {
    "hasBroadSynonym": 1,
    "hasRelatedSynonym": 1,
    "hasNarrowSynonym": 2,
    "hasExactSynonym": 3,
}
LABEL_RANK = 4

HEADER = (
    "nord_name",
    "nord_type",
    "nord_slug",
    "nord_url",
    "mondo_id",
    "mondo_label",
    "match",
    "mondo_rare",
    "search_terms",
)


def normalize(name: str) -> str:
    """
    Fold a disease name to its comparison key.

    Accents, case and punctuation all vary between NORD, MONDO and the curated
    sample annotations for the same disease. So does the possessive: the archives
    write "Sjogren's syndrome" where MONDO writes "Sjogren syndrome", and dropping
    the 's is what lets those meet. Stripping punctuation alone would leave
    "sjogren s syndrome" and miss 157 annotated values, Crohn's among them.
    """
    # The possessive has to go before the ascii fold, or a curly apostrophe
    # (NORD serves them as &#8217;) is dropped and "sjogrens" never meets "sjogren".
    s = re.sub(r"[\u2018\u2019\u02bc`\u00b4]", "'", unescape(name))
    s = re.sub(r"(?<=[a-zA-Z])'s\b", "", s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9]+", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def fetch_nord(post_type: str, *, timeout: int, retries: int) -> list[dict]:
    """Page through one WordPress post type. X-WP-TotalPages bounds the walk."""
    out: list[dict] = []
    page = 1
    while True:
        r = send_req(
            "GET",
            f"{NORD_API}/{post_type}",
            params={"per_page": 100, "page": page, "_fields": "id,slug,link,title"},
            headers={
                "User-Agent": "seqout-ingestion/rare_disease (+https://rarediseases.org)"
            },
            timeout=timeout,
            num_retries=retries,
            max_wait=60,
        )
        rows = r.json()
        if not rows:
            break
        out.extend(rows)
        total_pages = int(r.headers.get("X-WP-TotalPages", 0))
        logger.info("%s page %d/%d (%d rows)", post_type, page, total_pages, len(out))
        if page >= total_pages:
            break
        page += 1
    return out


def mondo_index(
    path: Path,
) -> tuple[dict[str, tuple[str, str, bool]], dict[str, list[str]], int]:
    """
    Index MONDO twice: name -> (curie, label, is_rare), and curie -> its names.

    The second index feeds the `search_terms` column and holds the label plus
    exact synonyms only. Narrow synonyms name subtypes, so carrying them would
    file a subtype's datasets under its parent disease.

    Obsolete terms are skipped: they keep their label, so leaving them in would
    let a retired id win a name a live term also carries.
    """
    graph = json.loads(path.read_text())["graphs"][0]
    index: dict[str, tuple[str, str, bool]] = {}
    names: dict[str, list[str]] = {}
    ranks: dict[str, int] = {}
    rare = 0

    for node in graph["nodes"]:
        node_id = node.get("id", "")
        if "MONDO_" not in node_id:
            continue
        meta = node.get("meta") or {}
        if meta.get("deprecated"):
            continue
        label = node.get("lbl")
        if not label:
            continue
        curie = "MONDO:" + node_id.rsplit("MONDO_", 1)[1]
        is_rare = RARE_SUBSET in (meta.get("subsets") or [])
        rare += is_rare

        candidates = [(label, LABEL_RANK)]
        candidates += [
            (s["val"], SYNONYM_RANK.get(s.get("pred", ""), 0))
            for s in meta.get("synonyms") or []
            if s.get("val")
        ]
        for name, rank in candidates:
            key = normalize(name)
            if not key:
                continue
            if rank > ranks.get(key, -1):
                index[key], ranks[key] = (curie, label, is_rare), rank
            if rank >= SYNONYM_RANK["hasExactSynonym"]:
                names.setdefault(curie, []).append(key)

    return index, names, rare


def main() -> int:
    data_dir = Path(os.environ.get("DATA_DIR", "/data/share/seqout"))
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "rare_diseases.csv",
    )
    p.add_argument(
        "--ontology-dir",
        type=Path,
        default=data_dir / "ontology",
        help="ontology stage root; mondo.json is read from (or downloaded into) raw/",
    )
    p.add_argument(
        "--types",
        nargs="+",
        default=list(NORD_TYPES),
        choices=NORD_TYPES,
    )
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--log-level", default="info")
    args = p.parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Shares the ontology stage's cache: a no-op once `ontology download` has run.
    mondo_json = download_ontology(MONDO, args.ontology_dir, workers=4, force=False)
    index, names_by_curie, n_rare = mondo_index(mondo_json)
    logger.info(
        "MONDO: %d names indexed, %d terms in the 'rare' subset",
        len(index),
        n_rare,
    )

    seen: dict[str, tuple] = {}
    for post_type in args.types:
        for row in fetch_nord(post_type, timeout=args.timeout, retries=args.retries):
            name = unescape((row.get("title") or {}).get("rendered", "")).strip()
            if not name:
                continue
            key = normalize(name)
            if key in seen:  # same disease listed under two post types
                continue
            hit = index.get(key)
            # The NORD name always searches, mapped or not; MONDO only widens it.
            widened = names_by_curie.get(hit[0], []) if hit else []
            terms = dict.fromkeys([key, *widened])
            seen[key] = (
                name,
                post_type,
                row.get("slug", ""),
                row.get("link", ""),
                hit[0] if hit else "",
                hit[1] if hit else "",
                "mondo" if hit else "unmatched",
                int(hit[2]) if hit else "",
                ";".join(terms),
            )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        w.writerows(sorted(seen.values()))

    matched = sum(1 for v in seen.values() if v[4])
    logger.info(
        "wrote %s: %d diseases, %d mapped to MONDO (%.0f%%)",
        args.out,
        len(seen),
        matched,
        100 * matched / max(len(seen), 1),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
