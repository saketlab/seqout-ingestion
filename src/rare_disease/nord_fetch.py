"""
Fetch NORD disease posts and map them onto MONDO by name.

Matching tries primary label, exact synonym, then broad/related synonym. WordPress
REST endpoints serve the needed post types directly.
"""

from __future__ import annotations

import csv
import json
import logging
import re
import unicodedata
from html import unescape
from typing import TYPE_CHECKING

from helpers import send_req
from ontology.extract import download as download_ontology
from ontology.sources import SOURCES

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

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

    Drop possessives before ASCII folding so "Sjogren's" meets "Sjogren".
    """
    s = re.sub(r"[\u2018\u2019\u02bc`\u00b4]", "'", unescape(name))
    s = re.sub(r"(?<=[a-zA-Z])'s\b", "", s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9]+", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def fetch_post_type(post_type: str, *, timeout: int, num_retries: int) -> list[dict]:
    """Page through one WordPress post type. X-WP-TotalPages bounds the walk."""
    out: list[dict] = []
    page = 1
    while True:
        r = send_req(
            "GET",
            f"{NORD_API}/{post_type}",
            params={"per_page": 100, "page": page, "_fields": "id,slug,link,title"},
            headers={"User-Agent": "seqout-ingestion/nord (+https://rarediseases.org)"},
            timeout=timeout,
            num_retries=num_retries,
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
    Index MONDO by comparison name and by curie.

    The curie index feeds search_terms with labels and exact synonyms. Obsolete
    terms keep labels, so live terms own name matches.
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


def fetch_nord(
    output_csv_file: Path,
    ontology_dir: Path,
    post_types: tuple[str, ...] = NORD_TYPES,
    timeout: int = 60,
    num_retries: int = 5,
) -> int:
    """Write one csv row per NORD disease. Returns the number of rows written."""
    mondo_json = download_ontology(MONDO, ontology_dir, workers=4, force=False)
    index, names_by_curie, n_rare = mondo_index(mondo_json)
    logger.info("MONDO: %d names indexed, %d rare-subset terms", len(index), n_rare)

    seen: dict[str, tuple] = {}
    for post_type in post_types:
        for row in fetch_post_type(post_type, timeout=timeout, num_retries=num_retries):
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

    output_csv_file.parent.mkdir(parents=True, exist_ok=True)
    with output_csv_file.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        w.writerows(sorted(seen.values()))

    matched = sum(1 for v in seen.values() if v[4])
    logger.info(
        "wrote %s: %d diseases, %d mapped to MONDO (%.0f%%)",
        output_csv_file,
        len(seen),
        matched,
        100 * matched / max(len(seen), 1),
    )
    return len(seen)
