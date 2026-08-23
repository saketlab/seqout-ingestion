"""
Fetch GARD disease rows and resolve them onto MONDO ids.

MONDO carries GARD xrefs for disease mapping. rarediseases.info.nih.gov has no
public API; its JS fetches a static asset.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from helpers import exec_query_from_file, send_req, sqlite_connect
from ontology.extract import _load_obo_graph
from ontology.extract import download as download_ontology
from ontology.sources import SOURCES

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

CATALOGUE_URL = "https://rarediseases.info.nih.gov/assets/diseases.trimmed.json"

MONDO = next(s for s in SOURCES if s.name == "mondo")
RARE_SUBSET = "http://purl.obolibrary.org/obo/mondo#rare"

#: In the column order _INSERT expects. tagsCause is excluded: present on every
#: record, empty on every one.
TAG_FIELDS = ("tagsDiseaseCategory", "tagsSpecialist", "tagsAccount")

_INSERT = """
INSERT INTO gard_disease (
    gard_id, gard_name, mondo_id, mondo_label, mondo_rare,
    categories, specialties, groups, synonyms, fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(gard_id) DO UPDATE SET
    gard_name   = excluded.gard_name,
    mondo_id    = excluded.mondo_id,
    mondo_label = excluded.mondo_label,
    mondo_rare  = excluded.mondo_rare,
    categories  = excluded.categories,
    specialties = excluded.specialties,
    groups      = excluded.groups,
    synonyms    = excluded.synonyms,
    fetched_at  = excluded.fetched_at
"""


def mondo_by_gard(path: Path) -> tuple[dict[str, tuple[str, str, bool]], int]:
    """
    Index MONDO by the GARD id it cross-references.

    Ids compare as integers so zero-padded MONDO xrefs meet bare GARD ids.
    Deprecated terms are skipped before xref indexing.
    """
    graph = _load_obo_graph(path)
    out: dict[str, tuple[str, str, bool]] = {}
    collisions = 0

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
        for xref in meta.get("xrefs") or []:
            val = xref.get("val", "")
            if not val.upper().startswith("GARD:"):
                continue
            try:
                key = str(int(val.split(":", 1)[1]))
            except ValueError:
                continue
            # Count conflicts explicitly; keep the first indexed live term.
            if key in out:
                collisions += 1
                continue
            out[key] = (curie, label, is_rare)
    return out, collisions


def _rows(
    diseases: list[dict[str, Any]],
    xref: dict[str, tuple[str, str, bool]],
    fetched_at: str,
) -> tuple[list[tuple], int]:
    rows: list[tuple] = []
    matched = 0
    for d in diseases:
        gard_id = str(d.get("id", "")).strip()
        name = (d.get("name") or "").strip()
        if not gard_id or not name:
            continue
        hit = xref.get(gard_id)
        matched += hit is not None
        tags = [
            ";".join(t.strip() for t in (d.get(field) or []) if t and t.strip())
            for field in TAG_FIELDS
        ]
        synonyms = ";".join(
            s.strip() for s in (d.get("synonyms") or []) if s and s.strip()
        )
        rows.append(
            (
                f"GARD:{gard_id}",
                name,
                hit[0] if hit else None,
                hit[1] if hit else None,
                int(hit[2]) if hit else None,
                *tags,
                synonyms,
                fetched_at,
            )
        )
    return rows, matched


def fetch_gard(
    output_db_file: Path,
    ontology_dir: Path,
    timeout: int = 120,
    num_retries: int = 5,
) -> dict[str, int]:
    """
    Write one row per GARD disease into `output_db_file`.

    Upserts by gard_id; retired diseases keep rows already linked from views.
    """
    output_db_file.parent.mkdir(parents=True, exist_ok=True)
    exec_query_from_file("tables/gard.sql", db_file=output_db_file)

    response = send_req(
        "GET",
        CATALOGUE_URL,
        headers={"User-Agent": f"seqout-ingestion (+{CATALOGUE_URL})"},
        timeout=timeout,
        num_retries=num_retries,
        max_wait=60,
    )
    diseases = response.json()
    logger.info("GARD: %d diseases", len(diseases))

    # shares the ontology stage's cache
    mondo_json = download_ontology(MONDO, ontology_dir, workers=4, force=False)
    xref, collisions = mondo_by_gard(mondo_json)
    logger.info(
        "MONDO: %d GARD xrefs indexed (%d duplicate ids ignored)",
        len(xref),
        collisions,
    )

    fetched_at = datetime.now(UTC).isoformat()
    rows, matched = _rows(diseases, xref, fetched_at)
    with sqlite_connect(output_db_file) as conn:
        conn.executemany(_INSERT, rows)
        conn.commit()

    counts = {"diseases": len(rows), "mondo_matched": matched}
    logger.info(
        "gard fetch: %d diseases, %d resolved to MONDO by xref (%.1f%%)",
        len(rows),
        matched,
        100 * matched / max(len(rows), 1),
    )
    return counts
