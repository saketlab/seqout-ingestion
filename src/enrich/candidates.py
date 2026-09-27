"""
Pick one representative accession per deduped study.

For embedding-only enrichment; see
docs/superpowers/specs/2026-09-14-embedding-only-enrichment.md. Reads
unified_metadata straight from production Postgres, never the local
staging SQLite, which lags production by design (see CLAUDE.md, "Look things
up in Postgres, not the staging SQLite").

Source precedence here (ae > geo > sra > ena > gsa > dra > gea) is scoped to
this feature only: ArrayExpress records tend to carry richer structured
per-sample columns than GEO. It differs from
src/unifier/unified_metadata.py's geo > sra > ae > ena > gsa > dra > gea,
used for merging dominant_scientific_name/title/description; that order is
unrelated and untouched by this module.
"""

from __future__ import annotations

import json
import logging
import re

import psycopg

logger = logging.getLogger(__name__)

PRECEDENCE = ("ae", "geo", "sra", "ena", "gsa", "dra", "gea")

# unified_metadata.aliases keys don't match the `source` column's vocabulary:
# "arrayexpress" there is "ae" here, and "bioproject" is a cross-reference id
# with no fetchable sample sheet of its own.
_ALIAS_KEY_TO_SOURCE = {"arrayexpress": "ae"}
_SKIP_ALIAS_KEYS = {"bioproject"}

# The seqout bulk API 400s outright on a bare BioProject accession (confirmed
# against production: PRJNA1144155 -> "Invalid accession format"), unlike a
# merely-unknown accession, which comes back 200 with an empty list. Some
# ena-source canonical_accession rows are bare BioProject ids themselves, not
# just the "bioproject" alias key above, so this must be checked on every
# candidate value, not just on how it was found.
_BIOPROJECT_RE = re.compile(r"^PRJ[A-Z]{2}\d+$")


def _is_unusable(value: str) -> bool:
    """
    Report whether `value` isn't one fetchable accession.

    Two shapes seen live against production, both causing the bulk API to
    400 the whole batch it's in rather than just skip it: a bare BioProject
    id (PRJNA1144155), and a semicolon-joined multi-value string (e.g.
    "SRP008164;SRP005854", a merged/renamed ena-source study where the
    catalog builder concatenated both accessions instead of picking one).
    """
    return bool(_BIOPROJECT_RE.match(value)) or ";" in value


def select_representative(
    canonical_accession: str, source: str, aliases: dict | str | None
) -> tuple[str, str] | None:
    """
    Pick (source, accession) for one canonical_accession group, by PRECEDENCE.

    `aliases` may already be a dict (psycopg's jsonb adapter) or a JSON string
    (observed elsewhere in this repo, e.g. transfer/postgres.py); accept both
    rather than assume one. Returns None when every candidate for this group
    is unusable (see _is_unusable): there is nothing fetchable to enrich.
    """
    candidates: dict[str, str] = {source: canonical_accession}
    alias_map: dict[str, str] = (
        json.loads(aliases) if isinstance(aliases, str) else (aliases or {})
    )
    for key, acc in alias_map.items():
        if key in _SKIP_ALIAS_KEYS:
            continue
        candidates.setdefault(_ALIAS_KEY_TO_SOURCE.get(key, key), acc)

    candidates = {src: acc for src, acc in candidates.items() if not _is_unusable(acc)}

    for src in PRECEDENCE:
        if src in candidates:
            return src, candidates[src]
    return None


def fetch_candidates(pg_url: str) -> list[tuple[str, str, str]]:
    """
    Return one representative row per canonical_accession group.

    Each row is (canonical_accession, chosen_source, chosen_accession).
    """
    rows_out: list[tuple[str, str, str]] = []
    no_source = 0
    nothing_usable = 0
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT canonical_accession, source, aliases FROM unified_metadata")
        for canonical_accession, source, aliases in cur:
            if not source:
                no_source += 1
                continue
            picked = select_representative(canonical_accession, source, aliases)
            if picked is None:
                nothing_usable += 1
                continue
            chosen_source, chosen_acc = picked
            rows_out.append((canonical_accession, chosen_source, chosen_acc))
    if no_source:
        logger.warning(
            "skipped %d canonical_accession rows with no source column", no_source
        )
    if nothing_usable:
        logger.warning(
            "skipped %d canonical_accession rows with no usable single "
            "accession available (bare BioProject id or a multi-value alias)",
            nothing_usable,
        )
    return rows_out
