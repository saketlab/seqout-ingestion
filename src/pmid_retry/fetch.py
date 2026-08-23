"""
Resolve PubMed ids for a manifest of accessions, one JSON per accession.

Payload and file naming match the fleet worker, so `pmid_retry ingest` reads
either. pysradb tries the archive's own link, then PMC, then Europe PMC.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# Same mapping as backfill/pubmed_ids.py: SRA and ENA by SRP elink, GEO and
# ArrayExpress by GSE, the rest by literature search.
RESOLVERS: dict[str, tuple[str, str]] = {
    "sra": ("sra_to_pmid", "srp_accession"),
    "ena": ("sra_to_pmid", "srp_accession"),
    "geo": ("gse_to_pmid", "gse_accession"),
    "ae": ("gse_to_pmid", "gse_accession"),
    "gea": ("ae_to_pmid", "ae_accession"),
    "gsa": ("ae_to_pmid", "ae_accession"),
}


def read_manifest(manifest_file: Path) -> list[tuple[str, str]]:
    """Read `<source>,<accession>` per line."""
    items = []
    for line in manifest_file.read_text().splitlines():
        source, _, accession = line.strip().partition(",")
        if accession and source in RESOLVERS:
            items.append((source, accession))
    return items


def resolve_one(source: str, accession: str, out_dir: Path, keys: list[str]) -> str:
    """Write `<accession>.json` and return the outcome for logging."""
    out = out_dir / f"{accession}.json"
    if out.exists():
        return "skipped"

    import pandas as pd
    from pysradb.sraweb import SRAweb

    if keys:
        os.environ["NCBI_API_KEY"] = random.choice(keys)  # noqa: S311

    method, column = RESOLVERS[source]
    started = time.time()
    pmids: list[str] = []
    error = None
    try:
        frame = getattr(SRAweb(), method)([accession])
        for _, row in frame.iterrows():
            value = row.get("pmid")
            # pysradb returns pd.NA for an unresolved row, and pd.NA raises
            # in a boolean context
            if value is None or pd.isna(value):
                continue
            pmid = str(value).strip()
            if pmid and pmid.lower() not in {"nan", "<na>"}:
                pmids.append(pmid)
    except KeyError:
        # pysradb raises KeyError when the response carries no pmid column,
        # which is a real nothing-found result
        pmids = []
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    out.write_text(
        json.dumps(
            {
                "accession": accession,
                "source": source,
                "resolver": method,
                "accession_column": column,
                "pmids": sorted(set(pmids)),
                "error": error,
                "elapsed_s": round(time.time() - started, 2),
                "checked_at": datetime.now(UTC).isoformat(),
            }
        )
    )
    if error:
        return "errored"
    return "resolved" if pmids else "empty"


def pmid_retry_fetch(
    manifest_file: Path,
    out_dir: Path,
    num_workers: int = 4,
    keys_file: Path | None = None,
) -> dict[str, int]:
    """Resolve every manifest entry into out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    items = read_manifest(manifest_file)
    keys = (
        [k.strip() for k in keys_file.read_text().splitlines() if k.strip()]
        if keys_file and keys_file.is_file()
        else []
    )
    logger.info("resolving %d accessions with %d workers", len(items), num_workers)

    counts: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        for outcome in pool.map(
            lambda item: resolve_one(item[0], item[1], out_dir, keys), items
        ):
            counts[outcome] = counts.get(outcome, 0) + 1
    return counts
