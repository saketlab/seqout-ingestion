"""
Fleet worker for retrying PubMed resolution with current pysradb.

710,306 studies have no PubMed link. One accession per invocation spreads NCBI
load across hosts and API keys; missing result files mean unattempted.
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

# Same mapping as src/backfill/pubmed_ids.py: SRA/ENA by SRP elink, GEO and
# ArrayExpress by GSE, the rest by literature search. DRA needs BioProject
# plus the local DB, so it stays off the fleet.
RESOLVERS: dict[str, tuple[str, str]] = {
    "sra": ("sra_to_pmid", "srp_accession"),
    "ena": ("sra_to_pmid", "srp_accession"),
    "geo": ("gse_to_pmid", "gse_accession"),
    "ae": ("gse_to_pmid", "gse_accession"),
    "arrayexpress": ("gse_to_pmid", "gse_accession"),
    "gea": ("ae_to_pmid", "ae_accession"),
    "gsa": ("ae_to_pmid", "ae_accession"),
}

_ARGV_MIN = 2


def _load_key() -> str | None:
    """One random key per invocation: the fleet spreads load across all of them."""
    path = Path(os.environ.get("NCBI_KEYS_FILE", Path.home() / "ncbi_keys.txt"))
    if not path.is_file():
        logger.warning(
            "no NCBI key file at %s; running unauthenticated (3 req/s)", path
        )
        return None
    keys = [k.strip() for k in path.read_text().splitlines() if k.strip()]
    return random.choice(keys) if keys else None


def main() -> int:
    if len(sys.argv) < _ARGV_MIN:
        sys.stderr.write("usage: yuj_worker.py <source>,<ACCESSION>\n")
        return 2

    item = sys.argv[-1].strip()
    source, _, accession = item.partition(",")
    source, accession = source.strip().lower(), accession.strip()
    if not accession:
        source, accession = "sra", item  # bare accession, assume SRA

    if source not in RESOLVERS:
        sys.stderr.write(f"FATAL: no resolver for source {source!r}\n")
        return 3

    out_dir = Path(os.environ.get("PMID_OUT_DIR", Path.cwd() / "results"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{accession}.json"
    if out.exists():
        logger.info("%s already done", accession)
        return 0

    # Import per accession so a broken host venv fails on the accession being run.
    try:
        from pysradb.sraweb import SRAweb
    except ImportError:
        sys.stderr.write("FATAL: pysradb not importable; run install_deps.sh\n")
        return 4

    try:
        import pysradb

        version = getattr(pysradb, "__version__", "unknown")
    except Exception:
        version = "unknown"

    key = _load_key()
    if key:
        os.environ["NCBI_API_KEY"] = key

    method, col = RESOLVERS[source]
    started = time.time()
    pmids: list[str] = []
    error = None
    try:
        sra = SRAweb()
        df = getattr(sra, method)([accession])
        for _, row in df.iterrows():
            pmid = row.get("pmid")
            if (
                pmid is not None
                and str(pmid).strip()
                and str(pmid).strip().lower() != "nan"
            ):
                pmids.append(str(pmid).strip())
    except KeyError:
        # pysradb raises KeyError when the response lacks a pmid column: a real
        # nothing-found result.
        pmids = []
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        logger.error("%s failed: %s", accession, error)

    payload = {
        "accession": accession,
        "source": source,
        "resolver": method,
        "accession_column": col,
        "pmids": sorted(set(pmids)),
        "error": error,
        "pysradb_version": version,
        "elapsed_s": round(time.time() - started, 2),
        "checked_at": datetime.now(UTC).isoformat(),
    }
    out.write_text(json.dumps(payload))
    logger.info(
        "%s -> %d pmid(s)%s",
        accession,
        len(payload["pmids"]),
        " [ERROR]" if error else "",
    )
    # A transport error must be non-zero so yuj retries the item; "no paper
    # found" is a success and must not be retried forever.
    return 1 if error else 0


if __name__ == "__main__":
    raise SystemExit(main())
