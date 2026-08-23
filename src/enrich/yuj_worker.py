"""Fleet worker for `enrich batch`, one study per invocation."""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
_ARGV_MIN = 2

_EXIT_USAGE = 2
_EXIT_MISSING_DEPENDENCY = 3
_EXIT_NO_METADATA = 4


def _bool_env(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "false", "no")


def _fatal(message: str, code: int) -> int:
    # loud, and non-zero: a host that cannot enrich must not report success
    sys.stderr.write(f"FATAL: {message}\n")
    return code


def main() -> int:
    if len(sys.argv) < _ARGV_MIN:
        sys.stderr.write("usage: yuj_worker.py <ACCESSION>\n")
        return _EXIT_USAGE
    accession = sys.argv[-1]

    try:
        from enrich.batch import enrich_batch, load_projects
    except ImportError as exc:
        return _fatal(
            f"cannot import enrich.batch ({exc}). Install this repo's base deps "
            f"plus the `enrich` extra on the host; a worker without them would "
            f"otherwise report success having enriched nothing",
            _EXIT_MISSING_DEPENDENCY,
        )

    module = os.environ.get("ENRICH_MODULE", "enrich.evidence_gate")
    try:
        # Probe the engine, not just wrappers. The wrappers import without the
        # heavy dependencies; the lazy engine import is what fails on bare hosts.
        importlib.import_module(module)
        importlib.import_module("enrich.extract")
    except ImportError as exc:
        return _fatal(
            f"cannot import the extraction engine for {module} ({exc}). Install "
            f"the `enrich` extra on this host",
            _EXIT_MISSING_DEPENDENCY,
        )

    cache = os.environ.get("ENRICH_CACHE_FILE", str(HERE / "metadata_cache.pkl"))
    if not Path(cache).is_file():
        return _fatal(
            f"metadata cache not found at {cache}. Compute nodes are offline, so "
            f"without it every study resolves to no samples and is skipped",
            _EXIT_MISSING_DEPENDENCY,
        )

    projects = load_projects([accession], Path(cache))
    if accession not in projects:
        # The slice and cache disagree; report studies that were never attempted.
        return _fatal(f"{accession} is not in {cache}", _EXIT_NO_METADATA)

    species = {}
    for env_name, key in (
        ("EXPECTED_TAXID", "expected_taxid"),
        ("EXPECTED_SPECIES", "expected_species"),
    ):
        if os.environ.get(env_name):
            species[key] = os.environ[env_name]

    _, totals = enrich_batch(
        projects,
        Path(os.environ.get("ENRICH_OUT_DIR", HERE / "results")),
        module=module,
        model=os.environ.get("ENRICH_MODEL", "rnj-1"),
        embedding_model=os.environ.get(
            "ENRICH_EMBEDDING_MODEL", "abhinand/MedEmbed-large-v0.1"
        ),
        tierc_policy=os.environ.get("ENRICH_TIERC_POLICY", "evidence"),
        vocab=os.environ.get("ENRICH_VOCAB", "v5"),
        species=species,
        declared=_bool_env("ENRICH_DECLARED"),
    )
    logger.info("enrich %s: %s", accession, json.dumps(totals, sort_keys=True))
    # a study the module could not enrich is a failure of THIS item, not of the
    # host; report it so yuj does not count an empty result as done
    return 1 if totals.get("failed_studies") else 0


if __name__ == "__main__":
    raise SystemExit(main())
