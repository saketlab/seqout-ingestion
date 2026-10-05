"""Fleet worker for `enrich batch`, accepting an accession or accession file."""

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
    # Report host failures with a non-zero exit.
    sys.stderr.write(f"FATAL: {message}\n")
    return code


def _missing_reference_files() -> list[str]:
    """Ontology and Cellosaurus files the engine needs but cannot read."""
    from enrich.cellosaurus import lookup_path
    from enrich.ontology_map import FIELD_ONTOLOGY_MAP, ONTOLOGY_DIR

    paths = {ONTOLOGY_DIR / name for name, _, _ in FIELD_ONTOLOGY_MAP.values()}
    paths.add(Path(lookup_path()))
    return sorted(str(p) for p in paths if not p.is_file())


def main() -> int:  # noqa: PLR0911 (early-return fatal guards)
    if len(sys.argv) < _ARGV_MIN:
        sys.stderr.write("usage: yuj_worker.py <ACCESSION|accessions.txt>\n")
        return _EXIT_USAGE
    arg = sys.argv[-1]
    # Batch mode loads the embedding model once for the accession file.
    batch_path = Path(arg)
    if batch_path.is_file() and batch_path.suffix == ".txt":
        accessions = [
            line.strip() for line in batch_path.read_text().splitlines() if line.strip()
        ]
    else:
        accessions = [arg]

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
        # Import the lazy engine too; wrappers alone do not check heavy dependencies.
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

    missing_refs = _missing_reference_files()
    if missing_refs:
        return _fatal(
            f"ontology/cellosaurus files missing: {', '.join(missing_refs)}. The "
            f"engine only warns and carries on, writing output with no ontology "
            f"ids that the resume check then counts as done",
            _EXIT_MISSING_DEPENDENCY,
        )

    out_dir = Path(os.environ.get("ENRICH_OUT_DIR", HERE / "results"))
    # Resume partial runs by skipping accessions with output files.
    pending = [a for a in accessions if not (out_dir / f"{a}_enriched.csv").exists()]
    if not pending:
        logger.info("batch of %d: all already enriched, nothing to do", len(accessions))
        return 0

    projects = load_projects(pending, Path(cache))
    missing = [a for a in pending if a not in projects]
    if missing:
        logger.warning(
            "%d/%d accessions not in %s, skipping: %s",
            len(missing),
            len(pending),
            cache,
            ", ".join(missing[:10]),
        )
    if not projects:
        return _fatal(
            f"none of {len(pending)} accessions are in {cache}",
            _EXIT_NO_METADATA,
        )

    species = {}
    for env_name, key in (
        ("EXPECTED_TAXID", "expected_taxid"),
        ("EXPECTED_SPECIES", "expected_species"),
    ):
        if os.environ.get(env_name):
            species[key] = os.environ[env_name]

    _, totals = enrich_batch(
        projects,
        out_dir,
        module=module,
        model=os.environ.get("ENRICH_MODEL", "rnj-1"),
        embedding_model=os.environ.get(
            "ENRICH_EMBEDDING_MODEL", "abhinand/MedEmbed-large-v0.1"
        ),
        tierc_policy=os.environ.get("ENRICH_TIERC_POLICY", "evidence"),
        vocab=os.environ.get("ENRICH_VOCAB", "v5"),
        species=species,
        declared=_bool_env("ENRICH_DECLARED"),
        embedding_only=_bool_env("ENRICH_EMBEDDING_ONLY", default="0"),
    )
    logger.info(
        "enrich batch of %d (%d pending): %s",
        len(accessions),
        len(pending),
        json.dumps(totals, sort_keys=True),
    )
    # Fail if every pending study failed. Partial successes return 0;
    # per-item output checks allow later runs to retry missing results.
    failed = totals.get("failed_studies", 0)
    return 1 if failed and failed >= len(pending) else 0


if __name__ == "__main__":
    raise SystemExit(main())
