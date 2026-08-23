"""Cellosaurus policy: lookup paths, donated fields, and placeholders."""

from __future__ import annotations

import functools
import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_ONTOLOGY_DIR = "/NAS/pysraweb/ontology/data/parsed"


def ontology_path(var: str, filename: str) -> Path:
    """Resolve an ontology file: own env var, then ONTOLOGY_DIR, then site default."""
    direct = os.environ.get(var)
    if direct:
        return Path(direct)
    return Path(os.environ.get("ONTOLOGY_DIR", DEFAULT_ONTOLOGY_DIR)) / filename


def lookup_path() -> Path:
    """Read at call time, not import time, so a test can point at a fixture."""
    return ontology_path("CELLOSAURUS_LOOKUP", "cellosaurus_api_lookup.json")


def disease_primary_tissue_path() -> Path:
    return ontology_path("DISEASE_PRIMARY_TISSUE", "disease_primary_tissue.json")


# F2. Fields apply_cellosaurus_entries() writes with provenance "cellosaurus"
CELLOSAURUS_DONATED_FIELDS = (
    "cell_line",
    "age",
    "sex",
    "ethnicity",
    "tissue",
    "disease",
    "sample_type",
    "organism",
)

BACKPROP_FIELDS = tuple(f for f in CELLOSAURUS_DONATED_FIELDS if f != "cell_line")

CELL_LINE_PLACEHOLDERS = frozenset(
    {
        "na",
        "n",
        "none",
        "null",
        "nan",
        "unknown",
        "unspecified",
        "notapplicable",
        "notavailable",
        "notcollected",
        "notprovided",
        "missing",
        "wt",
        "wildtype",
        "control",
        "ctrl",
        "untreated",
        "primary",
        "other",
        "tbd",
    }
)


def normalize_cell_line_name(name: str) -> str:
    """Normalize a cell line name for lookup: lowercase, punctuation stripped."""
    return re.sub(r"[\s\-_./()]+", "", str(name).lower().strip())


def is_placeholder(name: str) -> bool:
    """Report whether a name is a placeholder; a placeholder is not an ID."""
    return normalize_cell_line_name(name) in CELL_LINE_PLACEHOLDERS


@functools.lru_cache(maxsize=2)
def _load(path: str) -> dict:
    try:
        with Path(path).open() as handle:
            return json.load(handle)
    except (OSError, ValueError):
        logger.warning(
            "cellosaurus table not readable at %s; continuing without it", path
        )
        return {}


def load_lookup() -> dict:
    """Load the name -> entry table.  Empty dict when the table is not present."""
    return _load(str(lookup_path()))


def load_disease_primary_tissue() -> dict:
    return _load(str(disease_primary_tissue_path()))


def lookup(cell_line_name: str) -> dict | None:
    if not cell_line_name or not isinstance(cell_line_name, str):
        return None
    key = normalize_cell_line_name(cell_line_name)
    if key in CELL_LINE_PLACEHOLDERS:
        return None
    return load_lookup().get(key)
