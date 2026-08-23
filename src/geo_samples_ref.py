"""
Policy for `geo_series.samples_ref`: JSON array of GSM strings.

Readers accept legacy `{"@ref": ...}` rows until SQLite databases are flattened.
Bad input yields [] so corrupt GEO rows leave builds running.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

#: The single MINiML `<Sample-Ref>` attribute; legacy dict rows carried
#: this key.
REF_KEY = "@ref"


def flatten_samples_ref(parsed: object) -> list[str]:
    """Flatten a parsed `samples_ref` value to a list of GSM accessions."""
    if parsed is None:
        return []

    # a single <Sample-Ref> that a parser handed back unwrapped
    items = parsed if isinstance(parsed, list) else [parsed]

    refs: list[str] = []
    for item in items:
        if isinstance(item, str):
            if item:
                refs.append(item)
        # Keep until the one-off SQLite GEO flattening migration has run everywhere.
        elif isinstance(item, dict):
            ref = item.get(REF_KEY)
            if isinstance(ref, str) and ref:
                refs.append(ref)
        else:
            logger.debug("samples_ref element ignored: %r", item)

    return refs


def load_samples_ref(raw: object) -> list[str]:
    """Read a `samples_ref` cell into a list of GSM accessions."""
    if raw is None or raw == "":
        return []

    if isinstance(raw, (list, tuple)):
        return flatten_samples_ref(list(raw))

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")

    if not isinstance(raw, str):
        logger.warning("samples_ref of unexpected type %s ignored", type(raw).__name__)
        return []

    try:
        parsed = json.loads(raw)
    except ValueError:
        logger.warning("samples_ref is not valid JSON (%d chars), skipped", len(raw))
        return []

    return flatten_samples_ref(parsed)


def dump_samples_ref(parsed: object) -> str | None:
    """
    Serialise parsed `samples_ref` in the string-array shape.

    Empty results stay NULL, matching both ingest paths.
    """
    refs = flatten_samples_ref(parsed)
    return json.dumps(refs) if refs else None
