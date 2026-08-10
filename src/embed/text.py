"""
Source text extraction + cleaning for the embed pipeline.

Each source has its own recipe for turning a study row into the single string we
embed. The cleaning (lower-case, strip urls/html/non-alphanumerics, collapse
whitespace) is identical across sources so the shared embedding space stays
consistent. The iter_new_* helpers stream only accessions that are *not*
already present in chroma, yielding (accession, cleaned_text) pairs and
skipping rows whose text cleans down to nothing.
"""

import logging
import re
import sqlite3
from collections.abc import Iterator
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import sqlite_connect

logger = logging.getLogger(__name__)

_ASCII_MAX = 128


def remove_non_ascii(text: str) -> str:
    return "".join(char for char in text if ord(char) < _ASCII_MAX)


def remove_html_tags(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ")


def clean_text(
    text: str,
    *,
    convert_to_lower: bool = True,
    remove_non_alnum: bool = True,
) -> str:
    if convert_to_lower:
        text = text.lower()

    text = text.replace("\n", " ")
    text = re.sub(r"https?://\S+|www\.\S+", "", text)

    if remove_non_alnum:
        text = re.sub(r"\W", " ", text)

    text = remove_html_tags(text)
    text = re.sub(r" +", " ", text)
    return text.strip()


def _combine(*fields: str | None) -> str:
    return clean_text(" ".join(remove_non_ascii(f or "") for f in fields))


def iter_new_geo_texts(
    geo_db_file: Path,
    existing: set[str],
) -> Iterator[tuple[str, str]]:
    """Yield (accession, text) for geo_series rows missing from chroma."""
    with sqlite_connect(geo_db_file) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute("SELECT COUNT(*) FROM geo_series").fetchone()[0]
        cur = conn.execute(
            "SELECT accession, title, summary, overall_design FROM geo_series",
        )
        for row in tqdm(cur, total=total, desc="geo diff+clean"):
            acc = row["accession"]
            if acc in existing:
                continue
            text = _combine(row["title"], row["summary"], row["overall_design"])
            if text:
                yield acc, text


def iter_new_sra_texts(
    sra_db_file: Path,
    existing: set[str],
) -> Iterator[tuple[str, str]]:
    """
    Yield (accession, text) for sra_studies rows missing from chroma.

    GSE-aliased studies are excluded: they are geo duplicates already covered by
    the geo collection (matches the original cache_sra recipe).
    """
    where = "WHERE alias IS NULL OR alias NOT LIKE 'GSE%'"
    with sqlite_connect(sra_db_file) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute(
            f"SELECT COUNT(*) FROM sra_studies {where}",  # noqa: S608
        ).fetchone()[0]
        cur = conn.execute(
            f"SELECT accession, title, abstract FROM sra_studies {where}",  # noqa: S608
        )
        for row in tqdm(cur, total=total, desc="sra diff+clean"):
            acc = row["accession"]
            if acc in existing:
                continue
            text = _combine(row["title"], row["abstract"])
            if text:
                yield acc, text


def _ae_canonical_accessions(unified_metadata_db_file: Path) -> list[str]:
    """
    AE accessions that are canonical (not aliasing to sra/ena/geo).

    chroma's AE set is deduped against the other sources via unified_metadata, so
    we embed only rows that do not alias into sra/ena/geo.
    """
    with sqlite_connect(unified_metadata_db_file) as conn:
        rows = conn.execute(
            """
            SELECT canonical_accession
            FROM unified_metadata
            WHERE source = 'ae'
              AND (
                    aliases IS NULL
                 OR aliases = '{}'
                 OR (
                        json_extract(aliases, '$.sra') IS NULL
                    AND json_extract(aliases, '$.ena') IS NULL
                    AND json_extract(aliases, '$.geo') IS NULL
                 )
              )
            """,
        ).fetchall()
    return [r[0] for r in rows]


def iter_new_ae_texts(
    ae_db_file: Path,
    unified_metadata_db_file: Path,
    existing: set[str],
    lookup_batch_size: int = 900,
) -> Iterator[tuple[str, str]]:
    """Yield (accession, text) for new, canonical arrayexpress experiments."""
    canonical = _ae_canonical_accessions(unified_metadata_db_file)
    new_accs = [a for a in canonical if a not in existing]
    logger.info(
        "arrayexpress: %d canonical, %d new (of %d already in chroma)",
        len(canonical),
        len(new_accs),
        len(existing),
    )
    if not new_accs:
        return

    with sqlite_connect(ae_db_file) as conn:
        conn.row_factory = sqlite3.Row
        for i in tqdm(
            range(0, len(new_accs), lookup_batch_size),
            desc="ae lookup+clean",
        ):
            chunk = new_accs[i : i + lookup_batch_size]
            placeholders = ",".join("?" for _ in chunk)
            query = (
                "SELECT accession, title, description "  # noqa: S608
                f"FROM arrayexpress_experiments WHERE accession IN ({placeholders})"
            )
            cur = conn.execute(query, chunk)
            for row in cur:
                text = _combine(row["title"], row["description"])
                if text:
                    yield row["accession"], text
