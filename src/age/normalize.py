"""
Age-normalization stage: parse raw per-sample ``age`` into ``age_norm.sqlite3``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tqdm import tqdm

from age.parser import parse_age
from helpers import exec_query_from_file, sqlite_connect

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_AGE_FILTER = "age IS NOT NULL AND age <> ''"


def normalize_age(source_db: Path, output_db: Path, batch_size: int) -> None:
    """Normalize every non-empty raw age from ``source_db`` into ``output_db``."""
    exec_query_from_file("tables/age.sql", output_db)

    with sqlite_connect(source_db) as src:
        distinct = [
            r[0]
            for r in src.execute(
                f"SELECT DISTINCT age FROM ontology_samples WHERE {_AGE_FILTER}"  # noqa: S608
            )
        ]
        parsed = {raw: parse_age(raw) for raw in distinct}
        numeric = sum(1 for a in parsed.values() if a.age_days is not None)
        logger.info(
            "parsed %d distinct age strings (%d numeric, %d null)",
            len(parsed),
            numeric,
            len(parsed) - numeric,
        )

        with sqlite_connect(output_db) as out:
            out.execute("PRAGMA journal_mode=WAL")
            out.execute("PRAGMA synchronous=NORMAL")
            row_sql = f"SELECT study_accession, sample, age FROM ontology_samples WHERE {_AGE_FILTER}"  # noqa: S608, E501
            cursor = src.execute(row_sql)
            batch: list[tuple] = []
            written = 0
            for study, sample, raw in tqdm(cursor, unit="row", desc="normalizing age"):
                norm = parsed[raw]
                batch.append(
                    (
                        study,
                        sample,
                        norm.age_days,
                        norm.age_group,
                        1 if norm.prenatal else 0,
                        norm.age_raw,
                    )
                )
                if len(batch) >= batch_size:
                    _flush(out, batch)
                    written += len(batch)
                    batch = []
            if batch:
                _flush(out, batch)
                written += len(batch)
            out.commit()

    logger.info("age normalization complete: %d rows -> %s", written, output_db)


def _flush(conn, batch: list[tuple]) -> None:  # noqa: ANN001
    conn.executemany(
        "INSERT OR REPLACE INTO age_norm "
        "(study_accession, sample, age_days, age_group, prenatal, age_raw) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        batch,
    )
