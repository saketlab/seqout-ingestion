import logging
from pathlib import Path

from helpers import sqlite_connect

SC_ENRICH_OUTPUT_DIR = "sc_enrich"

logger = logging.getLogger(__name__)


def yuj_setup(
    base_output_dir: Path,
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    ae_db_file: Path,
    batch_size: int,
) -> None:
    output_dir = base_output_dir / SC_ENRICH_OUTPUT_DIR

    conn = sqlite_connect(unified_metadata_db_file)
    geo_conn = sqlite_connect(geo_db_file)
    ae_conn = sqlite_connect(ae_db_file)

    rows: list[tuple[str, str]] = conn.execute(
        "SELECT canonical_accession, source "
        "FROM unified_metadata WHERE is_single_cell = '1' "
        "AND (single_cell_count IS NULL OR single_cell_count = 0);",
    ).fetchall()

    logger.info(rows)
