import json
import sqlite3
from pathlib import Path


def sc_fastcounter(
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    num_workers: int,
):
    conn = sqlite3.connect(unified_metadata_db_file)
    rows: list[tuple[str, str]] = conn.execute(
        "SELECT canonical_accession, source FROM unified_metadata WHERE is_single_cell = 1;"
    ).fetchall()

    all_accessions: set[str] = set()
    accessions_to_process: set[str] = set()

    for accession, source in rows:
        all_accessions.add(accession)
        if source != "geo" and source != "ae":
            continue

        accessions_to_process.add(accession)

    non_geo_ae_accessions = all_accessions - accessions_to_process
    placeholders = ", ".join(repr(acc) for acc in non_geo_ae_accessions)
    rows = conn.execute(
        f"SELECT canonical_accession, aliases FROM unified_metadata WHERE canonical_accession IN ({placeholders}) GROUP BY canonical_accession"
    ).fetchall()

    non_geo_ae_mapping: dict[str, str] = {}

    # for accession, aliases_json in rows:
    #     aliases: dict[str, str] = json.loads(aliases_json)
    #     for _, value in aliases.items():
    #         if value.startswith("GSE")

    conn.close()
