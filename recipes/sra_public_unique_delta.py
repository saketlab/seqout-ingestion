r"""
How many PUBLIC, unique SRA studies in a local sra db are not yet in Postgres.

"Unique" = distinct study accessions in sra_studies. "Public" is read from
NCBI's own SRA_Accessions.tab (Visibility column), not inferred: a study
present in a metadata dump can still be dbGaP/controlled-access, and this
report must not count those as ready to transfer.

Usage:
    .venv/bin/python recipes/sra_public_unique_delta.py \\
        --sra-db-file /data/share/seqout/sra/db/2026-09-13/sra_metadata_2026-09-13.db \\
        --accessions-tab /data/share/seqout/sra/SRA_Accessions.tab

Read-only: only SELECTs against Postgres, via recipes/pg_connect.py (which
brings the ssh tunnel up itself if it's down). Nothing here writes anywhere.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

from recipes.pg_connect import connect  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


def _public_study_accessions(accessions_tab: Path) -> set[str]:
    """Study accessions NCBI itself marks Type=STUDY, Visibility=public."""
    public: set[str] = set()
    with accessions_tab.open(newline="") as f:
        # QUOTE_NONE: this is plain TSV, not CSV; a stray literal `"` in a
        # free-text field (Alias, etc.) otherwise makes the csv module think
        # it is inside one huge quoted field spanning the rest of the file.
        reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(reader)
        col = {name: i for i, name in enumerate(header)}
        for row in reader:
            if row[col["Type"]] == "STUDY" and row[col["Visibility"]] == "public":
                public.add(row[col["Accession"]])
    return public


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sra-db-file", type=Path, required=True)
    parser.add_argument(
        "--accessions-tab",
        type=Path,
        default=Path("/data/share/seqout/sra/SRA_Accessions.tab"),
    )
    args = parser.parse_args()

    with sqlite3.connect(f"file:{args.sra_db_file}?mode=ro", uri=True) as conn:
        local = {r[0] for r in conn.execute("SELECT accession FROM sra_studies")}
    logger.info("%d distinct study accessions in %s", len(local), args.sra_db_file)

    public = _public_study_accessions(args.accessions_tab)
    local_public = local & public
    logger.info(
        "%d/%d local studies are public (per SRA_Accessions.tab)",
        len(local_public),
        len(local),
    )

    with connect() as pg, pg.cursor() as cur:
        cur.execute("SET statement_timeout=0")
        cur.execute("CREATE TEMP TABLE _chk(acc text primary key)")
        with cur.copy("COPY _chk(acc) FROM STDIN") as cp:
            for acc in local_public:
                cp.write_row((acc,))
        cur.execute("ANALYZE _chk")
        cur.execute(
            "SELECT COUNT(*) FROM _chk c "
            "WHERE EXISTS (SELECT 1 FROM sra_studies s WHERE s.accession = c.acc)",
        )
        row = cur.fetchone()
        already_have = row[0] if row else 0

    new = len(local_public) - already_have
    print(
        f"public unique study accessions in {args.sra_db_file.name}: "
        f"{len(local_public)}"
    )
    print(f"already in postgres sra_studies: {already_have}")
    print(f"new, public, ready to transfer: {new}")


if __name__ == "__main__":
    main()
