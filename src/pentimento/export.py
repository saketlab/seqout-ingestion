"""Mirror the Pentimento SQLite tables, never views, to parquet, one per table."""

import logging
import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from helpers import sqlite_connect

logger = logging.getLogger(__name__)

BATCH_SIZE = 100_000
ROW_GROUP_SIZE = 1_000_000

# SQLite declared type -> arrow. Affinity rules mean only the first word matters
# (VARCHAR(50) -> TEXT affinity).
_AFFINITY = (
    ("INT", pa.int64()),
    ("CHAR", pa.string()),
    ("CLOB", pa.string()),
    ("TEXT", pa.string()),
    ("BLOB", pa.binary()),
    ("REAL", pa.float64()),
    ("FLOA", pa.float64()),
    ("DOUB", pa.float64()),
)


def _arrow_type(column: str, declared: str) -> pa.DataType:
    """
    Map a declared SQLite type to arrow, refusing to guess.

    An UNDECLARED type is an error, not NUMERIC affinity. A column built by
    CREATE TABLE AS SELECT from an expression has an empty declared type, and
    silently defaulting to float64 wrote every flag, count and boolean of the
    rollup tables into the parquet mirror as a double -- turning the tri-state
    into 1.0/0.0/null. Declare the column instead of widening this fallback.
    """
    upper = declared.upper()
    for token, atype in _AFFINITY:
        if token in upper:
            return atype
    raise ValueError(f"{column}: undeclared type {declared!r}; declare it in the DDL")


def _schema(conn: sqlite3.Connection, table: str) -> pa.Schema:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if not rows:
        raise ValueError(f"no such table: {table}")
    return pa.schema([pa.field(r[1], _arrow_type(r[1], r[2] or "")) for r in rows])


def _tables(conn: sqlite3.Connection) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
    ]


def _export_table(
    conn: sqlite3.Connection,
    table: str,
    output_dir: Path,
    batch_size: int,
    compression: str,
) -> int:
    schema = _schema(conn, table)

    final_path = output_dir / f"{table}.parquet"
    tmp_path = output_dir / f"{table}.parquet.tmp"
    cur = conn.execute(f"SELECT * FROM {table}")  # noqa: S608
    written = 0
    # .tmp then atomic rename: a present <table>.parquet always means a complete
    # table, so a killed export is resumable by simply rerunning.
    with (
        pq.ParquetWriter(
            tmp_path,
            schema,
            compression=compression,
            use_dictionary=True,
        ) as writer,
        tqdm(desc=table, unit="row", leave=False) as pbar,
    ):
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            columns = list(zip(*rows, strict=True))
            writer.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(columns[i], type=schema.field(i).type)
                        for i in range(len(schema))
                    ],
                    schema=schema,
                ),
                row_group_size=ROW_GROUP_SIZE,
            )
            written += len(rows)
            pbar.update(len(rows))

    tmp_path.replace(final_path)
    return written


def export_parquet(
    db_file: Path,
    output_dir: Path,
    tables: list[str] | None = None,
    batch_size: int = BATCH_SIZE,
    compression: str = "zstd",
    *,
    resume: bool = False,
) -> dict[str, int]:
    """Write <output_dir>/<table>.parquet for every table in the Pentimento DB."""
    output_dir.mkdir(parents=True, exist_ok=True)
    # immutable=1, not mode=ro: Pentimento may carry a live -wal and a read-only
    # open would fail with "attempt to write a readonly database".
    conn = sqlite_connect(f"file:{db_file}?immutable=1", uri=True)
    try:
        todo = tables or _tables(conn)
        if resume:
            todo = [t for t in todo if not (output_dir / f"{t}.parquet").exists()]
        counts: dict[str, int] = {}
        for table in todo:
            counts[table] = _export_table(
                conn, table, output_dir, batch_size, compression
            )
            logger.info("%s -> %s rows", table, counts[table])
    finally:
        conn.close()
    return counts
