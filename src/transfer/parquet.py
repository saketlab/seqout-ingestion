import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


def parquet_transfer(
    output_dir: Path,
    sra_db_file: Path,
    geo_db_file: Path,
    ena_db_file: Path,
    ae_db_file: Path,
    pubmed_metadata_db_file: Path,
    unified_metadata_db_file: Path,
    download_links_db_file: Path,
    num_workers: int,
    batch_size: int,
) -> None:
    output_dir.mkdir(exist_ok=True, parents=True)
    table_infos: list[tuple[str, Path]] = []

    for db_file in [
        sra_db_file,
        geo_db_file,
        ena_db_file,
        ae_db_file,
        pubmed_metadata_db_file,
        unified_metadata_db_file,
        download_links_db_file,
    ]:
        _fetch_all_tables(db_file, table_infos)

    with ThreadPoolExecutor(num_workers) as pool:
        futures = []

        for table_name, db_file in table_infos:
            if (
                ("_fts" in table_name)
                or ("sqlite_" in table_name)
                or (table_name == "study_fetch_log")
            ):
                continue

            futures.append(
                pool.submit(_export_to_pq, output_dir, table_name, db_file, batch_size),
            )

        with tqdm(
            total=len(futures),
            desc="exporting tables",
            unit="table",
            leave=False,
            position=0,
        ) as pbar:
            for f in as_completed(futures):
                f.result()
                pbar.update(1)


def _export_to_pq(
    output_dir: Path,
    table_name: str,
    db_file: Path,
    batch_size: int,
) -> None:
    with sqlite3.Connection(db_file) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}",  # noqa: S608
        ).fetchone()[0]
        chunks = []
        with tqdm(total=total, desc=f"{table_name}", unit="row", leave=False) as pbar:
            for chunk in pd.read_sql_query(
                f"SELECT * FROM {table_name}",  # noqa: S608
                conn,
                chunksize=batch_size,
            ):
                chunks.append(pa.Table.from_pandas(chunk))
                pbar.update(len(chunk))

        if not chunks:
            return

        unified_schema = _reconcile_schemas([c.schema for c in chunks])
        chunks = [c.cast(unified_schema) for c in chunks]

        output_path = output_dir / f"{table_name}.parquet"

        with pq.ParquetWriter(
            output_path,
            unified_schema,
            compression="snappy",
        ) as writer:
            for table_chunk in chunks:
                writer.write_table(table_chunk)


def _fetch_all_tables(db_file: Path, tables: list[tuple[str, Path]]) -> None:
    with sqlite3.Connection(db_file) as conn:
        result = [
            (r[0], db_file)
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall()
        ]

        tables.extend(result)


def _reconcile_schemas(schemas: list[pa.Schema]) -> pa.Schema:
    fields = []

    for i, field in enumerate(schemas[0]):
        t = field.type
        for schema in schemas[1:]:
            other = schema[i].type
            if other == t:
                continue
            if pa.types.is_null(t):
                t = other
            elif pa.types.is_null(other):
                continue
            elif pa.types.is_string(t) or pa.types.is_string(other):
                t = pa.large_string()
            elif pa.types.is_integer(t) or pa.types.is_integer(other):
                t = pa.float64()
            else:
                t = pa.large_string()

        fields.append(pa.field(field.name, t))
    return pa.schema(fields)
