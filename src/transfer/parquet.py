from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# pg source table -> parquet basename the DuckDB client expects. Names match
# unless the pg table diverged from the historical SQLite-dump name.
_PG_PARQUET_TABLES: list[tuple[str, str]] = [
    ("arrayexpress_experiments", "arrayexpress_experiments"),
    ("arrayexpress_samples", "arrayexpress_samples"),
    ("ena_studies", "ena_studies"),
    ("ena_experiments", "ena_experiments"),
    ("ena_samples", "ena_samples"),
    ("geo_series", "geo_series"),
    ("geo_samples", "geo_samples"),
    ("geo_contributors", "geo_contributors"),
    ("geo_platforms", "geo_platforms"),
    ("sra_studies", "sra_studies"),
    ("sra_experiments", "sra_experiments"),
    ("sra_samples", "sra_samples"),
    ("sra_runs", "sra_runs"),
    ("sra_submissions", "sra_submissions"),
    ("unified_metadata", "unified_metadata"),
    ("unified_metadata_centers", "unified_centers"),
    ("pubmed_metadata", "pubmed_metadata"),
    ("sra_run_download_links", "run_download_links"),
    # new sources
    ("gsa_studies", "gsa_studies"),
    ("gsa_experiments", "gsa_experiments"),
    ("gsa_samples", "gsa_samples"),
    ("gsa_projects", "gsa_projects"),
    ("dra_studies", "dra_studies"),
    ("dra_experiments", "dra_experiments"),
    ("dra_samples", "dra_samples"),
    ("dra_runs", "dra_runs"),
    ("dra_submissions", "dra_submissions"),
    ("gea_experiments", "gea_experiments"),
    ("gea_samples", "gea_samples"),
]


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
    gsa_db_file: Path | None = None,
    gsa_human_db_file: Path | None = None,
    gsa_bioproject_db_file: Path | None = None,
    dra_db_file: Path | None = None,
    gea_db_file: Path | None = None,
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
        gsa_db_file,
        gsa_human_db_file,
        gsa_bioproject_db_file,
        dra_db_file,
        gea_db_file,
    ]:
        if db_file is None:  # ponytail: new sources are optional, skip if not passed
            continue
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
    from helpers import sqlite_connect  # lazy: pulls socks, sqlite-only path

    with sqlite_connect(db_file) as conn:
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


def pg_parquet_transfer(
    pg_url: str,
    output_dir: Path,
    num_workers: int,
    batch_size: int,
) -> None:
    """Build the parquet dump straight from Postgres (source of truth) instead
    of the per-source SQLite dumps. Covers the same public tables the DuckDB
    client reads, plus the GSA/DRA/GEA sources."""
    output_dir.mkdir(exist_ok=True, parents=True)

    with ThreadPoolExecutor(num_workers) as pool:
        futures = [
            pool.submit(_export_pg_table, pg_url, src, out, output_dir, batch_size)
            for src, out in _PG_PARQUET_TABLES
        ]
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


def _pg_select_columns(conn: psycopg.Connection, table: str) -> list[str]:
    """Build the SELECT list, casting jsonb/array columns to text so the parquet
    matches the JSON-string shape the SQLite-origin dump had (keeps the client's
    json.loads(...) accessors working)."""
    rows = conn.execute(
        "SELECT column_name, data_type, udt_name "
        "FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s "
        "ORDER BY ordinal_position",
        [table],
    ).fetchall()
    exprs: list[str] = []
    for name, data_type, udt in rows:
        col = f'"{name}"'
        if data_type == "ARRAY":
            exprs.append(f'to_json({col})::text AS {col}')
        elif udt in ("jsonb", "json"):
            exprs.append(f'{col}::text AS {col}')
        else:
            exprs.append(col)
    return exprs


def _export_pg_table(
    pg_url: str,
    src_table: str,
    out_name: str,
    output_dir: Path,
    batch_size: int,
) -> None:
    with psycopg.connect(pg_url) as conn:
        total = conn.execute(
            f'SELECT COUNT(*) FROM "{src_table}"',  # noqa: S608
        ).fetchone()[0]
        select = ", ".join(_pg_select_columns(conn, src_table))
        chunks: list[pa.Table] = []
        # server-side cursor so we stream instead of loading the whole table
        with (
            conn.cursor(name=f"pq_{out_name}") as cur,
            tqdm(total=total, desc=out_name, unit="row", leave=False) as pbar,
        ):
            cur.itersize = batch_size
            cur.execute(f"SELECT {select} FROM \"{src_table}\"")  # noqa: S608
            cols = None
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                if cols is None:
                    cols = [d.name for d in cur.description]
                chunks.append(pa.Table.from_pandas(pd.DataFrame(batch, columns=cols)))
                pbar.update(len(batch))

    if not chunks:
        return

    # ponytail: buffers all chunks per table then reconciles, same as the
    # sqlite path. Fine at current sizes; go incremental if a table outgrows RAM.
    unified_schema = _reconcile_schemas([c.schema for c in chunks])
    chunks = [c.cast(unified_schema) for c in chunks]
    with pq.ParquetWriter(
        output_dir / f"{out_name}.parquet",
        unified_schema,
        compression="snappy",
    ) as writer:
        for table_chunk in chunks:
            writer.write_table(table_chunk)


def _fetch_all_tables(db_file: Path, tables: list[tuple[str, Path]]) -> None:
    from helpers import sqlite_connect  # lazy: pulls socks, sqlite-only path

    with sqlite_connect(db_file) as conn:
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
