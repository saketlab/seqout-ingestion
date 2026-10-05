"""Load a batch of enrichment CSVs into SQLite."""

from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path

from age.parser import parse_age
from helpers import exec_query_from_file, sqlite_connect

logger = logging.getLogger(__name__)

MIN_FILE_SIZE = 100
COMMIT_INTERVAL = 500

ENRICHED_COLS = [
    "age",
    "sex",
    "ethnicity",
    "phenotype",
    "cell_type",
    "tissue",
    "strain",
    "disease",
    "assay",
    "assay_category",
    "cell_line",
    "treatment",
    "development_stage",
    "sample_type",
    "genetic_modification",
]

CELLOSAURUS_COLS = [
    *ENRICHED_COLS,
    "organism",
    "taxid",
    "tissue_primary_site",
    "tissue_site_type",
]

_ONTOLOGY_FIELDS = ("disease", "tissue", "cell_type", "assay", "development_stage")
_ONTOLOGY_SUFFIXES = (
    "ontology_id",
    "ontology_name",
    "ontology_score",
    "match_method",
    "ancestors",
)
# Normalize age strings (hours or Cellosaurus tokens such as 30Y6M) into
# shared columns. _derive_age adds these fields; source CSVs omit them.
AGE_CANON_COLS = ["age_days", "age_group", "prenatal", "age_raw"]

ONTOLOGY_COLS = [
    *CELLOSAURUS_COLS,
    *[f"{f}_{s}" for f in _ONTOLOGY_FIELDS for s in _ONTOLOGY_SUFFIXES],
    *AGE_CANON_COLS,
]

_PROVENANCE_SUFFIXES = (
    "source",
    "score",
    "column",
    "original_source",
    "original_column",
    "original_score",
)
PROVENANCE_COLS = [f"{f}_{s}" for f in ENRICHED_COLS for s in _PROVENANCE_SUFFIXES]

DECLARED_COLS = [
    "insdc_analyte",
    "insdc_assay_modality",
    "insdc_library_strategy",
    "insdc_library_source",
    "insdc_library_selection",
    "library_strategy",
    "insdc_conflict",
    "ena_cell_line",
    "ena_cell_type",
    "ena_tissue_type",
    "ena_tissue_lib",
    "ena_disease",
    "ena_dev_stage",
    "ena_strain",
    "ena_sub_strain",
    "ena_sex",
    "ena_host_sex",
    "ena_age",
]

TABLES: dict[str, list[str]] = {
    "enriched_samples": ENRICHED_COLS,
    "cellosaurus_samples": CELLOSAURUS_COLS,
    "ontology_samples": ONTOLOGY_COLS,
    "provenance_samples": PROVENANCE_COLS,
    "declared_samples": DECLARED_COLS,
}

_SUFFIX = {
    "enriched_samples": "enriched",
    "cellosaurus_samples": "cellosaurus",
    "ontology_samples": "ontology",
    "provenance_samples": "provenance",
    "declared_samples": "declared",
}

CORRECTIONS_COLS = (
    "study_accession",
    "sample",
    "table_name",
    "field",
    "old_value",
    "new_value",
    "action",
    "reason",
    "source_value",
    "detector",
    "framework_version",
    "ts",
)


def _fast_name(table: str) -> str:
    return f"{table}_fast"


def schema_sql() -> str:
    """Generate the DDL from the column lists above."""
    parts = [
        "CREATE TABLE IF NOT EXISTS enriched_studies (\n"
        "    accession TEXT PRIMARY KEY,\n"
        "    title     TEXT,\n"
        "    n_samples INTEGER NOT NULL DEFAULT 0\n);",
        # Keep embedding-only results in separate _fast tables; full enrichment
        # leaves them untouched, and consumers prefer the non-_fast tables. See
        # docs/superpowers/specs/2026-09-14-embedding-only-enrichment.md.
        f"CREATE TABLE IF NOT EXISTS {_fast_name('enriched_studies')} (\n"
        "    accession TEXT PRIMARY KEY,\n"
        "    title     TEXT,\n"
        "    n_samples INTEGER NOT NULL DEFAULT 0\n);",
    ]
    for table, cols in TABLES.items():
        defs = ",\n    ".join(f"{c} TEXT" for c in cols)
        parts.extend(
            f"CREATE TABLE IF NOT EXISTS {name} (\n"
            f"    study_accession TEXT NOT NULL,\n"
            f"    sample          TEXT NOT NULL,\n"
            f"    {defs},\n"
            f"    PRIMARY KEY (study_accession, sample)\n);"
            for name in (table, _fast_name(table))
        )
    parts.append(
        "CREATE TABLE IF NOT EXISTS enrichment_corrections_log (\n    "
        + ",\n    ".join(f"{c} TEXT" for c in CORRECTIONS_COLS)
        + "\n);"
    )
    parts += [
        f"CREATE INDEX IF NOT EXISTS idx_{name}_study ON {name}(study_accession);"
        for t in TABLES
        for name in (t, _fast_name(t))
    ]
    return "\n\n".join(parts) + "\n"


def _read_csv(path: Path) -> list[dict]:
    try:
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            return list(csv.DictReader(handle))
    except OSError as exc:
        logger.warning("SKIP %s: %s", path.name, exc)
        return []


def _derive_age(rows: list[dict]) -> list[dict]:
    """Add the canonical age columns, in place, from each row's raw ``age``."""
    for row in rows:
        raw = (row.get("age") or "").strip()
        if not raw:
            # Treat blank age as missing, not parse_age's "unspecified" category.
            row.update(dict.fromkeys(AGE_CANON_COLS, ""))
            continue
        norm = parse_age(raw)
        row["age_days"] = "" if norm.age_days is None else repr(norm.age_days)
        row["age_group"] = norm.age_group or ""
        # 't'/'f' so Postgres COPY reads it as boolean straight out of SQLite
        row["prenatal"] = "t" if norm.prenatal else "f"
        row["age_raw"] = raw
    return rows


def _insert(
    conn: sqlite3.Connection,
    table: str,
    cols: list[str],
    accession: str,
    rows: list[dict],
) -> int:
    all_cols = ["study_accession", "sample", *cols]
    batch = [
        (accession, sample_id, *[(r.get(c) or "").strip() or None for c in cols])
        for r in rows
        if (sample_id := (r.get("Sample") or "").strip())
    ]
    if not batch:
        return 0
    conn.executemany(
        f"INSERT OR REPLACE INTO {table} ({', '.join(all_cols)}) "  # noqa: S608
        f"VALUES ({', '.join(['?'] * len(all_cols))})",
        batch,
    )
    return len(batch)


def load_titles(unified_db: Path | None) -> dict[str, str]:
    """Study titles for enriched_studies.  Empty when no unified DB is named."""
    if unified_db is None or not Path(unified_db).exists():
        logger.warning("no unified DB; enriched_studies.title will be NULL")
        return {}
    try:
        conn = sqlite_connect(f"file:{unified_db}?immutable=1", uri=True)
        try:
            rows = conn.execute(
                "SELECT canonical_accession, title FROM unified_metadata"
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        logger.warning(
            "cannot read titles from %s (%s); titles will be NULL", unified_db, exc
        )
        return {}
    return dict(rows)


def ingest(
    csv_dir: Path,
    db_file: Path,
    unified_db: Path | None = None,
    limit: int | None = None,
    *,
    fast: bool = False,
) -> dict[str, int]:
    """
    Load every study in ``csv_dir`` into ``db_file``.  Returns row counts.

    fast=True routes every table to its _fast twin (an embedding-only batch's
    output), never touching the trusted tables a full `enrich batch` run
    writes to.
    """
    files = [
        f
        for f in sorted(csv_dir.glob("*_enriched.csv"))
        if f.stat().st_size >= MIN_FILE_SIZE
    ]
    if limit:
        files = files[:limit]
    accessions = [f.name.removesuffix("_enriched.csv") for f in files]
    logger.info("%d studies with an _enriched.csv", len(accessions))

    titles = load_titles(unified_db)
    studies_table = _fast_name("enriched_studies") if fast else "enriched_studies"

    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite_connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    exec_query_from_file("tables/enrichment.sql", conn=conn)

    studies: list[tuple] = []
    for acc in accessions:
        rows = _read_csv(csv_dir / f"{acc}_enriched.csv")
        if not rows:
            continue
        studies.append((acc, titles.get(acc), len(rows)))
        for table, cols in TABLES.items():
            dest = _fast_name(table) if fast else table
            path = csv_dir / f"{acc}_{_SUFFIX[table]}.csv"
            if table == "enriched_samples":
                _insert(conn, dest, cols, acc, rows)
            elif path.exists() and (extra := _read_csv(path)):
                if table == "ontology_samples":
                    _derive_age(extra)
                _insert(conn, dest, cols, acc, extra)
        if len(studies) >= COMMIT_INTERVAL:
            _flush(conn, studies, studies_table)
    _flush(conn, studies, studies_table)

    counts = {
        t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]  # noqa: S608
        for t in (studies_table, *(_fast_name(t) if fast else t for t in TABLES))
    }
    conn.close()
    return counts


def _flush(conn: sqlite3.Connection, studies: list[tuple], table: str) -> None:
    if studies:
        conn.executemany(
            f"INSERT OR REPLACE INTO {table} (accession, title, n_samples) "  # noqa: S608
            "VALUES (?, ?, ?)",
            studies,
        )
    conn.commit()
    studies.clear()


def install_corrections(db_file: Path, audits: list[tuple]) -> int:
    """Append the inline correction audit rows.  Same 12 columns as the post-hoc log."""
    conn = sqlite_connect(db_file)
    exec_query_from_file("tables/enrichment.sql", conn=conn)
    if audits:
        # S608: only placeholder counts are interpolated; all values are bound.
        conn.executemany(
            "INSERT INTO enrichment_corrections_log VALUES "  # noqa: S608
            f"({', '.join(['?'] * len(CORRECTIONS_COLS))})",
            audits,
        )
    conn.commit()
    conn.close()
    return len(audits)
