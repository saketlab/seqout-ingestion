import logging
import re
import sqlite3
from collections import Counter
from pathlib import Path

from tqdm import tqdm

from helpers import exec_query_from_file, sqlite_connect

logger = logging.getLogger(__name__)

SC_PATTERNS = re.compile(
    r"scRNA[\-\s]?seq|sc[\-\s]RNA[\-\s]?seq"
    r"|snRNA[\-\s]?seq|sn[\-\s]RNA[\-\s]?seq"
    r"|single[\-\s]cell[\-\s]+RNA"
    r"|single[\-\s]cell[\-\s]+transcriptom"
    r"|single[\-\s]nucle\w*[\-\s]+RNA"
    r"|single[\-\s]nucle\w*[\-\s]+transcriptom"
    r"|10x\s+Genomics|10X\s+Genomics"
    r"|Drop[\-\s]?seq|dropseq"
    r"|Smart[\-\s]?seq\d?|smartseq"
    r"|inDrop|CEL[\-\s]?seq|MARS[\-\s]?seq"
    r"|sci[\-\s]RNA[\-\s]?seq"
    r"|Seq[\-\s]?Well|SPLiT[\-\s]?seq"
    r"|CITE[\-\s]?seq|CITEseq",
    re.IGNORECASE,
)

MODALITY_RULES: list[tuple[str, re.Pattern]] = [
    (
        "Spatial Transcriptomics",
        re.compile(
            r"spatial\s+transcriptom"
            r"|Visium|MERFISH|seqFISH|STARmap|Slide[\-\s]?seq"
            r"|Xenium|CosMx|10[xX]\s+Visium",
            re.IGNORECASE,
        ),
    ),
    (
        "Multiome",
        re.compile(
            r"multiom|multi[\-\s]?om"
            r"|ATAC\s*\+\s*RNA|RNA\s*\+\s*ATAC"
            r"|10[xX]\s+Multiome|paired\s+ATAC",
            re.IGNORECASE,
        ),
    ),
    (
        "scATAC-seq",
        re.compile(
            r"scATAC[\-\s]?seq|sc[\-\s]ATAC[\-\s]?seq|single[\-\s]cell[\-\s]+ATAC",
            re.IGNORECASE,
        ),
    ),
    (
        "snATAC-seq",
        re.compile(
            r"snATAC[\-\s]?seq|sn[\-\s]ATAC[\-\s]?seq"
            r"|single[\-\s]nucle\w*[\-\s]+ATAC",
            re.IGNORECASE,
        ),
    ),
    (
        "snRNA-seq",
        re.compile(
            r"snRNA[\-\s]?seq|sn[\-\s]RNA[\-\s]?seq"
            r"|single[\-\s]nucle\w*[\-\s]+RNA"
            r"|single[\-\s]nucle\w*[\-\s]+transcriptom",
            re.IGNORECASE,
        ),
    ),
    (
        "scRNA-seq",
        re.compile(
            r"scRNA[\-\s]?seq|sc[\-\s]RNA[\-\s]?seq"
            r"|single[\-\s]cell[\-\s]+RNA"
            r"|single[\-\s]cell[\-\s]+transcriptom"
            r"|10x\s+Genomics|10X\s+Genomics"
            r"|Drop[\-\s]?seq|dropseq"
            r"|Smart[\-\s]?seq\d?|smartseq"
            r"|inDrop|CEL[\-\s]?seq|MARS[\-\s]?seq"
            r"|sci[\-\s]RNA[\-\s]?seq"
            r"|Seq[\-\s]?Well|SPLiT[\-\s]?seq"
            r"|CITE[\-\s]?seq|CITEseq",
            re.IGNORECASE,
        ),
    ),
]

SC_FTS_QUERY = (
    'scrnaseq OR "scrna seq"'
    ' OR "sc rna seq" OR "sc rnaseq"'
    ' OR snrnaseq OR "snrna seq" OR "sn rna seq" OR "sn rnaseq"'
    ' OR "single cell rna"'
    ' OR ("single cell" AND transcriptom*)'
    " OR (single AND nucle* AND rna)"
    " OR (single AND nucle* AND transcriptom*)"
    ' OR "10x genomics"'
    " OR dropseq OR (drop AND seq*)"
    " OR smartseq OR (smart AND seq*)"
    " OR indrop"
    " OR celseq OR (cel AND seq*)"
    " OR marsseq OR (mars AND seq*)"
    ' OR "sci rna seq" OR "sci rnaseq"'
    ' OR seqwell OR "seq well"'
    ' OR splitseq OR "split seq"'
    ' OR citeseq OR "cite seq"'
)


def sc_classify(
    unified_metadata_db_file: Path,
    batch_size: int,
    is_dry_run: bool,
) -> None:
    conn = sqlite_connect(str(unified_metadata_db_file))
    logger.info("re-building fts index, might take a while...")
    _build_fts_index(conn)

    candidates: set[str] = set()
    for col in ("title", "description"):
        cursor = conn.execute(
            "SELECT DISTINCT m.canonical_accession "
            "FROM unified_metadata_fts fts "
            "JOIN unified_metadata m ON m.rowid = fts.rowid "
            "WHERE unified_metadata_fts MATCH ?",
            (f"{col}:({SC_FTS_QUERY})",),
        )
        candidates.update(row[0] for row in cursor.fetchall())

    cursor = conn.execute(
        "SELECT canonical_accession FROM unified_metadata "
        "WHERE dominant_assay_l2 = 'scRNA-seq'",
    )
    candidates.update(row[0] for row in cursor.fetchall())

    conn.executescript(
        "CREATE TEMP TABLE IF NOT EXISTS _sc_candidates ("
        "  canonical_accession TEXT PRIMARY KEY"
        "); DELETE FROM _sc_candidates;",
    )
    candidate_list = list(candidates)
    for i in range(0, len(candidate_list), batch_size):
        batch = candidate_list[i : i + batch_size]
        conn.executemany(
            "INSERT OR IGNORE INTO _sc_candidates VALUES (?)",
            [(a,) for a in batch],
        )

    rows = conn.execute(
        "SELECT m.canonical_accession, m.title, m.description, m.dominant_assay_l2 "
        "FROM unified_metadata m "
        "JOIN _sc_candidates c ON m.canonical_accession = c.canonical_accession",
    ).fetchall()
    conn.execute("DROP TABLE IF EXISTS _sc_candidates")

    sc_count = 0
    modality_counter: Counter[str] = Counter()
    updates: list[tuple[bool, str | None, str]] = []

    for accession, title, description, dominant_assay_l2 in tqdm(
        rows,
        unit="acc",
        desc="classifying studies",
    ):
        is_sc = bool(
            dominant_assay_l2 == "scRNA-seq"
            or SC_PATTERNS.search(title)
            or SC_PATTERNS.search(description),
        )
        modality = _classify_modality(title, description) if is_sc else None

        if is_sc:
            updates.append((is_sc, modality, accession))
            sc_count += 1

        if modality:
            modality_counter[modality] += 1

    logger.info("single-cell studies: %d", sc_count)
    for mod, cnt in sorted(modality_counter.items(), key=lambda x: -x[1]):
        logger.info("  %s: %d", mod, cnt)

    if is_dry_run:
        conn.execute("DROP TABLE IF EXISTS unified_metadata_fts")
        conn.close()
        return

    for i in tqdm(
        range(0, len(updates), batch_size),
        unit="batch",
        desc="writing updates",
    ):
        updates_batch = updates[i : i + batch_size]
        conn.executemany(
            "UPDATE unified_metadata SET is_single_cell = ?, "
            "single_cell_modality = ? WHERE canonical_accession = ?",
            updates_batch,
        )
        conn.commit()

    exec_query_from_file("indexes/unified_metadata.sql", unified_metadata_db_file)
    conn.close()


def _build_fts_index(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS unified_metadata_fts")
    conn.execute(
        "CREATE VIRTUAL TABLE unified_metadata_fts USING fts5("
        "canonical_accession UNINDEXED, title, description, dominant_assay_l2, "
        "content=unified_metadata, content_rowid=rowid"
        ")"
    )
    conn.execute(
        "INSERT INTO unified_metadata_fts(unified_metadata_fts) VALUES('rebuild')",
    )
    conn.commit()


def _classify_modality(title: str | None, description: str | None) -> str:
    for modality, pattern in MODALITY_RULES:
        if (title and pattern.search(title)) or (
            description and pattern.search(description)
        ):
            return modality
    return "Other single-cell"
