import re
import sqlite3
from collections import Counter
from pathlib import Path

from tqdm import tqdm

_SC_PATTERNS = re.compile(
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

_MODALITY_RULES: list[tuple[str, re.Pattern]] = [
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
            r"scATAC[\-\s]?seq|sc[\-\s]ATAC[\-\s]?seq" r"|single[\-\s]cell[\-\s]+ATAC",
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
    # snRNA before scRNA so single-nuclei titles don't match scRNA first
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


def sc_regex_enricher(
    unified_metadata_db_file: Path, batch_size: int, is_dry_run: bool
):
    conn = sqlite3.connect(unified_metadata_db_file)
    existing_tables = [
        r[1] for r in conn.execute("PRAGMA table_info(unified_metadata)").fetchall()
    ]

    if "is_single_cell" not in existing_tables:
        conn.execute(
            "ALTER TABLE unified_metadata ADD COLUMN is_single_cell INTEGER DEFAULT 0"
        )
    if "single_cell_modality" not in existing_tables:
        conn.execute(
            "ALTER TABLE unified_metadata ADD COLUMN single_cell_modality TEXT"
        )

    rows = conn.execute(
        "SELECT canonical_accession, title, dominant_assay_l2 FROM unified_metadata"
    ).fetchall()

    sc_count = 0
    modality_counter: Counter[str] = Counter()
    updates: list[tuple[bool, str | None, str]] = []

    for accession, title, dominant_assay_l2 in tqdm(
        rows, unit="acc", desc="classifying studies"
    ):
        is_sc = bool(dominant_assay_l2 == "scRNA-seq" or _SC_PATTERNS.search(title))
        modality = _classify_modality(title) if is_sc else None

        if is_sc:
            updates.append((is_sc, modality, accession))
            sc_count += 1

        if modality:
            modality_counter[modality] += 1

    print(
        f"classified {sc_count:,} single-cell studies ({sc_count / len(rows) * 100:.1f}% of total):"
    )
    for mod, cnt in sorted(modality_counter.items(), key=lambda x: -x[1]):
        print(f"\t{mod:30s} {cnt:>6,}")
    print(f"\t{'(non-single-cell)':30s} {len(rows) - sc_count:>6,}")

    if is_dry_run:
        conn.close()
        return

    for i in tqdm(
        range(0, len(updates), batch_size), unit="batch", desc="writing updates"
    ):
        batch = updates[i : i + batch_size]
        conn.executemany(
            "UPDATE unified_metadata SET is_single_cell = ?, single_cell_modality = ? WHERE canonical_accession = ?",
            batch,
        )
        conn.commit()

    _create_indexes(conn)
    conn.close()


def _classify_modality(title: str | None) -> str:
    if title:
        for modality, pattern in _MODALITY_RULES:
            if pattern.search(title):
                return modality

    return "Other single-cell"


def _create_indexes(conn: sqlite3.Connection):
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_um_is_single_cell ON unified_metadata(is_single_cell)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_um_modality ON unified_metadata(single_cell_modality)"
    )
    conn.commit()
