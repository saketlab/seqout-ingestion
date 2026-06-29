import re
import sqlite3
from collections import Counter
from pathlib import Path

from tqdm import tqdm

from helpers import exec_query_from_file

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


def sc_regex_enricher(
    unified_metadata_db_file: Path,
    batch_size: int,
    is_dry_run: bool,
) -> None:
    conn = sqlite3.connect(unified_metadata_db_file)
    rows = conn.execute(
        "SELECT canonical_accession, title, description, "
        "dominant_assay_l2 FROM unified_metadata",
    ).fetchall()

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
        modality = _classify_modality(title) if is_sc else None

        if is_sc:
            updates.append((is_sc, modality, accession))
            sc_count += 1

        if modality:
            modality_counter[modality] += 1

    for _mod, _cnt in sorted(modality_counter.items(), key=lambda x: -x[1]):
        pass

    if is_dry_run:
        conn.close()
        return

    for i in tqdm(
        range(0, len(updates), batch_size),
        unit="batch",
        desc="writing updates",
    ):
        batch = updates[i : i + batch_size]
        conn.executemany(
            "UPDATE unified_metadata SET is_single_cell = ?, "
            "single_cell_modality = ? WHERE canonical_accession = ?",
            batch,
        )
        conn.commit()

    exec_query_from_file("indexes/unified_metadata.sql", unified_metadata_db_file)
    conn.close()


def _classify_modality(title: str | None) -> str:
    if title:
        for modality, pattern in MODALITY_RULES:
            if pattern.search(title):
                return modality

    return "Other single-cell"
