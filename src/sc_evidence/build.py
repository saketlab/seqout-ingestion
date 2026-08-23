"""
Derive single_cell_evidence from per-archive SQLite databases.

Rows are deterministic rule outputs. sc_rule is rebuilt from RULES every run;
--only-rules replaces only named rule/archive evidence rows.
"""

import logging
import sqlite3
from collections import Counter
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path

from helpers import exec_query_from_file, sqlite_connect
from sc_evidence.rules import (
    MAX_VALUE_LEN,
    RULE_SLUGS,
    RULE_VERSION,
    RULES,
    Rule,
    analyte_of,
    modality_of,
)

logger = logging.getLogger(__name__)

TABLES_SQL = "tables/single_cell_evidence.sql"
INDEXES_SQL = "indexes/single_cell_evidence.sql"

_INSERT = (
    "INSERT OR IGNORE INTO single_cell_evidence "
    "(accession, grain, study_accession, rule_id, value, library_source, "
    "library_strategy, analyte, assay_modality) VALUES (?,?,?,?,?,?,?,?,?)"
)

_EXPECTED_COLUMNS = 6


def open_archive_readonly(
    db_file: Path,
    attach: dict[str, Path | None] | None = None,
) -> sqlite3.Connection:
    """
    Open an archive database read-only, with optional ATTACHed archives.
    """
    conn = sqlite_connect(f"file:{db_file}?immutable=1", uri=True)
    for name, path in (attach or {}).items():
        if path is not None and Path(path).exists():
            conn.execute(f"ATTACH ? AS {name}", (f"file:{path}?immutable=1",))
        else:
            logger.warning(
                "attaching an EMPTY database as %s: no file given or it does "
                "not exist; rules reading it contribute nothing",
                name,
            )
            conn.execute(f"ATTACH ':memory:' AS {name}")
    return conn


def _selected_rules(only_rules: Sequence[str] | None) -> list[Rule]:
    if not only_rules:
        return list(RULES)
    wanted = {slug for slug in only_rules if slug}
    unknown = wanted - RULE_SLUGS
    if unknown:
        msg = f"unknown rule slug(s): {sorted(unknown)}; known: {sorted(RULE_SLUGS)}"
        raise ValueError(msg)
    return [r for r in RULES if r.rule in wanted]


def _rebuild_sc_rule(
    conn: sqlite3.Connection,
    built_at: str,
    rebuilt_ids: set[int],
) -> None:
    """
    Rebuild sc_rule from RULES and drop orphaned evidence.

    Deleted rules lose rows. Under --only-rules, untouched rules keep their
    existing built_at so timestamps match the evidence rows they govern.
    """
    previous = {
        row[0]: row[1] for row in conn.execute("SELECT id, built_at FROM sc_rule")
    }
    known_ids = {r.id for r in RULES}
    stale = [
        row[0]
        for row in conn.execute("SELECT DISTINCT rule_id FROM single_cell_evidence")
        if row[0] not in known_ids
    ]
    for rule_id in stale:
        n = conn.execute(
            "DELETE FROM single_cell_evidence WHERE rule_id = ?",
            (rule_id,),
        ).rowcount
        logger.warning("dropped %d row(s) for retired rule_id %d", n, rule_id)

    conn.execute("DELETE FROM sc_rule")
    conn.executemany(
        "INSERT INTO sc_rule (id, rule, archive, grain, field, tier, gate, "
        "rule_version, built_at) VALUES (?,?,?,?,?,?,?,?,?)",
        [
            (
                r.id,
                r.rule,
                r.archive,
                r.grain,
                r.field,
                r.tier,
                r.gate,
                RULE_VERSION,
                built_at if r.id in rebuilt_ids else previous.get(r.id, built_at),
            )
            for r in RULES
        ],
    )
    conn.commit()


def _iter_rule_rows(
    archive_conn: sqlite3.Connection,
    rule: Rule,
    batch_size: int,
) -> Iterator[tuple]:
    """
    Run one rule and yield evidence tuples.

    Stored value is raw matched text; extractors preserve match/context before
    truncation.
    """
    for statement in rule.prepare:
        archive_conn.execute(statement)

    cursor = archive_conn.execute(rule.sql)
    n_unextractable = 0
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        for row in batch:
            if len(row) < _EXPECTED_COLUMNS:
                msg = (
                    f"rule {rule.rule}/{rule.archive} returned {len(row)} columns, "
                    f"expected {_EXPECTED_COLUMNS}"
                )
                raise ValueError(msg)
            accession, grain, study, value, source, strategy = row[:_EXPECTED_COLUMNS]
            if not accession:
                continue
            if rule.extract is not None:
                match = rule.extract.search(value or "")
                if match is None:
                    # SQL gate and extractor can disagree. Store nothing when the
                    # extractor fails to narrow the gated value.
                    #
                    # SQLite LIKE has no lookaround, so expected disagreements include
                    # "single nucleotide polymorphism"; Python rejects them and counts
                    # the drop. See the trap table in sc_evidence.rules.
                    n_unextractable += 1
                    continue
                value = match.group(0)
            value = value[:MAX_VALUE_LEN] if value else None
            yield (
                accession,
                grain or rule.grain,
                study,
                rule.id,
                value,
                source,
                strategy,
                analyte_of(source),
                modality_of(value, strategy, source),
            )

    if n_unextractable:
        logger.warning(
            "%s/%s: %d row(s) matched the SQL gate but not the extractor and "
            "were dropped. Expected for the lookaround guards (see the trap "
            "table in sc_evidence.rules); anything else is gate/extractor drift",
            rule.rule,
            rule.archive,
            n_unextractable,
        )


def sc_evidence_build(
    sc_evidence_db_file: Path,
    archive_db_files: dict[str, Path | None],
    only_rules: Sequence[str] | None = None,
    batch_size: int = 20000,
    is_dry_run: bool = False,  # noqa: FBT002
) -> None:
    """
    Populate single_cell_evidence from archive SQLite databases.

    Missing optional archive files are logged as skipped. only_rules replaces
    named rule/archive rows; other rules keep their evidence.
    """
    rules = _selected_rules(only_rules)
    built_at = datetime.now(UTC).isoformat(timespec="seconds")

    runnable: list[Rule] = []
    for rule in rules:
        db_file = archive_db_files.get(rule.archive)
        if db_file is None:
            logger.warning(
                "%s/%s: SKIPPED, no %s database given",
                rule.rule,
                rule.archive,
                rule.archive,
            )
            continue
        if not Path(db_file).exists():
            logger.warning(
                "%s/%s: SKIPPED, %s does not exist",
                rule.rule,
                rule.archive,
                db_file,
            )
            continue
        runnable.append(rule)

    if is_dry_run:
        _dry_run(runnable, archive_db_files, batch_size)
        return

    exec_query_from_file(TABLES_SQL, sc_evidence_db_file)
    conn = sqlite_connect(sc_evidence_db_file)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _rebuild_sc_rule(conn, built_at, {r.id for r in runnable})

        for rule in runnable:
            archive_conn = open_archive_readonly(
                Path(archive_db_files[rule.archive]),
                {name: archive_db_files.get(name) for name in rule.attach},
            )
            try:
                deleted = conn.execute(
                    "DELETE FROM single_cell_evidence WHERE rule_id = ?",
                    (rule.id,),
                ).rowcount
                conn.commit()
                logger.info(
                    "%s/%s: rebuilding (%d existing row(s) cleared)",
                    rule.rule,
                    rule.archive,
                    max(deleted, 0),
                )

                written = 0
                batch: list[tuple] = []
                for evidence in _iter_rule_rows(archive_conn, rule, batch_size):
                    batch.append(evidence)
                    if len(batch) >= batch_size:
                        conn.executemany(_INSERT, batch)
                        conn.commit()
                        written += len(batch)
                        batch.clear()
                if batch:
                    conn.executemany(_INSERT, batch)
                    conn.commit()
                    written += len(batch)
                logger.info("%s/%s: %d row(s)", rule.rule, rule.archive, written)
            finally:
                archive_conn.close()

        exec_query_from_file(INDEXES_SQL, conn=conn)
        _log_summary(conn)
    finally:
        conn.close()


def _dry_run(
    rules: Sequence[Rule],
    archive_db_files: dict[str, Path | None],
    batch_size: int,
) -> None:
    """Run every rule and report what it would write, touching no output file."""
    logger.info("DRY RUN: nothing is written")
    total = 0
    for rule in rules:
        archive_conn = open_archive_readonly(
            Path(archive_db_files[rule.archive]),
            {name: archive_db_files.get(name) for name in rule.attach},
        )
        try:
            n = 0
            studies: set[str] = set()
            for evidence in _iter_rule_rows(archive_conn, rule, batch_size):
                n += 1
                if evidence[2]:
                    studies.add(evidence[2])
            logger.info(
                "%s/%s: would write %d row(s) across %d study/studies",
                rule.rule,
                rule.archive,
                n,
                len(studies),
            )
            total += n
        finally:
            archive_conn.close()
    logger.info("DRY RUN total: %d row(s)", total)


def _log_summary(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT count(*) FROM single_cell_evidence").fetchone()[0]
    studies = conn.execute(
        "SELECT count(DISTINCT study_accession) FROM single_cell_evidence "
        "WHERE study_accession IS NOT NULL",
    ).fetchone()[0]
    logger.info("evidence rows %d across %d studies", rows, studies)

    for label, column in (("assay modality", "assay_modality"), ("analyte", "analyte")):
        counts: Counter[str] = Counter()
        for value, n in conn.execute(
            f"SELECT coalesce({column}, '<null>'), count(DISTINCT study_accession) "  # noqa: S608
            "FROM single_cell_evidence GROUP BY 1",
        ):
            counts[value] = n
        for value, n in counts.most_common():
            logger.info("  %s %s: %d studies", label, value, n)
