#!/usr/bin/env python3
"""
Adversarial checks on the staged organism fill. Read-only, no writes anywhere.

The routes are individually guarded, but a guard only proves a route is
self-consistent. These checks ask the harder question: where two independent
routes can both answer, do they agree, and does the result still look right for
studies that genuinely contain more than one species?

Run after build.py and before transfer.py --apply.
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sqlite3
import sys
from typing import TYPE_CHECKING

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pg_connect import connect

if TYPE_CHECKING:
    from psycopg import Connection

logger = logging.getLogger("recall_fix.verify")

# Above this share of multi-organism studies coming out uniform, the fill is not
# behaving like a per-sample lookup any more and must not be applied.
UNIFORM_ALARM = 0.9

# Disagreements smaller than this are noise in a cross-route comparison of
# millions of rows; larger ones get named individually.
NOTABLE_DISAGREEMENT = 50

COPY_BATCH = 100_000

# Routes that can both answer for the same sample. Everything else targets a
# disjoint id space (SRS vs GSM vs SAMC vs a MAGE-TAB source name), so there is
# nothing to cross-check; only these pairs overlap.
#
# `_build_organism` applies routes in order with INSERT OR IGNORE, so the higher
# route silently wins. That is the right precedence, but it also means a
# disagreement leaves no trace. These queries put it back.
OVERLAPPING = (
    (
        "gsm_alias",
        "geo_platform",
        """
        SELECT a.scientific_name, p.organism::jsonb->0->>'#text', COUNT(*)
        FROM ontology_samples_v4 s
        JOIN sra_samples a ON a.alias = s.sample
        JOIN geo_samples g ON g.accession = s.sample
        JOIN geo_platforms p ON p.accession = g.platform_ref
        WHERE s.organism IS NULL AND s.sample ~ '^GSM[0-9]+$'
          AND a.scientific_name IS NOT NULL AND p.organism IS NOT NULL
          AND jsonb_array_length(p.organism::jsonb) = 1
        GROUP BY 1, 2
    """,
    ),
    (
        "ae_ena_run",
        "ae_source_name",
        """
        SELECT e.scientific_name, a.organism, COUNT(*)
        FROM ontology_samples_v4 s
        JOIN arrayexpress_samples a
          ON a.experiment_accession = s.study_accession AND a.source_name = s.sample
        JOIN ena_experiments e ON e.run_accession = a.ena_run
        WHERE s.organism IS NULL
          AND e.scientific_name IS NOT NULL AND a.organism IS NOT NULL
        GROUP BY 1, 2
    """,
    ),
)


def cross_route(pg: Connection) -> None:
    """Compare routes that can both answer for the same sample."""
    cur = pg.cursor()
    for left, right, sql in OVERLAPPING:
        cur.execute(sql)
        rows = cur.fetchall()
        agree = sum(n for a, b, n in rows if a == b)
        total = sum(n for _, _, n in rows)
        if not total:
            logger.info("%s vs %s: no overlap", left, right)
            continue
        logger.info(
            "%s vs %s: agree on %s of %s (%.1f%%)",
            left,
            right,
            f"{agree:,}",
            f"{total:,}",
            agree / total * 100,
        )
        for a, b, n in sorted(rows, key=lambda r: -r[2]):
            if a != b and n > NOTABLE_DISAGREEMENT:
                logger.warning(
                    "    %-30s != %-30s %s", str(a)[:29], str(b)[:29], f"{n:,}"
                )


def multi_organism(sq: sqlite3.Connection, pg: Connection) -> None:
    """
    Check that multi-species studies stay multi-species.

    Guards against study-level fills that assign the dominant organism to every
    sample. For studies unified_metadata already knows are multi-organism, count
    how many end up with one organism across all filled samples.

    A uniform result is not automatically wrong: a few control samples outside
    the fill set can make a study multi-organism. Report rather than fail; the
    dangerous pattern is widespread uniformity.
    """
    cur = pg.cursor()
    cur.execute(
        "CREATE TEMP TABLE _multi AS"
        " SELECT canonical_accession FROM unified_metadata"
        " WHERE jsonb_typeof(organism_counts) = 'object'"
        "   AND (SELECT COUNT(*) FROM jsonb_object_keys(organism_counts)) > 1"
    )
    cur.execute("CREATE INDEX ON _multi (canonical_accession)")
    cur.execute("SELECT COUNT(*) FROM _multi")
    logger.info(
        "multi-organism studies in unified_metadata: %s", f"{cur.fetchone()[0]:,}"
    )

    cur.execute("CREATE TEMP TABLE _fill (sample TEXT PRIMARY KEY, organism TEXT)")
    rows = sq.execute("SELECT sample, organism FROM sample_organism")
    buf = []
    for r in rows:
        buf.append(r)
        if len(buf) >= COPY_BATCH:
            with cur.copy("COPY _fill (sample, organism) FROM STDIN") as cp:
                for x in buf:
                    cp.write_row(x)
            buf = []
    if buf:
        with cur.copy("COPY _fill (sample, organism) FROM STDIN") as cp:
            for x in buf:
                cp.write_row(x)
    cur.execute("ANALYZE _fill")

    cur.execute(
        "SELECT COUNT(*) FILTER (WHERE n_org = 1), COUNT(*)"
        "  FROM (SELECT s.study_accession, COUNT(DISTINCT f.organism) AS n_org"
        "          FROM ontology_samples_v4 s"
        "          JOIN _fill f ON f.sample = s.sample"
        "          JOIN _multi m ON m.canonical_accession = s.study_accession"
        "         GROUP BY 1 HAVING COUNT(*) > 1) t"
    )
    uniform, total = cur.fetchone()
    if total:
        logger.info(
            "multi-organism studies with >1 filled sample: %s;"
            " %s came out UNIFORM (%.1f%%)",
            f"{total:,}",
            f"{uniform:,}",
            uniform / total * 100,
        )
        if uniform / total > UNIFORM_ALARM:
            logger.error(
                "OVER 90%% UNIFORM -- the fill is behaving like a study-level"
                " assignment. Do not apply; investigate first."
            )
    else:
        logger.info("no multi-organism study has >1 filled sample")

    cur.execute(
        "SELECT s.study_accession, COUNT(DISTINCT f.organism) AS n_org, COUNT(*) AS n"
        "  FROM ontology_samples_v4 s"
        "  JOIN _fill f ON f.sample = s.sample"
        "  JOIN _multi m ON m.canonical_accession = s.study_accession"
        " GROUP BY 1 ORDER BY 2 DESC, 3 DESC LIMIT 8"
    )
    logger.info("most species-diverse filled studies (sanity, should NOT be all 1):")
    for acc, n_org, n in cur.fetchall():
        logger.info("    %-18s %d distinct organisms over %d samples", acc, n_org, n)
    pg.rollback()


def route_summary(sq: sqlite3.Connection) -> None:
    logger.info("staged fills by route:")
    for route, n in sq.execute(
        "SELECT route, COUNT(*) FROM sample_organism GROUP BY 1 ORDER BY 2 DESC"
    ):
        logger.info("    %-32s %s", route, f"{n:,}")
    logger.info("staged corrections by route:")
    for route, n in sq.execute(
        "SELECT route, COUNT(*) FROM sample_organism_overwrite"
        " GROUP BY 1 ORDER BY 2 DESC"
    ):
        logger.info("    %-32s %s", route, f"{n:,}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=pathlib.Path, required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    sq = sqlite3.connect(args.db)
    pg = connect()
    route_summary(sq)
    cross_route(pg)
    multi_organism(sq, pg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
