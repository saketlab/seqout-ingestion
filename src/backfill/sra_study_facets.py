#!/usr/bin/env python
"""Recompute sra_studies facet columns at the study level.

They were derived per *submission* (see the fix in
src/transfer/postgres.py::_sra_studies_batch_modifier), which is wrong in both
directions: one submission can cover many studies, so each inherited its
siblings' values; and a study's samples can span many submissions, so its own
submission missed most of them. Measured against prod, ~12.6% of SRA studies
carried a wrong organism list — and search filters on that column directly
(`s.organisms @> ARRAY[...]`).

This rewrites organisms / library_strategies / instrument_models / platforms
from the study's own experiments and samples, then refreshes the
organism_growth_monthly matview that reads them (skip with --skip-refresh).

An empty recompute never overwrites a stored value: ~4% of studies have samples
but no experiments linking them, and blanking those would drop them out of
organism search entirely.

DRY RUN BY DEFAULT — it only reports what would change. Pass --apply to write.

    python -m backfill.sra_study_facets                  # report only
    python -m backfill.sra_study_facets --limit 5000     # sample it
    python -m backfill.sra_study_facets --apply          # write

Connection: --pg-url, or the PG_URL / DATABASE_URL env var.
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg
from psycopg import sql

# One statement, run server-side: the join stays in Postgres rather than
# shipping millions of sample rows to this process.
_RECOMPUTE_SQL = """
WITH target AS (
    SELECT accession FROM sra_studies
    {where}
    ORDER BY accession
    {limit}
),
computed AS (
    SELECT
        t.accession,
        ARRAY(
            SELECT DISTINCT sm.scientific_name
            FROM sra_experiments e
            JOIN sra_samples sm ON sm.accession = ANY (e.samples)
            WHERE e.study = t.accession
              AND sm.scientific_name IS NOT NULL
              AND sm.scientific_name <> ''
            ORDER BY sm.scientific_name
        ) AS organisms,
        ARRAY(
            SELECT DISTINCT e.library_strategy FROM sra_experiments e
            WHERE e.study = t.accession AND e.library_strategy IS NOT NULL
            ORDER BY e.library_strategy
        ) AS library_strategies,
        ARRAY(
            SELECT DISTINCT e.instrument_model FROM sra_experiments e
            WHERE e.study = t.accession AND e.instrument_model IS NOT NULL
            ORDER BY e.instrument_model
        ) AS instrument_models,
        ARRAY(
            SELECT DISTINCT e.platform FROM sra_experiments e
            WHERE e.study = t.accession AND e.platform IS NOT NULL
            ORDER BY e.platform
        ) AS platforms
    FROM target t
),
facts AS (
    -- An empty recompute means "no evidence", not "no organisms": roughly 4 in
    -- 100 studies have samples but no experiments linking them, and psycopg
    -- parses a percent sign even inside a comment, so do not write one here.
    -- (DRP001120 is the worked example: it has
    -- organisms and zero rows in every experiments table.) Overwriting those
    -- with {{}} would blank some 29k studies, so the stored value is kept
    -- whenever the study-level derivation comes back empty.
    SELECT
        c.accession,
        CASE WHEN cardinality(c.organisms) > 0
             THEN c.organisms ELSE COALESCE(s.organisms, '{{}}') END AS organisms,
        CASE WHEN cardinality(c.library_strategies) > 0
             THEN c.library_strategies
             ELSE COALESCE(s.library_strategies, '{{}}') END AS library_strategies,
        CASE WHEN cardinality(c.instrument_models) > 0
             THEN c.instrument_models
             ELSE COALESCE(s.instrument_models, '{{}}') END AS instrument_models,
        CASE WHEN cardinality(c.platforms) > 0
             THEN c.platforms ELSE COALESCE(s.platforms, '{{}}') END AS platforms
    FROM computed c
    JOIN sra_studies s ON s.accession = c.accession
)
SELECT f.*
FROM facts f
JOIN sra_studies s ON s.accession = f.accession
WHERE f.organisms          IS DISTINCT FROM COALESCE(s.organisms, '{{}}')
   OR f.library_strategies IS DISTINCT FROM COALESCE(s.library_strategies, '{{}}')
   OR f.instrument_models  IS DISTINCT FROM COALESCE(s.instrument_models, '{{}}')
   OR f.platforms          IS DISTINCT FROM COALESCE(s.platforms, '{{}}')
"""

# One statement per batch, not per row. The DB does this work in microseconds;
# the cost is the round-trip, and over an SSH tunnel that was measured at 622ms
# — 125,888 single-row UPDATEs is over a day of pure waiting, and a window that
# long is very likely to lose the tunnel mid-run (it did). Folding a batch into
# one VALUES list makes it ~25 round-trips instead.
_UPDATE_SQL = """
UPDATE sra_studies AS s
SET organisms = v.organisms,
    library_strategies = v.library_strategies,
    instrument_models = v.instrument_models,
    platforms = v.platforms
FROM (VALUES %s) AS v(accession, organisms, library_strategies,
                      instrument_models, platforms)
WHERE s.accession = v.accession
"""

_WRITE_BATCH = 5000


def connect(pg_url: str | None) -> psycopg.Connection:
    # --pg-url matches the rest of the pipeline (see src/cli/transfer.py);
    # PG_URL / DATABASE_URL are accepted so it can run unattended.
    url = pg_url or os.getenv("PG_URL") or os.getenv("DATABASE_URL")
    if not url:
        sys.exit("pass --pg-url, or set PG_URL / DATABASE_URL")
    return psycopg.connect(url, connect_timeout=15)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-url", default=None, help="postgres URL (or PG_URL env)")
    ap.add_argument("--apply", action="store_true", help="write (default: report only)")
    ap.add_argument("--limit", type=int, default=0, help="only consider N studies")
    ap.add_argument("--prefix", default="", help="restrict to studies LIKE 'X%%'")
    ap.add_argument("--show", type=int, default=10, help="example rows to print")
    ap.add_argument(
        "--skip-refresh",
        action="store_true",
        help="don't refresh organism_growth_monthly afterwards",
    )
    args = ap.parse_args()

    # not named `sql`: that shadows the psycopg sql module used to build the
    # batched UPDATE below
    select_sql = _RECOMPUTE_SQL.format(
        where="WHERE accession LIKE %(prefix)s" if args.prefix else "",
        limit=f"LIMIT {args.limit}" if args.limit else "",
    )
    params = {"prefix": f"{args.prefix}%"} if args.prefix else {}

    with connect(args.pg_url) as conn:
        conn.execute("SET statement_timeout = '0'")
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            print("computing study-level facets (this scans sra_experiments)…")
            cur.execute(select_sql, params or None)
            changed = cur.fetchall()

    print(f"\nstudies whose facets would change: {len(changed):,}")
    for row in changed[: args.show]:
        print(f"  {row['accession']}: organisms -> {row['organisms']}")

    if not changed:
        return
    if not args.apply:
        print("\ndry run — nothing written. Re-run with --apply to write.")
        return

    with connect(args.pg_url) as conn:
        conn.execute("SET statement_timeout = '0'")
        with conn.cursor() as cur:
            for start in range(0, len(changed), _WRITE_BATCH):
                batch = changed[start : start + _WRITE_BATCH]
                # The arrays need an explicit cast: a VALUES list gives Postgres
                # nothing to infer element types from.
                values = sql.SQL(", ").join(
                    sql.SQL("({}, {}::text[], {}::text[], {}::text[], {}::text[])").format(
                        sql.Literal(r["accession"]),
                        sql.Literal(r["organisms"]),
                        sql.Literal(r["library_strategies"]),
                        sql.Literal(r["instrument_models"]),
                        sql.Literal(r["platforms"]),
                    )
                    for r in batch
                )
                cur.execute(
                    sql.SQL(_UPDATE_SQL.replace("%s", "{}")).format(values)
                )
                conn.commit()
                print(f"  committed {min(start + _WRITE_BATCH, len(changed)):,}/{len(changed):,}")
    print(f"updated {len(changed):,} studies")

    if args.skip_refresh:
        print("skipping matview refresh — organism charts will show stale counts")
        return

    # organism_growth_monthly is a matview over sra_studies.organisms (and the
    # other archives'), and backs the organism charts. Nothing invalidates it
    # automatically, so it would keep serving pre-backfill numbers. CONCURRENTLY
    # keeps the stats page readable while it rebuilds (idx_ogm_organism_db_month
    # is the unique index that allows it).
    print("refreshing organism_growth_monthly (concurrently)…")
    with connect(args.pg_url) as conn:
        conn.execute("SET statement_timeout = '0'")
        with conn.cursor() as cur:
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY organism_growth_monthly",
            )
        conn.commit()
    print("matview refreshed")


if __name__ == "__main__":
    main()
