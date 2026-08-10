#!/usr/bin/env python
"""
Recompute sra_studies facet columns at the study level.

They were derived per *submission* (see the fix in
src/transfer/postgres.py::_sra_studies_batch_modifier), which is wrong in both
directions: one submission can cover many studies, so each inherited its
siblings' values; and a study's samples can span many submissions, so its own
submission missed most of them. Measured against prod, ~12.6% of SRA studies
carried a wrong organism list, and search filters on that column directly
(`s.organisms @> ARRAY[...]`).

This rewrites organisms / library_strategies / instrument_models / platforms /
library_sources from the study's own experiments and samples, then refreshes the
organism_growth_monthly matview that reads them (skip with --skip-refresh).

An empty recompute never overwrites a stored value: ~4% of studies have samples
but no experiments linking them, and blanking those would drop them out of
organism search entirely.

Dry run by default, reporting only what would change; pass --apply to write.

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
        ) AS platforms,
        ARRAY(
            -- the explicit charset is required: single-arg btrim strips spaces
            -- only, so tab/newline-padded values become distinct facet values
            SELECT DISTINCT btrim(e.library_source, E' \t\n\r') FROM sra_experiments e
            WHERE e.study = t.accession AND btrim(e.library_source, E' \t\n\r') <> ''
            ORDER BY btrim(e.library_source, E' \t\n\r')
        ) AS library_sources
    FROM target t
),
facts AS (
    -- An empty recompute means "no evidence": roughly 4 in 100 studies have
    -- samples but no experiments linking them, and blanking those would drop
    -- some 29k studies. psycopg parses a percent sign even inside a comment,
    -- so never write one here.
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
             THEN c.platforms ELSE COALESCE(s.platforms, '{{}}') END AS platforms,
        CASE WHEN cardinality(c.library_sources) > 0
             THEN c.library_sources
             ELSE COALESCE(s.library_sources, '{{}}') END AS library_sources
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
   OR f.library_sources    IS DISTINCT FROM COALESCE(s.library_sources, '{{}}')
"""

# batched into one VALUES list: the cost is the round-trip, and a row-by-row
# run over an SSH tunnel takes long enough to lose the tunnel mid-run
_UPDATE_SQL = """
UPDATE sra_studies AS s
SET organisms = v.organisms,
    library_strategies = v.library_strategies,
    instrument_models = v.instrument_models,
    platforms = v.platforms,
    library_sources = v.library_sources
FROM (VALUES %s) AS v(accession, organisms, library_strategies,
                      instrument_models, platforms, library_sources)
WHERE s.accession = v.accession
"""

_WRITE_BATCH = 5000


def connect(pg_url: str | None) -> psycopg.Connection:
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

    # not named sql, which shadows the psycopg module used to build the batched
    # UPDATE below
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
        print("\ndry run, nothing written. Re-run with --apply to write.")
        return

    with connect(args.pg_url) as conn:
        conn.execute("SET statement_timeout = '0'")
        with conn.cursor() as cur:
            for start in range(0, len(changed), _WRITE_BATCH):
                batch = changed[start : start + _WRITE_BATCH]
                # the arrays need an explicit cast: a VALUES list gives Postgres
                # nothing to infer element types from
                values = sql.SQL(", ").join(
                    sql.SQL(
                        "({}, {}::text[], {}::text[], {}::text[], {}::text[],"
                        " {}::text[])",
                    ).format(
                        sql.Literal(r["accession"]),
                        sql.Literal(r["organisms"]),
                        sql.Literal(r["library_strategies"]),
                        sql.Literal(r["instrument_models"]),
                        sql.Literal(r["platforms"]),
                        sql.Literal(r["library_sources"]),
                    )
                    for r in batch
                )
                cur.execute(sql.SQL(_UPDATE_SQL.replace("%s", "{}")).format(values))
                conn.commit()
                print(
                    f"  committed {min(start + _WRITE_BATCH, len(changed)):,}/{len(changed):,}"
                )
    print(f"updated {len(changed):,} studies")

    if args.skip_refresh:
        print("skipping matview refresh; organism charts will show stale counts")
        return

    # organism_growth_monthly is a matview over sra_studies.organisms backing the
    # organism charts, and nothing invalidates it automatically, so it would keep
    # serving pre-backfill numbers. CONCURRENTLY keeps the stats page readable
    # while it rebuilds; idx_ogm_organism_db_month is the unique index it needs.
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
