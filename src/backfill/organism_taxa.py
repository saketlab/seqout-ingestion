#!/usr/bin/env python
"""Build the organism_taxa lookup: scientific name -> NCBI taxon id.

An organism's taxon id is a property of the organism, not of the study that
happens to contain it — measured on prod, 315,817 of 315,997 distinct names map
to exactly one taxid (180 ambiguous, 0.06%). So it lives in one table rather
than being copied into every project row, and the project endpoints join it
when they build a response.

This leaves the `organisms text[]` columns and their GIN indexes alone, so the
search filters (`organisms @> ARRAY[...]`) are unaffected.

Sources, in precedence order — later ones only fill names the earlier lack:
    sra_samples   (scientific_name, taxon_id)   ~316k names
    dra_samples   (scientific_name, taxon_id)   +1,297
    geo_samples   channels[].Organism #text/@taxid  +319
    gea_samples   (organism, taxon_id)          +3
Ambiguous names are resolved to their most frequently observed taxid.

DRY RUN BY DEFAULT — reports what it would build. Pass --apply to write.

    python -m backfill.organism_taxa            # report only
    python -m backfill.organism_taxa --apply    # create + populate
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg

_CREATE = """
CREATE TABLE IF NOT EXISTS organism_taxa (
    name      text PRIMARY KEY,
    taxon_id  text NOT NULL,
    source    text NOT NULL
);
"""

# One statement: every source unioned, ranked, and de-duplicated server-side.
# `rank` is the source precedence; `n` breaks ties within a source by how often
# the pairing was observed, so an ambiguous name lands on its commonest taxid.
_POPULATE = """
WITH pairs AS (
    SELECT scientific_name AS name, taxon_id, 1 AS rank, COUNT(*) AS n, 'sra' AS source
    FROM sra_samples
    WHERE scientific_name IS NOT NULL AND scientific_name <> ''
      AND taxon_id IS NOT NULL AND taxon_id <> ''
    GROUP BY 1, 2

    UNION ALL
    SELECT scientific_name, taxon_id, 2, COUNT(*), 'dra'
    FROM dra_samples
    WHERE scientific_name IS NOT NULL AND scientific_name <> ''
      AND taxon_id IS NOT NULL AND taxon_id <> ''
    GROUP BY 1, 2

    UNION ALL
    SELECT ch->'Organism'->>'#text', ch->'Organism'->>'@taxid', 3, COUNT(*), 'geo'
    FROM geo_samples sm
    CROSS JOIN LATERAL unnest(COALESCE(sm.channels, '{}'::jsonb[])) ch
    WHERE NULLIF(ch->'Organism'->>'#text', '') IS NOT NULL
      AND NULLIF(ch->'Organism'->>'@taxid', '') IS NOT NULL
    GROUP BY 1, 2

    UNION ALL
    SELECT organism, taxon_id, 4, COUNT(*), 'gea'
    FROM gea_samples
    WHERE organism IS NOT NULL AND organism <> ''
      AND taxon_id IS NOT NULL AND taxon_id <> ''
    GROUP BY 1, 2
),
ranked AS (
    SELECT name, taxon_id, source,
           ROW_NUMBER() OVER (
               PARTITION BY name
               ORDER BY rank, n DESC, taxon_id
           ) AS pick
    FROM pairs
)
INSERT INTO organism_taxa (name, taxon_id, source)
SELECT name, taxon_id, source FROM ranked WHERE pick = 1
ON CONFLICT (name) DO UPDATE
    SET taxon_id = EXCLUDED.taxon_id, source = EXCLUDED.source
"""

_PREVIEW = (
    _POPULATE[_POPULATE.index("WITH pairs") : _POPULATE.index("INSERT INTO")]
    + """
SELECT COUNT(*) AS names,
       COUNT(*) FILTER (WHERE source = 'sra') AS from_sra,
       COUNT(*) FILTER (WHERE source = 'dra') AS from_dra,
       COUNT(*) FILTER (WHERE source = 'geo') AS from_geo,
       COUNT(*) FILTER (WHERE source = 'gea') AS from_gea
FROM ranked WHERE pick = 1
"""
)


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
    args = ap.parse_args()

    with connect(args.pg_url) as conn:
        conn.execute("SET statement_timeout = '0'")

        print("scanning sources…")
        with conn.cursor() as cur:
            cur.execute(_PREVIEW)
            names, sra, dra, geo, gea = cur.fetchone()
        print(f"\n  names resolved: {names:,}")
        print(f"    from sra_samples : {sra:,}")
        print(f"    from dra_samples : {dra:,}")
        print(f"    from geo_samples : {geo:,}")
        print(f"    from gea_samples : {gea:,}")

        if not args.apply:
            print("\ndry run — nothing written. Re-run with --apply to build.")
            return

        with conn.cursor() as cur:
            cur.execute(_CREATE)
            cur.execute(_POPULATE)
            written = cur.rowcount
        conn.commit()
        print(f"\norganism_taxa populated: {written:,} rows")


if __name__ == "__main__":
    main()
