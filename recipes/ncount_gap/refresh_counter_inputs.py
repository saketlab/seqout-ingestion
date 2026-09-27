#!/usr/bin/env python3
"""
Build `sc_enrich counter-setup` inputs from production Postgres, not from the
staging SQLite snapshots.

`yuj_setup` reads three SQLite files: `unified_metadata.db` (the universe and
its aliases), the GEO db (`geo_series.samples_ref` / `supplementary_data` and
`geo_samples.supplementary_data`, which is where every download link comes
from) and the ArrayExpress db. All three are per-ingest-window staging files
that nothing re-syncs after transfer, and an accession missing from any of them
is dropped without an error: absent from `unified_metadata` it never enters the
universe at all, absent from `geo_series` it lands in `no_link_accessions.txt`
looking like a study with nothing to download.

Production Postgres is the source of truth: it accumulates across staging
generations, while SQLite staging files only hold ingest windows. Running the
counter from staging files can turn missing staging rows into false no-link
findings.

Scoped to a target list rather than copying GEO whole; these files feed one
counter-setup run.

Keyed by the GEO accession. Following current canonical_accession values would
rename work items and split results from the existing ncount corpus, which is
keyed by GSE. The unified row only supplies aliases.

Writes new files and refuses to overwrite, same as `refresh_download_links.py`.

Usage:
    refresh_counter_inputs.py --targets targets_strong.txt --out-dir DIR
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from geo_samples_ref import dump_samples_ref, flatten_samples_ref  # noqa: E402
from helpers import exec_query_from_file  # noqa: E402

logger = logging.getLogger("refresh_counter_inputs")


def _read_targets(paths: list[Path]) -> list[str]:
    seen: dict[str, None] = {}
    for p in paths:
        for line in p.read_text().splitlines():
            acc = line.strip()
            if acc:
                seen.setdefault(acc, None)
    return list(seen)


def _fresh(path: Path, ddl: str) -> tuple[Path, sqlite3.Connection]:
    tmp = path.with_suffix(".partial")
    tmp.unlink(missing_ok=True)
    exec_query_from_file(ddl, db_file=tmp)
    conn = sqlite3.connect(tmp)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    return tmp, conn


def _js(v: object) -> str | None:
    """Postgres array/jsonb -> the JSON text the SQLite schema stores."""
    return None if v is None else json.dumps(v)


def main() -> int:  # noqa: PLR0915
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", type=Path, action="append", required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--pg-url", default=os.environ.get("PG_URL"))
    args = ap.parse_args()
    if not args.pg_url:
        ap.error("--pg-url or $PG_URL required")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
    )
    targets = _read_targets(args.targets)
    logger.info("%d target accessions", len(targets))

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_u = args.out_dir / "unified_metadata.db"
    out_g = args.out_dir / "geo.db"
    out_a = args.out_dir / "ae.db"
    for p in (out_u, out_g, out_a):
        if p.exists():
            ap.error(f"{p} exists; refusing to overwrite")

    tmp_u, cu = _fresh(out_u, "tables/unified_metadata.sql")
    tmp_g, cg = _fresh(out_g, "tables/geo.sql")
    tmp_a, ca = _fresh(out_a, "tables/ae.sql")

    with psycopg.connect(args.pg_url) as pg:
        with pg.cursor() as cur:
            # aliases, by whichever route the unified row still carries the GSE
            aliases: dict[str, dict] = {}
            cur.execute(
                "SELECT canonical_accession, aliases FROM unified_metadata "
                "WHERE canonical_accession = ANY(%s)",
                (targets,),
            )
            direct = {a: (al or {}) for a, al in cur.fetchall()}
            cur.execute(
                "SELECT aliases->>'geo', canonical_accession, source, aliases "
                "FROM unified_metadata WHERE aliases->>'geo' = ANY(%s)",
                (targets,),
            )
            demoted: dict[str, dict] = {}
            for gse, canon, source, al in cur.fetchall():
                merged = {k: v for k, v in (al or {}).items() if k != "geo"}
                merged.setdefault(source, canon)
                demoted.setdefault(gse, merged)
            # geo_series.alias_all is the last resort for series unify has not
            # reached yet; without it those rows would carry no aliases at all
            cur.execute(
                "SELECT accession, alias, alias_all FROM geo_series "
                "WHERE accession = ANY(%s)",
                (targets,),
            )
            from_geo: dict[str, dict] = {}
            for acc, alias, alias_all in cur.fetchall():
                vals = [v for v in ([alias, *(alias_all or [])]) if v]
                if vals:
                    from_geo[acc] = {"sra": vals[0]}

            n_direct = n_demoted = n_geo = n_none = 0
            for acc in targets:
                if acc in direct:
                    aliases[acc] = direct[acc]
                    n_direct += 1
                elif acc in demoted:
                    aliases[acc] = demoted[acc]
                    n_demoted += 1
                elif acc in from_geo:
                    aliases[acc] = from_geo[acc]
                    n_geo += 1
                else:
                    aliases[acc] = {}
                    n_none += 1
            logger.info(
                "aliases: %d canonical, %d demoted-to-alias, %d from geo_series, "
                "%d none",
                n_direct,
                n_demoted,
                n_geo,
                n_none,
            )

            cu.executemany(
                "INSERT OR REPLACE INTO unified_metadata "
                "(canonical_accession, source, aliases) VALUES (?, 'geo', ?)",
                [(acc, json.dumps(aliases[acc])) for acc in targets],
            )
            cu.commit()

            cur.execute(
                "SELECT accession, samples_ref, supplementary_data FROM geo_series "
                "WHERE accession = ANY(%s)",
                (targets,),
            )
            series = cur.fetchall()
            cg.executemany(
                "INSERT OR REPLACE INTO geo_series "
                "(accession, samples_ref, supplementary_data) VALUES (?, ?, ?)",
                # samples_ref is a JSON array of GSM strings, one shape
                # everywhere; see src/geo_samples_ref.py. Postgres already
                # stores text[], so this is a no-op today. It stays so the rule
                # holds if the source ever changes.
                [(a, dump_samples_ref(sr), _js(sd)) for a, sr, sd in series],
            )
            cg.commit()
            logger.info("%d / %d series found in postgres", len(series), len(targets))

            gsms = sorted({g for _, sr, _ in series for g in flatten_samples_ref(sr)})
            logger.info("%d samples referenced", len(gsms))
            cur.execute(
                "SELECT accession, supplementary_data FROM geo_samples "
                "WHERE accession = ANY(%s)",
                (gsms,),
            )
            samples = cur.fetchall()
            cg.executemany(
                "INSERT OR REPLACE INTO geo_samples "
                "(accession, supplementary_data) VALUES (?, ?)",
                [(a, _js(sd)) for a, sd in samples],
            )
            cg.commit()
            logger.info("%d / %d samples found", len(samples), len(gsms))

            # ArrayExpress. yuj_setup routes by accession prefix: non-GSE targets
            # look in arrayexpress_experiments.processed_data_files. Keep ae.db
            # populated so E-* targets do not become false no-link results.
            ae_targets = [a for a in targets if not a.startswith("GSE")]
            if ae_targets:
                cur.execute(
                    "SELECT accession, processed_data_files "
                    "FROM arrayexpress_experiments WHERE accession = ANY(%s)",
                    (ae_targets,),
                )
                ae = cur.fetchall()
                ca.executemany(
                    "INSERT OR REPLACE INTO arrayexpress_experiments "
                    "(accession, processed_data_files) VALUES (?, ?)",
                    [(a, _js(pdf)) for a, pdf in ae],
                )
                ca.commit()
                logger.info(
                    "%d / %d arrayexpress experiments found in postgres",
                    len(ae),
                    len(ae_targets),
                )

    for c in (cu, cg, ca):
        c.execute("ANALYZE")
        c.commit()
        c.close()
    tmp_u.replace(out_u)
    tmp_g.replace(out_g)
    tmp_a.replace(out_a)
    logger.info("wrote %s, %s, %s", out_u, out_g, out_a)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
