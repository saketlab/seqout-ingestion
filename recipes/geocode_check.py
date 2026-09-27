#!/usr/bin/env python
"""
Invariants and discrepancy report for the staged geocodes.

Every check here exists because the corresponding mistake was actually made
during this work, not because it seemed prudent. Run it before any transfer:
it exits non-zero when an invariant is violated, so it can gate the load.

  geocode_check.py                 # report, exit non-zero on a violation
  geocode_check.py --warn-only     # report, always exit 0
"""

from __future__ import annotations

import argparse
import collections
import logging
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import psycopg

from geocode.geocode import _canonical_country, _distinctive_tokens

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

#: A country dominated this heavily by ONE centre is the shape of a bad match:
#: "BI" put every Broad study in Burundi and made it a top-ten country.
_CONCENTRATION = 0.9
#: Below this a country is too small for concentration to mean anything.
_MIN_ROWS_FOR_CONCENTRATION = 200
#: Share of a source's checkable rows that may disagree with contributors.
_MAX_SOURCE_ERROR = 0.05


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/data/saket/geocode/study_geocode.db")
    ap.add_argument("--warn-only", action="store_true")
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    violations: list[str] = []
    warnings: list[str] = []

    # --- INVARIANTS: these must hold, and each one broke at least once -------
    lengths = dict(
        con.execute(
            "SELECT length(country_code), COUNT(*) FROM study_geocode "
            "WHERE country_code IS NOT NULL GROUP BY 1"
        )
    )
    if set(lengths) - {2}:
        violations.append(
            f"country_code is not uniformly alpha-2: lengths {sorted(lengths)}. "
            "A stdlib-only fleet worker stores ArcGIS alpha-3 verbatim; "
            "canonicalise at the merge boundary."
        )

    empty = con.execute(
        "SELECT COUNT(*) FROM study_geocode WHERE country IS NULL OR country = ''"
    ).fetchone()[0]
    if empty:
        violations.append(
            f"{empty} rows carry no country. A geocode with no country holds "
            "nothing this table exists for and would blank a real value."
        )

    bare = con.execute(
        "SELECT COUNT(*) FROM study_geocode WHERE length(country) <= 3"
    ).fetchone()[0]
    if bare:
        violations.append(
            f"{bare} rows have a country NAME that is really a code. "
            "Canonicalise the name, not just the code."
        )

    # A centre with nothing distinctive in its name cannot be verified by any
    # route, so it must never carry a geocode: this is the "BI" -> Burundi bug.
    # Scoped to rows whose answer came from the NAME. An address-sourced row is
    # validated by the address, so a centre called "University" is fine there --
    # the first version of this check flagged those and was wrong.
    unverifiable = [
        cn
        for (cn,) in con.execute(
            "SELECT DISTINCT center_name FROM study_geocode "
            "WHERE center_name IS NOT NULL AND center_name <> '' "
            "AND source <> 'address'"
        )
        if not _distinctive_tokens(cn)
    ]
    if unverifiable:
        violations.append(
            f"{len(unverifiable)} centres with no verifiable name carry a "
            f"geocode, e.g. {unverifiable[:5]}"
        )

    # --- DISCREPANCIES: worth a human eye, not necessarily wrong ------------
    multi = con.execute(
        "SELECT COUNT(*) FROM (SELECT center_name FROM study_geocode "
        "WHERE center_name IS NOT NULL GROUP BY 1 "
        "HAVING COUNT(DISTINCT country_code) > 1)"
    ).fetchone()[0]
    if multi:
        warnings.append(f"{multi} centres resolve to more than one country")

    # One centre owning nearly a whole country is how a bad match looks from
    # the outside, whatever produced it.
    per_country = collections.Counter()
    per_pair = collections.Counter()
    for country, centre, n in con.execute(
        "SELECT country, center_name, COUNT(*) FROM study_geocode "
        "WHERE country IS NOT NULL GROUP BY 1,2"
    ):
        per_country[country] += n
        per_pair[(country, centre)] += n
    for (country, centre), n in per_pair.most_common():
        total = per_country[country]
        if total >= _MIN_ROWS_FOR_CONCENTRATION and n / total >= _CONCENTRATION:
            warnings.append(
                f"{country}: one centre ({centre}) accounts for nearly all of it"
            )

    # --- MEASURED ERROR: each source against an independent signal ----------
    if os.environ.get("PG_URL"):
        with psycopg.connect(os.environ["PG_URL"]) as pg:
            truth: dict[str, set] = collections.defaultdict(set)
            for cn, ac in pg.execute(
                "SELECT u.center_name, gc.address_country FROM unified_metadata u "
                "JOIN geo_contributors gc ON gc.accession = u.canonical_accession "
                "WHERE gc.address_country IS NOT NULL AND u.center_name IS NOT NULL "
                "GROUP BY 1,2"
            ):
                code = _canonical_country(None, ac)[0]
                if code:
                    truth[cn].add(code)
        tally: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0])
        for cn, cc, src in con.execute(
            "SELECT center_name, country_code, source FROM study_geocode "
            "WHERE center_name IS NOT NULL GROUP BY 1,2,3"
        ):
            ref = truth.get(cn)
            if ref:
                tally[src][0 if cc in ref else 1] += 1
        logger.info("\nmeasured against contributor countries:")
        for src, (agree, dis) in sorted(tally.items()):
            rate = dis / (agree + dis) if agree + dis else 0
            logger.info(
                "  %-16s %5d checked  %5.1f%% disagree",
                src,
                agree + dis,
                100 * rate,
            )
            if rate > _MAX_SOURCE_ERROR:
                violations.append(
                    f"source {src!r} disagrees with contributor countries on "
                    f"{100 * rate:.1f}% of checkable rows"
                )
    else:
        warnings.append("PG_URL unset: skipped the measured-error check")

    logger.info("")
    for w in warnings:
        logger.info("WARN  %s", w)
    for v in violations:
        logger.info("FAIL  %s", v)
    if not violations and not warnings:
        logger.info("all checks passed")
    logger.info("")
    logger.info("%d violations, %d warnings", len(violations), len(warnings))
    return 1 if violations and not args.warn_only else 0


if __name__ == "__main__":
    raise SystemExit(main())
