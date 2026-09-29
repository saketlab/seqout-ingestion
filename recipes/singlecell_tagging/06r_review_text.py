#!/usr/bin/env python3
"""06r: Reviewer aid. Print full stored text around single-cell keywords.

Judges see packets truncated at 900 characters. For each accession given, this
prints keyword windows from the full Postgres title/description extract (else the
local unify output), used to resolve `unclear` calls into out/06{b,c}_overrides.csv.

Usage: 06r_review_text.py ACC [ACC ...]
"""

from __future__ import annotations

import re
import sys

from _db import local

RX = re.compile(r".{0,120}(single[- ]?(cell|nucle)|scRNA|snRNA|10x|chromium|visium|geomx|drop-?seq"
                r"|smart-?seq|sorted|pooled|bulk).{0,120}", re.IGNORECASE)


def main(accs: list[str]) -> None:
    con = local(readonly=True)
    for a in accs:
        row = con.execute("SELECT txt FROM pg_um_text WHERE raw_study = ?", (a,)).fetchone()
        if row is None:
            um = con.execute("SELECT title, description FROM um WHERE canonical_accession = ?", (a,)).fetchone()
            row = (" | ".join(x or "" for x in um),) if um else ("",)
        txt = row[0] or ""
        print(f"## {a} ({len(txt)} chars)\n  {txt[:160]!r}")
        for m in list(RX.finditer(txt))[:5]:
            print("  >>", m.group(0).replace("\n", " "))


if __name__ == "__main__":
    main(sys.argv[1:])
