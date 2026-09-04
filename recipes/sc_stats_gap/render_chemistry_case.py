#!/usr/bin/env python3
"""
Splice sc_evidence.rules.CHEMISTRIES into the sc-quality matview.

The chemistry vocabulary lives in one place. This rewrites the region between
the SQL_BEGIN/SQL_END markers; everything else in the file, including the 10x
version ladder, is hand-written and left alone.

    render_chemistry_case.py [--check]

--check exits non-zero when the file has drifted, for CI and for the test.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from sc_evidence.rules import SQL_BEGIN, SQL_END, chemistry_case_sql

SQL_FILE = Path(__file__).resolve().parents[2] / "sql/tables/sc_quality_by_year_pg.sql"


def render(text: str) -> str:
    """Replace the marked region, keeping the BEGIN line's own indentation."""
    start = text.index(SQL_BEGIN)
    line_start = text.rindex("\n", 0, start) + 1
    indent = len(text[line_start:start])
    end = text.index(SQL_END) + len(SQL_END)
    return text[:line_start] + chemistry_case_sql(indent=indent) + text[end:]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    current = SQL_FILE.read_text()
    wanted = render(current)
    if current == wanted:
        return 0
    if args.check:
        sys.stderr.write(
            f"{SQL_FILE} has drifted from CHEMISTRIES; rerun without --check\n"
        )
        return 1
    SQL_FILE.write_text(wanted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
