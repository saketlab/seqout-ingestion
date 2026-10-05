"""
Merge the fleet's results/*.json into one table of supplementary files with
Excel gene-name errors: accession, url, hit count, kinds, examples.

  python recipes/excel_genes/merge_results.py yuj/excel_genes/results \
      --out excel_gene_errors.csv [--redo-out redo_studies.txt]

Scientific-notation hits are re-filtered here to exponents 10-24 (RIKEN clone
shape) because the first worker build also counted count-column values such as
2.03E+01; new scans apply the same rule at source.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import yuj_worker as w  # noqa: E402

RE_SCI_STRICT = re.compile(r"^\d(?:\.\d+)?[eE]\+(1\d|2\d|30)$")
DATES = w.load_dates(
    Path(os.environ.get("XG_DATES", "/home/saket/quant/excel_genes/dates.txt"))
)


def file_hits(x: dict) -> tuple[int, dict, list]:
    """Re-classify the recorded examples under the current rules.

    Early scans counted count-column values and plate wells as sci hits and
    lacked later date pairs; examples hold the raw strings, so the current
    classifier decides. Examples are the top 30 distinct strings, so a file
    with more distinct corrupted names is undercounted, never missed.
    """
    kinds = collections.Counter()
    examples = []
    for v, n in x.get("examples", []):
        k = w.classify(v, DATES)
        if (
            k == "sci"
            and RE_SCI_STRICT.match(v) is None
            and not any(
                w.classify(v2, DATES) == "date" for v2, _ in x.get("examples", [])
            )
        ):
            continue  # E+07..E+09 alone is too close to an ordinary count
        if k:
            kinds[k] += n
            examples.append((v, n))
    kinds["date"] += x.get("kinds", {}).get("date_serial", 0)
    return sum(kinds.values()), dict(kinds), examples


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "results_dir",
        type=Path,
        nargs="+",
        help="pass dirs, later ones win per (accession, url)",
    )
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--redo-out", type=Path, help="write studies with any errored file")
    ap.add_argument("--min-hits", type=int, default=1)
    a = ap.parse_args()

    status = collections.Counter()
    latest: dict[tuple[str, str], dict] = {}
    skipped = 0
    for rd in a.results_dir:
        for f in sorted(rd.glob("*.json")):
            try:
                d = json.loads(f.read_text())
            except json.JSONDecodeError:
                skipped += 1
                continue
            for x in d["files"]:
                key = (d["accession"], x["url"])
                if (
                    key not in latest
                    or x.get("status") == "ok"
                    or latest[key].get("status") != "ok"
                ):
                    latest[key] = x
    rows, redo = [], set()
    for (acc, url), x in sorted(latest.items()):
        status[x.get("status")] += 1
        if x.get("status") == "error":
            redo.add(acc)
            continue
        n, kinds, examples = file_hits(x)
        if n >= a.min_hits:
            rows.append(
                {
                    "accession": acc,
                    "url": url,
                    "n_hits": n,
                    "fmt": x.get("fmt"),
                    "rows_scanned": x.get("rows", x.get("members_scanned")),
                    "truncated": x.get("truncated", False),
                    "kinds": ";".join(f"{k}={v}" for k, v in kinds.items() if v),
                    "members": ";".join(
                        m["member"] for m in x.get("hits", []) if m.get("n_hits")
                    )[:500],
                    "examples": ";".join(f"{v}x{c}" for v, c in examples[:10]),
                }
            )
    with a.out.open("w", newline="") as fh:
        w = csv.DictWriter(
            fh, fieldnames=list(rows[0]) if rows else ["accession", "url"]
        )
        w.writeheader()
        w.writerows(rows)
    if a.redo_out:
        a.redo_out.write_text("".join(f"{s}\n" for s in sorted(redo)))
    print(f"skipped malformed json: {skipped}")
    print(f"files by status: {dict(status)}")
    print(
        f"flagged files: {len(rows)} in {len({r['accession'] for r in rows})} studies -> {a.out}"
    )
    print(f"studies with errored files: {len(redo)}")


if __name__ == "__main__":
    main()
