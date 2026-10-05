"""
Second-pass manifest: every GEO series (tabular files and RAW tars), every
non-GEOD ArrayExpress experiment (tabular and zip), every DDBJ GEA experiment,
minus files the first pass already scanned cleanly.

  python recipes/excel_genes/build_manifest_v2.py --results-dir yuj/excel_genes/results \
      --out-dir /home/saket/quant/excel_genes/v2

Writes files.tsv and studies.txt; the worker reads files.tsv beside itself.
Postgres is read SELECT-only.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pg_connect import connect  # noqa: E402

MAX_BYTES = 200 * 1024 * 1024

GEO_SQL = """
select g.accession, x->>'#text'
from geo_series g, unnest(supplementary_data) x
where x->>'@type' in ('TXT','CSV','TSV','TAB','XLSX','TAR')
order by 1, 2
"""

AE_SQL = r"""
select a.accession, x->>'url'
from arrayexpress_experiments a, jsonb_array_elements(a.processed_data_files) x
where jsonb_typeof(a.processed_data_files)='array'
  and a.accession not like 'E-GEOD-%%'
  and lower(x->>'path') ~ '\.(txt|csv|tsv|tab|xlsx|zip)(\.gz)?$'
  and coalesce((x->>'size')::bigint, 0) < %s
order by 1, 2
"""

GEA_SQL = r"""
select e.accession, x->>'url'
from gea_experiments e, jsonb_array_elements(e.data_files) x
where jsonb_typeof(e.data_files)='array'
  and lower(x->>'name') ~ '\.(txt|csv|tsv|tab|xlsx|zip)(\.gz)?$'
  and lower(x->>'name') !~ '\.(idf|sdrf)\.txt$'
  and coalesce((x->>'size')::bigint, 0) < %s
order by 1, 2
"""


def scanned_ok(results_dir: Path) -> set[tuple[str, str]]:
    done = set()
    for f in results_dir.glob("*.json"):
        d = json.loads(f.read_text())
        for x in d["files"]:
            if x.get("status") == "ok":
                done.add((d["accession"], x["url"]))
    return done


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    done = scanned_ok(a.results_dir)
    with connect() as c, c.cursor() as cur:
        cur.execute(GEO_SQL)
        rows = cur.fetchall()
        cur.execute(AE_SQL, (MAX_BYTES,))
        rows += cur.fetchall()
        cur.execute(GEA_SQL, (MAX_BYTES,))
        rows += cur.fetchall()
    todo = [(acc, u) for acc, u in rows if (acc, u) not in done]
    with (a.out_dir / "files.tsv").open("w") as fh:
        for acc, u in todo:
            fh.write(f"{acc}\t{u}\n")
    accs = sorted({acc for acc, _ in todo})
    (a.out_dir / "studies.txt").write_text("".join(f"{x}\n" for x in accs))
    tars = sum(u.lower().endswith(".tar") for _, u in todo)
    print(
        f"all={len(rows)} already_ok={len(done)} todo_files={len(todo)} tars={tars} studies={len(accs)}"
    )


if __name__ == "__main__":
    main()
