"""
Build the fleet inputs for the Excel gene-name scan.

Writes, into --out-dir:
  dates.txt     (month-day pairs Excel makes from gene symbols, from S1 Table)
  files.tsv     accession<TAB>url, one line per tabular supplementary file
  studies.txt   one accession per line (yuj work list)
  excel_error_names.txt  the sure-shot corrupted strings, for humans

Postgres is read SELECT-only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pg_connect import connect  # noqa: E402

GEO_SQL = r"""
with f as (
  select g.accession, x->>'#text' u, x->>'@type' t,
   (g.library_strategies && array['RNA-Seq','scRNA-seq','ssRNA-seq','miRNA-Seq','ncRNA-Seq']
    or exists (select 1 from unnest(g.series_type) s where s ilike '%expression%')) expr
  from geo_series g, unnest(supplementary_data) x
)
select accession, u from f
where t in ('TXT','CSV','TSV','TAB','XLSX')
  and (expr or lower(u) ~ '(count|tpm|fpkm|rpkm|expr|matrix|cpm|rsem|salmon|kallisto|deseq|edger|normali|abundance|quant)')
order by accession, u
"""

AE_SQL = r"""
select a.accession, x->>'url'
from arrayexpress_experiments a, jsonb_array_elements(a.processed_data_files) x
where jsonb_typeof(a.processed_data_files)='array'
  and a.accession not like 'E-GEOD-%%'
  and lower(x->>'path') ~ '\.(txt|csv|tsv|tab|xlsx)(\.gz)?$'
  and coalesce((x->>'size')::bigint, 0) < %s
order by 1, 2
"""

MON = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()


def s1_dates(xlsx: Path) -> set[tuple[int, int]]:
    ws = openpyxl.load_workbook(xlsx, read_only=True).worksheets[0]
    out = set()
    for row in list(ws.iter_rows(values_only=True))[1:]:
        if len(row) < 4:
            continue
        v = row[3]
        if isinstance(v, dt.datetime):
            out.add((v.month, v.day))
        elif isinstance(v, str) and "-" in v:
            d, m = v.split("-", 1)
            if d.isdigit() and m[:3].title() in MON:
                out.add((MON.index(m[:3].title()) + 1, int(d)))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--s1", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--max-ae-bytes", type=int, default=2_000_000_000)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    dates = sorted(s1_dates(a.s1))
    (a.out_dir / "dates.txt").write_text("".join(f"{m}-{d}\n" for m, d in dates))
    names = []
    for m, d in dates:
        names += [f"{d}-{MON[m - 1]}", f"{MON[m - 1]}-{d:02d}", f"{d:02d}-{MON[m - 1]}"]
    names += ["<d>.<dd>E+<nn>  (RIKEN clone ids, e.g. 2310009E13 -> 2.31E+13)"]
    (a.out_dir / "excel_error_names.txt").write_text("\n".join(names) + "\n")

    with connect() as c, c.cursor() as cur:
        cur.execute(GEO_SQL)
        rows = cur.fetchall()
        cur.execute(AE_SQL, (a.max_ae_bytes,))
        rows += cur.fetchall()
    with (a.out_dir / "files.tsv").open("w") as fh:
        for acc, u in rows:
            fh.write(f"{acc}\t{u}\n")
    accs = sorted({acc for acc, _ in rows})
    (a.out_dir / "studies.txt").write_text("".join(f"{x}\n" for x in accs))
    print(f"dates={len(dates)} files={len(rows)} studies={len(accs)}")


if __name__ == "__main__":
    main()
