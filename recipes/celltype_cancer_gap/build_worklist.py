"""
Cancer-relevant single-cell studies the sc-count fleet hasn't scanned yet: build a
yuj_setup-compatible worklist (sidecars + master accessions.txt) sourced from live
Postgres, not the local staging sqlite counter-setup normally reads.

Universe = unified_metadata.is_single_cell studies whose enriched_samples_v5
tissue/disease looks cancer-related, minus anything already in pentimento_study
(i.e. already run through sc-count at least once).

Output shape matches sc_enrich.deep.yuj_setup exactly, so the existing yuj fleet
job (job: seqout-sc-count) can consume it unmodified: <out-dir>/accessions/<ACC>.json
sidecars ({"accession","source","aliases","links"}), <out-dir>/accessions.txt
(accessions with a download link), <out-dir>/no_link_accessions.txt (without).

  python recipes/celltype_cancer_gap/build_worklist.py --out-dir /data/saket/celltype_cancer_gap/yuj_setup
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pg_connect import connect  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from geo_samples_ref import flatten_samples_ref  # noqa: E402
from sc_enrich.deep._process import _classify_file  # noqa: E402
from sc_enrich.deep.counter_common import DownloadLink, FileType  # noqa: E402

CANCER_PATTERN = (
    "tumor|cancer|carcinoma|neoplasm|leukemia|lymphoma|sarcoma|melanoma|glioma|adenoma"
)

UNIVERSE_SQL = f"""
select distinct u.canonical_accession, u.source, u.aliases
from unified_metadata u
join enriched_samples_v5 e on e.study_accession = u.canonical_accession
where u.is_single_cell
  and (e.tissue ~* %(pat)s or e.disease ~* %(pat)s)
  and not exists (
      select 1 from pentimento_study p where p.study_accession = u.canonical_accession
  )
"""


def _fetch_universe(cur) -> list[tuple[str, str, dict]]:  # noqa: ANN001
    cur.execute(UNIVERSE_SQL, {"pat": CANCER_PATTERN})
    return cur.fetchall()


def _resolve_links(
    cur, gse_list: list[str], ae_list: list[str]
) -> dict[str, list[DownloadLink]]:  # noqa: ANN001
    links: dict[str, list[DownloadLink]] = {acc: [] for acc in (*gse_list, *ae_list)}

    if gse_list:
        # per-GSM supplementary files, rolled up to their GSE
        cur.execute(
            "select accession, samples_ref from geo_series "
            "where accession = any(%s) and samples_ref is not null",
            (gse_list,),
        )
        gsm_to_gse: dict[str, str] = {}
        for gse, samples_ref in cur.fetchall():
            for gsm in flatten_samples_ref(samples_ref):
                gsm_to_gse[gsm] = gse

        if gsm_to_gse:
            cur.execute(
                "select accession, supplementary_data from geo_samples "
                "where accession = any(%s) and supplementary_data is not null",
                (list(gsm_to_gse),),
            )
            for gsm, items in cur.fetchall():
                gse = gsm_to_gse.get(gsm)
                if gse is None:
                    continue
                for item in items or []:
                    url = item.get("#text")
                    if not url:
                        continue
                    ft = _classify_file(url)
                    if ft != FileType.Unknown:
                        links[gse].append(DownloadLink(type=ft, url=url))

        # series-level supplementary only for GSEs the GSM pass found nothing for
        need_series = [g for g in gse_list if not links.get(g)]
        if need_series:
            cur.execute(
                "select accession, supplementary_data from geo_series "
                "where accession = any(%s) and supplementary_data is not null",
                (need_series,),
            )
            for gse, items in cur.fetchall():
                for item in items or []:
                    url = item.get("#text")
                    if not url:
                        continue
                    ft = _classify_file(url)
                    if ft != FileType.Unknown:
                        links[gse].append(DownloadLink(type=ft, url=url))

    if ae_list:
        cur.execute(
            "select accession, processed_data_files from arrayexpress_experiments "
            "where accession = any(%s) and processed_data_files is not null",
            (ae_list,),
        )
        for acc, items in cur.fetchall():
            for item in items or []:
                url = item.get("url")
                if not url:
                    continue
                ft = _classify_file(url)
                if ft != FileType.Unknown:
                    links[acc].append(DownloadLink(type=ft, url=url))

    return links


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, required=True)
    a = ap.parse_args()
    acc_dir = a.out_dir / "accessions"
    acc_dir.mkdir(parents=True, exist_ok=True)

    with connect() as c, c.cursor() as cur:
        universe = _fetch_universe(cur)
        print(
            f"universe: {len(universe)} cancer-relevant unscanned single-cell studies"
        )

        link_key: dict[str, str] = {}
        for accession, source, aliases in universe:
            if source in {"geo", "ae"}:
                link_key[accession] = accession
            else:
                aliases = aliases or {}
                key = aliases.get("geo") or aliases.get("arrayexpress")
                if key:
                    link_key[accession] = key

        gse_list = sorted({k for k in link_key.values() if k.startswith("GSE")})
        ae_list = sorted({k for k in link_key.values() if not k.startswith("GSE")})
        links_by_key = _resolve_links(cur, gse_list, ae_list)

    n_with = n_without = 0
    with (
        (a.out_dir / "accessions.txt").open("w") as mf,
        (a.out_dir / "no_link_accessions.txt").open("w") as nf,
    ):
        for accession, source, aliases in universe:
            key = link_key.get(accession, accession)
            dl_items = links_by_key.get(key) or []
            download_links = [(str(i.type), i.url) for i in dl_items]
            alias_values = list((aliases or {}).values())

            (acc_dir / f"{accession}.json").write_text(
                json.dumps(
                    {
                        "accession": accession,
                        "source": source,
                        "aliases": "|".join(alias_values),
                        "links": download_links,
                    }
                )
            )
            if download_links:
                mf.write(f"{accession}\n")
                n_with += 1
            else:
                nf.write(f"{accession}\n")
                n_without += 1

    print(
        f"emitted {n_with + n_without} accessions: {n_with} with links -> "
        f"{a.out_dir / 'accessions.txt'}, {n_without} without -> "
        f"{a.out_dir / 'no_link_accessions.txt'}"
    )


if __name__ == "__main__":
    main()
