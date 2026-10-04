#!/usr/bin/env python3
"""05: Emulate transfer.py + the singlecell_study matview locally; before/after.

singlecell_study = run_ev UNION {unified_metadata.is_single_cell}. transfer.py
sets is_single_cell from the staged table for canonicals it names and resets
every other canonical to NULL / not_screened. So after a transfer of staging X:

    members(X) = run_ev  UNION  {k : X[k].is_single_cell = 1, k a canonical}

run_ev (pentimento matrix/barcode/r1len, alias-mapped) is taken from the live
matview (prod_singlecell_study.ev_matrix/ev_barcode/ev_r1len) and is unchanged
by this branch.

Scenarios
  S0  production today (live singlecell_study, 42,033)
  S1  transfer the pipeline's CURRENT staging as-is (recall_fix.db, 2026-09-26)
  S2  transfer the patched build (04, no external evidence)
  S3  S2 + the labelled external-resource pass

A key counts as a canonical if it is one in the local unify output, or it is in
the live matview. Keys that are neither (PRJ* canonicals the local file lacks) are
counted separately, and local canonicals first published after the last
Postgres load (2026-09-26) are reported as `local_only_recent`, not as new tags.

Writes out/05_scenarios.csv, out/05_scbasecount.csv, out/05_census.csv,
out/05_new_tags.csv (every S2/S3 new tag with its pass) and table
`new_tags` in the local copy (with --apply).
"""

from __future__ import annotations

import argparse

import pandas as pd

from _db import OUT, local

PG_LOAD = "2026-09-26"


def members(run_ev: set[str], staged: pd.DataFrame, canon: set[str]) -> set[str]:
    pos = set(staged.loc[staged.is_single_cell == 1, "study_accession"])
    return run_ev | {k for k in pos if k in canon}


def main(apply: bool) -> None:
    con = local(readonly=not apply)
    prod = pd.read_sql("SELECT * FROM prod_singlecell_study", con)
    ev = prod[["ev_matrix", "ev_barcode", "ev_r1len"]].astype(bool).any(axis=1)
    run_ev = set(prod.loc[ev, "study_accession"])
    s0 = set(prod.study_accession)
    um = pd.read_sql("SELECT canonical_accession, first_published FROM um", con)
    canon = set(um.canonical_accession) | s0
    recent = set(um.loc[um.first_published.fillna("") > PG_LOAD, "canonical_accession"])

    sc = {
        "S1": pd.read_sql(
            "SELECT study_accession, is_single_cell FROM staged_pipeline", con
        ),
        "S2": pd.read_sql(
            "SELECT study_accession, is_single_cell FROM study_single_cell_new", con
        ),
        "S3": pd.read_sql(
            "SELECT study_accession, is_single_cell FROM study_single_cell_new_ext", con
        ),
    }
    mem = {"S0": s0} | {k: members(run_ev, v, canon) for k, v in sc.items()}
    for k, v in sc.items():
        pos = set(v.loc[v.is_single_cell == 1, "study_accession"])
        print(
            f"{k}: staged positives not a known canonical (cannot land): {len(pos - canon):,}"
        )

    added_by = (
        pd.read_sql("SELECT * FROM new_added_by", con).set_index("canonical").added_by
    )
    scb = pd.read_sql("SELECT study, canonical, status FROM ext_scbasecount", con)
    ext_ids = set(scb.study) | set(scb.canonical.dropna())
    cen = pd.read_sql(
        "SELECT collection_id, verdict, seqout_canonical, pmid_seqout_studies,"
        " public_accessions FROM ext_census",
        con,
    )
    for c in ["seqout_canonical", "pmid_seqout_studies", "public_accessions"]:
        ext_ids |= {a for s in cen[c].dropna() for a in s.split(";")}

    rows, scb_rows, cen_rows, new_rows = [], [], [], []
    for name, m in mem.items():
        new = m - s0 - recent
        lost = s0 - m
        rows.append(
            {
                "scenario": name,
                "members": len(m - recent),
                "new_vs_S0": len(new),
                "lost_vs_S0": len(lost),
                "local_only_recent_excluded": len(m & recent - s0),
                "new_in_external_sets": len(new & ext_ids),
                "new_not_in_external": len(new - ext_ids),
            }
        )
        cov = scb.assign(
            hit=[(s in m) or (c in m) for s, c in zip(scb.study, scb.canonical)]
        )
        was = cov.status.str.startswith("covered")
        scb_rows.append(
            {
                "scenario": name,
                "studies": len(cov),
                "covered": int(
                    (was & (cov.hit | (name == "S0"))).sum() + (~was & cov.hit).sum()
                ),
                "recovered_of_690": int(
                    (cov.status == "miss_tagging_gap").mul(cov.hit).sum()
                ),
                "covered_lost": int((was & ~cov.hit).sum()) if name != "S0" else 0,
            }
        )
        cen_rows.append(_census(cen, m, name))
        if name in ("S2", "S3"):
            for k in sorted(new):
                new_rows.append(
                    {
                        "scenario": name,
                        "canonical": k,
                        "added_by": added_by.get(k, "unknown"),
                        "in_external_sets": k in ext_ids,
                    }
                )
    out = pd.DataFrame(rows)
    out.to_csv(OUT / "05_scenarios.csv", index=False)
    print(out.to_string(index=False))
    s = pd.DataFrame(scb_rows)
    s["pct_covered"] = (100 * s.covered / s.studies).round(2)
    s.to_csv(OUT / "05_scbasecount.csv", index=False)
    print(s.to_string(index=False))
    c = pd.DataFrame(cen_rows)
    c.to_csv(OUT / "05_census.csv", index=False)
    print(c.to_string(index=False))
    nt = pd.DataFrame(new_rows)
    nt.to_csv(OUT / "05_new_tags.csv", index=False)
    print(
        nt.groupby(["scenario", "added_by", "in_external_sets"])
        .size()
        .unstack(fill_value=0)
        .to_string()
    )
    if apply:
        nt.to_sql("new_tags", con, if_exists="replace", index=False)
        con.commit()


def _census(cen: pd.DataFrame, m: set[str], name: str) -> dict:
    public = cen.verdict.isin(
        [
            "covered_accession",
            "covered_pmid",
            "tagging_gap_accession",
            "tagging_gap_pmid_only",
            "catalogue_gap_accession",
        ]
    )

    def hit(r: pd.Series) -> bool:
        accs = (
            r.seqout_canonical
            if r.verdict in ("covered_accession", "tagging_gap_accession")
            else r.pmid_seqout_studies
        )
        return (
            any(a in m for a in str(accs).split(";"))
            if isinstance(accs, str)
            else False
        )

    h = cen.apply(hit, axis=1)
    covered_before = cen.verdict.isin(["covered_accession", "covered_pmid"])
    covered = (covered_before if name == "S0" else h) & public
    gaps = cen.verdict.isin(["tagging_gap_accession", "tagging_gap_pmid_only"])
    return {
        "scenario": name,
        "public_deposit_collections": int(public.sum()),
        "covered": int(covered.sum()),
        "pct_covered": round(100 * covered.sum() / public.sum(), 1),
        "recovered_of_25": int((gaps & h).sum()) if name != "S0" else 0,
        "recovered_accession_of_21": int(
            (cen.verdict.eq("tagging_gap_accession") & h).sum()
        )
        if name != "S0"
        else 0,
        "covered_lost": int((covered_before & ~h).sum()) if name != "S0" else 0,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--apply", action="store_true", help="write table new_tags into the LOCAL copy"
    )
    main(ap.parse_args().apply)
