#!/usr/bin/env python3
"""08: Which pass recovered each scBaseCount / Census target, and what is left.

Joins 03_targets.csv (cause per target) with the new staged tables (04) and the
first pass that made each canonical single-cell (new_added_by). A target counts
as recovered under S2 when its canonical (or raw study) is single-cell in
study_single_cell_new or in run_ev; S3 adds the labelled external pass.

Writes out/08_attribution.csv (per target) and out/08_attribution_summary.csv.
"""

from __future__ import annotations

import pandas as pd

from _db import OUT, local


def main() -> None:
    con = local(readonly=True)
    t = pd.read_csv(OUT / "03_targets.csv", low_memory=False)
    by = (
        pd.read_sql("SELECT canonical, added_by FROM new_added_by", con)
        .set_index("canonical")
        .added_by
    )
    s2 = set(
        pd.read_sql(
            "SELECT study_accession FROM study_single_cell_new WHERE is_single_cell = 1",
            con,
        ).study_accession
    )
    s3 = set(
        pd.read_sql(
            "SELECT study_accession FROM study_single_cell_new_ext WHERE is_single_cell = 1",
            con,
        ).study_accession
    )
    prod = pd.read_sql("SELECT * FROM prod_singlecell_study", con)
    ev = prod[["ev_matrix", "ev_barcode", "ev_r1len"]].astype(bool).any(axis=1)
    run_ev = set(prod.loc[ev, "study_accession"])

    def how(r: pd.Series) -> str:
        keys = [k for k in (r.canonical, r.study) if isinstance(k, str)]
        if any(k in s2 or k in run_ev for k in keys):
            return next((by[k] for k in keys if k in by.index), "run_ev_or_prior")
        if any(k in s3 for k in keys):
            return "external"
        return "not_recovered"

    t["recovered_by"] = t.apply(how, axis=1)
    t.to_csv(OUT / "08_attribution.csv", index=False)
    rows = []
    for tgt, g in t.groupby("target"):
        if tgt == "Census":
            order = {"not_recovered": 9, "external": 8}
            g = (
                g.assign(r=g.recovered_by.map(order).fillna(0))
                .sort_values("r")
                .drop_duplicates("target_id")
            )
        rows.append(
            g.groupby(["cause", "recovered_by"])
            .size()
            .rename("n")
            .reset_index()
            .assign(target=tgt)
        )
    out = pd.concat(rows)[["target", "cause", "recovered_by", "n"]]
    out.to_csv(OUT / "08_attribution_summary.csv", index=False)
    print(out.to_string(index=False))
    for tgt, g in out.groupby("target"):
        print(tgt, g.groupby("recovered_by").n.sum().to_dict())


if __name__ == "__main__":
    main()
