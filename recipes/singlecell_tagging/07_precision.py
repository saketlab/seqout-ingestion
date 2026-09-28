#!/usr/bin/env python3
"""07: Precision of the new tags (not in scBaseCount/Census), with 95% Wilson CIs.

Population: the FINAL rules' S2 new tags not in scBaseCount/Census
(05_new_tags.csv). Each round's sampled studies are matched to it; round-1
studies the refined rules no longer tag drop out (they are the removed FPs and
TPs), and the stratum (pass) is the one in the final build.

  --round 2   validation sample (drawn after refinement): the headline
  --round 1   development sample (the rules were tuned on it): optimistic

TP = sc, spatial. FP = bulk_*, microarray, crispr_bulk, spatial_roi. `unclear`
is excluded (a second row counts it as FP). Estimates are weighted to the
population (stratum N / stratum n), and the CI is Wilson on the Kish effective n,
as in seqout-paper singlecell_audit/08_precision.py. Subsets: `code_fix` (tags the
current staging would not add) and `lag` (the untransferred staging already has them).

Reviewer overrides: out/06{,b}_overrides.csv (accession,label,note), applied last.
Writes out/07_precision_round{1,2}.csv and out/07_labels_round{1,2}.csv.
"""

from __future__ import annotations

import argparse
import math

import pandas as pd

from _db import OUT

TP = {"sc", "spatial"}
FP = {"bulk_companion", "bulk_sorted", "bulk_other", "microarray", "crispr_bulk", "spatial_roi"}
Z = 1.959964
PREFIX = {1: "06", 2: "06b"}


def wilson(p: float, n: float) -> tuple[float, float]:
    d = 1 + Z * Z / n
    c = (p + Z * Z / (2 * n)) / d
    h = Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def estimate(d: pd.DataFrame, unclear_is_fp: bool = False) -> dict:
    j = d[d.label.isin(TP | FP | ({"unclear"} if unclear_is_fp else set()))]
    if j.empty:
        return {"n_judged": 0}
    w, y = j.w, j.label.isin(TP).astype(float)
    p = float((w * y).sum() / w.sum())
    n_eff = float(w.sum() ** 2 / (w**2).sum())
    lo, hi = wilson(p, n_eff)
    return {"n_judged": len(j), "tp": int(y.sum()), "fp": int(len(j) - y.sum()),
            "unclear": int((d.label == "unclear").sum()), "precision": round(p, 3),
            "ci_low": round(lo, 3), "ci_high": round(hi, 3), "n_eff": round(n_eff, 1)}


def weighted(d: pd.DataFrame, pop: pd.DataFrame) -> pd.DataFrame:
    """Weight = population N of the unit's stratum (within the subset) / sampled n."""
    n = d.groupby("added_by").size()
    big_n = pop.groupby("added_by").size()
    return d.assign(w=d.added_by.map(big_n / n))


def main(rnd: int) -> None:
    pre = PREFIX[rnd]
    s = pd.read_csv(OUT / f"{pre}_sample.csv")[["canonical"]]
    lab = pd.concat([pd.read_csv(p) for p in sorted((OUT / f"{pre}_labels").glob("labels_*.csv"))])
    lab = lab.drop_duplicates("accession", keep="last").set_index("accession")
    ov = OUT / f"{pre}_overrides.csv"
    if ov.exists():
        o = pd.read_csv(ov)
        lab.loc[o.accession, "label"] = o.label.to_numpy()
    s = s.join(lab, on="canonical")
    assert s.label.notna().all(), "unlabelled sampled studies"

    nt = pd.read_csv(OUT / "05_new_tags.csv")
    pop = nt[(nt.scenario == "S2") & ~nt.in_external_sets & (nt.added_by != "unknown")]
    pop = pop.assign(in_S1=pop.canonical.isin(_s1_new()))
    d = s.merge(pop[["canonical", "added_by", "in_S1"]], on="canonical", how="inner")
    dropped = len(s) - len(d)

    rows = []
    for scope, sub, psub in [
        ("ALL new tags (not in scBaseCount/Census)", d, pop),
        ("code_fix (not in current staging)", d[~d.in_S1], pop[~pop.in_S1]),
        ("lag (current staging, untransferred)", d[d.in_S1], pop[pop.in_S1]),
    ]:
        sub = weighted(sub, psub)
        rows.append({"scope": scope, "N": len(psub), **estimate(sub)})
        rows.append({"scope": scope + ", unclear as FP", "N": len(psub), **estimate(sub, True)})
    for h, g in d.groupby("added_by"):
        rows.append({"scope": f"stratum {h}", "N": int((pop.added_by == h).sum()),
                     **estimate(g.assign(w=1.0))})
    out = pd.DataFrame(rows)
    out.to_csv(OUT / f"07_precision_round{rnd}.csv", index=False)
    print(f"round {rnd}: {len(s)} sampled, {dropped} no longer tagged by the final rules")
    print(out.to_string(index=False))
    print(d.groupby(["added_by", "label"]).size().unstack(fill_value=0).to_string())
    d.to_csv(OUT / f"07_labels_round{rnd}.csv", index=False)


def _s1_new() -> set[str]:
    from _db import local  # noqa: PLC0415

    con = local(readonly=True)
    prod = set(pd.read_sql("SELECT study_accession FROM prod_singlecell_study", con).study_accession)
    st = pd.read_sql("SELECT study_accession FROM staged_pipeline WHERE is_single_cell = 1", con)
    return set(st.study_accession) - prod


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--round", type=int, choices=(1, 2), required=True)
    main(ap.parse_args().round)
