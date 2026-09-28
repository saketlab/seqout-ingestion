#!/usr/bin/env python3
"""06: Stratified precision sample of NEW tags that are not in scBaseCount/Census.

Population: S2 new tags (05_new_tags.csv, scenario S2, in_external_sets False),
the studies the patched build would add to singlecell_study that no external
resource vouches for. Strata = the pass that first made the study single-cell
(04 new_added_by). N_PER draws per stratum (all of it when smaller), seeded.

Each study gets a blinded evidence packet (the pass that added it and seqout's
status are hidden): title, description (Postgres text where extracted, else the
local unify output), unread study fields from the coverage-gap extract (GEO
overall_design, SRA abstract, ENA description, AE protocols), the platform
snippet from protocol text, declared library_source counts, library strategies,
organism and pentimento read/matrix counts. Packets are shuffled into batches for
independent judging under ../../../seqout-paper/scripts/singlecell_audit/05_rubric.md.

Two rounds. Round 1 (development, N_PER=25, seed SEED) was drawn before the rule
refinements and used to find false-positive classes, so its precision is
optimistic for the final rules. Round 2 (validation, N_PER=20, seed SEED+1) is
drawn from the final population excluding round-1 studies and gives the headline.

Writes out/06{,b}_sample.csv (stratum, weights, in_S1) and out/06{,b}_batches/batch_NN.md.
Usage: 06_precision_sample.py --round 1|2
"""

from __future__ import annotations

import argparse

import pandas as pd

from _db import GAPS, OUT, SEED, local

ROUNDS = {1: ("06", 25, 6, 0), 2: ("06b", 20, 4, 1)}  # prefix, n per stratum, batches, seed offset
FIELDS = ["geo_overall_design", "sra_abstract", "ena_description", "ae_protocols", "gsa_summary"]


def main(rnd: int) -> None:
    prefix, n_per, n_batches, off = ROUNDS[rnd]
    con = local(readonly=True)
    nt = pd.read_csv(OUT / "05_new_tags.csv")
    s1_new = _s1_new(con)
    pop = nt[(nt.scenario == "S2") & ~nt.in_external_sets].copy()
    pop["in_S1"] = pop.canonical.isin(s1_new)
    sizes = pop.groupby("added_by").size().rename("N_stratum")
    draw = pop
    if rnd == 2:
        draw = pop[~pop.canonical.isin(pd.read_csv(OUT / "06_sample.csv").canonical)]
        draw = draw[draw.added_by != "unknown"]
    samp = pd.concat([g.sample(min(n_per, len(g)), random_state=SEED + off)
                      for _, g in draw.groupby("added_by")])
    samp = samp.join(sizes, on="added_by")
    samp["n_stratum"] = samp.added_by.map(samp.added_by.value_counts())
    samp["weight"] = samp.N_stratum / samp.n_stratum
    samp["round"] = rnd
    print(pop.groupby(["added_by", "in_S1"]).size().unstack(fill_value=0).to_string())
    print(samp.added_by.value_counts().to_string())

    keys = samp.canonical.tolist()
    q = ",".join("?" * len(keys))
    um = pd.read_sql(f"SELECT canonical_accession canonical, source, title, description, dominant_scientific_name"
                     f" organism, dominant_assay_l2, library_strategy_counts, first_published FROM um"
                     f" WHERE canonical_accession IN ({q})", con, params=keys).set_index("canonical")
    pgt = pd.read_sql(f"SELECT raw_study canonical, txt FROM pg_um_text WHERE raw_study IN ({q})",
                      con, params=keys).drop_duplicates("canonical").set_index("canonical").txt
    ev = pd.read_sql(f"SELECT * FROM evidence WHERE canonical IN ({q})", con, params=keys).set_index("canonical")
    k2c = pd.read_sql("SELECT raw, canonical FROM key2canon WHERE canonical IN (%s)" % q, con, params=keys)
    raw_for = k2c.groupby("canonical").raw.apply(list).to_dict()
    extra = _unread_fields({r for c in keys for r in raw_for.get(c, [c])} | set(keys))

    samp = samp.sample(frac=1, random_state=SEED + off).reset_index(drop=True)
    samp["batch"] = samp.index % n_batches + 1
    bdir = OUT / f"{prefix}_batches"
    bdir.mkdir(exist_ok=True)
    for b, g in samp.groupby("batch"):
        lines = [f"# Batch {b:02d} ({len(g)} studies)\n"]
        for c in g.canonical:
            lines.append(_packet(c, um, pgt, ev, extra, raw_for.get(c, [c])))
        (bdir / f"batch_{b:02d}.md").write_text("\n".join(lines))
    samp.to_csv(OUT / f"{prefix}_sample.csv", index=False)


def _s1_new(con) -> set[str]:
    prod = set(pd.read_sql("SELECT study_accession FROM prod_singlecell_study", con).study_accession)
    st = pd.read_sql("SELECT study_accession FROM staged_pipeline WHERE is_single_cell = 1", con)
    return set(st.study_accession) - prod


def _unread_fields(raws: set[str]) -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    for chunk in pd.read_csv(GAPS / "03_study_text.csv", chunksize=200_000):
        hit = chunk[chunk.raw_study.isin(raws) & chunk.fld.isin(FIELDS)]
        for r in hit.itertuples():
            out[(r.raw_study, r.fld)] = str(r.txt)[:900]
    return out


def _get(frame: pd.DataFrame, c: str, col: str):
    return frame[col].get(c) if c in frame.index else None


def _packet(c: str, um, pgt, ev, extra, raws: list[str]) -> str:
    desc = pgt.get(c) or ((_get(um, c, "title") or "") + " | " + (_get(um, c, "description") or ""))
    lines = [f"## {c}", f"- archive ids: {sorted(set(raws) | {c})}",
             f"- source: {_get(um, c, 'source')}  first_published: {_get(um, c, 'first_published')}",
             f"- organism: {_get(um, c, 'organism')}",
             f"- library strategies: {_get(um, c, 'library_strategy_counts')}  dominant assay: "
             f"{_get(um, c, 'dominant_assay_l2')}",
             f"- title | description: {str(desc)[:900]}"]
    for r in sorted(set(raws) | {c}):
        for f in FIELDS:
            if (r, f) in extra:
                lines.append(f"- {f} ({r}): {extra[(r, f)]}")
    if c in ev.index:
        e = ev.loc[c]
        if pd.notna(e.prot_snip):
            lines.append(f"- experiment/sample protocol text: {e.prot_snip}")
        if pd.notna(e.lib_n_sc):
            lines.append(f"- archive library_source SINGLE CELL on {int(e.lib_n_sc)} of {int(e.lib_n_exp)} experiments")
        if pd.notna(e.pent_n_runs):
            lines.append(f"- read scan: {int(e.pent_n_runs)} runs; barcode-positive {int(e.pent_n_barcode_sc)}, "
                         f"R1-length single-cell {int(e.pent_n_r1len_sc)}, R1-length bulk {int(e.pent_n_bulk)}")
        if pd.notna(e.ps_cells):
            lines.append(f"- counted matrix cells (study level): {int(e.ps_cells)}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--round", type=int, choices=(1, 2), required=True)
    main(ap.parse_args().round)
