#!/usr/bin/env python3
"""02: Per-canonical single-cell evidence over the local copy.

Resolves every raw archive key (SRP/ERP/PRJEB/GSE/E-MTAB/CRA ...) in the
evidence tables to a unified_metadata canonical, as recall_rules.resolve_canonical
does: a canonical is itself, else the alias map (unified_metadata.aliases, min
canonical), else the archive twins from 01b (SRP<->GSE, SRP/ERP<->PRJ). It then applies the rule
functions in src/sc_enrich/recall_rules.py, so the local replay and the pipeline
share one implementation.

Writes into the local copy:
  key2canon   raw key -> canonical, with the route that resolved it
  evidence    one row per canonical with any evidence:
              staged status (pipeline staging, at the canonical and under alias keys),
              declared library_source counts, protocol-text hits + snippet,
              pentimento run counts, study-level matrix, production flag

Usage: 02_evidence.py [--apply]     (dry run prints counts only)
"""

from __future__ import annotations

import argparse
import ast

import pandas as pd

from _db import local
from sc_enrich import recall_rules as rr

KEY_SQL = """
WITH keys AS (
    SELECT raw_study AS raw FROM ev_libsource UNION SELECT raw_study FROM ev_exp_text
    UNION SELECT raw_study FROM ev_gsm_text UNION SELECT raw_study FROM ev_pent
    UNION SELECT raw_study FROM ev_matrix UNION SELECT study_accession FROM staged_pipeline
    UNION SELECT raw_study FROM prod_um_status UNION SELECT study FROM ext_scbasecount
)
SELECT k.raw,
       COALESCE((SELECT u.canonical_accession FROM um u WHERE u.canonical_accession = k.raw),
                a.canonical_accession,
                (SELECT MIN(a2.canonical_accession) FROM twins t JOIN alias_map a2 ON a2.alias = t.b
                  WHERE t.a = k.raw)) AS canonical,
       CASE WHEN EXISTS (SELECT 1 FROM um u WHERE u.canonical_accession = k.raw) THEN 'canonical'
            WHEN a.canonical_accession IS NOT NULL THEN 'alias_map'
            WHEN EXISTS (SELECT 1 FROM twins t JOIN alias_map a2 ON a2.alias = t.b WHERE t.a = k.raw)
            THEN 'twin' END AS route
FROM keys k LEFT JOIN alias_map a ON a.alias = k.raw
"""


def _texts(con, table: str) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT raw_study, txt FROM {table}", con)
    df["hit"] = df.txt.map(rr.protocol_says_single_cell)
    return df


def build(apply: bool) -> None:
    con = local(readonly=not apply)
    k2c = pd.read_sql(KEY_SQL, con)
    print(
        "raw keys:",
        len(k2c),
        "resolved:",
        k2c.canonical.notna().sum(),
        k2c.route.value_counts().to_dict(),
    )
    c = dict(zip(k2c.raw, k2c.canonical))

    def canon(s: pd.Series) -> pd.Series:
        return s.map(c)

    # staged pipeline verdicts: at the canonical itself, and the best one under
    # any other key that resolves to it
    st = pd.read_sql(
        "SELECT study_accession raw, single_cell_status, n_runs_screened, n_runs_single_cell"
        " FROM staged_pipeline",
        con,
    )
    st["canonical"] = canon(st.raw)
    at = (
        st[st.raw == st.canonical]
        .set_index("canonical")
        .single_cell_status.rename("staged_canon")
    )
    al = st[st.canonical.notna() & (st.raw != st.canonical)]
    al = (
        al.groupby("canonical")
        .single_cell_status.agg(
            lambda s: pd.Series(s).pipe(
                lambda x: x.iloc[0] if len(x) == 1 else _merge_all(x)
            )
        )
        .rename("staged_alias")
    )

    lib = pd.read_sql("SELECT raw_study, n_exp, n_sc_libsource FROM ev_libsource", con)
    lib["canonical"] = canon(lib.raw_study)
    lib = lib.groupby("canonical").agg(
        lib_n_sc=("n_sc_libsource", "max"), lib_n_exp=("n_exp", "max")
    )

    prot = pd.concat([_texts(con, "ev_exp_text"), _texts(con, "ev_gsm_text")])
    prot["canonical"] = canon(prot.raw_study)
    hits = prot[prot.hit]
    prot_n = hits.groupby("canonical").size().rename("prot_hits")
    prot_snip = (
        hits.groupby("canonical")
        .txt.first()
        .map(rr.platform_snippet)
        .rename("prot_snip")
    )

    pent = pd.read_sql("SELECT * FROM ev_pent", con)
    pent["canonical"] = canon(pent.raw_study)
    pent = (
        pent.groupby("canonical")[
            [
                "n_runs",
                "n_barcode_sc",
                "n_r1len_sc",
                "n_r1len_sc_uncertain",
                "n_bulk",
                "n_atac",
            ]
        ]
        .sum()
        .add_prefix("pent_")
    )

    mat = pd.read_sql("SELECT src, raw_study, cells FROM ev_matrix", con)
    mat["canonical"] = canon(mat.raw_study)
    ps = (
        mat[mat.src == "pentimento_study"]
        .groupby("canonical")
        .cells.max()
        .rename("ps_cells")
    )
    sss = (
        mat[mat.src == "sc_stats_study"]
        .groupby("canonical")
        .cells.max()
        .rename("sss_n_matrices")
    )

    prod = pd.read_sql(
        "SELECT raw_study, is_single_cell, single_cell_status FROM prod_um_status", con
    )
    prod["canonical"] = canon(prod.raw_study)
    prod = prod[prod.raw_study == prod.canonical].set_index("canonical")[
        ["is_single_cell", "single_cell_status"]
    ]
    prod.columns = ["prod_is_sc", "prod_status"]

    um = pd.read_sql(
        "SELECT canonical_accession canonical, source, title, dominant_scientific_name organism,"
        " dominant_assay_l2, library_strategy_counts, first_published FROM um",
        con,
    ).set_index("canonical")
    ev = pd.concat([at, al, lib, prot_n, prot_snip, pent, ps, sss, prod], axis=1)
    ev = ev.join(um, how="left")
    ev.index.name = "canonical"
    ev["in_local_um"] = ev.source.notna()
    ev["strategies"] = ev.library_strategy_counts.map(_strategies)
    ev["lib_veto"] = [
        rr.libsource_vetoed(o, t, s) if n > 0 else None
        for o, t, s, n in zip(
            ev.organism, ev.title, ev.strategies, ev.lib_n_sc.fillna(0)
        )
    ]
    ev = ev.drop(columns=["library_strategy_counts"])
    ev["strategies"] = ev.strategies.map(lambda s: ";".join(sorted(s)))
    print("evidence rows:", len(ev))
    print(
        "  protocol-hit canonicals:",
        int(ev.prot_hits.notna().sum()),
        " libsource canonicals:",
        int(ev.lib_n_sc.notna().sum()),
        " vetoed:",
        ev.lib_veto.value_counts().to_dict(),
    )
    if not apply:
        print("DRY RUN: would write key2canon, evidence")
        return
    k2c.to_sql("key2canon", con, if_exists="replace", index=False)
    ev.reset_index().to_sql(
        "evidence", con, if_exists="replace", index=False, chunksize=50_000
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS evidence_pk ON evidence(canonical)")
    con.commit()


def _merge_all(statuses: pd.Series) -> str:
    out = "not_screened"
    for s in statuses:
        out = rr.merge_status(out, s)
    return out


def _strategies(js: str | None) -> set[str]:
    if not js:
        return set()
    try:
        d = ast.literal_eval(js.replace("null", "None"))
    except (ValueError, SyntaxError):
        return set()
    return set(d) if isinstance(d, dict) else set()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="write key2canon/evidence into the LOCAL copy",
    )
    build(ap.parse_args().apply)
