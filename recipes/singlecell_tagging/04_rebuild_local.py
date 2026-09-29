#!/usr/bin/env python3
"""04: Replay the patched recall_fix single-cell build on the LOCAL copy.

The patched build (recall_fix_build.patch) runs these passes in order, all from
src/sc_enrich/recall_rules.py; this script runs the same functions on local inputs:

  read       the read-derived rows of the pipeline's current staging
             (measured_* and not_screened; text_disagrees is reduced to its
             read verdict, measured_not_single_cell, and re-derived below)
  canon      canonicalise(): re-key SRP/ERP/PRJ-keyed rows onto the canonical
  text       apply_text(): SC_TEXT_REGEX on the Postgres title/description extract
             (pg_um_text) plus the staging's own text verdicts, with the
             abstention bug fixed; a restored row needs archive corroboration
             (protocol hit or unvetoed declared library_source) and must pass
             text_promotion_vetoed(). Added-by labels: text_restored vs text
             (a study with no staged row, old rule)
  libsource  declared library_source (vetoes: microbial, genome project,
             chromatin-only), additive as in the existing build
  whitelist  apply_whitelist_override(): declared majority beats whitelist-only
             negatives
  (matrix)   a study-level pentimento_study pass was evaluated and dropped
             (development-sample precision 0.58)
  protocol   apply_additive(PROTOCOL_STATUS): platform named in protocol text
  external   optional (--external): scBaseCount/Census membership, labelled

Local-input limits (see README): protocol text is the coverage-gap extract (top-3
experiment texts / top-2 GSM texts per study, studies outside singlecell_study
only), and local unified_metadata lacks ~250k PRJ* canonicals that Postgres has.

Writes (with --apply) into the LOCAL copy only:
  study_single_cell_new       the new staged table (same columns as recall_fix.db)
  study_single_cell_new_ext   the same plus the external pass
  new_added_by                canonical -> first pass that made it single-cell

Usage: 04_rebuild_local.py [--apply]
"""

from __future__ import annotations

import argparse
import re

import pandas as pd

from _db import local
from sc_enrich import recall_rules as rr

# recipes/recall_fix/build.py SC_TEXT_REGEX (unchanged by this branch)
SC_TEXT_REGEX = re.compile(
    r"scRNA[- ]?seq|sc[- ]RNA[- ]?seq|snRNA[- ]?seq|sn[- ]RNA[- ]?seq|single[- ]cell[- ]+RNA"
    r"|single[- ]cell[- ]+transcriptom|single[- ]nucle\w*[- ]+RNA|single[- ]nucle\w*[- ]+transcriptom"
    r"|10x Genomics|Drop[- ]?seq|dropseq|Smart[- ]?seq[0-9]?|smartseq|inDrop|CEL[- ]?seq|MARS[- ]?seq"
    r"|sci[- ]RNA[- ]?seq|Seq[- ]?Well|SPLiT[- ]?seq|CITE[- ]?seq|CITEseq|droplet[- ]based"
    r"|individual (?:liver |immune |tumou?r )?cells|single cells were",
    re.IGNORECASE,
)
READ_STATUSES = {"measured_single_cell", "measured_not_single_cell",
                 "measured_not_single_cell_text_disagrees", "not_screened"}
COLS = ["study_accession", "is_single_cell", "single_cell_status", "single_cell_modality",
        "single_cell_modality_evidence", "max_cells", "max_genes", "n_samples_counted",
        "n_runs_screened", "n_runs_single_cell"]


def resolver(con):
    amap = dict(con.execute("SELECT alias, canonical_accession FROM alias_map").fetchall())
    canon = {a for (a,) in con.execute("SELECT canonical_accession FROM um")}
    twins: dict[str, str] = {}
    for a, b in con.execute("SELECT a, b FROM twins"):
        if b in amap:
            twins.setdefault(a, b)
    return (lambda acc: rr.resolve_canonical(acc, amap, twins, canon)), amap


def main(apply: bool) -> None:
    con = local(readonly=not apply)
    resolve, _ = resolver(con)
    st = pd.read_sql("SELECT * FROM staged_pipeline", con)
    st.columns = COLS
    read = st[st.single_cell_status.isin(READ_STATUSES)].copy()
    read.loc[read.single_cell_status == "measured_not_single_cell_text_disagrees",
             ["single_cell_status"]] = "measured_not_single_cell"
    staged = {r[0]: tuple(None if pd.isna(v) else v for v in r) for r in read.itertuples(index=False)}
    log = [("read", len(staged))]
    first: dict[str, str] = {}

    def mark(before: rr.Staged, after: rr.Staged, name: str) -> None:
        for k, row in after.items():
            if row[1] == 1 and (k not in before or before[k][1] != 1):
                first.setdefault(k, name)

    first.update({k: "read_measured" for k, r in staged.items() if r[1] == 1})
    staged, moved = rr.canonicalise(staged, resolve)
    log.append(("canon_moved", moved))
    ev = pd.read_sql("SELECT canonical, lib_n_sc, lib_n_exp, lib_veto, prot_hits, ps_cells,"
                     " pent_n_bulk FROM evidence", con).set_index("canonical")
    old_lib = set(st.loc[st.single_cell_status == rr.LIBSOURCE_STATUS, "study_accession"].map(
        lambda a: resolve(a) or a))
    declared = set(ev.index[ev.lib_n_sc.fillna(0) > 0]) | old_lib
    veto = ev.lib_veto.dropna().to_dict()
    protocol_keys = set(ev.index[ev.prot_hits.fillna(0) > 0])
    # archive evidence independent of the text: restored text verdicts need it
    corroborated = {a for a in declared if a not in veto} | protocol_keys
    before = dict(staged)
    # Text hits from Postgres text (pg_um_text: studies outside singlecell_study),
    # plus the pipeline's own text verdicts for everything else, so tagged studies
    # keep theirs. Local unify output is newer than Postgres and is not used here.
    pgt = pd.read_sql("SELECT raw_study, txt FROM pg_um_text", con)
    hits = {a: None for a, t in zip(pgt.raw_study, pgt.txt.fillna("")) if SC_TEXT_REGEX.search(t)}
    text_statuses = ["text_indicates_single_cell", "measured_not_single_cell_text_disagrees"]
    for a in st.loc[st.single_cell_status.isin(text_statuses), "study_accession"]:
        hits.setdefault(resolve(a) or a, None)
    # production's own text verdicts: TEXT_SQL on Postgres hits these too, and
    # neither the staging (older) nor pg_um_text (untagged only) carries all of them
    pg_text = pd.read_sql("SELECT raw_study FROM prod_um_status WHERE single_cell_status IN (?, ?)",
                          con, params=text_statuses)
    for a in pg_text.raw_study:
        hits.setdefault(a, None)
    # veto for restored (abstained) rows, from the same Postgres text
    um_txt = pd.read_sql("SELECT canonical_accession, title, description FROM um", con)
    tt = {a: (t, d) for a, t, d in um_txt.itertuples(index=False)}
    for a, t in zip(pgt.raw_study, pgt.txt.fillna("")):
        title, _, desc = t.partition(" | ")
        tt[a] = (title, desc)
    vetoed = {a for a in hits if a in tt and rr.text_promotion_vetoed(*tt[a])}
    restorable = {k for k, r in staged.items() if r[2] == "not_screened"}
    staged, n = rr.apply_text(staged, hits, vetoed=vetoed, corroborated=corroborated)
    mark({k: r for k, r in before.items() if k not in restorable}, {k: staged[k] for k in restorable}, "text_restored")
    mark(before, staged, "text")
    log.append(("text_added", n))

    before = dict(staged)
    lib_ok = sorted(a for a in declared if a not in veto)
    staged, n = rr.apply_additive(staged, lib_ok, rr.LIBSOURCE_STATUS)
    mark(before, staged, "libsource")
    log += [("libsource_declared", len(declared)), ("libsource_vetoed", len(declared & set(veto))),
            ("libsource_added", n)]

    before = dict(staged)
    counts = {a: (int(r.lib_n_sc), int(r.lib_n_exp)) for a, r in ev[ev.lib_n_sc > 0].iterrows()
              if a not in veto}
    bulk_r1 = ev.pent_n_bulk.fillna(0)
    reads = {a: (int(row[9] or 0), 1, int(bulk_r1.get(a, 0)))
             for a, row in staged.items() if row[1] == 0}
    staged, n = rr.apply_whitelist_override(staged, counts, reads)
    mark(before, staged, "whitelist_override")
    log.append(("whitelist_flipped", n))

    before = dict(staged)
    staged, n = rr.apply_additive(staged, sorted(protocol_keys), rr.PROTOCOL_STATUS)
    mark(before, staged, "protocol")
    log.append(("protocol_added", n))

    scb = pd.read_sql("SELECT study, canonical FROM ext_scbasecount", con)
    cen = pd.read_sql("SELECT public_accessions FROM ext_census WHERE public_accessions IS NOT NULL", con)
    ext = set(scb.study) | {a for s in cen.public_accessions for a in s.split(";")}
    ext_keys = {resolve(a) or a for a in ext}
    ext_staged, n_ext = rr.apply_additive(staged, ext_keys, rr.EXTERNAL_STATUS)
    ext_first = dict(first)
    mark(staged, ext_staged, "external")
    log.append(("external_added", n_ext))

    for k, v in log:
        print(f"  {k:22s} {v:>8,}")
    new = pd.DataFrame(list(staged.values()), columns=COLS)
    print(new.single_cell_status.value_counts().to_string())
    if not apply:
        print("DRY RUN: nothing written")
        return
    new.to_sql("study_single_cell_new", con, if_exists="replace", index=False)
    pd.DataFrame(list(ext_staged.values()), columns=COLS).to_sql(
        "study_single_cell_new_ext", con, if_exists="replace", index=False)
    pd.DataFrame([(k, v, k in ext_first) for k, v in first.items()],
                 columns=["canonical", "added_by", "without_external"]).to_sql(
        "new_added_by", con, if_exists="replace", index=False)
    pd.DataFrame(log, columns=["step", "n"]).to_sql("rebuild_log", con, if_exists="replace", index=False)
    con.commit()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write the new staged tables into the LOCAL copy")
    main(ap.parse_args().apply)
