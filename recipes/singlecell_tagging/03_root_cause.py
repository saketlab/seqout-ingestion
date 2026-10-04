#!/usr/bin/env python3
"""03: Why is each scBaseCount / Census study missing from singlecell_study?

Targets
  scBaseCount  the 690 miss_tagging_gap studies (+1 miss_catalogue_gap) from
               singlecell_coverage_gaps/out/07
  Census       the 21 tagging_gap_accession + 4 tagging_gap_pmid_only collections
               (+2 catalogue_gap_accession, reported but not recoverable by tagging)
               from 09. A collection's seqout studies are its listed canonicals;
               for PMID-only ones, the PMID-linked studies.

Primary cause, first rule that applies (the order goes from "the reads were
looked at" to "nothing in seqout says so"):

  scanner_called_bulk          production status measured_not_single_cell*. Split by
                               whether the negative is a barcode-whitelist miss only
  staged_not_transferred       the pipeline staging (recall_fix.db, 2026-09-26)
                               already calls it single-cell at the canonical, but that
                               staging was never transferred to Postgres
  staged_under_alias_only      staging calls it single-cell only under another
                               archive key (SRP/ERP/PRJ); transfer.py joins on the
                               canonical, so the verdict cannot land
  text_dropped_after_abstain   build.py bug: every pentimento run abstained, so the
                               study was staged 'not_screened' first, and the text
                               pass then only filled modality, never the verdict,
                               although SC_TEXT_REGEX matches its title/description
  libsource_declared_unstaged  archive library_source says SINGLE CELL, not staged
  protocol_text_only           a platform is named only in experiment/GSM protocol text
  study_matrix_only            a counted matrix exists only at pentimento_study or
                               sc_stats_study level
  title_wording_miss           title/description say single-cell in words the
                               current SC_TEXT_REGEX lacks ("single-cell atlas",
                               "single cell mRNA sequencing", "at single-cell resolution")
  no_evidence_in_seqout        none of the above (in the local copy)

Secondary flags: never_read_scanned (no pentimento run, or every run abstained),
atac_or_multiome, ena_prj_only (study is PRJEB/ERP), year >= 2025, no_local_evidence_row (no row in any local evidence table).

Writes out/03_targets.csv (one row per target study), out/03_cause_counts.csv,
out/03_cause_examples.csv (5 per cause, seeded) and prints hand-check packets.
"""

from __future__ import annotations

import re

import pandas as pd

from _db import OUT, SEED, local

# recipes/recall_fix/build.py SC_TEXT_REGEX, verbatim (what production tags on)
CURRENT_RX = re.compile(
    r"scRNA[- ]?seq|sc[- ]RNA[- ]?seq|snRNA[- ]?seq|sn[- ]RNA[- ]?seq|single[- ]cell[- ]+RNA"
    r"|single[- ]cell[- ]+transcriptom|single[- ]nucle\w*[- ]+RNA|single[- ]nucle\w*[- ]+transcriptom"
    r"|10x Genomics|Drop[- ]?seq|dropseq|Smart[- ]?seq[0-9]?|smartseq|inDrop|CEL[- ]?seq|MARS[- ]?seq"
    r"|sci[- ]RNA[- ]?seq|Seq[- ]?Well|SPLiT[- ]?seq|CITE[- ]?seq|CITEseq|droplet[- ]based"
    r"|individual (?:liver |immune |tumou?r )?cells|single cells were",
    re.I,
)
# wording the current regex misses (singlecell_coverage_gaps G5 "sc_generic_strong" + chromatin/spatial)
WORDING_RX = re.compile(
    r"single[- ](?:cell|nucle\w*)[- ]+(?:m?RNA|sequencing|seq\b|resolution|profil\w*|atlas|genomic\w*"
    r"|landscape|level|analys\w*|expression|omics|multi[- ]?omic\w*|ATAC|chromatin|methylom\w*)"
    r"|\bsc[- ]?seq\b|\bscRNA\b|\bsnRNA\b|\b(?:sc|sn)ATAC|multiome|Visium|Slide[- ]?seq|spatial transcriptom",
    re.I,
)

POS = {
    "measured_single_cell",
    "library_source_indicates_single_cell",
    "text_indicates_single_cell",
}
ORDER = [
    "scanner_called_bulk_whitelist_only",
    "scanner_called_bulk_r1len",
    "staged_not_transferred",
    "staged_under_alias_only",
    "text_dropped_after_abstain",
    "libsource_declared_unstaged",
    "protocol_text_only",
    "study_matrix_only",
    "title_wording_miss",
    "no_evidence_in_seqout",
]


def cause(r: pd.Series) -> str:
    staged_neg = str(r.staged_canon).startswith("measured_not") or str(
        r.staged_alias
    ).startswith("measured_not")
    if (str(r.prod_status).startswith("measured_not") or staged_neg) and not (
        r.staged_canon in POS or r.staged_alias in POS
    ):
        return (
            "scanner_called_bulk_r1len"
            if r.pent_n_bulk > 0
            else "scanner_called_bulk_whitelist_only"
        )
    if r.staged_canon in POS:
        return "staged_not_transferred"
    if r.staged_alias in POS:
        return "staged_under_alias_only"
    if r.staged_canon == "not_screened" and r.cur_hit:
        return "text_dropped_after_abstain"
    if r.lib_n_sc > 0:
        return "libsource_declared_unstaged"
    if r.prot_hits > 0:
        return "protocol_text_only"
    if r.ps_cells > 0 or r.sss_n_matrices > 0:
        return "study_matrix_only"
    if r.wording_miss:
        return "title_wording_miss"
    return "no_evidence_in_seqout"


def targets(con) -> pd.DataFrame:
    scb = pd.read_sql(
        "SELECT study, canonical, status, single_cell_status, title FROM ext_scbasecount"
        " WHERE status LIKE 'miss%'",
        con,
    )
    scb = scb.assign(target="scBaseCount", target_id=scb.study)
    cen = pd.read_sql(
        "SELECT collection_id, collection_name, verdict, seqout_canonical, pmid_seqout_studies"
        " FROM ext_census WHERE verdict IN ('tagging_gap_accession','tagging_gap_pmid_only',"
        "'catalogue_gap_accession')",
        con,
    )
    rows = []
    for r in cen.itertuples():
        accs = (
            r.seqout_canonical
            if r.verdict == "tagging_gap_accession"
            else r.pmid_seqout_studies
        )
        for a in str(accs).split(";") if isinstance(accs, str) else [None]:
            rows.append(
                {
                    "target": "Census",
                    "target_id": r.collection_id,
                    "status": r.verdict,
                    "canonical": a,
                    "study": a,
                    "title": r.collection_name,
                }
            )
    return pd.concat([scb, pd.DataFrame(rows)], ignore_index=True)


def main() -> None:
    con = local(readonly=True)
    t = targets(con)
    ev = pd.read_sql("SELECT * FROM evidence", con).set_index("canonical")
    t = t.join(ev.drop(columns=["title"]), on="canonical")
    # production status for canonicals with none in prod_um_status is not_screened
    um = pd.read_sql(
        "SELECT canonical_accession canonical, title um_title, description FROM um"
        " WHERE canonical_accession IN (%s)" % ",".join("?" * t.canonical.nunique()),
        con,
        params=list(t.canonical.dropna().unique()),
    ).set_index("canonical")
    t = t.join(um, on="canonical")
    txt = t.um_title.fillna("") + " | " + t.description.fillna("")
    t["cur_hit"] = [bool(CURRENT_RX.search(x)) for x in txt]
    t["wording_miss"] = [
        bool(WORDING_RX.search(x)) and not h for x, h in zip(txt, t.cur_hit)
    ]
    t["prod_status"] = t.prod_status.fillna(t.single_cell_status).fillna("not_screened")
    for col in [
        "lib_n_sc",
        "lib_n_exp",
        "prot_hits",
        "pent_n_runs",
        "pent_n_bulk",
        "pent_n_barcode_sc",
        "pent_n_r1len_sc",
        "ps_cells",
        "sss_n_matrices",
    ]:
        t[col] = t[col].fillna(0)
    t["cause"] = t.apply(cause, axis=1)
    t.loc[
        t.status.eq("miss_catalogue_gap") | t.status.eq("catalogue_gap_accession"),
        "cause",
    ] = "catalogue_gap"
    screened = (t.pent_n_barcode_sc + t.pent_n_r1len_sc + t.pent_n_bulk) > 0
    t["never_read_scanned"] = ~screened & ~t.prod_status.str.startswith("measured")
    t["atac_or_multiome"] = t.dominant_assay_l2.fillna("").str.contains(
        "ATAC|Multiome"
    ) | t.title.fillna("").str.contains(r"ATAC|[Mm]ultiome")
    t["ena_prj_only"] = t.study.fillna("").str.match(r"^(PRJEB|ERP)")
    t["year"] = pd.to_numeric(t.first_published.str[:4], errors="coerce")
    t["recent_2025plus"] = t.year >= 2025
    t["no_local_evidence_row"] = ~t.in_local_um.fillna(False).astype(bool)
    t.to_csv(OUT / "03_targets.csv", index=False)

    flags = [
        "never_read_scanned",
        "atac_or_multiome",
        "ena_prj_only",
        "recent_2025plus",
        "no_local_evidence_row",
    ]
    out = []
    for tgt, g in t.groupby("target"):
        # Census: one cause per collection = the most tractable cause among its studies
        if tgt == "Census":
            g = g.assign(
                rank=g.cause.map(
                    {c: i for i, c in enumerate(ORDER + ["catalogue_gap"])}
                )
            )
            g = g.sort_values("rank").drop_duplicates("target_id")
        for c, gg in g.groupby("cause"):
            rec = {
                "target": tgt,
                "cause": c,
                "n": len(gg),
                "examples": ";".join(gg.sort_values("study").study.fillna("").head(5)),
            }
            rec.update({f: int(gg[f].sum()) for f in flags})
            out.append(rec)
    counts = pd.DataFrame(out).sort_values(["target", "n"], ascending=[True, False])
    counts.to_csv(OUT / "03_cause_counts.csv", index=False)
    print(counts.to_string(index=False))

    ex = (
        t[t.target == "scBaseCount"]
        .groupby("cause", group_keys=False)
        .apply(lambda g: g.sample(min(5, len(g)), random_state=SEED))
    )
    cols = [
        "target",
        "cause",
        "study",
        "canonical",
        "title",
        "prod_status",
        "staged_canon",
        "staged_alias",
        "lib_n_sc",
        "lib_n_exp",
        "prot_hits",
        "prot_snip",
        "pent_n_runs",
        "pent_n_barcode_sc",
        "pent_n_r1len_sc",
        "pent_n_bulk",
        "ps_cells",
        "sss_n_matrices",
        "dominant_assay_l2",
        "year",
    ]
    pd.concat([ex, t[t.target == "Census"]])[cols].to_csv(
        OUT / "03_cause_examples.csv", index=False
    )


if __name__ == "__main__":
    main()
