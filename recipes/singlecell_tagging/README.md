# singlecell_tagging: false negatives against scBaseCount and CELLxGENE Census

This recipe explains why `singlecell_study` misses single-cell studies that Arc scBaseCount and CZI Census include, and fixes the causes in the recall_fix single-cell build.

Branch `fix/singlecell-tagging-gaps`, worktree `/NAS/saket/github/seqout-ingestion-sctag`. Everything here was run against a LOCAL SQLite copy, `/NAS/saket/github/seqout-ingestion-sctag-local/sctag_local.db`.

Postgres was not queried after 2026-09-29 early morning, because it went into crash recovery. Nothing was written to Postgres, the pipeline staging (`/data/saket/sqlites/recall_fix.db`, opened `mode=ro`) or `unified_metadata.db` (opened `mode=ro`).

Python: the lamindb_test venv. Seed `_db.SEED = 20260929`; the validation sample uses `SEED + 1`. Tests use the ingestion repo venv.

## Run order

| step | script | writes | notes |
|---|---|---|---|
| 01 | `01_build_local_copy.py [--apply]` | local copy | read-only ATTACH of `unified_metadata.db` and `recall_fix.db`; loads the read-only PG extracts from `seqout-paper/scripts/singlecell_coverage_gaps/out` (02/03/07/09) and `singlecell_audit/out/01_catalogue.parquet` (live matview, 42,033 rows) |
| 01b | `01b_twins_from_sra_accessions.py [--apply]` | table `twins` | SRP<->GSE and SRP/ERP<->PRJ from NCBI `SRA_Accessions.tab`; stands in for `sra_studies.alias` / `ena_studies`, since PG was unavailable |
| 02 | `02_evidence.py [--apply]` | `key2canon`, `evidence` | per-canonical evidence through `src/sc_enrich/recall_rules.py` |
| 03 | `03_root_cause.py` | `out/03_*` | cause per missed study (table below), with seeded examples |
| 04 | `04_rebuild_local.py [--apply]` | `study_single_cell_new`, `..._ext`, `new_added_by` | replays the patched build (the same `recall_rules` functions) |
| 05 | `05_membership.py [--apply]` | `out/05_*`, `new_tags` | emulates `transfer.py` and the matview; scenarios S0 to S3 |
| 06 | `06_precision_sample.py --round 1\|2` | `out/06{,b}_*` | stratified, blinded packets; round 1 is development, round 2 is validation |
| 06j | hand judging under `seqout-paper/scripts/singlecell_audit/05_rubric.md` | `out/06{,b}_labels/` | LLM judges blinded to the pass; reviewer overrides in `out/06b_overrides.csv` |
| 07 | `07_precision.py --round 1\|2` | `out/07_precision_round{1,2}.csv` | weighted precision, Wilson CI on the Kish effective n |
| 08 | `08_attribution.py` | `out/08_*` | which pass recovered each target, and what is left |

Code: `src/sc_enrich/recall_rules.py` (pure rules), `src/sc_enrich/recall_feeds.py` (read-only, keyset-paged PG feeds), `tests/test_singlecell_recall_rules.py` (27 tests).

`recipes/recall_fix/build.py` is untracked in the main checkout (the user's uncommitted work), so the change to it ships as `recall_fix_build.patch` (223 lines). The patch is made against sha256 `044b73c8…` of that file, and `patch -p1 --dry-run` applies cleanly. `tests/test_recall_fix.py` plus the new tests pass (34) with the patch applied.

## Causes (`out/03_cause_counts.csv`)

The first matching cause wins. Misses: 690 scBaseCount tagging gaps plus 1 catalogue gap, and 25 Census collections (21 accession + 4 PMID-only) plus 2 catalogue gaps. For Census, each collection takes the most tractable cause among its studies.

| cause | scBaseCount | Census | examples | hand check (5 seeded per cause) |
|---|---|---|---|---|
| **staged_not_transferred**: the pipeline staging (recall_fix.db, 2026-09-26) already calls it single-cell, mostly through the `LIBRARY_SOURCE_SQL` pass. Production carries **no** `library_source_indicates_single_cell` row, because that staging was never transferred | **459** | 7 | PRJEB39602, SRP332548, GSE236057, E-MTAB-10026, ERP123138 | 5/5 real (Chromium kits, CellRanger, HTO) |
| **protocol_text_only**: the platform is named only in per-experiment `design_description` / GEO `Extract-Protocol`, which the study-level regex never reads | **175** | **10** | GSE205603, GSE278281, GSE178341 (Pelka), GSE156793 (Cao sci-RNA-seq3), GSE190094 (Slide-seqV2) | 5/5 real |
| **scanner_called_bulk_whitelist_only**: every negative read call is a barcode-whitelist miss (`assay_is_single_cell = FALSE`), with no R1-length bulk call | 18 | 1 | SRP345341, SRP291341, GSE234167 (10x Multiome), SRP261714 (Travaglini lung) | 5/5 real; VDJ, Multiome and 150-bp-R1 uploads miss the RNA whitelist |
| **text_dropped_after_abstain**: a **build.py bug**. Every pentimento run abstained, so the read pass staged the study `not_screened`, and the text pass then only filled modality and never the verdict, even though `SC_TEXT_REGEX` matches. It hits 5,694 of the 6,276 staged `not_screened` rows catalogue-wide | 12 | 0 | SRP333476, SRP237382 | 5/5 real |
| **scanner_called_bulk_r1len**: R1-length `bulk` calls on VDJ/BCR, R2C2 nanopore and mixed bulk+sc studies | 8 | 0 | SRP336897, SRP495328, SRP240463 | real sc components, but the reads were genuinely read as bulk; left alone |
| study_matrix_only (pentimento_study or sc_stats_study only) | 8 | 0 | GSE160536, GSE137444 | 1/5 strong (14,901 cells); the rest are sc_stats file counts |
| title_wording_miss ("single-cell atlas", "single cell mRNA sequencing") | 2 | 4 | GSE197290, GSE286254 | real, but the wider wording measured 0.71, so not adopted |
| no_evidence_in_seqout (in the local copy) | 8 | 3 | GSE243964, GSE181902; Census PMID-only companions (GSE146525 snmC, GSE132929) | reads only (scBaseCount processed them) |
| catalogue_gap | 1 | 2 | SRP430632; E-HCAD-10, HRA000328 | not a tagging issue |

Flags from `out/03_cause_counts.csv`:
- 668 of 691 scBaseCount misses were never read-scanned: no pentimento run, or every run abstained.
- 35 are ATAC/Multiome.
- 21 are PRJEB/ERP.
- 70 were published in 2025 or later.

Two causes I tested for were not real. Alias-keyed staging rows lost at transfer came to 1 row, once canonicals resolve to themselves; the SRP keys are duplicate canonicals, as the audit found. Year lag: misses are spread over 2019 to 2025.

## Code changes (`recall_rules.py`, `recall_feeds.py`, `recall_fix_build.patch`)

1. **Text-pass abstention bug** (`recall_rules.apply_text`, used by `build._build_single_cell`). A `not_screened` row now gets the text verdict. On these restored rows only, `text_promotion_vetoed()` drops the audit's text false-positive classes:
   - bracketed non-single-cell SubSeries tags (`[ChIP-seq]`, `[PRO-seq]`, `[bulk RNA-seq]`)
   - 10x linked-read / Supernova genomes
   - "bulk RNA-seq … integrated with single-cell" citations
2. **Protocol-text evidence** (`protocol_says_single_cell`, `recall_feeds.protocol_hits_by_study`), with new status `protocol_indicates_single_cell`. It fills only unscreened studies.
   - Named kits and platforms count: Chromium Single Cell / Next GEM / GEM-X, "10x 3' v3", CellRanger, BD Rhapsody, Parse/Evercode, Drop-seq, inDrop, sci-RNA, scATAC, Visium/Slide-seq/Stereo-seq.
   - Bare "10x Genomics" and plate chemistries (MARS-seq, CEL-Seq2, Smart-seq3, …) count only with single-cell wording in the same record.
   - Vetoed: linked-read / GemCode / Chromium Genome / genome assembly / GeoMx / "bulk RNA".
   - The PG feed pages `sra_experiments`, `ena_experiments` and `geo_samples` by primary key (200k rows per statement), not as a whole-table scan.
3. **library_source vetoes** (`libsource_vetoed`): microbial organism, genome-project titles (JGI/SCGC, "Genome sequencing (and assembly)"), and chromatin-only strategies. The existing additive `LIBRARY_SOURCE_SQL` pass is otherwise unchanged.
4. **Whitelist-only negatives** (`apply_whitelist_override`). A study whose only negatives are whitelist misses, and that declares SINGLE CELL on at least 50% of its libraries, becomes `library_source_indicates_single_cell`. `SINGLE_CELL_SQL` gains `n_runs_bulk_r1len` to tell the two kinds of negative apart.
5. **Canonical re-keying** (`canonicalise`, `resolve_canonical`): pentimento keys are re-keyed onto the unified canonical, and a canonical resolves to itself. It is defensive; it moved 1 row here.
6. **Optional, labelled external evidence** (`--external-accessions FILE`), with status `external_resource_indicates_single_cell`. It fills only unscreened studies and is off by default.

Evaluated and **dropped**: a study-level `pentimento_study` cell-count pass. Development-sample precision was 0.58, with ChIP-seq SubSeries, bulk RNA and a microarray carrying spurious cell counts. Also not adopted: wider title wording (0.71), unread study fields (0.60), and ontology assay terms (0.40). Those figures are from `singlecell_coverage_gaps`.

`sql/matviews/singlecell_pg.sql` is **not** changed: membership follows `unified_metadata.is_single_cell`.

## Before / after (local emulation, `out/05_*`, `out/08_*`)

Scenarios:
- **S0**: production today.
- **S1**: transfer the current staging as-is, with no code change.
- **S2**: this branch.
- **S3**: S2 plus the external pass.

| metric | S0 | S1 | **S2** | S3 |
|---|---|---|---|---|
| scBaseCount studies covered (of 6,931) | 6,240 (90.03%) | 6,699 (96.65%) | **6,900 (99.55%)** | 6,914 (99.75%) |
| recovered of the 690 tagging gaps | – | 459 | **660** | 674 |
| Census public-deposit collections covered (of 209) | 182 (87.1%) | 189 (90.4%) | **200 (95.7%)** | 203 (97.1%) |
| recovered of the 25 Census gaps | – | 7 | **18** | 21 |
| singlecell_study members | 42,033 | 52,176 | **59,408** | 60,418 |
| new tags vs S0 (catalogue-wide) | – | 10,217 | **17,449** | 18,459 |
| lost vs S0 | – | 74 | 74 | 74 |

**Attribution of the 660 scBaseCount recoveries (S2)**
- 458 need only the untransferred staging (lag). The 459th is lost to the new genome-project veto and comes back only through the external pass.
- 202 need this branch's code: protocol 175, text 17 (12 from the abstention-bug fix, 5 where the fix now lets the text verdict land), whitelist override 10.
- The external pass adds 14 more (S3). External membership is therefore **2%** of the scBaseCount recovery (14/674) and **3 of 21** Census recoveries.

**Census (18 in S2)**
- 10 from protocol text, 6 from the transferred library_source pass, 1 from text, 1 from the whitelist override.

**Catalogue-wide S2 new tags: 17,449**
- 9,623 are lag (untransferred staging) and 7,826 are code fixes.
- 3,140 of the new tags are in the scBaseCount/Census sets; 14,309 are not.

**The 74 studies lost exist in S1 too.** Production says `measured_single_cell`, but the 2026-09-26 staging now calls them bulk (mostly plate-based / full-length protocols: SRP107036, SRP149632). Transferring the current staging would drop them regardless of this branch. That is the G1 "single bulk-run veto" class from `singlecell_coverage_gaps`, not fixed here.

### Residual misses (`out/08_attribution.csv`)

- **scBaseCount (16 left in S2 + S3):**
  - 8 are R1-length bulk calls (VDJ/BCR, R2C2, mixed); these need a rescan with BAM or all reads.
  - 7 are whitelist-only negatives without a declared majority.
  - 1 is matrix-only.
- **Census (7):**
  - 2 are catalogue gaps (E-HCAD-10, HRA000328).
  - 3 are PMID-only collections whose linked studies are methylation or bulk companions.
  - 1 is GSE286254 ("Single-Cell Atlas" in the title only).
  - 1 is the motor-cortex collection's snmC/companion series.

## Precision of new tags not in scBaseCount/Census (`out/07_precision_round{1,2}.csv`)

Population: S2 new tags outside the external sets, N = 14,308. Stratified by the pass that added each tag.

**Round 2 (validation, the headline).** 20 per stratum, 100 studies, seed 20260930, drawn after the rules were frozen. Four blinded judges, then a reviewer pass over every unclear against the full stored text (7 of 11 resolved; see `out/06b_overrides.csv`).

| scope | N | judged | precision | 95% CI |
|---|---|---|---|---|
| **all new tags** | 14,308 | 96 (+4 unclear) | **0.934** | 0.844–0.973 |
| all, unclear counted as FP | | 100 | 0.866 | 0.762–0.928 |
| code fixes only | 7,121 | 61 | 0.892 | 0.773–0.953 |
| lag only (untransferred staging) | 7,187 | 35 | 0.992 | 0.830–1.000 |

| stratum | precision | 95% CI |
|---|---|---|
| libsource | 1.000 | 0.816–1.000 (3 unclear) |
| protocol | 0.900 | 0.699–0.972 |
| read_measured (lag) | 0.950 | 0.764–0.991 |
| whitelist_override | 0.947 | 0.754–0.991 |
| **text (restored)** | **0.800** | 0.584–0.919 |

The point estimate clears the 0.854 catalogue baseline (0.842 with unclear counted as FP). The lower CI bound does not.

False positives in round 2:
- a bulk companion (GSE301092)
- 10x linked-read genomes (SRP223911)
- a *Bacillus* WGS mislabelled SINGLE CELL (SRP476505; the organism regex misses genus names)
- bulk sorted Smart-seq2 pools (SRP299168, SRP500320)
- a Drop-seq sample-barcode bulk run (SRP229312)
- a Smart-seq3 citation (GSE306281)

Round 1 (development, 151 studies, seed 20260929) was used to find the false-positive classes. Scored against the final rules it gives 0.941 (0.867–0.975), which is optimistic.

## Production rollout: **NOT RUN**

Run these only after Postgres is out of recovery and the user says go; ideally run the build off-peak.

```bash
# 1. merge (branch adds new files only; no conflict with the user's uncommitted edits)  -- NOT RUN
cd /NAS/saket/github/seqout-ingestion
git merge --no-ff fix/singlecell-tagging-gaps
# apply the build.py change to the (untracked) recall_fix recipe                       -- NOT RUN
patch -p1 --dry-run < recipes/singlecell_tagging/recall_fix_build.patch && \
patch -p1           < recipes/singlecell_tagging/recall_fix_build.patch
.venv/bin/python -m pytest -q tests/test_recall_fix.py tests/test_singlecell_recall_rules.py

# 2. back up the staging, rebuild the single-cell verdicts (PG reads only, keyset-paged) -- NOT RUN
cp -p /data/saket/sqlites/recall_fix.db /data/saket/sqlites/recall_fix.db.before_sctag_$(date +%Y%m%d-%H%M%S)
python3 recipes/recall_fix/build.py --db /data/saket/sqlites/recall_fix.db --only single-cell
#   (optional, labelled) add: --external-accessions /path/to/scbasecount_census_accessions.txt

# 3. transfer: dry run, read the transition matrix, then apply (writes before/after snapshots) -- NOT RUN
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --only single-cell
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --only single-cell --apply

# 4. matview refresh (singlecell_study CASCADEs into country/perturbation/spatial)     -- NOT RUN
just refresh_technology_matviews
```

After step 3, check that production `single_cell_status` includes `protocol_indicates_single_cell` and `library_source_indicates_single_cell`. Review the 74 measured-positive studies that the rebuilt staging flips to bulk before applying.

## Caveats and unverified items

- **Local inputs, not a PG rebuild.**
  - Protocol text comes from the coverage-gap extract: the top 3 experiment texts or top 2 GSM texts per study, for studies outside the matview only. The real paged scan reads every record, so protocol recoveries are a lower bound.
  - Text hits come from the PG title/description extract (`pg_um_text`) plus production's own text verdicts.
  - Local `unified_metadata.db` lacks about 250k PRJ* canonicals. 1,798 staged positives could not be confirmed as canonicals and were not counted as landing, so new-tag counts are a lower bound. 27 local-only canonicals newer than the last PG load were excluded.
- **Untested against PG.**
  - The paged feeds in `recall_feeds.py` have not been run against Postgres.
  - The assumed indexes (`ena_experiments.run_accession`, `sra_experiments.study`, `ena_experiments.study_accession`) are unverified.
  - `aliases` values are assumed to be strings.
- **Judging.** LLM judges worked from packets with descriptions truncated at 900 characters; the reviewer checked every unclear call and the round-1 false positives. The text stratum is the weakest (0.80). If a stricter bar is wanted, restoring text verdicts can be limited to studies with a protocol or library_source hit as well.
- **The 74 lost studies** need a decision before the transfer (see above).
- A 110-second search found no code in pysradb-server / pysraweb that enumerates `single_cell_status` values, so the new statuses should pass through. This is not exhaustive.
