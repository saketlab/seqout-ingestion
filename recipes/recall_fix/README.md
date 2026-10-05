# recall_fix -- the two columns that were hiding most of the corpus

Both defects produce the same user-visible symptom: a filtered search returns a
fraction of what seqout actually holds, with no indication anything was dropped.
Filed upstream as items 1, 2 and 3 of `seqout_upstream_feedback.md`; the evidence
is in `upstream_feedback_triage.md`.

Staging sqlites live under `/data/saket/sqlites/` (1.8 T), not `/home`. The
organism staging table alone is ~9.5M rows, and `sample_organism_overwrite` is
the undo log for a 175k-row correction -- losing it to a full disk loses the
ability to revert. `just recall_fix_*` already points there; override with
`SQLITE_DIR`.

```bash
python3 recipes/recall_fix/build.py    --db /data/saket/sqlites/recall_fix.db          # pg reads only
python3 recipes/recall_fix/verify.py   --db /data/saket/sqlites/recall_fix.db          # adversarial checks
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db          # dry run
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --apply  # writes
```

## What stops a sample getting the wrong species

Four layers, in order of how much they actually protect you:

1. **Every route reads a per-sample archive record**, keyed on that sample's own
   accession. Nothing is inherited from a study, with one labelled exception
   (`ae_experiment_single_organism`) that is confined to experiments where the
   archive itself records one species.
2. **Per-target single-organism guards.** The fuzzy-keyed routes carry
   `HAVING COUNT(DISTINCT organism) = 1`, so a target with conflicting evidence
   is left NULL rather than resolved by picking one.
3. **Cross-route agreement** (`verify.py`). Routes are applied in precedence
   order with `INSERT OR IGNORE`, so the stronger route silently wins and a
   disagreement would leave no trace. `verify.py` puts it back: it re-runs the
   overlapping pairs (`gsm_alias` vs `geo_platform`, `ae_ena_run` vs
   `ae_source_name`) and reports how often two independent routes reach the same
   answer.
4. **A multi-organism regression test** (`verify.py`). The failure that sank the
   study-level proposal was every sample of a mixed-species study inheriting the
   dominant organism. So: take studies `unified_metadata` already knows contain
   more than one species, and count how many come out of the staging table
   uniform. Over `UNIFORM_ALARM` (90%) the run is flagged as behaving like a
   study-level assignment and must not be applied.

Layer 4 is the one that answers "how do you know multi-organism studies are
right". It is a check on the *outcome*, not on the routes' intentions, which is
why it can catch a mistake none of the individual guards would.

`--only organism` / `--only single-cell` on either command. The organism build
takes ~30 min and streams through server-side cursors; the single-cell build
takes seconds.

## Refreshing after new preflightx / pentimento data

This is designed to be re-run, not run once:

```bash
python3 recipes/recall_fix/build.py    --db /data/saket/sqlites/recall_fix.db --only single-cell
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --only single-cell   # dry run
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --only single-cell --apply
```

Adding a new evidence source means editing `SINGLE_CELL_SQL` and `_classify()` in
`build.py` and nothing else. Both the staging table and the classification are
rebuilt whole from current Postgres state on every run, so a study that acquires
evidence tomorrow moves `not_screened -> measured_single_cell` on the next pass,
and one whose evidence is corrected moves back.

**The build must never read a column it writes.** The text signal is re-derived
from `title`/`description` with `SC_TEXT_REGEX`, not read back out of
`is_single_cell`. That is not stylistic: a study classified
`measured_not_single_cell_text_disagrees` has `is_single_cell` set to 0, so a
build that read the flag would stop seeing it as text-positive and silently
downgrade it to plain `measured_not_single_cell` on the second run, then differ
again on the third. Keeping the build a pure function of the archives is what
makes repeated runs converge instead of drift.

## What `single_cell_status` means

`is_single_cell` alone is too easy to misread -- `WHERE is_single_cell = FALSE`
and `WHERE NOT is_single_cell` both silently drop every unscreened row -- so the
verdict is spelled out alongside it:

| `single_cell_status` | `is_single_cell` | meaning |
|---|---|---|
| `measured_single_cell` | 1 | a counted matrix, or preflightx called it from the reads |
| `text_indicates_single_cell` | 1 | the study says so; nothing has measured it |
| `library_source_indicates_single_cell` | 1 | the archive's own `library_source` field declares it; nothing measured it and the title/description never say so |
| `measured_not_single_cell` | 0 | screened, came back bulk |
| `measured_not_single_cell_text_disagrees` | 0 | the study text says single-cell, the reads say bulk |
| `not_screened` | NULL | no evidence either way. **NULL is not "no".** |

`library_source_indicates_single_cell` is additive only: it fills a study that
neither the pentimento read-derived call nor the text regex already reached. It
never overwrites `measured_single_cell`, `measured_not_single_cell`,
`measured_not_single_cell_text_disagrees`, or `text_indicates_single_cell` --
the declared field is strong evidence, but a study already carrying one of
those verdicts is not touched, per the non-nulling-additive convention every
other write in this repo follows.

Text-derived positives are kept at `is_single_cell = 1` rather than reset to
NULL. Resetting them was the original instruction and it was withdrawn after
review: it would have dropped 18,249 real single-cell studies out of every
`is_single_cell IS TRUE` filter, reproducing the exact bug being fixed under
tidier-looking semantics. Provenance, not deletion.

`measured_not_single_cell_text_disagrees` is worth keeping visible. Sampling it
shows the reads are usually right and the regex was over-broad: spatial
transcriptomics studies, and titles like *"DC3: Deconvolution and coupled
clustering from bulk and single cell genomics data"* that name single-cell while
depositing bulk reads.

## Abstentions are not negatives

The largest `pentimento_run.bulk_vs_sc` value is `bulk_or_5prime_broken`
(1,197,709 runs) -- it names two possibilities and picks neither -- and
`uncertain` adds 132,690. Counting either as evidence of bulk writes a
measured-looking 0 onto studies nothing decided, which is the same class of lie
as the old `DEFAULT 0`. Only `bulk`, `single_cell`, `single_cell_uncertain` and a
non-NULL `assay_is_single_cell` count as verdicts. Treating them properly moved
4,363 studies out of a false negative and into `not_screened`.

Two special cases in `_classify()`:

- `single_cell_uncertain` counts as **positive**. It is hedged about confidence,
  not about which side it is on.
- scATAC trips `assay_is_single_cell = FALSE`, because that call is a barcode
  whitelist hit against RNA chemistries and 8,138 runs are ATAC. They are
  single-cell; they are just not scRNA-seq.

## Organism: deterministic routes only

`ontology_samples_v4.organism` was NULL on 10,944,347 of 16,081,369 rows (68%),
and the samples-search filter is exact equality, so those rows answered no
organism query at all -- including 104,029 of the 148,599 pentimento-counted
samples.

Every route reads the organism off the **original per-sample archive record**.
Nothing is inferred.

| route | join | resolves |
|---|---|---|
| `accession` | `sra_samples.accession` (SRS/ERS/DRS) | 4,912,591 of 4,919,249 NULLs (99.9%) |
| `gsm_alias` | `sra_samples.alias` | 4,337,854 of 5,498,998 GSM NULLs |
| `gsa` | `gsa_samples.sample_accession` | 155,687 of 156,676 (99.4%) |
| `ena_biosample` | `ena_samples.sample_accession` | the SAMEA/SAMN remainder |

The GSM route exists because **`geo_samples` carries no organism column at all**,
so a GSM resolves only through the SRA record that aliases it. Checks run before
trusting it: 0 GSM aliases map to more than one distinct `scientific_name`, 0 to
more than one `taxon_id`, and although 207 map to several SRS rows those always
agree -- hence `DISTINCT ON`.

### ArrayExpress: four routes, ranked by distance from a real accession

AE samples have **no sample accession**. `ontology_samples_v4.sample` is the
free-text MAGE-TAB source name, so none of the routes above reach 411,303 rows
across 7,022 studies.

| route | joins | note |
|---|---|---|
| `ae_ena_run` | source name -> `arrayexpress_samples.ena_run` -> `ena_experiments.run_accession` | lands on a real archive record; no free-text trust in the answer |
| `ae_source_name` | `(experiment_accession, source_name)` | AE's own archival organism |
| `ae_experiment_single_organism` | experiment has one organism across all its archival rows | not sample-level; labelled so on purpose |

Matching on free text is safe here for one reason: **every join is scoped by
`experiment_accession`**. The name need not be unique globally, only within its
own experiment.

The last route is close to the rejected `organism_counts` idea and the
difference is narrow: the single-organism constraint is checked against
ArrayExpress's own archival per-assay rows at rebuild time, not against a derived
rollup, so it cannot produce the Liver Cell Atlas failure. It is still
experiment-level inference. Do not describe it as ground truth.

`sample_title` resolves **0** rows. It looks like the obvious join and matches
nothing.

### Why route A's apparent 89.5% accuracy is not a reason to reject it

Against rows whose organism is already known, `ae_source_name` agrees
only 89.5% of the time (496,006 of 554,361). That reads like a failing grade
until you look at the disagreements: 43,029 rows of `os_v4` saying
`Homo sapiens` where AE says `Mus musculus`, with the same signature down the
tail. That is the LLM-enrichment defect this recipe corrects elsewhere -- the
identical `Homo sapiens -> Mus musculus` pattern the SRA route found 101,983
times. The metric assumed the stored value was truth. It is not.

### Existing values ARE corrected, and the change is reversible

The archive record is ground truth for what species a sample is, so this does two
writes, not one:

| write | rows | guard |
|---|---|---|
| fill | 8,830,011 samples | `WHERE organism IS NULL` |
| correct | 174,025 samples / 175,449 table rows | `WHERE organism = <recorded prior value>` |

**174,025 existing values contradict the archive**, led by 108,175 saying
`Homo sapiens` where SRA says `Mus musculus`, plus `Cricetulus griseus` ->
`Mus musculus` (3,545) and a long tail of `Homo sapiens` -> `synthetic construct`
/ `Mycobacterium tuberculosis` / `human gut metagenome`. They came from the LLM
enrichment, not from an archive.

`sample_organism_overwrite` records every prior value **before** the write, so the
staging file is its own undo log:

```bash
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --rollback-organism            # dry run
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --rollback-organism --apply    # revert
```

Do not rebuild `--only organism-overwrite` after applying: the rebuild finds the
rows already corrected, so they stop disagreeing, and the undo log empties.

Two guards make the correction safe to re-run and safe to revert:

- the update only fires where the row **still holds the recorded prior value**,
  so anything changed since staging is skipped rather than clobbered;
- the rollback only fires where the row **still holds what we wrote**, so a later
  legitimate correction is not reverted along with ours.

`sample` is not unique in `ontology_samples_v4` -- 1,284,674 rows are a sample
repeated under another study -- which is why 174,025 staged corrections touch
175,449 rows. Only **4 samples** in the entire table carry two different non-NULL
organisms, and the 6 rows they account for are the only ones the guard skips.

### Before/after snapshots

`--apply` takes a snapshot of postgres into the staging file automatically, both
sides of the write, and prints the diff. There is no way to apply without one.

```bash
python3 recipes/recall_fix/transfer.py --db /data/saket/sqlites/recall_fix.db --snapshot-only before   # optional, manual
```

`pg_snapshot_sample` and `pg_snapshot_study` hold one row per affected
sample/study per `phase`, so what changed is a self-join rather than a claim in a
log. The diff reports three organism transitions; **`LOST (value -> NULL)` must
always be 0** -- it is the line that would show this recipe destroying
information, and if it is ever non-zero, roll back before doing anything else.

## Outcome

```
organism coverage   31.94%  ->  99.75%      39,777 of 16,081,369 rows stay NULL
samples filled      10,096,703
samples corrected      180,291   (prior values in sample_organism_overwrite)
```

Fills by route. The two strongest -- the sample's own SRA record, by accession
or by GSM alias -- carry 88% of the total; the experiment-level fallback that
needed the most justification carries 0.68%.

| route | samples | | correction route | samples |
|---|---|---|---|---|
| `accession` | 4,895,101 | | `accession` | 159,821 |
| `gsm_alias` | 3,775,930 | | `gsm_alias` | 12,541 |
| `geo_platform` | 952,936 | | `ae_source_name` | 6,266 |
| `ae_ena_run` | 188,972 | | `ena_biosample` | 1,624 |
| `gsa` | 155,425 | | `gsa` | 39 |
| `ae_experiment_single_organism` | 69,057 | | | |
| `ae_source_name` | 55,727 | | | |
| `ena_biosample` | 3,555 | | | |

### Cross-route agreement

```
gsm_alias     vs geo_platform    99.8%   (4,308,951 of 4,319,011)
ae_ena_run    vs ae_source_name  97.0%   (2,892,580 of 2,981,574)
```

Nearly every disagreement is **NCBI taxonomy vintage, not a wrong species**. The
two sources were snapshotted at different times, so one says `Candida auris` and
the other `Candidozyma auris` (2023 genus rename), `Clostridium difficile 630` vs
`Clostridioides difficile 630` (2016), `Ustilago maydis` vs `Mycosarcoma maydis`,
`Desulfovibrio vulgaris` vs `Nitratidesulfovibrio vulgaris`, `SARS coronavirus`
vs `Severe acute respiratory syndrome coronavirus`, `Canis familiaris` vs
`Canis lupus familiaris`. Several more are strain-vs-species
(`Streptococcus pneumoniae D39` vs `Streptococcus pneumoniae`).

Two differ in meaning: `Sus scrofa` vs
`mixed sample: pig cell line 3` (75 rows) and `Candida albicans` vs
`Homo sapiens + Candida albicans` (70 rows). Those are host-plus-pathogen
samples where one archive records the mixture and the other records one member.
Route precedence picks one; neither is wrong, and no single-species column can
represent them properly.

**This is the argument for a taxid-normalised column.** `organism` is free text
and its vintage differs by source, so `organism = 'Candida auris'` misses rows
that say `Candidozyma auris`. Filling the column does not fix that; it is a
separate change.

### Multi-organism regression test

```
multi-organism studies in unified_metadata            48,928
  ...with >1 filled sample                            11,169
  ...that came out UNIFORM                             2,833  (25.4%)
```

25.4% against a 90% alarm threshold. Multi-species studies stay multi-species,
which is the property a study-level assignment would have destroyed. The most
species-diverse results are sane rather than degenerate:

```
SRP141175   736 distinct organisms over 738 samples
SRP175152   432 distinct organisms over 554 samples
SRP153945   325 distinct organisms over 697 samples
```

A study-level fill would have reported 1 for every one of those.
