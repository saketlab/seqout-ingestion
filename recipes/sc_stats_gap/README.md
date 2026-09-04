# sc_stats gap: reaching the chemistries `/stats/sc-quality` cannot see

`sc_quality_by_year` plots 10x and read-called Drop-seq and nothing else. Three
separate causes, two now fixed in code, one that needs a fleet run.

## Why the chart is 10x-only

| channel | scale | status |
|---|---|---|
| the sweep is GEO-only | 0 of 20,341 SRP, 3,541 ERP, 42 E-MTAB single-cell studies attempted | inherent |
| GEO single-cell studies with no `sc_stats_study` row | 8,509 of 20,057 | **needs the fleet run below** |
| swept studies whose matrix is a dense table | 2,830 `no_matrix` + 148 `all_failed` | **fixed in code, needs a redo pass** |
| matrices with no GSM in the filename | 2,380 of 78,923 rows | fixed in SQL |

Matrices live in GEO supplementary files; SRA and ENA hold reads. SRA-first
studies are reachable only through a linked GSE, which is out of scope here.

The dense-table channel is the one that skews the chart by chemistry rather than
by volume. 14 of 14 sampled `no_matrix` studies deposit CSV/TXT gene x cell
tables and zero mtx or h5: dense tables are how plate and combinatorial
pipelines share output, mtx triples and h5ad are how 10x does. Reading only the
sparse formats measures 10x and reports it as the archive.

## What changed in code

`src/sc_stats/fetch.py`
- `DENSE_SUFFIXES` and `is_dense_table()`. A `.csv`/`.tsv`/`.txt` file is taken
  as a candidate matrix unless its name carries a `_NOT_A_MATRIX` token
  (`readme`, `metadata`, `sample_sheet`, `barcode`, ...).
- `is_wanted()` and `unpack()`'s tar-member filter accept them. For a study that
  ships one `_RAW.tar` this costs no extra download: the tar was already pulled
  whole and only its member filter widened. Loose top-level `.csv.gz` files are
  new downloads, so expect more bytes per study than the last sweep.

`src/sc_stats/stats.py`
- `_read_dense()`, one streaming pass accumulating **both** axes, plus
  `_dense_header()` (delimiter sniff, optional corner cell, `gene_id`+`gene_name`
  label columns) and `_dense_orientation()`.
- Orientation is decided from the labels, never from the shape: whichever axis
  scores higher on stable-ID prefixes and MT- symbols is the gene axis. A file
  where neither axis is recognisable **raises** rather than guessing, because a
  transposed summary still looks like a plausible QC table.
- A `#` preamble above the header is skipped (BD Rhapsody writes seven `##`
  lines). A targeted panel carries no stable IDs and no MT gene, so neither
  axis scores. The fallback is then the axis that cannot be genes: barcodes,
  bare cell indices, `-1` suffixes.
- Guards: `_MIN_DENSE_AXIS = 20` columns, `_MAX_DENSE_VALUES = 3e8` parsed values.

`src/sc_stats/yuj_worker.py`
- `_local_matrices()` picks up dense tables too. `MATRIX_SUFFIXES` there and
  `MATRIX_HINTS` in `fetch.py` remain two lists; only the dense path is shared.

`tests/test_sc_stats_dense.py` asserts that a table and its transpose produce
identical summaries.

Verified against a real Parse SPLiT-seq deposit, `GSE167859`
(`GSM5114029_expr_matrix.csv.gz`, previously `no_matrix`, `0/0 files`):

```
n_cells 5054   species Caenorhabditis elegans (WBGene prefixes)
filtered: median 1,115 UMIs / 189 genes over 2,264 cells
```

That is a chemistry an order of magnitude below 10x, the shape the chart is
missing, and the reason a log y-axis suits it.

## What was fixed in SQL

`sql/tables/sc_quality_by_year_pg.sql`.

1. `LEFT JOIN enriched_samples` -> `LEFT JOIN ontology_samples_v5`.
   `enriched_samples` is the legacy 5.3M-row table and has no `assay_category`
   column at all, so the matview would not even build. `ontology_samples_v5` is
   what `main.py` and `beacon.py` query, is unique on `(study_accession, sample)`
   so the join cannot fan out, and carries 16.6M rows.

2. The read-call lateral now rejects the `'Not single-cell'` sentinel:

   ```sql
   ps.assay IS NOT NULL
   AND ps.assay <> 'Not single-cell'          -- added
   AND ps.assay_is_single_cell IS NOT FALSE
   AND ps.calls_ambiguous IS NOT TRUE
   ```

   `rollup.py` writes that sentinel with `assay_is_single_cell` left NULL, and
   `IS NOT FALSE` passed it. 1,839 rows took the `read` branch, got
   `assay_raw = 'Not single-cell'`, matched no chemistry pattern and were dropped
   before the declared label was consulted, including every SPLiT-seq matrix
   in the corpus.

3. The declared branch no longer requires a category:

   ```sql
   WHEN es.assay_category IS NULL
        OR es.assay_category LIKE 'Single-cell%' THEN COALESCE(es.assay, ...)
   ```

   The old `= 'Single-cell Transcriptomics'` equality dropped everything whose
   category the deriver never filled: all 12 SPLiT-Seq, 30 of 43 Smart-Seq2, all
   Smart-Scrb, sci-RNA-seq3, inDrops, MARS-seq.

4. Series-scope fallback. A `LEFT JOIN LATERAL` supplies the study's commonest
   declared chemistry when `sm.sample_accession IS NULL`, which `ingest.py`
   leaves NULL for a file with no `GSM` prefix. Recovered 1,724 matrices; build
   time unchanged at ~5s.

5. `'Parse/Evercode'` and `'SPLiT-seq'` merged into one `'SPLiT-seq/Parse'`
   label, Evercode being Parse's commercial SPLiT-seq. Keep
   `utils/chart-theme.ts:TECHNOLOGY_COLOR` in the pysraweb repo in step; its keys
   must match this CASE.

6. `ATAC` dropped from the label list. Its counts are fragments over peaks,
   and once (3) let the declared branch through it entered the chart as a
   zigzagging series.

Rebuild with the recipe rather than raw psql; it purges the API cache:

```bash
just refresh_sc_quality
```

## The fleet run

Two passes over the same job. Target lists are in this directory, built from
production Postgres:

- `targets_gap.txt`: GEO single-cell studies with no `sc_stats_study` row. The
  original worklist was "every sample with a known `cells` or `genes` count", so
  a study the counter never reached could not enter it. This list is drawn from
  `ontology_samples_v5.assay_category LIKE 'Single-cell%'`, a wider universe.
- `targets_redo_dense.txt`: studies already swept as `no_matrix` or
  `all_failed`, worth retrying now that dense tables parse

### 0. Load what has already been measured

The last sweep finished after the last ingest, leaving 1,866 measured studies on
local disk and out of Postgres. Ingest, not transfer, was the stale step: the
staging SQLite matched pg exactly, and four pulls landed after it was last
written.

```bash
# SC_STATS_DB_FILE's justfile default is /data/share/seqout/..., which does not
# exist on this host; the real staging file is under PULL_ROOT. Pass it or a
# fresh empty db is created and the transfer blanks pg.
SC_STATS_DB_FILE=/data/saket/sc_stats/sc_stats.db just sc_stats_ingest
SC_STATS_DB_FILE=/data/saket/sc_stats/sc_stats.db just transfer_sc_stats --is-dry-run
SC_STATS_DB_FILE=/data/saket/sc_stats/sc_stats.db just transfer_sc_stats
just refresh_sc_quality
```

Result:

```
sc_stats_matrix   78,923 -> 91,250
sc_stats_study    14,497 -> 16,368     ok 11,738 -> 13,383
sc_quality_by_year matrices 51,399 -> 58,293
```

`no_matrix` rose 2,614 -> 2,830 and `all_failed` 133 -> 148, which is why the
redo list below is 2,978 rather than 2,747. The target lists in this directory
were regenerated after the load; rebuild them from `sc_stats_study` if any time
passes before the fleet run.

### 1. Sidecars

`yuj_setup/accessions/<acc>.json` carries each study's download links. Build the
new ones from **production Postgres**, not the staging SQLite, which lags whole
ingest windows and turns a missing staging row into a false no-link finding:

```bash
cd /NAS/saket/github/seqout-ingestion
python3 recipes/ncount_gap/refresh_counter_inputs.py \
    --targets recipes/sc_stats_gap/targets_gap.txt \
    --targets recipes/sc_stats_gap/targets_redo_dense.txt \
    --out-dir /data/saket/sc_stats_gap/yuj_setup
```

It refuses to overwrite, so point `--out-dir` somewhere new and merge:

```bash
rsync -a /data/saket/sc_stats_gap/yuj_setup/accessions/ \
         yuj/sc_stats/yuj_setup/accessions/
```

Check `no_link_accessions.txt` before merging: a study listed there has no
supplementary link and will burn a work slot for nothing.

### 2. Clear the redo studies' results

`yuj_worker.main()` returns early when `results/<acc>.stats.json` exists, and
`no_matrix` **writes that file** (it is a terminal state, so yuj does not
re-dispatch). Without this the 2,978 redo studies are all skipped silently:

```bash
# pulled copies
cd ${PULL_ROOT:-/data/saket}/sc_stats/results
while read a; do rm -f "$a.stats.json" "$a.status"; done \
    < /NAS/saket/github/seqout-ingestion/recipes/sc_stats_gap/targets_redo_dense.txt

# and on every host
cd /NAS/saket/github/seqout-ingestion/yuj/sc_stats
recipes/fleet_ops/fleet_map.sh sc_stats '
  cd $HOME/seqout-sc-stats/results 2>/dev/null || exit 0
  while read a; do rm -f "$a.stats.json" "$a.status"; done < $HOME/redo.txt
  echo "left=$(ls *.stats.json 2>/dev/null | wc -l)"'
```

Copy `targets_redo_dense.txt` to each host as `~/redo.txt` first (rsync, or
`yuj exec`). The staging SQLite and Postgres both upsert on
`(accession, file)` / `accession`, so a re-measured study overwrites cleanly;
only the on-host result files block a redo.

### 3. Scatter and run

Run from `yuj/sc_stats/`, never the repo root: yuj reads `./yuj.yaml` and has no
`--config` flag, and the root config is a different job against a different fleet
file.

```bash
cd /NAS/saket/github/seqout-ingestion/yuj/sc_stats
cat ../../recipes/sc_stats_gap/targets_*.txt >> accessions.txt
sort -u accessions.txt -o accessions.txt        # 16,381 -> 24,881
yuj deploy && yuj exec 'bash install_deps.sh'   # ships the new stats.py/fetch.py
yuj scatter
yuj submit                                      # NOT `yuj run`
```

`yuj deploy` is not optional here: the dense reader is the whole point of the
pass, and the hosts hold their own copy of `src`.

`yuj run` aborts before scheduling the watchdog if any host fails deploy
(`JOBS.md:57`). Use `yuj submit`, then verify every host actually got the
generated scripts and crontab, which `yuj deploy` does not install:

```bash
recipes/fleet_ops/fleet_map.sh sc_stats '
  echo "scripts=$(ls $HOME/seqout-sc-stats/*.sh 2>/dev/null | wc -l) \
cron=$(crontab -l 2>/dev/null | grep -c seqout-sc-stats)"'
```

Expect `scripts=3 cron=1`. Two hosts (`athira`, `kcdh`) have a few GB free and
cannot hold a matrix at all; a host that cannot store one records every study as
having no data. Give them weight 0 rather than a slice.

### 4. Watch it

```bash
recipes/fleet_ops/scstats_cap.sh          # work / done / undone / freeGB / runner
T=180 recipes/fleet_ops/fleet_map.sh sc_stats 'tail -2 $HOME/seqout-sc-stats/*.log'
```

`freeGB` matters more this pass than last. Dense tables decompress large, and
`stall_min: 120` means a host that has produced nothing for two hours is stuck
rather than slow. `SC_STATS_MAX_BYTES` (default 40 GB) caps a single file.

### 5. Pull, ingest, transfer, rebuild

```bash
recipes/fleet_ops/pull_job.sh sc_stats seqout-sc-stats '*.stats.json' '*.status'
just sc_stats_ingest            # or: --results-dir DIR --results-dir DIR to union
just transfer_sc_stats          # non-nulling by default
just refresh_sc_quality
```

Then confirm the chart moved:

```sql
SELECT technology, evidence, count(*) yrs, min(year), max(year), sum(n_matrices)
FROM sc_quality_by_year GROUP BY 1,2 ORDER BY 6 DESC;
```

## Follow-up this sweep will create: targeted panels

`GSE330122` now parses, correctly, as 16,367 cells:

```
median 597 UMIs / 79 genes over the cells clearing min_counts
```

Those are true numbers for a **BD Rhapsody targeted panel** of roughly 400
genes. On a whole-transcriptome depth chart they read as a catastrophically
shallow chemistry, which is the same error `ATAC` was making. Once the sweep
lands, `BD Rhapsody` may clear the 20/3 gate on panel data alone.

The fix belongs in the schema, not the reader: the corpus should keep panel
matrices, and the chart should exclude them.

1. `summarise_matrix` already knows `len(raw["features"])`; return it as
   `n_features`.
2. Add the column to `sql/tables/sc_stats.sql`, `sql/tables/sc_stats_pg.sql`,
   `MATRIX_COLS` in `src/sc_stats/ingest.py` and `src/transfer/sc_stats.py`.
   All four are additive.
3. Gate the matview: `AND sm.n_features >= 5000`, alongside the existing
   `ncount_f_median IS NOT NULL` predicates.

Do this before the sweep lands if the chart matters more than the corpus;
after, if the reverse.

## What will still be missing afterwards

The `HAVING count(*) >= 20 AND count(DISTINCT accession) >= 3` gate is per
`(year, technology, evidence)` cell. Today SPLiT-seq/Parse has 12 matrices from
**2 studies** in one year and Smart-seq peaks at 7 matrices from 3 studies, so no
threshold short of trusting a single lab would show them. The gate is the right
call; the fix is more measured matrices, which is what this run is for. Revisit
the threshold only after the sweep, with the new counts in hand.
