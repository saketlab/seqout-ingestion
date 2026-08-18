# READ-THIS — migration handoff

Written on the old machine to hand this work to a session on the **lab GPU box**.
Read this before touching anything. Everything below was established by reading
the code, not assumed.

## The plan, as decided

Migrate to the lab GPU machine, **start with an empty `DATA_DIR`**, and treat the
production Postgres on the VPS as the source of truth.

- The VPS runs **nothing but Postgres** — with one deliberate exception, see
  `refresh publications` below. All other pipeline stages, CPU ones included, run
  on the lab box and push over an SSH tunnel. `DATA_DIR` holds ~15GB per SRA
  tarball plus the GEO/ENA/AE caches and chroma; none of that belongs on the VPS.
- SQLite stays as a **local staging buffer**. It is deliberately not being replaced
  by direct-to-Postgres writes — see "Rejected" below.
- **Out of scope, per the user: `sc_enrich` / cell counting, and the Neo4j ontology
  graph.** Don't propose work on either.

The hard requirement: **nothing already in Postgres may ever be deleted.**
Updates are fine. Everything below serves that.

## State of the working tree

Two changes were made on the old machine. **Check whether they actually arrived** —
they were uncommitted when this file was written:

```bash
git log --oneline -3
grep -n "non_nulling_update" src/transfer/pg_fields.py
grep -n "prune_publications" src/cli/transfer.py
```

**1. Non-nulling upserts.** `non_nulling_update()` in `src/transfer/pg_fields.py`,
used by `postgres.py`, `gsa.py`, and `ddbj.py`. Generates:

```sql
SET title = COALESCE(EXCLUDED.title, sra_studies.title)
WHERE (EXCLUDED.title IS NOT NULL AND sra_studies.title IS DISTINCT FROM EXCLUDED.title)
```

An incoming NULL loses to what Postgres holds; a real value still wins. The `WHERE`
guard is **not** an optimisation — Postgres cannot see that `COALESCE(x, ...) = x`,
so without it every conflicting row is rewritten into a dead tuple. `ddbj.py:482`
documents the same trap. Covered by `tests/test_pg_upsert.py`.

**2. `study_publications` orphan cleanup is gated off.** `--prune-publications`,
default false. Do not enable it on this machine. Details below.

## Do these before the first transfer

**Never pass `--prune-publications`.** `_transfer_study_publications` in
`src/transfer/postgres.py` deletes every `(accession, source)` not in the current
run's SQLite buffers. It does not compare against a date window — it compares
against *everything in Postgres*. With a fresh `DATA_DIR` that is the entire table
minus the first week. It is off by default; keep it that way until the buffers
provably hold the full history.

**Pass `--pg-url` to DRA ingest**, not `--sra-db-file`/`--ena-db-file`.
`existing_study_accessions` (`src/ingest/dra.py`) already supports checking novelty
against Postgres — the exact "Postgres is the source of truth" idiom wanted here.
With empty local buffers, the SQLite path makes every DRA study look
DDBJ-exclusive and stamps `is_only_ddbj` wrong across the board.

**Carry over the chroma directory if it exists anywhere.** This is the one
irrecoverable thing. `src/embed/text.py` reads the text to embed from the *SQLite*
source DBs, not from Postgres. Fresh SQLite + fresh chroma means historical studies
can never be re-embedded — the text is no longer on the machine. SRA partially
recovers (its monthly Full tarball repopulates the table); GEO/ENA/AE do not.

**Carry over the geocoding cache JSON if convenient.** Pure cache, no correctness
risk, but rebuilding it re-hits rate-limited geocoders.

## Facts that took digging — don't re-derive these

**SRA cannot run weekly.** `src/ingest/sra.py:42` matches only
`NCBI_SRA_Metadata_Full_*.tar.gz`. Those are published **monthly, ~15th–17th**
(verified against the live FTP listing). A 7-day window finds nothing and
`sra.py:126` raises `ValueError`. Run SRA monthly on the 18th. Do not widen the
weekly window to catch it — that re-downloads and reparses ~15GB for nothing.

**Every other source is fine weekly**: GEO, ENA, AE, GSA, GSA-Human, DRA, GEA.
GSA looks expensive (it enumerates all ~18k CRA studies before date-filtering) but
caches each study page to disk, so only genuinely new accessions get scraped after
the first crawl.

**The disk cache freezes already-fetched studies.** Every source skips a study
whose file exists: `geo.py:366`, `ena.py:378`, `ae.py:252`, `gsa_common.py:80`,
`dra.py:293`. So weekly runs pick up **new** studies but never **revisions**. GEO's
mtime filter and ENA's `--use-last-updated` correctly *select* revised studies and
then the download step skips them. SRA is the only source that truly refreshes,
because its tarball is a full snapshot. A `--refresh-cached` flag on the download
workers would fix this (~2 lines each); not yet written.

**Ingest writes a fresh dated SQLite per run** (`<source>/db/<today>/`), and no
`justfile` recipe passes `--db-file`. So each per-source DB is a *window slice*,
not cumulative — except SRA (always the full archive) and ENA (whose load step
globs the entire on-disk cache, so it grows monotonically).

**Consequence for `unify`:** `build_unified_catalog` reads whole per-source tables
(`SELECT ... FROM geo_series`). On weekly slices, cross-source dedup only ever sees
one week, so a GEO study from week 1 never links to its ENA mirror appearing in
week 5. Postgres still accumulates correctly (all upserts), but `unified_studies` /
`unified_metadata` quality degrades.

**The fix for that is blocked.** Pinning cumulative per-source DBs via `--db-file`
would fix unify, but GEO/SRA/ENA/AE ingest still use `INSERT OR REPLACE` — a
delete+insert that nulls `pubmed_id_pysradb` / `pubmed_last_checked`, which are
owned by `backfill`. DRA and GEA already solved this with
`ON CONFLICT(accession) DO UPDATE` (see the comment at `src/ingest/dra.py:78`).
**Four SQLite statements need the same treatment before cumulative DBs are safe.**
This is the main piece of outstanding work.

## Cadence

```
weekly    → geo, ena, ae, gsa, gsa_human, dra, gea            (lab box)
            + backfill, unify, geocode, transfer postgres, embed run
weekly    → refresh publications                              (VPS, Mondays 03:00)
monthly   → the above, preceded by ingest_sra (18th)
quarterly → embed coords / cluster / neighbors / keywords / labels, transfer embed
```

**`refresh publications` is the one stage that runs on the VPS**, as
`seqout-pubrefresh.timer` (units in `deploy/`). It is postgres-native — no SQLite,
no `DATA_DIR` — because publication linkage goes stale on its own: a dataset
released in 2024 gets its paper in 2026, and nothing in the lab-box pipeline ever
revisits it. It resolves unlinked studies via pysradb, fills `pubmed_metadata`
via OpenAlex, then refreshes `pub_tsv`.

It does not fight the lab box. `postgres.py` upserts through
`non_nulling_update()`, so an incoming NULL `pubmed_id_pysradb` from a fresh
SQLite window loses to what the VPS wrote. The reverse is not true — the VPS
never writes back to SQLite, so the lab box's `backfill pm-ids` will re-resolve
the same studies from scratch. That is wasted API calls, not lost data; if it
matters, drop `backfill pm-ids` from the weekly lab-box run and let the VPS own
publication linkage outright.

Also changed for this: `transfer/pub_tsv.py`'s two populate statements grew an
`IS DISTINCT FROM` no-op guard. Without it, a weekly `pub_tsv` refresh rewrites
every publication-carrying row into a dead tuple and re-fires the GEO tsv trigger
for 232k rows — fine quarterly, not fine weekly. Same reasoning as
`non_nulling_update`'s guard.

`embed run` is genuinely incremental — `embedder.py:190` diffs against existing
chroma IDs. The rest are **not**: `coords`, `cluster` and `neighbors` all call
`load_combined_matrix` and recompute over the whole corpus. UMAP is a fresh
`fit_transform`, Leiden re-partitions from scratch, and cluster IDs are not stable
across runs — so re-clustering weekly would churn the map and invalidate every
LLM-generated label. Hence quarterly. Accepted tradeoff: studies ingested between
recomputes have embeddings but no coordinates or cluster until the next recompute.

## Rejected, with reasons — don't re-propose

**Replacing SQLite with direct writes to production Postgres.** 128 `sqlite_connect`
call sites across 32 files; 23 `INSERT OR REPLACE`, 19 `json_extract`/`json_valid`,
35 PRAGMAs; `transfer/` is 5,232 lines that exist solely as the bridge and whose
batch modifiers encode real transformation logic. More importantly it deletes the
staging buffer that keeps partial parses and killed runs out of prod — which
directly contradicts the never-delete requirement. If one engine is ever genuinely
wanted, run a **local** Postgres on the lab box as the working store and keep
`transfer postgres` as the publish step.

**Revoking DELETE/TRUNCATE from the pipeline's Postgres role.** Proposed and
declined. The `--prune-publications` gate is the substitute.

**A pre-transfer `pg_dump` step in the scripts.** Declined — the user handles
backups manually.

## Still open

1. **Per-source last-ingestion dates.** The user offered these; they're needed to
   pick `--start-date` for the first catch-up run per source so there's no coverage
   gap. Ask if not supplied.
2. **Weekly / monthly / quarterly service scripts**, not yet written. Intended for
   `recipes/`. The VPS half is done (`deploy/seqout-pubrefresh.{service,timer}`);
   what is missing is the lab-box ingest/transfer chain. Include the SRA guard:
   ```bash
   if curl -s https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/ \
      | grep -qE "NCBI_SRA_Metadata_Full_$(date -u +%Y%m)[0-9]{2}\.tar\.gz"; then
       START_DATE=$(date -u -d '35 days ago' +%F) just ingest_sra
   fi
   ```
   No `pg_dump` line — that's handled manually.
3. **`date_first_added` / `date_last_updated` columns**, agreed in principle, not
   implemented. `date_first_added timestamptz NOT NULL DEFAULT now()` stays out of
   the SET clause; `date_last_updated = now()` goes in it. This is only accurate
   *because* of the `WHERE` guard above — without it, every no-op re-transfer would
   bump the timestamp.
4. **The four `INSERT OR REPLACE` → upsert conversions** in GEO/SRA/ENA/AE ingest,
   which unblock cumulative per-source DBs.

## Environment note

`uv sync` fails on the old machine because `vendor/yuj` is an uninitialised
submodule. On the lab box run `git submodule update --init --recursive` first, or
`uv run pytest` will not work.
