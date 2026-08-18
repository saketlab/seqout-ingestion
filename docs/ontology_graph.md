# The ontology graph (Neo4j)

_Last updated 2026-08-18._

A single Neo4j database of biomedical terms, their synonyms, and their
hierarchy. It powers two features in `pysradb-server` / `seqout`:

- **Query expansion** — a search for `MCF7` also matches studies that only ever
  wrote `MCF-7`; a search for `ATAC-seq` also matches `assay for
  transposase-accessible chromatin`.
- **Deep-dive** — the search page's term explorer, which lets a user drill from
  `T cell` down into its subtypes.

It is **not** part of the nightly ingest. It is rebuilt roughly once a year,
when the source ontologies publish new releases. This document exists because
that gap is long enough that nobody remembers how.

This module was moved here from a standalone `ontology-scripts` repo so that
the ingestion repo is the single source of truth. Behaviour is unchanged; the
differences are listed under [What changed in the move](#what-changed-in-the-move).

## TL;DR — rebuild from nothing

```bash
# 1. Point at the Neo4j server. Prod sits behind an SSH tunnel; open it first.
#    ssh -N -L 7687:localhost:7687 <ingest-box>
export NEO4J_URI=neo4j://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=...

# 2. Everything: download -> extract -> load -> stamp has_children.
just ontology_build

# 3. Check it looks right.
just ontology_stats
```

Expect **hours**, and about **1.5 GB of downloads** plus a similar amount of
extracted CSV. Most of the wall-clock is the Neo4j load, not the parsing.

`ontology build` is safe to re-run and safe to interrupt: downloads skip files
already on disk, and every Neo4j write is a `MERGE`. If it dies partway, run it
again — it will pick up where it stopped. To skip the ontologies that already
loaded, pass `--only`:

```bash
just ontology_build --only chebi --only mesh
```

## What the graph looks like

One node label, three relationship types.

```
(:Term {name, has_children, xrefs})
    -[:MAPS_TO]->      synonym -> canonical term
    -[:SUBCLASS_OF]->  child   -> parent   (from OBO is_a)
    -[:PART_OF]->      part    -> whole    (from OBO BFO_0000050)
```

Scale, for reference: roughly **984k `:Term` nodes**, of which about **139k**
have `has_children = true` (measured 2026-07-05).

### `:Term.name` is the only key

`name` is always normalized — lowercased, with `-`, `_` and `/` folded to
spaces and runs of whitespace collapsed. `ATAC-seq` is stored as `atac seq`.
The function is `norm()` in `src/ontology/extract.py`, and **every writer and
every reader must apply it identically** or lookups silently miss.

In Cypher, normalize the *parameter* side. Never write `toLower(t.name)` — that
cannot use the index and turns a point lookup into a full scan.

No per-ontology id **identifies** a node. A term contributed by both MONDO and
MeSH is one merged node. That is deliberate: the app looks terms up by the words
a user typed, not by CURIE. What the merge would otherwise discard is kept in
`xrefs`.

### `xrefs` is the provenance record

A list of the source CURIEs a term came from:

```cypher
MATCH (t:Term {name: 'breast'}) RETURN t.xrefs
// ["UBERON:0002107", "MeSH:D001940", "EFO:0000807"]
```

This is **the** answer to "which ontology did this term come from?" — the source
is readable off the CURIE prefix, so nodes carry no separate `source` property.
A term merged from three ontologies carries one entry per source.

Two things to know before relying on it:

- **Synonyms carry their descriptor's id**, because a synonym string has no id
  of its own. `t2dm` and `type 2 diabetes mellitus` both list `MONDO:0005148`.
- **Prefixes are not limited to the eight ontologies below.** `cl.json` and
  `uberon-full.json` bundle imported classes, so `PATO:`, `GO:`, `CHEBI:`,
  `NCBITaxon:` and others appear. That is accurate — the term really does
  originate there — but it means you cannot treat the prefix set as closed.

Appends are guarded, so re-loading an unchanged artifact writes nothing.

The ids come from `curie()` in `src/ontology/extract.py`, which is the one
fragile piece. Most OBO node ids are PURLs whose last path segment is
`PREFIX_local`, but cross-reference URLs keep the registry in the *path* or a
query parameter (`http://identifiers.org/ncbigene/1014`,
`genenames.org/...?hgnc_id=10000`), so naively taking the tail yields a bare
`1014` that names no registry. `curie()` matches those shapes explicitly and
returns `""` for anything that is not a term id; empty ids are skipped.

A regression there writes junk into `xrefs` rather than raising, so it is
checked two ways: `test_every_extracted_id_is_a_valid_curie` offline, and the
`bad_xrefs` counter in `ontology stats`, which is this query:

```cypher
MATCH (t:Term) UNWIND t.xrefs AS x
WITH x WHERE NOT x =~ '^([A-Za-z][A-Za-z0-9]*:[A-Za-z0-9.-]+|CVCL_[A-Za-z0-9]+)$'
RETURN count(x)
```

**Expect 0.** `CVCL_0030` is the documented exception — Cellosaurus issues no
`PREFIX:local` id, so its accession is recorded verbatim.

### `has_children` is a cache

A precomputed boolean, true iff the deep-dive children query would return at
least one child. Computing it at query time means an undirected synonym-cluster
walk per node, which is slow on dense anatomy hubs like `breast` or `brain`. The
graph is static between rebuilds, so we compute it once and the query just reads
the flag.

It can go **stale** but it cannot **lie**: `stamp_has_children` uses the same
definition as the query that reads it, and the reader does
`coalesce(child.has_children, false)`, so a missing or stale flag renders a term
as not-expandable rather than erroring.

> **Re-run `just ontology_has_children` after any load that adds nodes or
> `MAPS_TO`/`SUBCLASS_OF`/`PART_OF` edges.** `ontology build` and
> `ontology load` already do this at the end.

### Synonym transparency

A term's children are everything pointing (via `SUBCLASS_OF`/`PART_OF`) at the
term *or at any node in its `MAPS_TO*0..2` cluster*, excluding the cluster
itself. So synonyms inherit each other's children and a caller never has to know
which spelling is canonical. `_COMPUTE_HAS_CHILDREN` in `src/ontology/graph.py`
encodes this; `deep_dive.py` in `pysradb-server` matches it.

### Indexes

One constraint, `term_name_unique`, on `:Term(name)`. It guarantees no two nodes
share a name and its backing RANGE index serves every lookup and every `MERGE`.
Relationship traversals are pointer chases — no index applies and none is
needed. Do not add others without a measurement.

## What is ingested, and from where

`src/ontology/sources.py` is the registry and the authority. Every URL below was
verified reachable on 2026-08-02, and all of them are stable publisher endpoints
that always serve the current release — there is no version to pin except MeSH.

| Ontology | Contributes | `xrefs` form | Format | Download |
|---|---|---|---|---|
| MONDO | synonyms + hierarchy | `MONDO:0005148` | OBO-JSON | `http://purl.obolibrary.org/obo/mondo.json` |
| Cell Ontology (CL) | synonyms + hierarchy | `CL:0000084` | OBO-JSON | `http://purl.obolibrary.org/obo/cl.json` |
| Uberon | synonyms + hierarchy | `UBERON:0002107` | OBO-JSON | `http://purl.obolibrary.org/obo/uberon/uberon-full.json` |
| EFO | synonyms + hierarchy | `EFO:0000001` | OBO-JSON | `https://github.com/EBISPOT/efo/releases/latest/download/efo.json` |
| ChEBI | synonyms + hierarchy | `CHEBI:15377` | OBO-JSON | `http://purl.obolibrary.org/obo/chebi.json` |
| HGNC | synonyms only | `HGNC:5` | Solr JSON | `https://storage.googleapis.com/public-download-files/hgnc/json/json/hgnc_complete_set.json` |
| Cellosaurus | synonyms only | `CVCL_0030` | XML | `https://ftp.expasy.org/databases/cellosaurus/cellosaurus.xml` |
| MeSH | synonyms + hierarchy | `MeSH:D008099` | NLM descriptor XML (gz) | `https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc2026.gz` |

### Expected output sizes

Synonym pairs extracted from the releases current on 2026-08-02. Use these as a
smoke test: an order-of-magnitude drop means the parser hit a format change, not
that the ontology shrank.

| Ontology | Pairs | | Ontology | Pairs |
|---|---:|---|---|---:|
| MONDO | 76,484 | | ChEBI | 348,725 |
| CL | 38,038 | | HGNC | 152,108 |
| Uberon | 54,038 | | Cellosaurus | 116,340 |
| EFO | 5,038 | | MeSH | 215,889 |

MeSH also yields 31,110 descriptors and 42,519 `is_a` edges.

### Per-source notes

All of these cost real debugging to learn:

- **Uberon** — use `uberon-full.json`, *not* plain `uberon.json`. Only the full
  release keeps the `part_of` (`BFO_0000050`) edges, and anatomy is mostly
  part-of rather than is-a. With the wrong file the anatomy hierarchy is largely
  missing and deep-dive on body parts returns nothing.
- **EFO and ChEBI** ship their imports — large chunks of Uberon, CL and MONDO —
  inside the same file. `Source.id_prefixes` filters node ids down to the
  ontology's own terms so we do not ingest those twice and attribute them to the
  wrong source.
- **MeSH** is the only entry with a version baked into the URL. NLM publishes a
  new edition every November; bump both `url` and `filename` in the registry
  then. The old one keeps working until you do.
- **HGNC and Cellosaurus** are registries, not ontologies — flat lists with no
  usable parent/child structure. They contribute synonyms only, which is what
  `hierarchy=False` means.
- **NCBI Taxonomy is deliberately absent.** Organism is a controlled facet in
  the search UI with its own filter; expanding it through the graph would make
  organism-scoped queries leak across species.

### Which synonyms we keep

Only `hasExactSynonym` and `hasRelatedSynonym`. `hasBroadSynonym` and
`hasNarrowSynonym` both *change what the user asked for* — expanding
`breast cancer` to `cancer` is not a spelling variant — so they are dropped.

On top of that, each source has a `generic_terms` set: real synonyms that are so
generic they would match most of the corpus (`cell`, `tissue`, `disease`). Those
are dropped, and so is anything two characters or shorter — with HGNC excepted
down to one, because gene symbols really are that short (`AR`, `MB`).

## How the code fits together

```
sources.py    the registry: URL, format, parser, filters, build order
    |
extract.py    download -> parse -> two neutral artifacts
    |             expansions/<name>_term_expansions.csv   (short,long,id)
    |             the OBO-JSON whose `edges` array holds the hierarchy
    |
graph.py      MERGE both into Neo4j, stamp xrefs, then stamp has_children
    |
cli/ontology.py   typer commands; justfile recipes wrap these
```

Artifacts land under `$DATA_DIR/ontology/`:

```
raw/         the publisher files, verbatim and unrenamed
expansions/  <name>_term_expansions.csv
derived/     mesh.json   -- OBO-JSON we synthesize; MeSH ships none
```

Whatever a source's native format, it is reduced to those two shapes before it
touches Neo4j. Adding a ninth ontology means adding a `Source` row and, only if
its format is genuinely new, one parser function.

### MeSH needs a synthesized OBO-JSON

MeSH publishes no OBO release, but it *is* a poly-hierarchy, encoded in **tree
numbers**. A descriptor owns one or more strings like `C04.557.470`; each tree
number belongs to exactly one descriptor; and a tree number's parent is that
string minus its last `.NNN` segment. `_extract_mesh` resolves
parent-tree → parent-descriptor and emits `child is_a parent` into
`derived/mesh.json`, which then loads through the same path as everything else.

Top-level tree numbers (no dot) hang off a single-letter category that is not
itself a descriptor, so they come out as roots with no edge. That is correct,
not a bug.

Node ids in the synthesized file are written as `MeSH:<ui>`, not the bare
descriptor ui. The hierarchy load itself would not care — it resolves ids
against the same file — but it also runs `curie()` over them to stamp `xrefs`,
and a bare `D008099` names no registry and would resolve to `""`.

Only **descriptors** are ingested. Supplementary concept records (chemicals) and
qualifiers are skipped on purpose: they overlap ChEBI heavily and would add
~250k mostly-chemical nodes for very little search benefit.

## The MeSH tag, and why order matters

MeSH nodes and edges are tagged — `:MeSH` on the node, `source = 'MeSH'` on the
relationship — **`ON CREATE` only**, so anything the other seven ontologies
already contributed is never relabelled. That makes `:MeSH` mean *"this term is
in the graph only because of MeSH"*, which is exactly what a frontend on/off
toggle needs:

```cypher
// MeSH excluded
MATCH (t:Term {name: $q}) WHERE NOT t:MeSH ...
```

Existing queries match `(:Term {name})` and the bare relationship types without
touching the label, so **MeSH is on by default** and nothing changed when it was
added.

> **This only works because MeSH is loaded last.** The tag marks whatever a
> source creates *first*. Move MeSH earlier in `SOURCES` and it will claim nodes
> that MONDO or CL would have created anyway, and the toggle will start hiding
> terms it should not. If you ever want a toggle for another ontology, set its
> `label` **and** move it to the end of the build order.

The frontend toggle itself is not built yet; the graph is ready for it.

## Operational notes

- **Prod is behind an SSH tunnel.** The connection URI is
  `neo4j://localhost:7687` once the tunnel is up. An older internal note said
  `:7688`; that is wrong for the current deployment. Set `NEO4J_URI` explicitly
  rather than trusting a default.
- **This writes to the live graph.** There is no staging instance. The writes
  are all `MERGE`, so a rebuild is additive and a re-run is a no-op — but a
  rebuild will not *remove* terms that a new ontology release retired. Terms
  deprecated upstream are skipped on the way in, so they never enter; ones
  already in the graph from an earlier release stay. If you need a genuinely
  clean graph, drop the database and rebuild rather than trying to diff.
- **Memory.** `_load_obo_graph` reads each OBO-JSON whole. ChEBI is the worst
  case — a 500 MB file, measured at **1.9 GB peak RSS**. Comfortable on the
  ingest box; switch to streaming JSON if that stops being true. Cellosaurus and
  MeSH are already parsed streaming because their XML dumps are 700 MB and
  340 MB and a full DOM of those is several GB.
- **`ontology stats`** is the sanity check after a rebuild — node count,
  `has_children` count, `with_xrefs`, `bad_xrefs` (expect 0), and one count per
  relationship type.
- **Backfilling `xrefs` onto an existing graph: use `ontology xrefs`, not
  `ontology build`.** This is the one place where "every write is a MERGE" is
  not good enough. `load_synonyms` MERGEs the synonym edges, and a freshly
  regenerated extract never reproduces an older one exactly — when the MONDO
  extractor was rewritten it reproduced 76,445 of its 76,482 pairs, or 99.95%.
  Running a full `build` would have MERGEd the 37 divergent pairs in *beside*
  the originals: not a rebuild, a slow corruption. `ontology xrefs` stamps
  provenance through `MATCH` only and cannot create a node or a relationship, so
  it is safe against a graph whose edges predate the `id` column.

  ```bash
  just ontology_extract          # CSVs on disk; no Neo4j writes
  just ontology_xrefs            # MATCH-only provenance pass
  just ontology_stats            # with_xrefs should jump; bad_xrefs stays 0
  ```

  It needs the extracted CSVs, not the raw downloads. `has_children` is
  unaffected — no edges change — so it does not need re-running afterwards.

## What changed in the move

The old `ontology-scripts` repo had one script per ontology, each a copy of the
last with a different filename and generic-terms list. Consolidating them here:

- The five OBO scripts (`cl.py`, `uberon.py`, `efo.py`, `chebi.py`, and the
  MONDO one) became a single `_extract_obo` driven by the registry. The parsing
  logic and every filter it applies are unchanged.
- **A MONDO synonym extractor now exists.** The old repo had `mondo.json` on
  disk and listed MONDO in its docs, but neither the script that produced its
  synonym CSV nor the CSV itself was in the repo — and that repo had no commits,
  so nothing was recoverable from history. MONDO synonyms are in the live graph,
  so a script existed at some point and was lost. `_extract_obo` covers it now,
  and produces 76,484 pairs from the current release (`als` → `amyotrophic
  lateral sclerosis`, and so on).
- Downloads are scripted. They used to be manual `curl`s whose URLs lived
  nowhere.
- Cellosaurus is parsed with `iterparse` instead of a full DOM, which drops its
  peak memory from several GB.
- Tagging (`:MeSH`) is a per-source registry field rather than a separate
  `ingest_mesh.py`. MeSH remains the only tagged source, so the graph is
  unchanged.
- **`xrefs` provenance is included.** The old repo grew it late, after the
  consolidation had already happened, in `ingest_csv.py` / `ingest_hierarchy.py`
  / `ingest_xrefs_only.py`. Those three are folded into `load_synonyms`,
  `load_hierarchy` and `load_xrefs` here; `curie()` is carried over verbatim,
  because it is load-bearing and its edge cases were found the hard way.

### Verified against the old output

The consolidated extractors were run over the same source files the old scripts
last used, and the resulting CSVs diffed. **CL, Uberon, EFO, ChEBI, HGNC and
Cellosaurus are byte-identical**, as is MeSH's derived hierarchy (31,110 nodes /
42,519 `is_a` edges, exactly matching).

MeSH synonyms differ by **three pairs**, all of which the new code drops:

```
io -> jupiter
ph -> hydrogen ion concentration
un -> united nations
```

The old `mesh.py` was the one script that applied no minimum-length filter, so
these two-character entry terms got in. Expanding every query containing "ph" to
"hydrogen ion concentration" is exactly the noise the length floor exists to
stop, so the new behaviour is intended. Three rows out of 215,892.

One caveat if you re-verify this yourself: the old repo's
`hgnc_term_expansions.csv` was generated in May from an HGNC file that was
re-downloaded in July and never re-extracted, so the committed CSV is stale and
smaller. Running the *old* script against the *current* file reproduces the new
output exactly (152,108 pairs both ways).

Two files in the old repo were **not** ported: `crawl_phrases.csv` / `new.csv`
and `clean_csv.py`. They are a filtered dump of crawled phrases containing
"seq", unrelated to the ontology graph and not loaded into it.

## Downstream consumers (`pysradb-server`)

- `expansions.py` — finds query sub-phrases that exist as `:Term` nodes and
  cross-multiplies their `MAPS_TO` clusters into query variants.
- `deep_dive.py` — `fetch_deep_dive_terms` (which of the query's terms are
  expandable) and `fetch_deep_dive_children` (the direct children of a term,
  each flagged with the precomputed `has_children`).

Both read `:Term.name` with the normalization described above. If you change
`norm()`, you must change it in both repos in the same deploy, and rebuild.
