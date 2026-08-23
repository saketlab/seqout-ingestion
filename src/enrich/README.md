# Enrichment

Extracts metadata fields from archive free text and typed sample columns:
age, sex, ethnicity, phenotype, cell_type, tissue, strain, disease, assay,
cell_line, treatment, development_stage, sample_type, genetic_modification,
plus a derived assay_category.


## Install

    uv sync --extra enrich

You also need a reachable Ollama or vLLM server.

## Run

    .venv/bin/python ./src/main.py enrich batch \
        --accession-file my_batch.csv \
        --work-dir      ~/batches/my_batch \
        --model         rnj-1 \
        --workers       4

Or through just, which fills in the site's DB paths:

    just enrich_batch my_batch.csv --workers 4

`--accession-file` takes a file (one accession per line, or the first column of
a CSV) or a comma-separated list.

Commands:

    enrich batch          enrich, guard, ingest, then corpus and verify
    enrich ingest         load a results dir into SQLite without re-enriching
    enrich prefetch       cache project metadata for an offline worker
    enrich ontology-map   re-map an enriched CSV after the ontology tables change

`--module` selects the wrapper, `enrich.evidence_gate` by default or
`enrich.archive_fields`. `--stage1-only` enriches without ingesting,
`--skip-enrich` reuses CSVs already on disk, and `--expected-taxid` with
`--expected-species` arms the species gate. ENRICH_MODEL, ENRICH_MODULE,
EXPECTED_TAXID and EXPECTED_SPECIES set the same four.


Prompt are in prompts.yaml, keyed `<field>_primary` for the
embedding-matched path and `<field>_fallback` for the description path.
prompts.py loads them and extract.py only references keys, so edit the YAML.


## Configuration

    export ONTOLOGY_DIR=$HOME/ontology_data
    export CELLOSAURUS_LOOKUP=$ONTOLOGY_DIR/cellosaurus_api_lookup.json
    export DISEASE_PRIMARY_TISSUE=$ONTOLOGY_DIR/disease_primary_tissue.json

SEQOUT_API_URL overrides the bulk endpoint, which defaults to
https://seqout.org/api/bulk/project-samples-metadata.

## Stages and output

1. enrich. Runs the selected module per accession and applies the inline guards
   before writing anything. Produces
   `<work-dir>/results/{acc}_{enriched,cellosaurus,provenance,ontology}.csv`,
   plus `{acc}_declared.csv` and `_inline_corrections*.json`.
2. ingest. enrich.store.ingest builds `<work-dir>/<name>.sqlite` holding
   enriched_studies, enriched_samples, cellosaurus_samples, ontology_samples,
   provenance_samples, declared_samples and enrichment_corrections_log.
3. corpus. Reads organism truth from the source archives for rows whose sample
   frame never stated a taxid, logging each as `detector='wrong_organism'`.
4. verify. Re-runs every detector against the batch DB and exits 1 if any
   returns a non-zero count.


declared.py attaches archive declarations to the Project before enrichment, so
some output values come from a submitter rather than the model.

The INSDC triple (library_strategy, library_source, library_selection)
are used as provenance to refine the assay. 

ENA's typed columns (cell_line, cell_type, tissue_type, tissue_lib, disease,
dev_stage, strain, sub_strain, sex, host_sex, age) fill fields. 

## Fleet

The pipeline is also setup to use [yuj](https://github.com/saketlab/yuj) to
distribute jobs to idle desktops.
  
First prefetch the metadata:
    .venv/bin/python ./src/main.py enrich prefetch \
        --accession-file accessions.txt --cache-file metadata_cache.pkl

and then provide the yuj fleet:

yuj.yaml:

```yaml
fleet: fleet.local.csv
job: seqout-enrich
remote_dir: seqout-enrich
results_glob: ~/seqout-enrich/results/*_enriched.csv
stall_min: 120

work_command: >-
  ENRICH_OUT_DIR=$HOME/seqout-enrich/results
  ENRICH_CACHE_FILE=$HOME/seqout-enrich/metadata_cache.pkl
  ENRICH_MODULE=enrich.evidence_gate
  ENRICH_MODEL=rnj-1
  ONTOLOGY_DIR=$HOME/seqout-enrich/ontology
  HF_HOME=$HOME/seqout-enrich/hf HF_HUB_OFFLINE=1
  $HOME/seqout-enrich/.venv/bin/python $HOME/seqout-enrich/enrich/yuj_worker.py
input_file: accessions.txt
output_dir: results
output_suffix: _enriched.csv

scatter:
  input: accessions.txt
  into: accessions.txt

bootstrap:
  env_manager: uv
  python: "3.13"          # not 3.13t/3.14t: torch and sentence-transformers
  extras: [enrich]

deploy:
  code:
    - ../../src/enrich
    - ../../src/helpers.py
    - ../../pyproject.toml
  payload:
    - /path/to/metadata_cache.pkl
    - /path/to/ontology
```

To ingest:

    .venv/bin/python ./src/main.py enrich ingest \
        --csv-dir pulled_results --db-file my_batch.sqlite

