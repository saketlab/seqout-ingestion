set dotenv-load

export PYTHON_GIL := "0"
export DATA_DIR := env("DATA_DIR", "/data/share/seqout")

LOG_LEVEL := env("LOG_LEVEL", "info")
START_DATE := env("START_DATE", "2026-04-28")
END_DATE := env("END_DATE", "2026-05-31")
RETRIES := env("RETRIES", "20")
WORKERS := env("WORKERS", "8")
BATCH_SIZE := env("BATCH_SIZE", "500")
TIMEOUT := env("TIMEOUT", "30")
MAX_TIMEOUT := env("MAX_TIMEOUT", "120")

SRA_DB_FILE := env("SRA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/sra/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/sra/db/$latest/sra_metadata_$latest.db\""))
GEO_DB_FILE := env("GEO_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/geo/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/geo/db/$latest/geo_metadata_$latest.db\""))
ENA_DB_FILE := env("ENA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/ena/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/ena/db/$latest/ena_metadata_$latest.db\""))
AE_DB_FILE := env("AE_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/ae/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/ae/db/$latest/ae_metadata_$latest.db\""))
GSA_DB_FILE := env("GSA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/gsa/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/gsa/db/$latest/gsa_metadata_$latest.db\""))
GSA_HUMAN_DB_FILE := env("GSA_HUMAN_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/gsa_human/db/ | sort -r | head -1) && echo \"" + DATA_DIR + "/gsa_human/db/$latest/gsa_human_metadata_$latest.db\""))
GSA_BIOPROJECT_DB_FILE := env("GSA_BIOPROJECT_DB_FILE", "")
# tolerate missing dirs before first ingest
DRA_DB_FILE := env("DRA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/dra/db/ 2>/dev/null | sort -r | head -1); if [ -n \"$latest\" ]; then echo \"" + DATA_DIR + "/dra/db/$latest/dra_metadata_$latest.db\"; fi"))
GEA_DB_FILE := env("GEA_DB_FILE", shell("latest=$(ls " + DATA_DIR + "/gea/db/ 2>/dev/null | sort -r | head -1); if [ -n \"$latest\" ]; then echo \"" + DATA_DIR + "/gea/db/$latest/gea_metadata_$latest.db\"; fi"))

AGE_SOURCE_DB_FILE := env("AGE_SOURCE_DB_FILE", "/data/share/saket_dump/enrichment/enriched_metadata_v4_combined.sqlite")
AGE_NORM_DB_FILE := env("AGE_NORM_DB_FILE", DATA_DIR + "/age/age_norm.sqlite3")

SC_EVIDENCE_DB_FILE := env("SC_EVIDENCE_DB_FILE", DATA_DIR + "/sc_evidence/single_cell_evidence.db")
SC_DB_DIR := env("SC_DB_DIR", env("HOME") + "/sc_db")
SC_DB_FILE := env("SC_DB_FILE", SC_DB_DIR + "/single_cell.db")
SC_QUEUE_CSV_FILE := env("SC_QUEUE_CSV_FILE", SC_DB_DIR + "/sc_work_queue.csv")
GEO_COMBINED_DB_FILE := env("GEO_COMBINED_DB_FILE", DATA_DIR + "/combined/geo_metadata_combined.db")
AE_COMBINED_DB_FILE := env("AE_COMBINED_DB_FILE", DATA_DIR + "/combined/ae_metadata_combined.db")
NCOUNT_EXTRA_DB_FILE := env("NCOUNT_EXTRA_DB_FILE", shell("ls -1 " + justfile_directory() + "/data/ncount/*.db 2>/dev/null | sort -r | head -1"))
ENA_DELTA_DB_FILE := env("ENA_DELTA_DB_FILE", "")
ENRICHED_DB_FILE := env("ENRICHED_DB_FILE", "/data/share/saket_dump/enrichment/enriched_metadata_v4_combined.sqlite")

UNIFIED_CATALOG_DB_FILE := env("UNIFIED_CATALOG_DB_FILE", DATA_DIR + "/unified_catalog/unified_catalog.db")
UNIFIED_METADATA_DB_FILE := env("UNIFIED_METADATA_DB_FILE", DATA_DIR + "/unified_metadata/unified_metadata.db")
DOWNLOAD_LINKS_DB_FILE := env("DOWNLOAD_LINKS_DB_FILE", DATA_DIR + "/download_links/download_links.db")
PUBMED_METADATA_DB_FILE := env("PUBMED_METADATA_DB_FILE", DATA_DIR + "/pubmed_metadata/pubmed_metadata.db")
CHROMA_DB_DIR := env("CHROMA_DB_DIR", DATA_DIR + "/embed/chroma_db")
NEIGHBORS_DB_FILE := env("NEIGHBORS_DB_FILE", DATA_DIR + "/embed/neighbors.sqlite3")
LABELS_DIR := env("LABELS_DIR", DATA_DIR + "/embed/labels")
KEYWORDS_DIR := env("KEYWORDS_DIR", DATA_DIR + "/embed/keywords")

NCBI_KEYS_FILE := env("NCBI_KEYS_FILE", DATA_DIR + "/keys/ncbi.txt")
OPENALEX_KEYS_FILE := env("OPENALEX_KEYS_FILE", DATA_DIR + "/keys/openalex.txt")
ASSAY_MAPPING_FILE := env("ASSAY_MAPPING_FILE", justfile_directory() + "/data/assay_mapping.csv")
TAXONOMY_MAPPING_FILE := env("TAXONOMY_MAPPING_FILE", DATA_DIR + "/data/ncbi_taxonomy_common_names.csv")
DATA_DIR_LOCAL := env("DATA_DIR_LOCAL", "/data/saket")

ROR_DATA := env("ROR_DATA", DATA_DIR_LOCAL + "/ror/ror_data.json")
GEOCODING_CACHE_FILE := env("GEOCODING_CACHE_FILE", DATA_DIR + "/geocoding/geocoding_cache.json")

PYTHON := ".venv/bin/python"
ENTRYPOINT := "./src/main.py --log-level " + LOG_LEVEL

default:
    @just --list

[private]
_ingest_args := " --output-dir " + DATA_DIR + " --start-date " + START_DATE + " --end-date " + END_DATE + " --workers " + WORKERS + " --retries " + RETRIES
[private]
_all_db_args := "--sra-db-file " + SRA_DB_FILE + " --geo-db-file " + GEO_DB_FILE + " --ena-db-file " + ENA_DB_FILE + " --ae-db-file " + AE_DB_FILE + " "
[private]
_gsa_db_args := "--gsa-db-file " + GSA_DB_FILE + " --gsa-human-db-file " + GSA_HUMAN_DB_FILE + " " + (if GSA_BIOPROJECT_DB_FILE != "" { "--gsa-bioproject-db-file " + GSA_BIOPROJECT_DB_FILE + " " } else { "" })
[private]
_sc_extra_db_args := (if ENA_DELTA_DB_FILE != "" { "--ena-delta-db-file " + ENA_DELTA_DB_FILE + " " } else { "" }) + (if ENRICHED_DB_FILE != "" { "--enriched-db-file " + ENRICHED_DB_FILE + " " } else { "" })
[private]
_ddbj_db_args := "--dra-db-file " + DRA_DB_FILE + " --gea-db-file " + GEA_DB_FILE + " "
[private]
_transfer_args := _all_db_args + " --pubmed-metadata-db-file " + PUBMED_METADATA_DB_FILE + " --unified-metadata-db-file " + UNIFIED_METADATA_DB_FILE + " --download-links-db-file " + DOWNLOAD_LINKS_DB_FILE + " --read-batch-size " + BATCH_SIZE + " --workers " + WORKERS + " "
[private]
_embed_db_args := "--geo-db-file " + GEO_DB_FILE + " --sra-db-file " + SRA_DB_FILE + " --ae-db-file " + AE_DB_FILE + " --unified-metadata-db-file " + UNIFIED_METADATA_DB_FILE + " --chroma-db-dir " + CHROMA_DB_DIR + " "
[private]
_tox_cuda := "tox run -e cuda -- "

[group("ingest")]
ingest_sra *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest sra \
        {{ _ingest_args }} \
        --write-batch-size {{ BATCH_SIZE }} \
        {{ flags }}

[group("ingest")]
ingest_dra *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest dra \
        {{ _ingest_args }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        --ena-db-file {{ ENA_DB_FILE }} \
        {{ flags }}

[group("ingest")]
ingest_dra_all *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest dra \
        --output-dir {{ DATA_DIR }} \
        --start-date 2007-01-01 \
        --end-date $(date -u +%F) \
        --workers {{ WORKERS }} \
        --retries {{ RETRIES }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        --ena-db-file {{ ENA_DB_FILE }} \
        {{ flags }}

[group("ingest")]
ingest_gea *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gea \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_geo *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest geo \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_ena *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest ena \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_ae *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest ae \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_gsa *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gsa \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_gsa_human *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gsa-human \
        {{ _ingest_args }} \
        {{ flags }}

[group("ingest")]
ingest_gsa_bioproject *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ingest gsa-bioproject \
        --output-dir {{ DATA_DIR }} --workers {{ WORKERS }} --retries {{ RETRIES }} \
        {{ _gsa_db_args }} \
        {{ flags }}

[group("backfill")]
backfill_download_links *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill download-links \
        --output-dir {{ DATA_DIR }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        --ena-db-file {{ ENA_DB_FILE }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --workers {{ WORKERS }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

[group("backfill")]
backfill_dra_download_links *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill dra-download-links \
        --output-dir {{ DATA_DIR }} \
        --dra-db-file {{ DRA_DB_FILE }} \
        --download-links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --workers {{ WORKERS }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

PREFLIGHT_TIER := env("PREFLIGHT_TIER", "high")
PMID_RETRY_MANIFEST := env("PMID_RETRY_MANIFEST", DATA_DIR + "/pmid_retry/manifest.txt")
PMID_RETRY_DB_FILE := env("PMID_RETRY_DB_FILE", DATA_DIR + "/pmid_retry/pmid_retry.db")
PMID_RETRY_RESULTS_DIR := env("PMID_RETRY_RESULTS_DIR", env("HOME") + "/pmid_retry_pull")

SC_STATS_DB_FILE := env("SC_STATS_DB_FILE", DATA_DIR + "/sc_stats/sc_stats.db")
# Change this if required
SC_STATS_RESULTS_DIR := env("SC_STATS_RESULTS_DIR", env("PULL_ROOT", "/data/saket") + "/sc_stats/results")

[group("sc_stats")]
sc_stats_ingest *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_stats ingest \
        --results-dir {{ SC_STATS_RESULTS_DIR }} \
        --db-file {{ SC_STATS_DB_FILE }} \
        {{ flags }}

[group("pmid_retry")]
pmid_retry_gaps *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pmid_retry gaps \
        --pg-url "$PG_URL" \
        --manifest-file {{ PMID_RETRY_MANIFEST }} \
        --exclude-db-file {{ PMID_RETRY_DB_FILE }} \
        {{ flags }}

[group("pmid_retry")]
pmid_retry_fetch *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pmid_retry fetch \
        --manifest-file {{ PMID_RETRY_MANIFEST }} \
        --out-dir {{ PMID_RETRY_RESULTS_DIR }} \
        --keys-file {{ NCBI_KEYS_FILE }} \
        {{ flags }}

[group("pmid_retry")]
pmid_retry_ingest *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pmid_retry ingest \
        --results-dir {{ PMID_RETRY_RESULTS_DIR }} \
        --db-file {{ PMID_RETRY_DB_FILE }} \
        {{ flags }}

[group("pmid_retry")]
pmid_retry_verify *flags:
    NCBI_KEYS_FILE={{ NCBI_KEYS_FILE }} {{ PYTHON }} {{ ENTRYPOINT }} pmid_retry verify \
        --db-file {{ PMID_RETRY_DB_FILE }} \
        {{ flags }}

[group("pmid_retry")]
pmid_retry_transfer *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pmid_retry transfer \
        --db-file {{ PMID_RETRY_DB_FILE }} \
        --pg-url "$PG_URL" \
        {{ flags }}

[group("backfill")]
backfill_ddbj *flags:
    just backfill_pm_ids {{ flags }}
    just backfill_pm_meta --fill-metrics {{ flags }}
    just backfill_dra_download_links {{ flags }}

[group("backfill")]
backfill_pm_ids *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-ids \
        {{ _all_db_args }} \
        {{ _ddbj_db_args }} \
        {{ _gsa_db_args }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

[group("backfill")]
backfill_citation_pmids *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill citation-pmids \
        --geo-db-file {{ GEO_DB_FILE }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

[group("backfill")]
backfill_pm_meta *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} backfill pm-meta \
        --output-dir {{ DATA_DIR }} \
        {{ _all_db_args }} \
        {{ _ddbj_db_args }} \
        {{ _gsa_db_args }} \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --openalex-keys-file {{ OPENALEX_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

[group("unify")]
unify_build_catalog *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} unify build-catalog \
        --output-dir {{ DATA_DIR }} \
        {{ _all_db_args }} \
        {{ _gsa_db_args }} \
        {{ _ddbj_db_args }} \
        {{ flags }}

[group("unify")]
unify_build_metadata *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} unify build-metadata \
        --output-dir {{ DATA_DIR }} \
        {{ _all_db_args }} \
        {{ _gsa_db_args }} \
        {{ _ddbj_db_args }} \
        --unified-catalog-db-file {{ UNIFIED_CATALOG_DB_FILE }} \
        --pubmed-metadata-db-file {{ PUBMED_METADATA_DB_FILE }} \
        --assay-mapping-csv-file {{ ASSAY_MAPPING_FILE }} \
        --taxonomy-mapping-csv-file {{ TAXONOMY_MAPPING_FILE }} \
        {{ flags }}

[group("geocode")]
geocode *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} geocode \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --geo-db-file {{ GEO_DB_FILE }} \
        --ror-data-file {{ ROR_DATA }} \
        --cache-file {{ GEOCODING_CACHE_FILE }} \
        --save-every {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        {{ flags }}

[group("sc_enrich")]
sc_enrich_classify *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich classify \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --assay-mapping-csv-file {{ ASSAY_MAPPING_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        {{ flags }}

[group("sc_enrich")]
sc_enrich_yuj_setup *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_enrich counter-setup \
        --unified-metadata-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --geo-db-file {{ GEO_DB_FILE }} \
        --ae-db-file {{ AE_DB_FILE }} \
        --output-dir {{ DATA_DIR }}/yuj_setup \
        {{ flags }}

[group("sc_evidence")]
sc_evidence_build *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_evidence build \
        --sc-evidence-db-file {{ SC_EVIDENCE_DB_FILE }} \
        {{ _all_db_args }} \
        {{ _gsa_db_args }} \
        {{ _ddbj_db_args }} \
        {{ _sc_extra_db_args }} \
        --batch-size {{ BATCH_SIZE }} \
        {{ flags }}

[group("sc_evidence")]
sc_evidence_coverage *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_evidence coverage \
        --sc-evidence-db-file {{ SC_EVIDENCE_DB_FILE }} \
        {{ _all_db_args }} \
        {{ _gsa_db_args }} \
        {{ _ddbj_db_args }} \
        {{ _sc_extra_db_args }} \
        {{ flags }}

[group("sc_evidence")]
sc_db_build *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_db build \
        --sc-db-file {{ SC_DB_FILE }} \
        --sc-evidence-db-file {{ SC_EVIDENCE_DB_FILE }} \
        --pg-url "$PG_URL" \
        --geo-db-file {{ GEO_COMBINED_DB_FILE }} \
        --ae-db-file {{ AE_COMBINED_DB_FILE }} \
        --ncount-extra-db-file {{ NCOUNT_EXTRA_DB_FILE }} \
        --queue-csv-file {{ SC_QUEUE_CSV_FILE }} \
        {{ flags }}

[group("sc_evidence")]
sc_db_summary *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_db summary --sc-db-file {{ SC_DB_FILE }} {{ flags }}

[group("transfer")]
transfer_postgres *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer postgres \
        --pg-url "$PG_URL" \
        {{ _transfer_args }} \
        {{ _gsa_db_args }} \
        {{ flags }}

[group("transfer")]
transfer_sc_stats *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer sc-stats \
        --pg-url "$PG_URL" \
        --sc-stats-db-file {{ SC_STATS_DB_FILE }} \
        {{ flags }}

[group("pentimento")]
transfer_pentimento *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer pentimento \
        --pg-url "$PG_URL" \
        --pentimento-db-file {{ PENTIMENTO_DB_FILE }} \
        --non-nulling \
        {{ flags }}

[group("transfer")]
transfer_ddbj *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer ddbj \
        --pg-url "$PG_URL" \
        {{ _ddbj_db_args }} \
        --download-links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --read-batch-size {{ BATCH_SIZE }} \
        {{ flags }}

[group("transfer")]
transfer_ena_organisms *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer ena-organisms \
        --pg-url "$PG_URL" --ena-db-file {{ ENA_DB_FILE }} {{ flags }}

[group("transfer")]
transfer_dra_organisms *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer dra-organisms \
        --pg-url "$PG_URL" --dra-db-file {{ DRA_DB_FILE }} {{ flags }}

[group("transfer")]
transfer_gea_organisms *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer gea-organisms \
        --pg-url "$PG_URL" --gea-db-file {{ GEA_DB_FILE }} {{ flags }}

[group("transfer")]
transfer_parquet *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer parquet \
        {{ _transfer_args }} \
        {{ flags }}

[group("ontology")]
ontology_build *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology build \
        --output-dir {{ DATA_DIR }} \
        {{ flags }}

[group("ontology")]
ontology_download *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology download --output-dir {{ DATA_DIR }} {{ flags }}

[group("ontology")]
ontology_extract *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology extract --output-dir {{ DATA_DIR }} {{ flags }}

[group("ontology")]
ontology_load *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology load --output-dir {{ DATA_DIR }} {{ flags }}

[group("ontology")]
ontology_xrefs *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology xrefs --output-dir {{ DATA_DIR }} {{ flags }}

[group("ontology")]
ontology_has_children *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology has-children {{ flags }}

[group("ontology")]
ontology_stats *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} ontology stats {{ flags }}

[group("age")]
age_normalize *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} age normalize \
        --source-db-file {{ AGE_SOURCE_DB_FILE }} \
        --output-dir {{ DATA_DIR }} \
        --batch-size {{ BATCH_SIZE }} \
        {{ flags }}

[group("transfer")]
transfer_age *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer age \
        --pg-url "$PG_URL" \
        --age-norm-db-file {{ AGE_NORM_DB_FILE }} \
        {{ flags }}

[group("embed")]
embed_run *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed run \
        {{ _embed_db_args }} \
        {{ flags }}

[group("embed")]
embed_coords *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed coords \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

[group("embed")]
embed_cluster *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed cluster \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

[group("embed")]
embed_cluster_leiden *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed cluster-leiden \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

[group("embed")]
embed_neighbors *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed neighbors \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --neighbors-db-file {{ NEIGHBORS_DB_FILE }} \
        {{ flags }}

[group("embed")]
embed_labels *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed labels \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

[group("embed")]
embed_cluster_leiden_coarse *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed cluster-leiden-coarse \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        {{ flags }}

# Phase 1 of labelling: cache TF-IDF keywords. 
[group("embed")]
embed_precompute_keywords *flags:
    {{ _tox_cuda }} {{ PYTHON }} {{ ENTRYPOINT }} embed precompute-keywords \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --keywords-dir {{ KEYWORDS_DIR }} \
        {{ flags }}

# Phase 2 of labelling: LLM-label from the cache.
[group("embed")]
embed_label_from_keywords *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} embed label-from-keywords \
        --keywords-dir {{ KEYWORDS_DIR }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

[group("transfer")]
transfer_embed *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer embed \
        --pg-url "$PG_URL" \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --neighbors-db-file {{ NEIGHBORS_DB_FILE }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

[group("transfer")]
transfer_leiden_swap *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} transfer leiden-swap \
        --pg-url "$PG_URL" \
        --chroma-db-dir {{ CHROMA_DB_DIR }} \
        --labels-dir {{ LABELS_DIR }} \
        {{ flags }}

# Weekly publication refresh. 
[group("refresh")]
refresh_publications *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} refresh publications \
        --pg-url "$PG_URL" \
        --ncbi-keys-file {{ NCBI_KEYS_FILE }} \
        --openalex-keys-file {{ OPENALEX_KEYS_FILE }} \
        --batch-size {{ BATCH_SIZE }} \
        --retries {{ RETRIES }} \
        --timeout {{ TIMEOUT }} \
        {{ flags }}

# Build study_pub_dates by hand; refresh_publications rebuilds it weekly.
[group("refresh")]
refresh_pub_dates: _pg_tunnel
    psql "{{ PG_SOCKET_URL }}" -v ON_ERROR_STOP=1 \
        -f sql/tables/study_pub_dates_pg.sql \
        -c "REFRESH MATERIALIZED VIEW CONCURRENTLY study_pub_dates"

# Rebuild tissue_microbe_rates behind /stats/tissue-microbes; rerun after a quant ingest.
[group("refresh")]
refresh_tissue_microbes: _pg_tunnel
    psql "{{ PG_SOCKET_URL }}" -v ON_ERROR_STOP=1 --single-transaction \
        -f sql/tables/tissue_microbe_rates_pg.sql \
        -c "SELECT count(*) AS cells FROM tissue_microbe_rates"
    curl -fsS -X POST "${SEQOUT_API_BASE:-https://seqout.org/api}/cache/purge" >/dev/null \
        && echo "cache purged"

# Rebuild sc_quality_by_year behind /stats/sc-quality; rerun after an sc_stats sweep.
[group("refresh")]
refresh_sc_quality: _pg_tunnel
    psql "{{ PG_SOCKET_URL }}" -v ON_ERROR_STOP=1 --single-transaction \
        -f sql/tables/sc_quality_by_year_pg.sql \
        -c "SELECT count(*) AS cells FROM sc_quality_by_year"
    curl -fsS -X POST "${SEQOUT_API_BASE:-https://seqout.org/api}/cache/purge" >/dev/null \
        && echo "cache purged"

[group("refresh")]
refresh_status:
    {{ PYTHON }} ./src/main.py --log-level warning refresh status --pg-url "$PG_URL"
PENTIMENTO_DB_FILE := env("PENTIMENTO_DB_FILE", DATA_DIR + "/pentimento/pentimento.db")
NCOUNT_PERSAMPLE_DIR := env("NCOUNT_PERSAMPLE_DIR", "/data/share/ncount_persample_July14")
PREFLIGHTX_DB_FILE := env("PREFLIGHTX_DB_FILE", "/home/saket/enrichment/enriched_metadata_v4_combined_July14.sqlite")
PREFLIGHTX_CSV_DIR := env("PREFLIGHTX_CSV_DIR", "/NAS/saket/github/pysradb-ingestion/sc_preflight_v4/annotated")
VIRUS_QUANT_DIR := env("VIRUS_QUANT_DIR", "/home/saket/tissue_pull")
PREFLIGHTX_BENCHMARK_FILE := env("PREFLIGHTX_BENCHMARK_FILE", "/NAS/saket/github/pysradb-ingestion/mukesh_single_cell_preflightx/preflightx_results/preflight_per_experiment.csv")

[group("pentimento")]
pentimento_build *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento build \
        --output-db-file {{ PENTIMENTO_DB_FILE }} \
        --links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --ncount-dir {{ NCOUNT_PERSAMPLE_DIR }} \
        --preflightx-db-file {{ PREFLIGHTX_DB_FILE }} \
        --preflightx-csv-dir {{ PREFLIGHTX_CSV_DIR }} \
        --quant-dir {{ VIRUS_QUANT_DIR }} \
        --quant-archive-db-file {{ QUANT_ARCHIVE_DB_FILE }} \
        --benchmark-file {{ PREFLIGHTX_BENCHMARK_FILE }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        {{ flags }}

PREFLIGHT_MANIFEST_DIR := env("PREFLIGHT_MANIFEST_DIR", DATA_DIR + "/pentimento/preflight/manifests")
PREFLIGHT_ANNOTATED_DIR := env("PREFLIGHT_ANNOTATED_DIR", DATA_DIR + "/pentimento/preflight/annotated")

[group("pentimento")]
preflight_manifest *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight manifest \
        --out-dir {{ PREFLIGHT_MANIFEST_DIR }} \
        --links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --unified-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        {{ flags }}

[group("pentimento")]
preflight_run *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight run \
        --in-dir {{ PREFLIGHT_MANIFEST_DIR }} \
        --out-dir {{ PREFLIGHT_ANNOTATED_DIR }} \
        {{ flags }}

[group("pentimento")]
preflight_ingest *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight ingest \
        --db-file {{ PENTIMENTO_DB_FILE }} \
        --preflightx-csv-dir {{ PREFLIGHT_ANNOTATED_DIR }} \
        --sra-db-file {{ SRA_DB_FILE }} \
        {{ flags }}

PREFLIGHTX_VERSION := env("PREFLIGHTX_VERSION", "v0.3.1")
PREFLIGHTX_TARGET := env("PREFLIGHTX_TARGET", "x86_64-unknown-linux-gnu")

[group("backfill")]
fetch_preflightx:
    mkdir -p {{ QUANT_DIR }}/bin
    cd {{ QUANT_DIR }}/bin && \
      gh release download {{ PREFLIGHTX_VERSION }} --repo saketlab/preflightx \
        --pattern 'preflightx-{{ PREFLIGHTX_VERSION }}-{{ PREFLIGHTX_TARGET }}.*' \
        --clobber && \
      sha256sum -c preflightx-{{ PREFLIGHTX_VERSION }}-{{ PREFLIGHTX_TARGET }}.sha256 && \
      tar xzf preflightx-{{ PREFLIGHTX_VERSION }}-{{ PREFLIGHTX_TARGET }}.tar.gz
    {{ QUANT_DIR }}/bin/preflightx --version

GAPS_DIR := env("GAPS_DIR", QUANT_DIR + "/gaps")
MAX_SAMPLES := env("MAX_SAMPLES", "500")
FLEET_SRC := env("FLEET_SRC", env("HOME") + "/fleet.csv")
FLEET_LOCAL := "yuj/fleet.local.csv"  

[group("backfill")]
gaps *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento gaps \
        --pg-url "$PG_URL" \
        --out-dir {{ GAPS_DIR }} \
        --max-samples {{ MAX_SAMPLES }} \
        --links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --unified-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        {{ flags }}

[private]
_fleet:
    @mkdir -p yuj
    @command cp -f {{ FLEET_SRC }} {{ FLEET_LOCAL }} && chmod 600 {{ FLEET_LOCAL }}
    @echo "fleet copied to {{ FLEET_LOCAL }} (edit this, never {{ FLEET_SRC }})"

[group("sc_evidence")]
sc_db_export bucket *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} sc_db export-accessions \
        --sc-db-file {{ SC_DB_FILE }} \
        --bucket {{ bucket }} \
        --out-file {{ GAPS_DIR }}/queue_{{ bucket }}.txt \
        {{ flags }}

[group("backfill")]
backfill_preflight_queue *flags: _fleet
    just sc_db_export preflightx --is-by-next-stage
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight manifest \
        --out-dir {{ QUANT_DIR }}/preflight_manifests_v2 \
        --links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --unified-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --accession-file {{ GAPS_DIR }}/queue_preflightx.txt
    {{ PYTHON }} {{ ENTRYPOINT }} sc_db export-accessions \
        --sc-db-file {{ SC_DB_FILE }} \
        --bucket preflightx --is-by-next-stage \
        --evidence-tier {{ PREFLIGHT_TIER }} \
        --manifest-dir {{ QUANT_DIR }}/preflight_manifests_v2 \
        --out-file {{ GAPS_DIR }}/scatter_preflightx.txt {{ flags }}
    command cp -f {{ GAPS_DIR }}/scatter_preflightx.txt yuj/preflight/studies.txt
    cd yuj/preflight && yuj scatter && yuj run

[group("backfill")]
backfill_preflight *flags: _fleet
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight manifest \
        --out-dir {{ QUANT_DIR }}/preflight_manifests_v2 \
        --links-db-file {{ DOWNLOAD_LINKS_DB_FILE }} \
        --unified-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        --accession-file {{ GAPS_DIR }}/sex-assay.txt {{ flags }}
    command cp -f {{ GAPS_DIR }}/sex-assay.txt yuj/preflight/studies.txt
    cd yuj/preflight && yuj scatter && yuj run

[group("backfill")]
backfill_collect: _fleet
    cd yuj/preflight && yuj pull
    cd yuj/quant && yuj pull
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento preflight ingest \
        --db-file {{ PENTIMENTO_DB_FILE }} \
        --preflightx-csv-dir {{ QUANT_DIR }}/preflight_annotated \
        --sra-db-file {{ SRA_DB_FILE }}

QUANT_DIR := env("QUANT_DIR", env("HOME") + "/quant")

QUANT_ARCHIVE_DB_FILE := env("QUANT_ARCHIVE_DB_FILE", QUANT_DIR + "/quant_archive.db")

[group("pentimento")]
quant_archive *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento quant-archive \
        --quant-dir {{ VIRUS_QUANT_DIR }} \
        --archive-db-file {{ QUANT_ARCHIVE_DB_FILE }} \
        {{ flags }}

PENTIMENTO_PARQUET_DIR := env("PENTIMENTO_PARQUET_DIR", DATA_DIR + "/pentimento/parquet")

[group("pentimento")]
pentimento_parquet *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} pentimento export-parquet \
        --db-file {{ PENTIMENTO_DB_FILE }} \
        --output-dir {{ PENTIMENTO_PARQUET_DIR }} \
        {{ flags }}

GARD_DB_FILE := env("GARD_DB_FILE", QUANT_DIR + "/gard/gard.db")
NORD_DB_FILE := env("NORD_DB_FILE", QUANT_DIR + "/nord/nord.db")
NORD_CSV_FILE := env("NORD_CSV_FILE", QUANT_DIR + "/nord/rare_diseases.csv")
INHERITANCE_DB_FILE := env("INHERITANCE_DB_FILE", QUANT_DIR + "/inheritance/inheritance.db")
GARD_ONTOLOGY_DIR := env("GARD_ONTOLOGY_DIR", QUANT_DIR + "/ontology")

PG_SSH_HOST := env("PG_SSH_HOST", "sramap@167.114.208.177")
PG_SOCKET_PORT := env("PG_SOCKET_PORT", "15433")
PG_SOCKET_URL := "postgresql://sramap@127.0.0.1:" + PG_SOCKET_PORT + "/pysradb"
SERVER_REPO := env("SERVER_REPO", justfile_directory() + "/../pysradb-server")

[private]
_pg_tunnel:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! (exec 3<>/dev/tcp/127.0.0.1/{{ PG_SOCKET_PORT }}) 2>/dev/null; then
        echo "opening tunnel 127.0.0.1:{{ PG_SOCKET_PORT }} -> {{ PG_SSH_HOST }}"
        ssh -f -N -o BatchMode=yes -o ExitOnForwardFailure=yes \
            -L {{ PG_SOCKET_PORT }}:/var/run/postgresql/.s.PGSQL.5432 {{ PG_SSH_HOST }}
    fi

[group("gard")]
gard_fetch *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} gard fetch \
        --output-db-file {{ GARD_DB_FILE }} \
        --ontology-dir {{ GARD_ONTOLOGY_DIR }} \
        {{ flags }}

[group("gard")]
gard_transfer *flags: _pg_tunnel
    {{ PYTHON }} {{ ENTRYPOINT }} transfer gard \
        --pg-url "{{ PG_SOCKET_URL }}" \
        --gard-db-file {{ GARD_DB_FILE }} \
        {{ flags }}

[group("gard")]
gard_inheritance *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} gard inheritance \
        --gard-db-file {{ GARD_DB_FILE }} \
        {{ flags }}

[group("gard")]
gard_index: _pg_tunnel
    psql "{{ PG_SOCKET_URL }}" -v ON_ERROR_STOP=1 \
        -f sql/indexes/ontology_ancestors_pg.sql

[group("disease")]
disease_matview collection: _pg_tunnel
    cd {{ SERVER_REPO }} && DATABASE_URL="{{ PG_SOCKET_URL }}" \
        bash scripts/create_rare_disease_matview.sh {{ collection }}

[group("disease")]
alias_matview: _pg_tunnel
    cd {{ SERVER_REPO }} && DATABASE_URL="{{ PG_SOCKET_URL }}" \
        bash scripts/create_disease_alias_matview.sh

[group("disease")]
inheritance_matview: _pg_tunnel
    cd {{ SERVER_REPO }} && DATABASE_URL="{{ PG_SOCKET_URL }}" \
        bash scripts/create_disease_inheritance_matview.sh

[group("gard")]
downloads_matview: _pg_tunnel
    cd {{ SERVER_REPO }} && DATABASE_URL="{{ PG_SOCKET_URL }}" \
        bash scripts/create_study_download_matview.sh

[group("gard")]
gard_matview: _pg_tunnel (disease_matview "gard")

[group("disease")]
disease_all: gard_fetch gard_inheritance gard_transfer \
             nord_fetch nord_load nord_transfer \
             inheritance_fetch inheritance_transfer \
             gard_index alias_matview inheritance_matview downloads_matview \
             (disease_matview "gard") (disease_matview "nord")
    @echo "disease stack rebuilt: /disease/{rare,nord}/{summary,facets,projects}"

[group("gard")]
gard_all: gard_fetch gard_inheritance gard_transfer gard_index alias_matview inheritance_matview downloads_matview gard_matview
    @echo "rare-disease stack rebuilt: /disease/rare/{summary,facets,projects}"

[group("nord")]
nord_fetch *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} nord fetch \
        --output-csv-file {{ NORD_CSV_FILE }} \
        --ontology-dir {{ GARD_ONTOLOGY_DIR }} \
        {{ flags }}

[group("nord")]
nord_load *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} nord load \
        --csv-file {{ NORD_CSV_FILE }} \
        --output-db-file {{ NORD_DB_FILE }} \
        {{ flags }}

[group("nord")]
nord_transfer *flags: _pg_tunnel
    {{ PYTHON }} {{ ENTRYPOINT }} transfer nord \
        --pg-url "{{ PG_SOCKET_URL }}" \
        --nord-db-file {{ NORD_DB_FILE }} \
        {{ flags }}

[group("nord")]
nord_matview: _pg_tunnel (disease_matview "nord")

[group("nord")]
nord_all: nord_fetch nord_load nord_transfer gard_index alias_matview inheritance_matview downloads_matview nord_matview
    @echo "nord stack rebuilt: /disease/nord/{summary,facets,projects}"

[group("inheritance")]
inheritance_fetch *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} inheritance fetch \
        --output-db-file {{ INHERITANCE_DB_FILE }} \
        --ontology-dir {{ GARD_ONTOLOGY_DIR }} \
        {{ flags }}

[group("inheritance")]
inheritance_transfer *flags: _pg_tunnel
    {{ PYTHON }} {{ ENTRYPOINT }} transfer inheritance \
        --pg-url "{{ PG_SOCKET_URL }}" \
        --inheritance-db-file {{ INHERITANCE_DB_FILE }} \
        {{ flags }}

[group("inheritance")]
inheritance_all: inheritance_fetch inheritance_transfer inheritance_matview
    @echo "inheritance rebuilt into disease_inheritance_all; run disease_all to roll it up"


ENRICH_WORK_DIR := env("ENRICH_WORK_DIR", DATA_DIR + "/enrich")
ENRICH_MODEL := env("ENRICH_MODEL", "rnj-1")

[group("enrich")]
enrich_batch ACCESSIONS *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} enrich batch \
        --accession-file {{ ACCESSIONS }} \
        --work-dir {{ ENRICH_WORK_DIR }} \
        --model {{ ENRICH_MODEL }} \
        --unified-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        {{ flags }}

[group("enrich")]
enrich_prefetch ACCESSIONS CACHE *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} enrich prefetch \
        --accession-file {{ ACCESSIONS }} --cache-file {{ CACHE }} {{ flags }}

[group("enrich")]
enrich_ingest CSV_DIR DB *flags:
    {{ PYTHON }} {{ ENTRYPOINT }} enrich ingest \
        --csv-dir {{ CSV_DIR }} --db-file {{ DB }} \
        --unified-db-file {{ UNIFIED_METADATA_DB_FILE }} \
        {{ flags }}
