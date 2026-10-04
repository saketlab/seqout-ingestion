"""
CLI for LLM metadata enrichment.

`enrich batch` runs enrich, guard, ingest and optional verify. Database paths
are site-specific; unset archive mirrors degrade to empty inputs.
"""

import json
import logging
from pathlib import Path

import typer

enrich_app = typer.Typer(help="LLM metadata enrichment (GEO / SRA / ENA / DDBJ).")

logger = logging.getLogger(__name__)


@enrich_app.command()
def batch(
    accession_file: Path = typer.Option(
        ...,
        help="one accession per line, or the first column of a CSV. A "
        "comma-separated list also works",
    ),
    work_dir: Path = typer.Option(..., help="receives results/ and the batch sqlite"),
    db_file: Path = typer.Option(
        None, help="batch sqlite; default <work-dir>/<work-dir name>.sqlite"
    ),
    model: str = typer.Option(
        "rnj-1",
        envvar="ENRICH_MODEL",
        help="Ollama model name, or vllm/<model> for an OpenAI-compatible server",
    ),
    module: str = typer.Option(
        "enrich.evidence_gate",
        envvar="ENRICH_MODULE",
        help="which wrapper to run: enrich.evidence_gate (the cell-line evidence "
        "gate, recommended) or enrich.archive_fields (the older tissue-overlap "
        "guard). This is the value a fleet job sets as ENRICH_MODULE",
    ),
    embedding_model: str = typer.Option("abhinand/MedEmbed-large-v0.1"),
    cache_file: Path = typer.Option(
        None,
        help="metadata_cache.pkl prefetched by whoever scattered the work. "
        "Without it the seqout bulk endpoint is called, which a compute node "
        "usually cannot reach",
    ),
    unified_db_file: Path = typer.Option(
        None,
        help="unified_metadata.db, only for study titles. Omitted means NULL "
        "titles, never a failure",
    ),
    tierc_policy: str = typer.Option(
        "evidence",
        help="evidence (default): adjudicate a cell line by whether the study "
        "NAMES it. legacy: also NULL every tissue-overlap mismatch, reproducing "
        "the post-hoc tier including its 3:1 false-positive rate",
    ),
    vocab: str = typer.Option("v5", help="v5 | v4compat -- the assay vocabulary"),
    expected_taxid: str = typer.Option(
        None,
        envvar="EXPECTED_TAXID",
        help="species gate on the Cellosaurus override. The lookup table is "
        "overwhelmingly human, so a mouse line name that collides with a human "
        "entry would inherit the human donor's demographics. Unset leaves it "
        "inert",
    ),
    expected_species: str = typer.Option(None, help="as --expected-taxid, by name"),
    workers: int = typer.Option(
        1,
        help="parallel enrichment processes for stage 1. Cap at the server's "
        "OLLAMA_NUM_PARALLEL -- more clients than it serves concurrently only "
        "queues requests and multiplies the embedding-model load",
    ),
    tier_scripts_dir: Path = typer.Option(
        None,
        help="directory holding the corpus-level tier scripts (detect_h9_v4.py "
        "and friends). They are NOT part of this package; without this, stages "
        "3 and 4 are skipped and the run says so",
    ),
    declared: bool = typer.Option(
        default=True,
        help="read ENA's typed sample columns and the INSDC library triple. "
        "Needs ENA_DB / SRA_DB; without them it finds nothing and costs nothing",
    ),
    skip_enrich: bool = typer.Option(
        default=False, help="reuse the CSVs already in <work-dir>/results"
    ),
    stage1_only: bool = typer.Option(
        default=False, help="enrich only; used internally by --workers"
    ),
    embedding_only: bool = typer.Option(
        default=False,
        help="no LLM call anywhere -- no Ollama/vLLM server needed. A field "
        "with a directly matched column keeps that column's raw value "
        "verbatim; everything else stays NULL. Ingests into the _fast twin "
        "of every table, never the trusted ones a normal run writes. Stages "
        "3/4 (organism truth, detector verify) are skipped: they check the "
        "trusted tables, which this run never touches. See "
        "docs/superpowers/specs/2026-09-14-embedding-only-enrichment.md",
    ),
    verbose: bool = typer.Option(default=False),
) -> None:
    """
    Enrich a batch into local SQLite.

    Ingest uses primary-key upserts; guards rewrite DataFrames before insert.
    """
    from enrich import batch as batch_mod
    from enrich import store

    work_dir = work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    csv_dir = work_dir / "results"
    db_file = db_file or work_dir / f"{work_dir.name}.sqlite"

    species = {}
    if expected_taxid:
        species["expected_taxid"] = expected_taxid
    if expected_species:
        species["expected_species"] = expected_species

    accessions = batch_mod.read_accessions(accession_file)
    logger.info(
        "batch %s: %d accessions, module=%s model=%s db=%s tier-C=%s",
        work_dir.name,
        len(accessions),
        module,
        model,
        db_file,
        tierc_policy,
    )

    if skip_enrich:
        logger.info("1/4 enrich -- SKIPPED, reusing CSVs")
        audits = batch_mod.collect_audits(csv_dir)
    elif workers > 1:
        logger.info("1/4 enrich + inline guards (%d shards)", workers)
        passthrough = [
            "--work-dir",
            str(work_dir),
            "--model",
            model,
            "--module",
            module,
            "--embedding-model",
            embedding_model,
            "--tierc-policy",
            tierc_policy,
            "--vocab",
            vocab,
        ]
        if cache_file:
            passthrough += ["--cache-file", str(cache_file)]
        if not declared:
            passthrough += ["--no-declared"]
        if embedding_only:
            passthrough += ["--embedding-only"]
        batch_mod.run_shards(accessions, csv_dir, workers, passthrough)
        audits = batch_mod.collect_audits(csv_dir)
    else:
        logger.info("1/4 enrich + inline guards")
        projects = batch_mod.load_projects(accessions, cache_file)
        missing = [a for a in accessions if a not in projects]
        if missing:
            logger.warning(
                "no metadata for %d accessions: %s", len(missing), missing[:10]
            )
        audits, totals = batch_mod.enrich_batch(
            projects,
            csv_dir,
            module=module,
            model=model,
            embedding_model=embedding_model,
            tierc_policy=tierc_policy,
            vocab=vocab,
            species=species,
            declared=declared,
            verbose=verbose,
            embedding_only=embedding_only,
        )
        logger.info("inline guard totals: %s", json.dumps(totals, sort_keys=True))

    logger.info("inline audit rows: %d", len(audits))
    if stage1_only:
        return

    logger.info("2/4 ingest CSVs + install audit log")
    if db_file.exists():
        db_file.unlink()  # fresh batch DB for this run
    counts = store.ingest(csv_dir, db_file, unified_db_file, fast=embedding_only)
    store.install_corrections(db_file, audits)
    logger.info("ingested: %s", json.dumps(counts, sort_keys=True))

    if embedding_only:
        logger.info(
            "3/4 corpus and 4/4 verify SKIPPED: --embedding-only wrote the _fast "
            "tables, which the shipped detectors don't check"
        )
        return

    if tier_scripts_dir is None:
        logger.warning(
            "3/4 corpus and 4/4 verify SKIPPED: no --tier-scripts-dir. This batch "
            "has NOT been checked by the shipped detectors"
        )
        return

    logger.info("3/4 corpus: organism truth from the SOURCE archives")
    batch_mod.run_tier_script(tier_scripts_dir, "build_organism_truth_v4.py", db_file)
    batch_mod.run_tier_script(
        tier_scripts_dir, "apply_organism_v4.py", db_file, "--apply"
    )

    logger.info("4/4 verify: every shipped detector must find zero")
    found = batch_mod.detector_counts(tier_scripts_dir, db_file)
    # bad_label_samples counts LLM/CVCL tissue conflicts. The evidence policy
    # resolves them using study names; the ledger supports post-hoc correction.
    failures = {k: v for k, v in found.items() if v != 0 and k != "bad_label_samples"}
    logger.info("detectors: %s", json.dumps(found, sort_keys=True))
    if failures:
        logger.error("VERIFICATION FAILED: %s", json.dumps(failures, sort_keys=True))
        raise typer.Exit(1)
    logger.info("BATCH CLEAN")


@enrich_app.command()
def ingest(
    csv_dir: Path = typer.Option(..., help="a results dir of per-study CSVs"),
    db_file: Path = typer.Option(..., help="batch sqlite; created if absent"),
    unified_db_file: Path = typer.Option(
        None, help="unified_metadata.db, only for study titles"
    ),
    limit: int = typer.Option(
        None, help="stop after this many studies, for a smoke run"
    ),
    embedding_only: bool = typer.Option(
        default=False,
        help="route into the _fast twin of every table, matching "
        "the batch that produced these CSVs -- see `enrich batch --embedding-only`",
    ),
) -> None:
    """
    Load result CSVs into SQLite after a fleet pull.

    Upserts refresh existing rows and preserve rows absent from the results.
    """
    from enrich import batch as batch_mod
    from enrich import store

    counts = store.ingest(csv_dir, db_file, unified_db_file, limit, fast=embedding_only)
    n = store.install_corrections(db_file, batch_mod.collect_audits(csv_dir))
    logger.info("ingest: %s (+%d audit rows)", json.dumps(counts, sort_keys=True), n)


@enrich_app.command()
def prefetch(
    accession_file: Path = typer.Option(..., help="one accession per line"),
    cache_file: Path = typer.Option(
        ...,
        help="cache to write -- .pkl/.pickle for the pickle format, "
        ".db/.sqlite/.sqlite3 for the SQLite format (easier to `deploy: "
        "payload` to a yuj fleet host as one file)",
    ),
    batch_size: int = typer.Option(25, help="accessions per bulk API call"),
    pg_url: str | None = typer.Option(
        None,
        envvar="PG_URL",
        help="Postgres to read AE/GEA (E-MTAB-*, E-GEAD-*, ...) sample sheets "
        "from, read-only; the bulk API does not serve them",
    ),
) -> None:
    """
    Cache project metadata where the seqout API is reachable.

    AE/GEA accessions are read from Postgres instead. Unknown accessions are
    reported and skipped by offline workers.
    """
    import pickle

    from enrich.ae_gea import fetch_ae_gea_projects, is_ae_gea
    from enrich.batch import _SQLITE_CACHE_SUFFIXES, read_accessions
    from enrich.projects import fetch_projects_batched, write_projects_sqlite

    def progress(_projects: list, done: int, total: int) -> None:
        logger.info("prefetched %d/%d", done, total)

    accessions = read_accessions(accession_file)
    # one E-accession in a bulk API batch fails the whole batch with a 400
    ae_gea = [a for a in accessions if is_ae_gea(a)]
    projects = fetch_projects_batched(
        [a for a in accessions if not is_ae_gea(a)], batch_size, on_batch=progress
    )
    if ae_gea:
        if not pg_url:
            logger.error("%d AE/GEA accessions need --pg-url", len(ae_gea))
            raise typer.Exit(1)
        projects |= fetch_ae_gea_projects(ae_gea, pg_url, on_batch=progress)
    missing = [a for a in accessions if a not in projects]
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    if cache_file.suffix in _SQLITE_CACHE_SUFFIXES:
        write_projects_sqlite(list(projects.values()), cache_file)
    else:
        with cache_file.open("wb") as handle:
            pickle.dump({"projects": projects}, handle)
    logger.info(
        "cached %d projects to %s (%d unresolved)",
        len(projects),
        cache_file,
        len(missing),
    )


@enrich_app.command("select-candidates")
def select_candidates(
    output_file: Path = typer.Option(
        ...,
        help="one representative accession per line, ready for "
        "`enrich batch --accession-file`",
    ),
    pg_url: str = typer.Option(..., envvar="PG_URL"),
) -> None:
    """
    Pick one accession per Postgres canonical study for embedding-only enrichment.

    Prefer ae > geo > sra > ena > gsa > dra > gea (see enrich.candidates).
    Feed the output to `enrich batch --embedding-only` to avoid enriching mirrors.
    """
    from enrich.candidates import fetch_candidates

    rows = fetch_candidates(pg_url)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n".join(acc for _, _, acc in rows) + "\n")
    logger.info("wrote %d accessions to %s", len(rows), output_file)


@enrich_app.command("ontology-map")
def ontology_map(
    input_file: Path = typer.Option(..., help="an enriched CSV"),
    output_file: Path = typer.Option(..., help="the same rows plus ontology columns"),
    field: list[str] = typer.Option(
        None,
        help="repeatable; default is disease, tissue, cell_type, assay, "
        "development_stage",
    ),
    fuzzy_threshold: int = typer.Option(90, help="rapidfuzz token_sort_ratio cutoff"),
    fuzzy: bool = typer.Option(default=True, help="exact matching only when off"),
    embedding_model: str = typer.Option(
        None, help="optional last-resort matcher, e.g. abhinand/MedEmbed-large-v0.1"
    ),
) -> None:
    """
    Map enriched free text to ontology term ids.

    Standalone mode remaps a CSV after ontology tables are rebuilt.
    """
    import pandas as pd

    from enrich.ontology_map import map_terms

    df = pd.read_csv(input_file)
    out = map_terms(
        df,
        fields=list(field) if field else None,
        fuzzy_threshold=fuzzy_threshold,
        use_fuzzy=fuzzy,
        embedding_model=embedding_model,
    )
    out.to_csv(output_file, index=False)
    logger.info("ontology-map: %d rows -> %s", len(out), output_file)
