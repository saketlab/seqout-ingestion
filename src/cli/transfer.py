import logging
from pathlib import Path

import typer

from embed.embed_constants import TransferStep

transfer_app = typer.Typer()
logger = logging.getLogger(__name__)


@transfer_app.command()
def delta(
    pg_url: str = typer.Option(..., envvar="PG_URL"),
    source_db_file: Path = typer.Option(...),
    out_db_file: Path = typer.Option(...),
    table: list[str] = typer.Option(
        ...,
        "--table",
        help="Repeatable. Transfer task names to include, e.g. --table sra_studies.",
    ),
    read_batch_size: int = typer.Option(default=500),
) -> None:
    """
    Write a SQLite delta containing rows missing from Postgres.

    Run after ingest, then pass out_db_file to `transfer postgres`.
    Uses the transfer task table's conflict keys.
    """
    from transfer.delta import build_delta

    counts = build_delta(pg_url, source_db_file, out_db_file, table, read_batch_size)
    logger.info("delta written to %s (%d rows)", out_db_file, sum(counts.values()))


@transfer_app.command()
def postgres(
    # Read credentials from the environment to keep them out of command arguments.
    pg_url: str = typer.Option(..., envvar="PG_URL"),
    # Database paths are optional unless a selected table's modifier needs them.
    # Tasks run after --table filtering; geo_series also needs sra_db_file,
    # while geo_samples/platforms/contributors do not.
    sra_db_file: Path | None = typer.Option(None),
    geo_db_file: Path | None = typer.Option(None),
    ena_db_file: Path | None = typer.Option(None),
    ae_db_file: Path | None = typer.Option(None),
    pubmed_metadata_db_file: Path | None = typer.Option(None),
    unified_metadata_db_file: Path | None = typer.Option(None),
    download_links_db_file: Path | None = typer.Option(None),
    read_batch_size: int = typer.Option(default=500),
    num_retries: int = typer.Option(3, "--retries"),
    num_workers: int = typer.Option(4, "--workers"),
    gsa_db_file: Path | None = typer.Option(None),
    gsa_human_db_file: Path | None = typer.Option(None),
    gsa_bioproject_db_file: Path | None = typer.Option(None),
    prune_publications: bool = typer.Option(
        False,  # noqa: FBT003
        "--prune-publications",
        help=(
            "delete study_publications rows absent from this run's sqlite "
            "buffers. Only safe when those buffers hold the full history -- "
            "on a fresh or partial DATA_DIR this wipes the table."
        ),
    ),
    insert_only: bool = typer.Option(
        False,  # noqa: FBT003
        "--insert-only",
        help=(
            "ON CONFLICT DO NOTHING everywhere, so an accession postgres "
            "already holds keeps every value it has. For the weekly window, "
            "whose sqlite covers only new records. Tables with no unique key "
            "are skipped rather than appended to."
        ),
    ),
    table: list[str] = typer.Option(
        None,
        "--table",
        help="Repeatable. Run ONLY these transfer tasks, e.g. --table geo_series. "
        "For a sqlite that is authoritative for some tables and behind on others.",
    ),
) -> None:
    from transfer.postgres import postgres_transfer

    postgres_transfer(
        pg_url,
        sra_db_file,
        geo_db_file,
        ena_db_file,
        ae_db_file,
        pubmed_metadata_db_file,
        unified_metadata_db_file,
        download_links_db_file,
        read_batch_size,
        num_retries,
        num_workers,
        gsa_db_file=gsa_db_file,
        gsa_human_db_file=gsa_human_db_file,
        gsa_bioproject_db_file=gsa_bioproject_db_file,
        prune_publications=prune_publications,
        insert_only=insert_only,
        tables=table or None,
    )


@transfer_app.command()
def leiden_swap(
    pg_url: str = typer.Option(...),
    chroma_db_dir: Path = typer.Option(...),
    labels_dir: Path = typer.Option(...),
) -> None:
    """Overwrite EVoC with Leiden in Postgres (remap leiden_l* -> cluster_l*)."""
    from transfer.leiden_swap import leiden_swap as run

    run(pg_url, chroma_db_dir, labels_dir)


@transfer_app.command()
def sample_tsv(
    pg_url: str = typer.Option(...),
) -> None:
    """Fold sample title/description into each study's search tsv (recall)."""
    from transfer.sample_tsv import sample_tsv_transfer

    sample_tsv_transfer(pg_url)


@transfer_app.command()
def pub_tsv(
    pg_url: str = typer.Option(...),
) -> None:
    """Fold linked publication titles into each study's search tsv (recall)."""
    from transfer.pub_tsv import pub_tsv_transfer

    pub_tsv_transfer(pg_url)


@transfer_app.command()
def ena_organisms(
    pg_url: str = typer.Option(...),
) -> None:
    """Populate ena_studies.organisms from ena_samples (multi-organism + facet)."""
    from transfer.ena_organisms import ena_organisms_transfer

    ena_organisms_transfer(pg_url)


@transfer_app.command()
def gsa_organisms(
    pg_url: str = typer.Option(...),
) -> None:
    """Populate gsa_studies.organisms from gsa_samples (multi-organism + facet)."""
    from transfer.gsa_organisms import gsa_organisms_transfer

    gsa_organisms_transfer(pg_url)


@transfer_app.command()
def gea_organisms(
    pg_url: str = typer.Option(...),
) -> None:
    """Populate gea_experiments.organisms from gea_samples (project page)."""
    from transfer.gea_organisms import gea_organisms_transfer

    gea_organisms_transfer(pg_url)


@transfer_app.command()
def dra_organisms(
    pg_url: str = typer.Option(...),
) -> None:
    """Populate dra_studies_search.organisms (federated organism facet)."""
    from transfer.dra_organisms import dra_organisms_transfer

    dra_organisms_transfer(pg_url)


@transfer_app.command()
def ddbj(
    pg_url: str = typer.Option(...),
    dra_db_file: Path | None = typer.Option(None),
    gea_db_file: Path | None = typer.Option(None),
    download_links_db_file: Path | None = typer.Option(None),
    read_batch_size: int = typer.Option(default=500),
    insert_only: bool = typer.Option(
        False,  # noqa: FBT003
        "--insert-only",
        help=(
            "ON CONFLICT DO NOTHING, for a per-window sqlite. Skips gea_samples, "
            "whose only unique key is an id that ingest restarts every window."
        ),
    ),
) -> None:
    """Upsert DDBJ DRA and GEA into the dra_* / gea_* Postgres tables."""
    from transfer.ddbj import ddbj_transfer

    ddbj_transfer(
        pg_url,
        dra_db_file,
        gea_db_file,
        read_batch_size,
        download_links_db_file=download_links_db_file,
        insert_only=insert_only,
    )


@transfer_app.command()
def age(
    pg_url: str = typer.Option(...),
    age_norm_db_file: Path = typer.Option(...),
    read_batch_size: int = typer.Option(default=5000),
) -> None:
    """Annotate ontology_samples_v4 with normalized age columns (age_days, ...)."""
    from transfer.age import age_transfer

    age_transfer(pg_url, age_norm_db_file, read_batch_size)


@transfer_app.command()
def gsa_search_tsv(
    pg_url: str = typer.Option(...),
) -> None:
    """
    Fold BioProject text, organism and sample text into the GSA search tsv.

    Run after the gsa_* tables are loaded.
    """
    from transfer.gsa_search import gsa_search_tsv_transfer

    gsa_search_tsv_transfer(pg_url)


@transfer_app.command()
def pentimento(
    pg_url: str = typer.Option(...),
    pentimento_db_file: Path = typer.Option(...),
    batch_size: int = typer.Option(default=5000),
    non_nulling: bool = typer.Option(
        default=False,
        help="ESCAPE HATCH: COALESCE so the load can only add, never blank. "
        "For a build known to be MISSING data pg already holds (corrupt or "
        "partial input). Forfeits the tri-state -- a real 'not measurable' "
        "NULL cannot clear a stale value, so the next whole-truth transfer "
        "must run without it.",
    ),
    table: list[str] = typer.Option(
        None,
        "--table",
        help="Repeatable. Load ONLY these pg tables, e.g. --table "
        "pentimento_microbe. Use when the build is authoritative for some "
        "tables but behind postgres on others.",
    ),
    dry_run: bool = typer.Option(
        default=False,
        help="Write NOTHING. Report per column how many cells this build would "
        "BLANK (postgres holds a value, the build has NULL) and how many it "
        "would fill. Counting nulls on each side separately would miss a build "
        "that fills 100 cells and blanks 100 others.",
    ),
) -> None:
    """Load Pentimento into pg (counts, preflightx, microbe flags)."""
    import json as _json

    from transfer.pentimento import pentimento_transfer

    report = pentimento_transfer(
        pg_url,
        pentimento_db_file,
        batch_size,
        non_nulling=non_nulling,
        dry_run=dry_run,
        tables=list(table) if table else None,
    )
    if report is not None:
        logger.info("dry run report: %s", _json.dumps(report, indent=2))
        if any(r["blanked"] for r in report):
            # Stop downstream transfers on failure.
            raise typer.Exit(1)


@transfer_app.command("sc-stats")
def sc_stats(
    pg_url: str = typer.Option(..., envvar="PG_URL"),
    sc_stats_db_file: Path = typer.Option(
        ..., help="sc_stats.db from `sc_stats ingest`"
    ),
    is_overwrite: bool = typer.Option(
        default=False,
        help="OVERWRITE instead of the default non-nulling load. Only for a "
        "sweep known to cover everything pg already holds: the sweep arrives "
        "incrementally, so a partial staging file would blank columns an "
        "earlier, larger pass had filled.",
    ),
    is_dry_run: bool = typer.Option(
        default=False, help="report rows held and staged, write nothing"
    ),
) -> None:
    """Load the sc_stats per-matrix QC summaries into pg."""
    from transfer.sc_stats import sc_stats_transfer

    counts = sc_stats_transfer(
        pg_url, sc_stats_db_file, overwrite=is_overwrite, is_dry_run=is_dry_run
    )
    typer.echo(" ".join(f"{k}={v}" for k, v in sorted(counts.items())))


@transfer_app.command()
def gard(
    pg_url: str = typer.Option(...),
    gard_db_file: Path = typer.Option(..., help="gard.db from `gard fetch`"),
) -> None:
    """Load the GARD rare-disease dimension table into pg."""
    from transfer.gard import gard_transfer

    gard_transfer(pg_url, gard_db_file)


@transfer_app.command()
def nord(
    pg_url: str = typer.Option(...),
    nord_db_file: Path = typer.Option(..., help="nord.db from `nord load`"),
) -> None:
    """Load the NORD rare-disease dimension table into pg."""
    from transfer.nord import nord_transfer

    nord_transfer(pg_url, nord_db_file)


@transfer_app.command()
def inheritance(
    pg_url: str = typer.Option(...),
    inheritance_db_file: Path = typer.Option(
        ..., help="inheritance.db from `inheritance fetch`"
    ),
) -> None:
    """Load the mode-of-inheritance dimension table into pg."""
    from transfer.inheritance import inheritance_transfer

    inheritance_transfer(pg_url, inheritance_db_file)


@transfer_app.command("ontology-closure")
def ontology_closure(
    pg_url: str = typer.Option(...),
    db_file: Path = typer.Option(
        ..., help="ontology_closure.db from `ontology_closure build`"
    ),
) -> None:
    """Load the ontology closure and term labels into pg."""
    from transfer.ontology_closure import ontology_closure_transfer

    ontology_closure_transfer(pg_url, db_file)


@transfer_app.command("ontology-closure-filtering-terms")
def ontology_closure_filtering_terms(
    pg_url: str = typer.Option(...),
) -> None:
    """Rebuild beacon_filtering_terms from ontology_samples_v4 + ontology_closure."""
    from transfer.ontology_closure import ontology_closure_filtering_terms as run

    run(pg_url)


@transfer_app.command("ontology-closure-verify-pg")
def ontology_closure_verify_pg(
    pg_url: str = typer.Option(...),
) -> None:
    """Read-only: confirm the closure landed and the descendant filter uses an index."""
    from transfer.ontology_closure import ontology_closure_verify_pg as run

    run(pg_url)


@transfer_app.command("longread-chemistry")
def longread_chemistry(
    pg_url: str = typer.Option(...),
    longread_chemistry_db_file: Path = typer.Option(
        ..., help="longread_chemistry.db from recipes/longread_chemistry_populate.py"
    ),
) -> None:
    """Load the long-read chemistry dimension table into pg (non-nulling)."""
    from transfer.longread_chemistry import longread_chemistry_transfer

    longread_chemistry_transfer(pg_url, longread_chemistry_db_file)


@transfer_app.command()
def parquet(
    output_dir: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    ena_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    pubmed_metadata_db_file: Path = typer.Option(...),
    unified_metadata_db_file: Path = typer.Option(...),
    download_links_db_file: Path = typer.Option(...),
    num_workers: int = typer.Option(4, "--workers"),
    batch_size: int = typer.Option(default=500),
    gsa_db_file: Path | None = typer.Option(None),
    gsa_human_db_file: Path | None = typer.Option(None),
    gsa_bioproject_db_file: Path | None = typer.Option(None),
    dra_db_file: Path | None = typer.Option(None),
    gea_db_file: Path | None = typer.Option(None),
) -> None:
    from transfer.parquet import parquet_transfer

    parquet_transfer(
        output_dir,
        sra_db_file,
        geo_db_file,
        ena_db_file,
        ae_db_file,
        pubmed_metadata_db_file,
        unified_metadata_db_file,
        download_links_db_file,
        num_workers,
        batch_size,
        gsa_db_file=gsa_db_file,
        gsa_human_db_file=gsa_human_db_file,
        gsa_bioproject_db_file=gsa_bioproject_db_file,
        dra_db_file=dra_db_file,
        gea_db_file=gea_db_file,
    )


@transfer_app.command()
def parquet_from_pg(
    pg_url: str = typer.Option(...),
    output_dir: Path = typer.Option(...),
    num_workers: int = typer.Option(4, "--workers"),
    batch_size: int = typer.Option(default=5000),
    resume: bool = typer.Option(False, "--resume", help="skip already-exported tables"),
) -> None:
    """
    Build the Parquet dump from Postgres, including GSA, DRA and GEA.

    Replaces the SQLite-fed parquet command.
    """
    from transfer.parquet import pg_parquet_transfer

    pg_parquet_transfer(pg_url, output_dir, num_workers, batch_size, resume=resume)


@transfer_app.command()
def embed(
    pg_url: str = typer.Option(...),
    chroma_db_dir: Path = typer.Option(...),
    neighbors_db_file: Path = typer.Option(...),
    labels_dir: Path = typer.Option(...),
    start_from: TransferStep = typer.Option(
        default=TransferStep.coords_neighbors,
        help="resume the transfer from this sub-step onward "
        "(coords-neighbors -> coords2d -> labels)",
    ),
) -> None:
    from transfer.embed import embed_transfer

    embed_transfer(
        pg_url,
        chroma_db_dir,
        neighbors_db_file,
        labels_dir,
        start_from=start_from,
    )
