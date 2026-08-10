import json
import logging
from pathlib import Path

import typer

from pentimento.build import build_pentimento

pentimento_app = typer.Typer()
preflight_app = typer.Typer(help="Run preflightx and load its output")
pentimento_app.add_typer(preflight_app, name="preflight")
logger = logging.getLogger(__name__)


@pentimento_app.command()
def build(
    output_db_file: Path = typer.Option(...),
    links_db_file: Path = typer.Option(
        ..., help="download_links.db -- the spine (run->experiment->study + fastq)"
    ),
    ncount_dir: list[Path] = typer.Option(
        ..., help="repeatable; ncount per-sample json dirs"
    ),
    preflightx_db_file: Path = typer.Option(
        ..., help="enrichment sqlite carrying preflightx_experiments"
    ),
    preflightx_csv_dir: Path = typer.Option(..., help="sc_preflight_v4/annotated"),
    quant_dir: Path = typer.Option(
        None,
        help="dir of <RUN>.quant.csv; omit to build a preflight-only Pentimento "
        "(microbe flags stay NULL = not measured)",
    ),
    legacy_csv_dir: list[Path] = typer.Option(
        None,
        help="repeatable; older mukesh_*_preflight/annotated dirs. Same schema, "
        "but NO provenance json exists for them -- tagged preflightx_source="
        "'csv_legacy' so un-provenanced calls can be excluded",
    ),
    benchmark_file: Path = typer.Option(
        None,
        help="preflight_per_experiment.csv -- the accuracy benchmark "
        "(gt_* vs pred_*) behind the calls",
    ),
    ena_db_file: Path = typer.Option(None, help="optional fallback for SRS->GSM"),
    sra_db_file: Path = typer.Option(None, help="preferred source of run->SRS->GSM"),
    geo_db_file: Path = typer.Option(None),
) -> None:
    build_pentimento(
        output_db_file,
        links_db_file,
        list(ncount_dir),
        preflightx_db_file,
        preflightx_csv_dir,
        quant_dir,
        benchmark_file,
        list(legacy_csv_dir or []),
        ena_db_file,
        sra_db_file,
        geo_db_file,
    )


@pentimento_app.command()
def flags(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
) -> None:
    """
    Rebuild run_microbe_flags on an existing Pentimento DB.

    `pentimento build` already does this; run it standalone to re-pivot after
    editing FLAG_PANEL, without redoing the whole multi-hour build.
    """
    from helpers import exec_query_from_file, sqlite_connect
    from pentimento.flags import build_flags

    # sqlite_connect, not sqlite3.connect: it sets busy_timeout=120s, and this
    # writes to the same DB a build may be holding.
    with sqlite_connect(db_file) as conn:
        exec_query_from_file("tables/pentimento.sql", conn=conn)
        build_flags(conn)


@pentimento_app.command()
def rollup(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
    sra_db_file: Path = typer.Option(
        None,
        help="sra_metadata.db; sra_studies.alias is the canonical SRP->GSE link",
    ),
) -> None:
    """
    Rebuild the sample- and study-grained rollups on an existing Pentimento DB.

    `pentimento build` already does this. Run it standalone to backfill a DB built
    before the rollups existed, without redoing the whole multi-hour build.
    """
    from helpers import exec_query_from_file, sqlite_connect
    from pentimento.rollup import build_rollups

    # uri=True: build_rollups ATTACHes the SRA db as file:...?immutable=1, and
    # sqlite refuses a URI filename unless the connection was opened for it.
    with sqlite_connect(db_file, uri=True) as conn:
        exec_query_from_file("tables/pentimento.sql", conn=conn)
        build_rollups(conn, sra_db_file)


@pentimento_app.command()
def export_parquet(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
    output_dir: Path = typer.Option(...),
    table: list[str] = typer.Option(None, help="repeatable; default is every table"),
    batch_size: int = typer.Option(100_000),
    compression: str = typer.Option("zstd", help="zstd | snappy | none"),
    resume: bool = typer.Option(
        False,  # noqa: FBT003
        "--resume",
        help="skip finished tables",
    ),
) -> None:
    """
    Mirror the Pentimento tables to one parquet file each.

    Views are excluded -- they are joins over these tables, so a consumer
    reproduces them by rerunning the view SQL over the parquet.
    """
    from pentimento.export import export_parquet as run

    counts = run(
        db_file,
        output_dir,
        list(table or []) or None,
        batch_size,
        compression,
        resume=resume,
    )
    logger.info("pentimento parquet: %s", json.dumps(counts))


@preflight_app.command("manifest")
def preflight_manifest(
    out_dir: Path = typer.Option(..., help="one <study>.csv per study to scan"),
    links_db_file: Path = typer.Option(..., help="download_links.db -- the fastq URLs"),
    unified_db_file: Path = typer.Option(
        ..., help="unified_metadata.db -- the population"
    ),
    enrichment_db_file: Path = typer.Option(
        None,
        help="optional; adds stated sex/assay so the mismatch flags have "
        "something to compare against. Omit for a scan-only manifest",
    ),
    accession_file: Path = typer.Option(
        None,
        help="one accession per line; default is every is_single_cell=1 study",
    ),
    batch_size: int = typer.Option(400),
) -> None:
    """
    Pick what to scan and where its reads are.

    Studies resolve to runs through their ALIASES, not accession equality: a GSE
    keys its download links under an SRA alias, and looking up the canonical
    accession alone leaves GEO at 0% coverage.
    """
    from pentimento.manifest import build_manifests

    accessions = (
        [ln.strip() for ln in accession_file.read_text().splitlines() if ln.strip()]
        if accession_file
        else None
    )
    counts = build_manifests(
        out_dir,
        links_db_file,
        unified_db_file,
        enrichment_db_file,
        accessions,
        batch_size,
    )
    logger.info("preflight manifest: %s", json.dumps(counts))


@preflight_app.command("run")
def preflight_run(
    in_dir: Path = typer.Option(..., help="manifests from `preflight manifest`"),
    out_dir: Path = typer.Option(..., help="annotated CSVs + provenance JSONs"),
    exe: str = typer.Option("preflightx", help="the preflightx binary"),
    n_reads: int = typer.Option(20_000, help="reads per run in the cheap pass"),
    max_runs: int = typer.Option(1, help="runs scanned per experiment"),
    deep_n_reads: int = typer.Option(
        400_000,
        help="reads for the deep sex pass, run ONLY when the cheap pass "
        "abstained (~80% of the time). 0 disables it -- fast, but sex stays "
        "mostly unknown",
    ),
    deep_max_runs: int = typer.Option(2),
    accession: str = typer.Option(
        None,
        help="scan ONE study instead of the whole dir. This is the fleet form: "
        "yuj appends one item per invocation, so put `--accession` last in "
        "work_command and it receives the item",
    ),
) -> None:
    """
    Scan the reads. Network- and CPU-bound; resumable by output existence.

    This is the per-slice worker: point it at one host's share of the manifests.
    Fleet scatter is yuj's job, not this command's.
    """
    from pentimento.preflight import annotate_dir

    counts = annotate_dir(
        in_dir,
        out_dir,
        exe,
        n_reads,
        max_runs,
        deep_n_reads,
        deep_max_runs,
        accession,
    )
    logger.info("preflight run: %s", json.dumps(counts))


@preflight_app.command("ingest")
def preflight_ingest(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
    preflightx_csv_dir: Path = typer.Option(
        ..., help="annotated CSVs + provenance JSONs"
    ),
    sra_db_file: Path = typer.Option(
        None, help="sra_metadata.db; sra_studies.alias is the canonical SRP->GSE link"
    ),
) -> None:
    """
    Load a preflight run into an existing Pentimento DB, without rebuilding it.

    This is what makes preflight independent: a fresh scan lands in Pentimento
    with no microbial quantification, no enrichment DB and no multi-hour build.
    """
    from helpers import exec_query_from_file, sqlite_connect
    from pentimento.build import ingest_preflight

    with sqlite_connect(db_file, uri=True) as conn:
        exec_query_from_file("tables/pentimento.sql", conn=conn)
        counts = ingest_preflight(conn, preflightx_csv_dir, sra_db_file)
    logger.info("preflight ingest: %s", json.dumps(counts))
