from pathlib import Path

import typer

from sc_db.build import PHASES, sc_db_build, summarise
from sc_db.export import BUCKETS
from sc_db.export import export_accessions as _export

sc_db_app = typer.Typer()


@sc_db_app.command()
def build(
    sc_db_file: Path = typer.Option(
        ...,
        help="output; created if absent, REPLACED table-by-table if not",
    ),
    sc_evidence_db_file: Path = typer.Option(
        ...,
        help="the sc_evidence build's output",
    ),
    pg_url: str = typer.Option(
        ...,
        help="production Postgres. READ-ONLY: every statement here is a SELECT",
    ),
    geo_db_file: Path = typer.Option(
        None,
        help="GEO archive SQLite. Without it, series-level matrices are not "
        "counted and studies whose matrix lives in a GSE_RAW.tar read "
        "has_matrix = 0.",
    ),
    ae_db_file: Path = typer.Option(
        None,
        help="ArrayExpress archive SQLite (the COMBINED file: the dated "
        "increment holds 135 experiments against 80,408). Without it no "
        "ArrayExpress study has a matrix.",
    ),
    ncount_extra_db_file: Path = typer.Option(
        None,
        help="ncount gap-run SQLite (data/ncount/*.db). Cell counts measured "
        "after the Postgres snapshot; omitting it reports work as outstanding "
        "that is already done.",
    ),
    queue_csv_file: Path = typer.Option(None, help="write sc_work_queue here as CSV"),
    phases: str = typer.Option(
        ",".join(PHASES),
        help="comma-separated subset of: " + ", ".join(PHASES),
    ),
    is_resume: bool = typer.Option(
        default=False,
        help="skip a phase whose target is already populated. Phases are "
        "minutes each, so this is coarse on purpose.",
    ),
    is_dry_run: bool = typer.Option(
        default=False,
        help="report the row counts each phase WOULD move, write nothing.",
    ),
) -> None:
    """
    Build the single-cell SQLite and quantification work queue.

    Borrows evidence, Pentimento and unified_metadata so SQLite views resolve locally.
    """
    sc_db_build(
        sc_db_file,
        sc_evidence_db_file,
        pg_url,
        geo_db_file=geo_db_file,
        ae_db_file=ae_db_file,
        ncount_extra_db_file=ncount_extra_db_file,
        queue_csv_file=queue_csv_file,
        phases=[p.strip() for p in phases.split(",") if p.strip()],
        is_resume=is_resume,
        is_dry_run=is_dry_run,
    )
    if not is_dry_run and "queue" in phases:
        typer.echo(summarise(sc_db_file))


@sc_db_app.command()
def summary(sc_db_file: Path = typer.Option(...)) -> None:
    """Print the queue's bucket counts from the existing DB."""
    typer.echo(summarise(sc_db_file))


@sc_db_app.command()
def export_accessions(
    sc_db_file: Path = typer.Option(...),
    bucket: str = typer.Option(..., help=f"one of {sorted(BUCKETS)}"),
    out_file: Path = typer.Option(..., help="one study accession per line"),
    limit: int = typer.Option(None, help="cap the list, for a trial slice"),
    max_runs: int = typer.Option(
        None, help="drop studies resolving to more runs than this"
    ),
    evidence_tier: str = typer.Option(
        None, help="keep one tier only; 'high' is led by insdc_library_source"
    ),
    manifest_dir: Path = typer.Option(
        None,
        help="preflight manifest dir; given, drops studies whose manifest "
        "carries no fastq_ftp and so would scan nothing",
    ),
    is_by_next_stage: bool = typer.Option(
        default=False,
        help="select on next_stage, so a study queued to another stage "
        "first is left out",
    ),
) -> None:
    count = _export(
        sc_db_file,
        bucket,
        out_file,
        limit,
        max_runs,
        evidence_tier,
        manifest_dir,
        by_next_stage=is_by_next_stage,
    )
    typer.echo(f"{count} studies -> {out_file}")
