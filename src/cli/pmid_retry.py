from pathlib import Path

import typer

from pmid_retry.fetch import pmid_retry_fetch
from pmid_retry.gaps import pmid_gaps, write_manifest
from pmid_retry.ingest import pmid_retry_ingest
from pmid_retry.promote import pmid_retry_promote
from pmid_retry.transfer import pmid_retry_transfer
from pmid_retry.verify import PROMOTABLE, pmid_retry_verify

pmid_retry_app = typer.Typer()


@pmid_retry_app.command()
def gaps(
    pg_url: str = typer.Option(..., envvar="PG_URL"),
    manifest_file: Path = typer.Option(
        ..., help="output; <source>,<accession> per line"
    ),
    sources: str = typer.Option(None, help="comma-separated; omit for every source"),
    limit: int = typer.Option(None, help="cap per source, for a trial slice"),
    exclude_db_file: Path = typer.Option(
        None, help="staging SQLite of a prior sweep; its accessions are skipped"
    ),
) -> None:
    found = pmid_gaps(
        pg_url,
        tuple(sources.split(",")) if sources else None,
        limit,
        exclude_db_file,
    )
    typer.echo(f"{write_manifest(found, manifest_file)} accessions -> {manifest_file}")


@pmid_retry_app.command()
def fetch(
    manifest_file: Path = typer.Option(...),
    out_dir: Path = typer.Option(..., help="one <accession>.json per result"),
    num_workers: int = typer.Option(4),
    keys_file: Path = typer.Option(None, envvar="NCBI_KEYS_FILE"),
) -> None:
    counts = pmid_retry_fetch(manifest_file, out_dir, num_workers, keys_file)
    typer.echo(" ".join(f"{k}={v}" for k, v in sorted(counts.items())))


@pmid_retry_app.command()
def ingest(
    results_dir: list[Path] = typer.Option(
        ..., help="sweep output dir; repeat the flag to union several pulls"
    ),
    db_file: Path = typer.Option(..., help="staging SQLite; created if absent"),
    manifest_file: Path = typer.Option(
        None, help="<source>,<accession> per line; records what was asked for"
    ),
) -> None:
    stats = pmid_retry_ingest(results_dir, db_file, manifest_file)
    typer.echo(" ".join(f"{k}={v}" for k, v in stats.items()))


@pmid_retry_app.command()
def verify(
    db_file: Path = typer.Option(...),
    num_workers: int = typer.Option(
        6, help="NCBI allows ten requests a second with a key"
    ),
    source: str = typer.Option(None, help="verify one source only"),
    is_recheck: bool = typer.Option(
        default=False, help="re-judge claims that already have a verdict"
    ),
) -> None:
    counts = pmid_retry_verify(db_file, num_workers, source, recheck=is_recheck)
    for verdict in sorted(counts):
        typer.echo(f"{verdict:14s} {counts[verdict]}")


@pmid_retry_app.command()
def promote(
    db_file: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(None),
    ena_db_file: Path = typer.Option(None),
    geo_db_file: Path = typer.Option(None),
    ae_db_file: Path = typer.Option(None),
    verdicts: str = typer.Option(
        ",".join(PROMOTABLE), help="comma-separated verdicts to promote"
    ),
    is_dry_run: bool = typer.Option(default=True, help="report what would be written"),
) -> None:
    source_db_files = {
        source: path
        for source, path in (
            ("sra", sra_db_file),
            ("ena", ena_db_file),
            ("geo", geo_db_file),
            ("ae", ae_db_file),
        )
        if path is not None
    }
    report = pmid_retry_promote(
        db_file, source_db_files, tuple(verdicts.split(",")), is_dry_run=is_dry_run
    )
    for source, stats in report.items():
        typer.echo(f"{source:5s} " + " ".join(f"{k}={v}" for k, v in stats.items()))


@pmid_retry_app.command()
def transfer(
    db_file: Path = typer.Option(...),
    pg_url: str = typer.Option(..., envvar="PG_URL"),
    verdicts: str = typer.Option(
        None,
        help="only transfer these verification verdicts; omit to transfer every claim",
    ),
    batch_size: int = typer.Option(1000),
    is_dry_run: bool = typer.Option(
        default=True, help="report the write without making it"
    ),
) -> None:
    report = pmid_retry_transfer(
        db_file,
        pg_url,
        tuple(verdicts.split(",")) if verdicts else None,
        batch_size,
        is_dry_run=is_dry_run,
    )
    for source, stats in report.items():
        typer.echo(f"{source:5s} " + " ".join(f"{k}={v}" for k, v in stats.items()))
