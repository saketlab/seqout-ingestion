from pathlib import Path

import typer

from cli.utils import _parse_timespan
from refresh.publications import (
    publication_status,
    refresh_publications,
    repair_truncated_titles,
)

refresh_app = typer.Typer(help="periodic re-enrichment, run against postgres directly")


@refresh_app.command()
def publications(
    pg_url: str = typer.Option(..., "--pg-url", envvar="PG_URL"),
    ncbi_keys_file: Path = typer.Option(..., envvar="NCBI_KEYS_FILE"),
    openalex_keys_file: Path = typer.Option(..., envvar="OPENALEX_KEYS_FILE"),
    limit_per_source: int = typer.Option(
        5000,
        help="max studies to look up per source per run",
    ),
    meta_limit: int = typer.Option(
        20000,
        help="max pmids to fetch metadata for per run",
    ),
    recheck_after_days: int = typer.Option(
        180,
        "--recheck-after",
        help="days before an already-checked study is queued again",
    ),
    batch_size: int = typer.Option(100),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    skip_journal_metrics: bool = typer.Option(default=False),
    skip_resolve: bool = typer.Option(default=False),
    skip_meta: bool = typer.Option(default=False),
    skip_tsv: bool = typer.Option(default=False),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
    """Resolve missing publications, fetch their metadata, refresh pub_tsv."""
    refresh_publications(
        pg_url,
        ncbi_keys_file,
        openalex_keys_file,
        limit_per_source,
        meta_limit,
        recheck_after_days,
        batch_size,
        retries,
        timeout,
        skip_journal_metrics,
        skip_resolve,
        skip_meta,
        skip_tsv,
        is_dry_run,
    )


@refresh_app.command()
def repair_titles(
    pg_url: str = typer.Option(..., "--pg-url", envvar="PG_URL"),
    ncbi_keys_file: Path | None = typer.Option(
        None,
        envvar="NCBI_KEYS_FILE",
        help="optional -- unkeyed efetch clears this backlog in about a minute",
    ),
    meta_limit: int = typer.Option(
        20000,
        help="max pmids to re-fetch per run",
    ),
    batch_size: int = typer.Option(100),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    skip_tsv: bool = typer.Option(default=False),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
    """Re-fetch pubmed titles cut short at an inline tag, then refresh pub_tsv."""
    repair_truncated_titles(
        pg_url,
        ncbi_keys_file,
        meta_limit,
        batch_size,
        retries,
        timeout,
        skip_tsv,
        is_dry_run,
    )


@refresh_app.command()
def status(
    pg_url: str = typer.Option(..., "--pg-url", envvar="PG_URL"),
) -> None:
    """Publication coverage per source -- how far the weekly runs have got."""
    typer.echo(publication_status(pg_url))
