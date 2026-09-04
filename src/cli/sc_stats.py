from pathlib import Path

import typer

from sc_stats.ingest import ingest_dirs

sc_stats_app = typer.Typer()


@sc_stats_app.command()
def ingest(
    results_dir: list[Path] = typer.Option(
        ..., help="sweep output dir; repeat the flag to union several pulls"
    ),
    db_file: Path = typer.Option(..., help="staging SQLite; created if absent"),
) -> None:
    counts = ingest_dirs(db_file, list(results_dir))
    typer.echo(" ".join(f"{k}={v}" for k, v in sorted(counts.items())))
