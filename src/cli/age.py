from pathlib import Path

import typer

age_app = typer.Typer()


@age_app.command()
def normalize(
    source_db_file: Path = typer.Option(...),
    output_dir: Path = typer.Option(...),
    batch_size: int = typer.Option(5000),
) -> None:
    from age.normalize import normalize_age

    out = output_dir / "age" / "age_norm.sqlite3"
    out.parent.mkdir(parents=True, exist_ok=True)
    normalize_age(source_db_file, out, batch_size)
