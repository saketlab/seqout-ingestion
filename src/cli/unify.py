from pathlib import Path

import typer

from unifier.unified_catalog import build_unified_catalog
from unifier.unified_metadata import build_unified_metadata

unify_app = typer.Typer()


@unify_app.command()
def build_catalog(
    base_output_dir: Path = typer.Option(..., "--output-dir"),  # noqa: B008
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    sra_db_file: Path = typer.Option(...),  # noqa: B008
    ena_db_file: Path = typer.Option(...),  # noqa: B008
    ae_db_file: Path = typer.Option(...),  # noqa: B008
    db_file: Path | None = typer.Option(None),  # noqa: B008
    dump_parquet: bool = typer.Option(default=False),
) -> None:
    build_unified_catalog(
        base_output_dir,
        geo_db_file,
        sra_db_file,
        ena_db_file,
        ae_db_file,
        db_file,
        dump_parquet,
    )


@unify_app.command()
def build_metadata(
    base_output_dir: Path = typer.Option(..., "--output-dir"),  # noqa: B008
    db_file: Path | None = typer.Option(None),  # noqa: B008
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    sra_db_file: Path = typer.Option(...),  # noqa: B008
    ena_db_file: Path = typer.Option(...),  # noqa: B008
    ae_db_file: Path = typer.Option(...),  # noqa: B008
    unified_catalog_db_file: Path = typer.Option(...),  # noqa: B008
    pubmed_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    assay_mapping_csv_file: Path = typer.Option(...),  # noqa: B008
    taxonomy_mapping_csv_file: Path = typer.Option(...),  # noqa: B008
    batch_size: int = typer.Option(2000),
) -> None:
    build_unified_metadata(
        base_output_dir,
        db_file,
        geo_db_file,
        sra_db_file,
        ena_db_file,
        ae_db_file,
        unified_catalog_db_file,
        pubmed_metadata_db_file,
        assay_mapping_csv_file,
        taxonomy_mapping_csv_file,
        batch_size,
    )
