from pathlib import Path
from typing import Optional

import typer

from unifier.unified_catalog import build_unified_catalog
from unifier.unified_metadata import build_unified_metadata

unify_app = typer.Typer()


@unify_app.command()
def build_catalog(
    base_output_dir: Path = typer.Option(..., "--output-dir"),
    geo_db_file: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    ena_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    db_file: Optional[Path] = typer.Option(None),
    dump_parquet: bool = typer.Option(False),
):
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
    geo_db_file: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    ena_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    unified_catalog_db_file: Path = typer.Option(...),
    pubmed_metadata_db_file: Path = typer.Option(...),
    assay_mapping_csv_file: Path = typer.Option(...),
    taxonomy_mapping_csv_file: Path = typer.Option(...),
):
    build_unified_metadata(
        geo_db_file,
        sra_db_file,
        ena_db_file,
        ae_db_file,
        unified_catalog_db_file,
        pubmed_metadata_db_file,
        assay_mapping_csv_file,
        taxonomy_mapping_csv_file,
    )
