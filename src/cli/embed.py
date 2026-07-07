from pathlib import Path

import typer

from embed.cluster import compute_clusters
from embed.coords import compute_coords
from embed.embedder import embed_new
from embed.labels import generate_labels
from embed.neighbors import compute_neighbors

embed_app = typer.Typer()


@embed_app.command()
def run(
    geo_db_file: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    unified_metadata_db_file: Path = typer.Option(...),
    chroma_db_dir: Path = typer.Option(...),
    model_name: str = typer.Option("codefuse-ai/F2LLM-v2-8B"),
) -> None:
    embed_new(
        geo_db_file,
        sra_db_file,
        ae_db_file,
        unified_metadata_db_file,
        chroma_db_dir,
        model_name,
    )


@embed_app.command()
def coords(
    chroma_db_dir: Path = typer.Option(...),
) -> None:
    compute_coords(chroma_db_dir)


@embed_app.command()
def cluster(
    chroma_db_dir: Path = typer.Option(...),
) -> None:
    compute_clusters(chroma_db_dir)


@embed_app.command()
def neighbors(
    chroma_db_dir: Path = typer.Option(...),
    neighbors_db_file: Path = typer.Option(...),
    k: int = typer.Option(300),
    query_chunk: int = typer.Option(1024),
) -> None:
    compute_neighbors(chroma_db_dir, neighbors_db_file, k, query_chunk)


@embed_app.command()
def labels(
    chroma_db_dir: Path = typer.Option(...),
    labels_dir: Path = typer.Option(...),
    vllm_base_url: str = typer.Option("http://localhost:1729/v1"),
    model: str = typer.Option("unsloth/Qwen3.6-27B-MTP-GGUF:UD-Q8_K_XL"),
) -> None:
    generate_labels(chroma_db_dir, labels_dir, vllm_base_url, model)
