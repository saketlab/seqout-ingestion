"""
CLI for the embed pipeline.

Heavy, GPU-only dependencies (torch / cuML / chromadb / sentence-transformers)
are imported lazily inside each command so that unrelated commands in the wider
CLI don't pay the import cost or require a CUDA runtime to even start up.
"""

from pathlib import Path

import typer

from embed.embed_constants import TransferStep

embed_app = typer.Typer(
    help="Embed new accessions and build the shared coordinate/cluster space.",
)


@embed_app.command()
def run(
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    sra_db_file: Path = typer.Option(...),  # noqa: B008
    ae_db_file: Path = typer.Option(...),  # noqa: B008
    unified_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    chroma_db_dir: Path = typer.Option(...),  # noqa: B008
    model_name: str = typer.Option("codefuse-ai/F2LLM-v2-8B"),
) -> None:
    """Embed accessions missing from chroma (geo, sra, arrayexpress)."""
    from embed.embedder import embed_new  # noqa: PLC0415

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
    chroma_db_dir: Path = typer.Option(...),  # noqa: B008
) -> None:
    """Refit UMAP (2D + 3D) over the shared space and write coords to chroma."""
    from embed.coords import compute_coords  # noqa: PLC0415

    compute_coords(chroma_db_dir)


@embed_app.command()
def cluster(
    chroma_db_dir: Path = typer.Option(...),  # noqa: B008
) -> None:
    """Recompute EVoC hierarchical clusters and write them to chroma."""
    from embed.cluster import compute_clusters  # noqa: PLC0415

    compute_clusters(chroma_db_dir)


@embed_app.command()
def neighbors(
    chroma_db_dir: Path = typer.Option(...),  # noqa: B008
    neighbors_db_file: Path = typer.Option(...),  # noqa: B008
    k: int = typer.Option(300),
    query_chunk: int = typer.Option(1024),
) -> None:
    """Compute K nearest neighbors (cosine) into neighbors.sqlite3."""
    from embed.neighbors import compute_neighbors  # noqa: PLC0415

    compute_neighbors(chroma_db_dir, neighbors_db_file, k, query_chunk)


@embed_app.command()
def labels(
    chroma_db_dir: Path = typer.Option(...),  # noqa: B008
    labels_dir: Path = typer.Option(...),  # noqa: B008
    vllm_base_url: str = typer.Option("http://localhost:1729/v1"),
    model: str = typer.Option("unsloth/Qwen3.6-27B-MTP-GGUF:UD-Q8_K_XL"),
) -> None:
    """Generate cluster labels (TF-IDF keywords + local LLM) into CSVs."""
    from embed.labels import generate_labels  # noqa: PLC0415

    generate_labels(chroma_db_dir, labels_dir, vllm_base_url, model)


@embed_app.command()
def transfer(
    pg_url: str = typer.Option(...),
    chroma_db_dir: Path = typer.Option(...),  # noqa: B008
    neighbors_db_file: Path = typer.Option(...),  # noqa: B008
    labels_dir: Path = typer.Option(...),  # noqa: B008
    start_from: TransferStep = typer.Option(  # noqa: B008
        TransferStep.coords_neighbors,
        help="Resume the transfer from this sub-step onward "
        "(coords-neighbors -> coords2d -> labels).",
    ),
) -> None:
    """Push coords_2d/3d + clusters + neighbors + cluster_labels to Postgres."""
    from embed.transfer import embed_transfer  # noqa: PLC0415

    embed_transfer(
        pg_url,
        chroma_db_dir,
        neighbors_db_file,
        labels_dir,
        start_from=start_from,
    )
