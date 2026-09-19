from pathlib import Path

import typer

embed_app = typer.Typer()


@embed_app.command()
def run(
    geo_db_file: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    unified_metadata_db_file: Path = typer.Option(...),
    chroma_db_dir: Path = typer.Option(...),
    model_name: str = typer.Option("abhinand/MedEmbed-large-v0.1"),
) -> None:
    from embed.embedder import embed_new

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
    from embed.coords import compute_coords

    compute_coords(chroma_db_dir)


@embed_app.command()
def cluster(
    chroma_db_dir: Path = typer.Option(...),
) -> None:
    from embed.cluster import compute_clusters

    compute_clusters(chroma_db_dir)


@embed_app.command()
def cluster_leiden(
    chroma_db_dir: Path = typer.Option(...),
) -> None:
    from embed.cluster_leiden import compute_leiden_clusters

    compute_leiden_clusters(chroma_db_dir)


@embed_app.command()
def cluster_leiden_coarse(
    chroma_db_dir: Path = typer.Option(...),
    levels: int = typer.Option(3, help="How many levels to add above the current root"),
) -> None:
    """
    Add coarser levels above the Leiden root by agglomeration.

    New levels nest over existing clusters, preserving finer-level labels.
    """
    from embed.cluster_leiden_coarse import compute_coarse_leiden_clusters

    compute_coarse_leiden_clusters(chroma_db_dir, levels=levels)


@embed_app.command()
def neighbors(
    chroma_db_dir: Path = typer.Option(...),
    neighbors_db_file: Path = typer.Option(...),
    k: int = typer.Option(300),
    query_chunk: int = typer.Option(1024),
) -> None:
    from embed.neighbors import compute_neighbors

    compute_neighbors(chroma_db_dir, neighbors_db_file, k, query_chunk)


@embed_app.command("neighbors-quick")
def neighbors_quick(
    chroma_db_dir: Path = typer.Option(...),
    pg_url: str = typer.Option(..., envvar="PG_URL"),
    k: int = typer.Option(300),
    query_chunk: int = typer.Option(1024),
) -> None:
    """
    Backfill similar projects after `embed run`.

    Write {accession, distance} neighbors directly to Postgres for geo/sra rows
    without neighbors, using non-nulling upserts. Skips UMAP and neighbors.sqlite3.
    See src/embed/quick_neighbors.py for coverage.
    """
    from embed.quick_neighbors import push_new_neighbors

    push_new_neighbors(chroma_db_dir, pg_url, k, query_chunk)


@embed_app.command()
def labels(
    chroma_db_dir: Path = typer.Option(...),
    labels_dir: Path = typer.Option(...),
    vllm_base_url: str = typer.Option("http://localhost:1729/v1"),
    model: str = typer.Option("unsloth/Qwen3.6-27B-GGUF:UD-Q8_K_XL"),
    prefix: str = typer.Option(
        "cluster_l", help="Cluster metadata key prefix, e.g. cluster_l or leiden_l"
    ),
    skip_level: list[str] = typer.Option(
        [], help="Level name(s) to skip, e.g. --skip-level leiden_l1 (repeatable)"
    ),
) -> None:
    """
    Read Chroma, extract keywords, and generate LLM labels in one pass.

    For large clusterings, use `precompute-keywords` then `label-from-keywords`
    to avoid reloading Chroma on reruns.
    """
    from embed.labels import generate_labels

    generate_labels(
        chroma_db_dir,
        labels_dir,
        vllm_base_url,
        model,
        prefix=prefix,
        skip_levels=skip_level,
    )


@embed_app.command()
def precompute_keywords(
    chroma_db_dir: Path = typer.Option(...),
    keywords_dir: Path = typer.Option(...),
    prefix: str = typer.Option(
        "cluster_l", help="Cluster metadata key prefix, e.g. cluster_l or leiden_l"
    ),
    skip_level: list[str] = typer.Option(
        [], help="Level name(s) to skip, e.g. --skip-level leiden_l1 (repeatable)"
    ),
) -> None:
    """
    Cache Chroma TF-IDF keywords and centroids in keywords_<level>.csv.

    Run once before `label-from-keywords`.
    """
    from embed.labels import precompute_keywords as run

    run(chroma_db_dir, keywords_dir, prefix=prefix, skip_levels=skip_level)


@embed_app.command()
def label_from_keywords(
    keywords_dir: Path = typer.Option(...),
    labels_dir: Path = typer.Option(...),
    vllm_base_url: str = typer.Option("http://localhost:1729/v1"),
    model: str = typer.Option("unsloth/Qwen3.6-27B-GGUF:UD-Q8_K_XL"),
    concurrency: int = typer.Option(
        12, help="In-flight requests; only needs to exceed the server's slot count"
    ),
    skip_level: list[str] = typer.Option(
        [], help="Level name(s) to skip, e.g. --skip-level leiden_l1 (repeatable)"
    ),
) -> None:
    """
    Generate LLM labels from cached keywords, resuming interrupted runs.

    Requires no Chroma or TF-IDF reload.
    """
    from embed.labels import label_from_keywords as run

    run(
        keywords_dir,
        labels_dir,
        skip_levels=skip_level,
        vllm_base_url=vllm_base_url,
        model=model,
        concurrency=concurrency,
    )
