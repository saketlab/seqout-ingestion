"""
CLI for rebuilding the Neo4j ontology graph.

``ontology build`` is the whole thing end to end. The individual stages are
exposed separately because the download and extract steps are slow and a
rebuild usually fails somewhere in the middle of the Neo4j load, not before it.
"""

import logging
from pathlib import Path

import typer

from ontology.sources import BY_NAME, SOURCES

ontology_app = typer.Typer(help="Rebuild the Neo4j ontology graph (query expansion).")

logger = logging.getLogger(__name__)


def _selected(only: list[str] | None) -> list:
    """Resolve ``--only`` names to Source objects, preserving build order."""
    if not only:
        return list(SOURCES)
    unknown = [n for n in only if n not in BY_NAME]
    if unknown:
        msg = f"unknown ontology {unknown}; known: {sorted(BY_NAME)}"
        raise typer.BadParameter(msg)
    return [s for s in SOURCES if s.name in set(only)]


@ontology_app.command("list")
def list_sources() -> None:
    """Show every ontology in the registry and what it contributes."""
    for s in SOURCES:
        hier = "synonyms + hierarchy" if s.hierarchy else "synonyms only"
        tag = f"  [tagged :{s.label}]" if s.label else ""
        typer.echo(f"{s.name:<12} {s.title}{tag}")
        typer.echo(f"{'':<12} {hier}  <-  {s.url}")
        if s.notes:
            typer.echo(f"{'':<12} {s.notes}")
        typer.echo("")


@ontology_app.command()
def download(
    output_dir: Path = typer.Option(...),
    only: list[str] = typer.Option(None, help="ontology names; default is all"),
    workers: int = typer.Option(4),
    force: bool = typer.Option(default=False, help="re-download even if present"),
) -> None:
    """Fetch the raw ontology releases into ``<output-dir>/ontology/raw/``."""
    from ontology.extract import download as download_source

    root = output_dir / "ontology"
    for source in _selected(only):
        download_source(source, root, workers=workers, force=force)


@ontology_app.command()
def extract(
    output_dir: Path = typer.Option(...),
    only: list[str] = typer.Option(None, help="ontology names; default is all"),
) -> None:
    """Parse the raw releases into synonym CSVs (and MeSH's derived OBO-JSON)."""
    from ontology.extract import extract as extract_source

    root = output_dir / "ontology"
    for source in _selected(only):
        extract_source(source, root)


@ontology_app.command()
def load(
    output_dir: Path = typer.Option(...),
    neo4j_uri: str = typer.Option("neo4j://localhost:7687", envvar="NEO4J_URI"),
    neo4j_user: str = typer.Option("neo4j", envvar="NEO4J_USER"),
    neo4j_password: str = typer.Option(..., envvar="NEO4J_PASSWORD"),
    only: list[str] = typer.Option(None, help="ontology names; default is all"),
    skip_has_children: bool = typer.Option(
        default=False,
        help="skip the has_children pass; only safe if another load follows",
    ),
) -> None:
    """Load the extracted artifacts into Neo4j, then stamp ``has_children``."""
    from ontology import graph

    root = output_dir / "ontology"
    with graph.connect(neo4j_uri, neo4j_user, neo4j_password) as driver:
        graph.ensure_constraint(driver)
        for source in _selected(only):
            graph.load_synonyms(driver, source, root)
            if source.hierarchy:
                graph.load_hierarchy(driver, source, root)
        if not skip_has_children:
            graph.stamp_has_children(driver)


@ontology_app.command()
def build(
    output_dir: Path = typer.Option(...),
    neo4j_uri: str = typer.Option("neo4j://localhost:7687", envvar="NEO4J_URI"),
    neo4j_user: str = typer.Option("neo4j", envvar="NEO4J_USER"),
    neo4j_password: str = typer.Option(..., envvar="NEO4J_PASSWORD"),
    only: list[str] = typer.Option(None, help="ontology names; default is all"),
    workers: int = typer.Option(4),
    force_download: bool = typer.Option(default=False),
) -> None:
    """
    Rebuild the whole graph: download, extract, load, stamp ``has_children``.

    Safe to re-run and safe to interrupt -- downloads resume, and every write
    is a MERGE. Expect this to take hours, most of it in the Neo4j load.
    """
    from ontology import graph
    from ontology.extract import download as download_source
    from ontology.extract import extract as extract_source

    root = output_dir / "ontology"
    sources = _selected(only)

    with graph.connect(neo4j_uri, neo4j_user, neo4j_password) as driver:
        graph.ensure_constraint(driver)
        # One source at a time, all the way through, so a failure costs one
        # ontology rather than the whole run.
        for source in sources:
            logger.info("=== %s (%s) ===", source.name, source.title)
            download_source(source, root, workers=workers, force=force_download)
            extract_source(source, root)
            graph.load_synonyms(driver, source, root)
            if source.hierarchy:
                graph.load_hierarchy(driver, source, root)

        graph.stamp_has_children(driver)
        for key, value in graph.stats(driver).items():
            logger.info("%-14s %d", key, value)


@ontology_app.command()
def xrefs(
    output_dir: Path = typer.Option(...),
    neo4j_uri: str = typer.Option("neo4j://localhost:7687", envvar="NEO4J_URI"),
    neo4j_user: str = typer.Option("neo4j", envvar="NEO4J_USER"),
    neo4j_password: str = typer.Option(..., envvar="NEO4J_PASSWORD"),
    only: list[str] = typer.Option(None, help="ontology names; default is all"),
) -> None:
    """
    Stamp ``xrefs`` provenance onto an already-built graph, touching no edge.

    This is the backfill path, for a graph whose edges predate the ``id``
    column. Prefer it over a full ``build`` in that case: ``build`` MERGEs the
    synonym edges, and a freshly regenerated extract never reproduces an old one
    exactly, so the pairs that drifted would be added *beside* the originals
    rather than replacing them. Every write here is MATCH-based and cannot
    create a node or a relationship.

    Needs the extracted CSVs on disk (``ontology extract``), not the raw
    downloads.
    """
    from ontology import graph

    root = output_dir / "ontology"
    with graph.connect(neo4j_uri, neo4j_user, neo4j_password) as driver:
        for source in _selected(only):
            graph.load_xrefs(driver, source, root)
        for key, value in graph.stats(driver).items():
            logger.info("%-14s %d", key, value)


@ontology_app.command("has-children")
def has_children(
    neo4j_uri: str = typer.Option("neo4j://localhost:7687", envvar="NEO4J_URI"),
    neo4j_user: str = typer.Option("neo4j", envvar="NEO4J_USER"),
    neo4j_password: str = typer.Option(..., envvar="NEO4J_PASSWORD"),
) -> None:
    """Recompute the ``has_children`` flag. Run after any load that skipped it."""
    from ontology import graph

    with graph.connect(neo4j_uri, neo4j_user, neo4j_password) as driver:
        graph.stamp_has_children(driver)


@ontology_app.command()
def stats(
    neo4j_uri: str = typer.Option("neo4j://localhost:7687", envvar="NEO4J_URI"),
    neo4j_user: str = typer.Option("neo4j", envvar="NEO4J_USER"),
    neo4j_password: str = typer.Option(..., envvar="NEO4J_PASSWORD"),
) -> None:
    """Print node and relationship counts, to sanity-check a rebuild."""
    from ontology import graph

    with graph.connect(neo4j_uri, neo4j_user, neo4j_password) as driver:
        for key, value in graph.stats(driver).items():
            typer.echo(f"{key:<14} {value}")
