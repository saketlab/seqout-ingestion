from pathlib import Path

import typer

from sc_evidence.build import sc_evidence_build
from sc_evidence.coverage import sc_evidence_coverage

sc_evidence_app = typer.Typer()


@sc_evidence_app.command()
def build(
    sc_evidence_db_file: Path = typer.Option(..., help="output; created if absent"),
    sra_db_file: Path = typer.Option(...),
    ena_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    gsa_db_file: Path = typer.Option(None),
    gsa_human_db_file: Path = typer.Option(
        None,
        help="GSA's controlled-access half. GSA is ingested into two files and "
        "the audit's GSA numbers cover both; omitting this loses 182 of its "
        "1,247 single-cell studies.",
    ),
    dra_db_file: Path = typer.Option(None),
    gea_db_file: Path = typer.Option(None),
    ena_delta_db_file: Path = typer.Option(
        None,
        help="ENA's dated increment. ENA is ingested as a base checkpoint plus "
        "increments, so omitting this reports a floor: the base alone is short "
        "of what the archive holds.",
    ),
    enriched_db_file: Path = typer.Option(
        None,
        help="enrichment generation SQLite (ontology_samples + "
        "provenance_samples). Drives enriched_assay_library_col, the only "
        "channel that sees a study whose archive fields say plain "
        "TRANSCRIPTOMIC and whose prose says nothing.",
    ),
    only_rules: str = typer.Option(
        None,
        help="comma-separated rule slugs to rebuild; the rest keep their existing "
        "rows. Omit to rebuild every rule.",
    ),
    batch_size: int = typer.Option(20000),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
    """
    Derive single-cell evidence from archive tables.

    Full rebuilds are deterministic outputs of sc_evidence.rules.
    """
    sc_evidence_build(
        sc_evidence_db_file,
        {
            "sra": sra_db_file,
            "ena": ena_db_file,
            "geo": geo_db_file,
            "ae": ae_db_file,
            "gsa": gsa_db_file,
            "gsa_human": gsa_human_db_file,
            "dra": dra_db_file,
            "gea": gea_db_file,
            "ena_delta": ena_delta_db_file,
            "enrichment": enriched_db_file,
        },
        only_rules=[r.strip() for r in only_rules.split(",")] if only_rules else None,
        batch_size=batch_size,
        is_dry_run=is_dry_run,
    )


@sc_evidence_app.command()
def coverage(
    sc_evidence_db_file: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    ena_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    gsa_db_file: Path = typer.Option(None),
    gsa_human_db_file: Path = typer.Option(None),
    dra_db_file: Path = typer.Option(None),
    gea_db_file: Path = typer.Option(None),
    ena_delta_db_file: Path = typer.Option(None),
    enriched_db_file: Path = typer.Option(None),
    fail_on_regression: bool = typer.Option(
        default=False,
        help="exit non-zero when an archive's coverage fell against the previous "
        "build, or when the archive declares more single-cell studies than the "
        "rules found. Use this in CI.",
    ),
) -> None:
    """
    Record one sc_archive_coverage row per archive for this build.

    Appends so rule losses appear as coverage drops.
    """
    sc_evidence_coverage(
        sc_evidence_db_file,
        {
            "sra": sra_db_file,
            "ena": ena_db_file,
            "geo": geo_db_file,
            "ae": ae_db_file,
            "gsa": gsa_db_file,
            "gsa_human": gsa_human_db_file,
            "dra": dra_db_file,
            "gea": gea_db_file,
            "ena_delta": ena_delta_db_file,
            "enrichment": enriched_db_file,
        },
        fail_on_regression=fail_on_regression,
    )
