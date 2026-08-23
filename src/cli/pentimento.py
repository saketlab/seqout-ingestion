import csv
import json
import logging
import time
from pathlib import Path

import typer

from pentimento.build import build_pentimento

pentimento_app = typer.Typer()
preflight_app = typer.Typer(help="Run preflightx and load its output")
pentimento_app.add_typer(preflight_app, name="preflight")
logger = logging.getLogger(__name__)


@pentimento_app.command()
def build(
    output_db_file: Path = typer.Option(...),
    links_db_file: Path = typer.Option(
        ..., help="download_links.db -- the spine (run->experiment->study + fastq)"
    ),
    ncount_dir: list[Path] = typer.Option(
        ..., help="repeatable; ncount per-sample json dirs"
    ),
    preflightx_db_file: Path = typer.Option(
        ..., help="enrichment sqlite carrying preflightx_experiments"
    ),
    preflightx_csv_dir: Path = typer.Option(..., help="sc_preflight_v4/annotated"),
    quant_dir: Path = typer.Option(
        None,
        help="dir of <RUN>.quant.csv; omit to build a preflight-only Pentimento "
        "(microbe flags stay NULL = not measured)",
    ),
    quant_archive_db_file: Path = typer.Option(
        None,
        help="quant_archive.db from `pentimento quant-archive`. Loaded BEFORE "
        "--quant-dir, so a newer scan in the dir wins. Pass both once the CSVs "
        "have been archived and deleted, or the rollups shrink to the corpus "
        "still on disk",
    ),
    legacy_csv_dir: list[Path] = typer.Option(
        None,
        help="repeatable; older mukesh_*_preflight/annotated dirs. Same schema, "
        "but NO provenance json exists for them -- tagged preflightx_source="
        "'csv_legacy' so un-provenanced calls can be excluded",
    ),
    benchmark_file: Path = typer.Option(
        None,
        help="preflight_per_experiment.csv -- the internal comparison "
        "(gt_* vs pred_*) behind the calls",
    ),
    ena_db_file: Path = typer.Option(None, help="optional fallback for SRS->GSM"),
    sra_db_file: Path = typer.Option(None, help="preferred source of run->SRS->GSM"),
    geo_db_file: Path = typer.Option(None),
) -> None:
    build_pentimento(
        output_db_file,
        links_db_file,
        list(ncount_dir),
        preflightx_db_file,
        preflightx_csv_dir,
        quant_dir,
        benchmark_file,
        quant_archive_db_file,
        list(legacy_csv_dir or []),
        ena_db_file,
        sra_db_file,
        geo_db_file,
    )


@pentimento_app.command()
def flags(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
) -> None:
    """
    Rebuild run_microbe_flags on an existing Pentimento DB.

    `pentimento build` already does this; run it standalone after editing
    FLAG_PANEL.
    """
    from helpers import exec_query_from_file, sqlite_connect
    from pentimento.flags import build_flags

    # sqlite_connect sets busy_timeout and handles same-DB build contention.
    with sqlite_connect(db_file) as conn:
        exec_query_from_file("tables/pentimento.sql", conn=conn)
        build_flags(conn)


@pentimento_app.command()
def rollup(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
    sra_db_file: Path = typer.Option(
        None,
        help="sra_metadata.db; sra_studies.alias is the canonical SRP->GSE link",
    ),
) -> None:
    """
    Rebuild the sample- and study-grained rollups on an existing Pentimento DB.

    `pentimento build` already does this; run it standalone for rollup backfills.
    """
    from helpers import exec_query_from_file, sqlite_connect
    from pentimento.rollup import build_rollups

    # uri=True: build_rollups ATTACHes the SRA db as file:...?immutable=1, and
    # sqlite refuses a URI filename unless the connection was opened for it.
    with sqlite_connect(db_file, uri=True) as conn:
        exec_query_from_file("tables/pentimento.sql", conn=conn)
        build_rollups(conn, sra_db_file)


@pentimento_app.command()
def export_parquet(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
    output_dir: Path = typer.Option(...),
    table: list[str] = typer.Option(None, help="repeatable; default is every table"),
    batch_size: int = typer.Option(100_000),
    compression: str = typer.Option("zstd", help="zstd | snappy | none"),
    resume: bool = typer.Option(
        False,  # noqa: FBT003
        "--resume",
        help="skip finished tables",
    ),
) -> None:
    """
    Mirror Pentimento tables to one parquet file each.

    Views are joins over these tables and are recreated from SQL.
    """
    from pentimento.export import export_parquet as run

    counts = run(
        db_file,
        output_dir,
        list(table or []) or None,
        batch_size,
        compression,
        resume=resume,
    )
    logger.info("pentimento parquet: %s", json.dumps(counts))


@preflight_app.command("manifest")
def preflight_manifest(
    out_dir: Path = typer.Option(..., help="one <study>.csv per study to scan"),
    links_db_file: Path = typer.Option(..., help="download_links.db -- the fastq URLs"),
    unified_db_file: Path = typer.Option(
        ..., help="unified_metadata.db -- the population"
    ),
    enrichment_db_file: Path = typer.Option(
        None,
        help="optional; adds stated sex/assay so the mismatch flags have "
        "something to compare against. Omit for a scan-only manifest",
    ),
    accession_file: Path = typer.Option(
        None,
        help="one accession per line; default is every is_single_cell=1 study",
    ),
    batch_size: int = typer.Option(400),
) -> None:
    """
    Pick studies to scan and resolve their reads.

    Alias-aware lookup keeps GEO coverage when download links use SRA keys.
    """
    from pentimento.manifest import build_manifests

    accessions = (
        [ln.strip() for ln in accession_file.read_text().splitlines() if ln.strip()]
        if accession_file
        else None
    )
    counts = build_manifests(
        out_dir,
        links_db_file,
        unified_db_file,
        enrichment_db_file,
        accessions,
        batch_size,
    )
    logger.info("preflight manifest: %s", json.dumps(counts))


@preflight_app.command("run")
def preflight_run(
    in_dir: Path = typer.Option(..., help="manifests from `preflight manifest`"),
    out_dir: Path = typer.Option(..., help="annotated CSVs + provenance JSONs"),
    exe: str = typer.Option("preflightx", help="the preflightx binary"),
    n_reads: int = typer.Option(20_000, help="reads per run in the cheap pass"),
    max_runs: int = typer.Option(1, help="runs scanned per experiment"),
    deep_n_reads: int = typer.Option(
        400_000,
        help="reads for the deep sex pass, run ONLY when the cheap pass "
        "abstained (~80% of the time). 0 disables it -- fast, but sex stays "
        "mostly unknown",
    ),
    deep_max_runs: int = typer.Option(2),
    accession: str = typer.Option(
        None,
        help="scan ONE study instead of the whole dir. This is the fleet form: "
        "yuj appends one item per invocation, so put `--accession` last in "
        "work_command and it receives the item",
    ),
) -> None:
    """
    Scan reads for one manifest slice.

    Resumable by output existence; fleet scatter belongs to yuj.
    """
    from pentimento.preflight import annotate_dir

    counts = annotate_dir(
        in_dir,
        out_dir,
        exe,
        n_reads,
        max_runs,
        deep_n_reads,
        deep_max_runs,
        accession,
    )
    logger.info("preflight run: %s", json.dumps(counts))


@preflight_app.command("backfill-calls")
def preflight_backfill_calls(
    annotated_dir: Path = typer.Option(
        ..., help="annotated CSVs + their .provenance.json sidecars"
    ),
) -> None:
    """
    Re-derive annotated CSV call columns from stored provenance.

    Local rewrite from retained reports. CSVs with no report stay untouched so
    calls are not blanked by guesswork.
    """
    from pentimento.preflight import backfill_dir

    counts = backfill_dir(annotated_dir)
    logger.info("preflight backfill-calls: %s", json.dumps(counts))


@preflight_app.command("ingest")
def preflight_ingest(
    db_file: Path = typer.Option(..., help="pentimento.db built by `pentimento build`"),
    preflightx_csv_dir: Path = typer.Option(
        ..., help="annotated CSVs + provenance JSONs"
    ),
    sra_db_file: Path = typer.Option(
        None, help="sra_metadata.db; sra_studies.alias is the canonical SRP->GSE link"
    ),
) -> None:
    """
    Load a preflight run into an existing Pentimento DB.

    Fresh scans populate preflight columns and can be enriched later.
    """
    from helpers import exec_query_from_file, sqlite_connect
    from pentimento.build import ingest_preflight

    with sqlite_connect(db_file, uri=True) as conn:
        exec_query_from_file("tables/pentimento.sql", conn=conn)
        counts = ingest_preflight(conn, preflightx_csv_dir, sra_db_file)
    logger.info("preflight ingest: %s", json.dumps(counts))


@pentimento_app.command("study-scope-gaps")
def study_scope_gaps_cmd(
    pg_url: str = typer.Option(..., help="read-only probe; nothing is written"),
    out_dir: Path = typer.Option(..., help="receives the accession lists + a TSV"),
    need: str = typer.Option(
        None, help="preflight | quant. Default is every study-scope study."
    ),
    limit: int = typer.Option(None, help="cap the list, for a smoke run"),
    min_cells: int = typer.Option(1, help="drop studies below this cell count"),
    include_unfiltered: bool = typer.Option(
        default=False,
        help="also list studies whose matrix is unfiltered. Their 'cells' are 10x "
        "barcodes, not cells, so they are excluded by default",
    ),
) -> None:
    """
    List study-scope cell counts that sample-grained probes miss.

    Pooled matrices have no pentimento_sample row but remain real single-cell
    data. Feed output to preflight manifest and quant-runs.
    """
    from pentimento.gaps import study_scope_gaps

    rows = study_scope_gaps(
        pg_url,
        need=need,
        limit=limit,
        min_cells=min_cells,
        include_unfiltered=include_unfiltered,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    keep = rows  # the SQL already applied the unfiltered filter
    (out_dir / "study_scope.txt").write_text("".join(f"{acc}\n" for acc, _, _ in keep))
    with (out_dir / "study_scope.tsv").open("w", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(("study_accession", "cells", "unfiltered"))
        writer.writerows((acc, cells, int(unf)) for acc, cells, unf in rows)
    dropped = len(rows) - len(keep)
    logger.info(
        "study-scope gaps: %s",
        json.dumps(
            {
                "studies": len(keep),
                "unfiltered_dropped": dropped,
                "cells": sum(c for _, c, _ in keep),
                "list": str(out_dir / "study_scope.txt"),
            }
        ),
    )


@pentimento_app.command("quant-runs")
def quant_runs(
    out_file: Path = typer.Option(..., help="one-column CSV for run_quant.sh"),
    links_db_file: Path = typer.Option(..., help="download_links.db"),
    unified_db_file: Path = typer.Option(..., help="unified_metadata.db, for aliases"),
    accession: list[str] = typer.Option(
        None, help="repeatable; a study (GSE/SRP/PRJ/...) or a run (SRR/ERR/DRR)"
    ),
    accession_file: Path = typer.Option(
        None, help="one accession per line; combined with any --accession"
    ),
    max_per_study: int = typer.Option(
        None,
        help="cap runs contributed by any one study; drops are logged, not silent",
    ),
    exclude_study: list[str] = typer.Option(
        None, help="repeatable; drop this study entirely rather than capping it"
    ),
) -> None:
    """
    Resolve accessions to the run list the external quantifier scans.

    Studies resolve through aliases; runs pass through. The quantifier remains a
    standalone fleet script.
    """
    from pentimento.manifest import run_accessions

    wanted = list(accession or [])
    if accession_file:
        wanted += [a for a in accession_file.read_text().split() if a]
    if not wanted:
        msg = "give --accession and/or --accession-file"
        raise typer.BadParameter(msg)
    runs = run_accessions(
        links_db_file,
        unified_db_file,
        wanted,
        max_per_study=max_per_study,
        exclude_studies=set(exclude_study or []),
    )
    if not runs:
        logger.warning("no runs resolved; nothing to quantify")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text("accession\n" + "".join(f"{r}\n" for r in runs))
    logger.info("quant-runs: %s", json.dumps({"runs": len(runs), "out": str(out_file)}))


@pentimento_app.command("gaps")
def gaps(
    pg_url: str = typer.Option(..., help="read-only probe; nothing is written"),
    out_dir: Path = typer.Option(..., help="one <gap>.txt of study accessions each"),
    need: list[str] = typer.Option(
        None, help="repeatable: sex-assay | quant. Default is every gap."
    ),
    limit: int = typer.Option(None, help="cap each list, for a smoke run"),
    max_samples: int = typer.Option(
        None, help="drop studies with more counted samples than this"
    ),
    links_db_file: Path = typer.Option(
        None, help="download_links.db; given, drops studies with no fastq"
    ),
    unified_db_file: Path = typer.Option(None, help="unified_metadata.db, for aliases"),
) -> None:
    """
    List counted studies missing read-derived calls.

    A sample with `cells` or `genes` is known single-cell data, so missing calls
    feed preflight and quant backfills.
    """
    from pentimento.gaps import GAP_SQL, gap_studies
    from pentimento.manifest import studies_with_runs

    if links_db_file and not unified_db_file:
        msg = "--links-db-file needs --unified-db-file to resolve aliases"
        raise typer.BadParameter(msg)
    wanted = list(need or GAP_SQL)
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for gap in wanted:
        studies = gap_studies(pg_url, gap, limit, max_samples)
        if links_db_file:
            # preflight reads FASTQ; quant fetches Logan by run accession
            studies = studies_with_runs(
                links_db_file,
                unified_db_file,
                studies,
                require_fastq=gap == "sex-assay",
            )
        (out_dir / f"{gap}.txt").write_text("".join(f"{s}\n" for s in studies))
        counts[gap] = len(studies)
    logger.info("gaps: %s", json.dumps(counts))


@pentimento_app.command("verify-quant")
def verify_quant(
    results_dir: Path = typer.Option(
        ..., help="a fleet results dir, or VIRUS_QUANT_DIR"
    ),
    sample: int = typer.Option(None, help="stop after this many files"),
    fail_on: float = typer.Option(
        1.0, help="exit 1 if this %% of scanned files have problems"
    ),
) -> None:
    """
    Check quantifier output before it joins the corpus.

    Parsed files with no targets look like measured-empty results; catch them
    before derived rollups consume them.
    """
    from pentimento.verify import verify_dir

    counts = verify_dir(results_dir, sample)
    scanned = counts.get("scanned", 0)
    clean = counts.get("clean", 0)
    logger.info("verify: %s", json.dumps(counts, sort_keys=True))
    if not scanned:
        logger.warning("no .quant.csv found in %s", results_dir)
        return
    bad_pct = 100 * (scanned - clean) / scanned
    logger.info("%d/%d clean (%.2f%% with problems)", clean, scanned, bad_pct)
    if bad_pct > fail_on:
        raise typer.Exit(1)


@pentimento_app.command("repair-quant-status")
def repair_quant_status(
    results_dir: Path = typer.Option(
        ..., help="VIRUS_QUANT_DIR or a fleet results dir"
    ),
    apply: bool = typer.Option(default=False, help="write; otherwise report only"),
) -> None:
    """
    Restore `ok` for complete results mislabelled as failures.

    Touches CSVs with real targets and total_reads. Zero-target files stay as
    they are because clean-empty and failed-early are indistinguishable there.
    """
    from pentimento.verify import repair_statuses

    counts = repair_statuses(results_dir, dry_run=not apply)
    mode = "applied" if apply else "dry run"
    logger.info("repair (%s): %s", mode, json.dumps(counts, sort_keys=True))


@pentimento_app.command("quant-archive")
def quant_archive(
    quant_dir: Path = typer.Option(..., help="VIRUS_QUANT_DIR, or a fleet results dir"),
    archive_db_file: Path = typer.Option(
        ..., help="accumulating quant_archive.db; created if absent"
    ),
    prune: bool = typer.Option(
        default=False,
        help="delete the archived files; refuses unless every run verified",
    ),
) -> None:
    """
    Fold quantifier output into the archive so CSVs can be deleted.

    `transfer pentimento` is append/update only; archived runs keep their
    Postgres rows. Verify archive membership prior to --prune.
    """
    from pentimento.quant_archive import (
        archive_quant,
        prune_archived,
        verify_archived,
    )

    # fold_started marks the archive snapshot used by prune verification.
    fold_started = time.time()
    counts = archive_quant(quant_dir, archive_db_file)
    logger.info("archive holds: %s", json.dumps(counts, sort_keys=True))

    n_disk, missing = verify_archived(quant_dir, archive_db_file)
    if missing:
        logger.error(
            "%d of %d runs on disk are NOT in the archive; not pruning (first: %s)",
            len(missing),
            n_disk,
            missing[:5],
        )
        raise typer.Exit(1)
    if not prune:
        logger.info(
            "all %d runs archived; rerun with --prune to delete the files", n_disk
        )
        return

    prune_archived(quant_dir, archive_db_file, newer_than=fold_started)
