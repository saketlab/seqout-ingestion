"""
Pipeline to enrich a batch of accessions into an already-correct SQLite.

STAGES
------
1  enrich   the selected module per accession, with the guards applied to the
            four frames before the CSVs are written.  Audit rows accumulate in
            the same 12 columns as ``enrichment_corrections_log``.
2  ingest   the CSVs into the batch SQLite, plus the audit log.
3  corpus   organism truth from the SOURCE archives, for rows whose sample frame
            never stated a taxid.  Needs ``--tier-scripts-dir``.
4  verify   re-run every shipped detector against the batch DB and require zero.
            Needs ``--tier-scripts-dir``.  A pipeline that claims correctness
            without re-checking is what produced the mess this replaces.

"""

from __future__ import annotations

import csv
import importlib
import json
import logging
import os
import pickle
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_MODULE = "enrich.evidence_gate"
DEFAULT_EMBEDDING_MODEL = "abhinand/MedEmbed-large-v0.1"

DECLARED_OUT_COLS = (
    "insdc_analyte",
    "insdc_assay_modality",
    "insdc_library_strategy",
    "insdc_library_source",
    "insdc_library_selection",
    "insdc_conflict",
    "library_strategy",
)

_FRAME_SUFFIXES = ("enriched", "cellosaurus", "provenance", "ontology")
_AUDIT_ROW_LEN = 12


def read_accessions(spec: str | Path) -> list[str]:
    """Read accessions: one per line, the first CSV column, or a comma list."""
    path = Path(spec)
    if not path.exists():
        return [a.strip() for a in str(spec).split(",") if a.strip()]
    out, seen = [], set()
    for line in path.read_text().splitlines():
        acc = line.split(",")[0].strip().strip('"')
        if not acc or acc.lower() in ("accession", "study_accession") or acc in seen:
            continue
        seen.add(acc)
        out.append(acc)
    return out


def load_projects(accessions: list[str], cache: Path | None) -> dict:
    """Projects from a prefetched cache if given, else the seqout bulk endpoint."""
    if cache:
        with Path(cache).open("rb") as handle:
            blob = pickle.load(handle)  # noqa: S301
        projects = (
            blob["projects"] if isinstance(blob, dict) and "projects" in blob else blob
        )
        return {a: projects[a] for a in accessions if a in projects}

    from enrich.projects import fetch_projects

    got = {}
    for i in range(0, len(accessions), 25):
        for project in fetch_projects(accessions[i : i + 25]):
            got[project.accession] = project
    return got


def enrich_batch(
    projects: dict,
    out_dir: Path,
    *,
    module: str = DEFAULT_MODULE,
    model: str,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    tierc_policy: str = "evidence",
    vocab: str = "v5",
    species: dict | None = None,
    declared: bool = True,
    verbose: bool = False,
) -> tuple[list[tuple], dict[str, int]]:
    """Enrich every project, guard the frames, write four CSVs each."""
    from enrich import declared as declared_mod
    from enrich.guards import apply_inline_guards, evidence_supported_fn

    source = declared_mod.DeclaredSource() if declared else None
    enrich_df = importlib.import_module(module).enrich_df
    out_dir.mkdir(parents=True, exist_ok=True)

    audits: list[tuple] = []
    totals: dict[str, int] = {}
    failed: list[tuple[str, str]] = []

    for n, (acc, original) in enumerate(projects.items(), 1):
        if _is_complete(acc, out_dir):
            logger.info("[%d/%d] %s: SKIP (already complete)", n, len(projects), acc)
            continue
        samples = getattr(original, "samples", None)
        if not isinstance(samples, pd.DataFrame) or samples.empty:
            logger.info("[%d/%d] %s: SKIP (no samples)", n, len(projects), acc)
            continue

        project = original
        if source is not None:
            project, dstats = declared_mod.annotate(project, source)
            for k, v in dstats.items():
                totals["declared_" + k] = totals.get("declared_" + k, 0) + v
            samples = project.samples

        pre = samples.copy()
        started = time.monotonic()
        try:
            frames = enrich_df(
                project,
                enrichment_backend=model,
                embedding_model=embedding_model,
                cellosaurus=True,
                verbose=verbose,
                **(species or {}),
            )
        except Exception as exc:  # one study must not kill the batch
            failed.append((acc, f"{type(exc).__name__}: {exc}"))
            logger.warning(
                "[%d/%d] %s: FAIL (%s: %s)",
                n,
                len(projects),
                acc,
                type(exc).__name__,
                exc,
            )
            continue

        guarded, rows, stats = apply_inline_guards(
            project,
            *frames,
            tierc_policy=tierc_policy,
            vocab=vocab,
            supported=evidence_supported_fn(project, pre),
            declared=declared,
        )
        audits.extend(rows)
        for k, v in stats.items():
            totals[k] = totals.get(k, 0) + v

        for frame, suffix in zip(guarded, _FRAME_SUFFIXES, strict=True):
            _to_csv(frame, out_dir / f"{acc}_{suffix}.csv")
        if source is not None:
            _write_declared(pre, out_dir / f"{acc}_declared.csv")
        logger.info(
            "[%d/%d] %s: OK %d samples (%.0fs) guards=%s",
            n,
            len(projects),
            acc,
            len(guarded[0]),
            time.monotonic() - started,
            {k: v for k, v in stats.items() if v},
        )

    tag = os.environ.get("ENRICH_BATCH_SHARD", "")
    (out_dir / f"_inline_corrections{tag}.json").write_text(json.dumps(audits))
    if failed:
        (out_dir / "_failed.json").write_text(json.dumps(failed, indent=1))
    totals["failed_studies"] = len(failed)
    return audits, totals


def _is_complete(acc: str, out_dir: Path) -> bool:
    return all((out_dir / f"{acc}_{k}.csv").exists() for k in _FRAME_SUFFIXES)


def _write_declared(pre: pd.DataFrame, path: Path) -> None:
    """
    Both INSDC axes and every ENA typed value, VERBATIM.

    ``insdc_conflict`` marks a sample whose own experiments declare different
    strategies.  That is information, and this file is where it stays visible.
    """
    from enrich.declared import ENA_PREFIX

    id_col = next((c for c in ("Sample", "Accession") if c in pre.columns), None)
    if id_col is None:
        return
    cols = [c for c in DECLARED_OUT_COLS if c in pre.columns]
    cols += sorted(c for c in pre.columns if c.startswith(ENA_PREFIX))
    if not cols:
        return
    frame = pre[[id_col, *cols]].copy()
    if id_col != "Sample":
        frame = frame.rename(columns={id_col: "Sample"})
    _to_csv(frame, path)


def _to_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a CSV, surviving the surrogates an LLM occasionally emits."""
    try:
        df.to_csv(path, index=False)
    except UnicodeEncodeError:
        df.to_csv(path, index=False, encoding="utf-8", errors="replace")


def collect_audits(csv_dir: Path) -> list[tuple]:
    """Inline audit rows, from this runner's JSON or from per-study CSVs."""
    rows: list[tuple] = []
    blobs = sorted(csv_dir.glob("_inline_corrections*.json"))
    if blobs:
        for blob in blobs:
            rows.extend(tuple(r) for r in json.loads(blob.read_text()))
        return rows
    for path in sorted(csv_dir.glob("*_corrections.csv")):
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle)
            if next(reader, None) is None:
                continue
            rows.extend(tuple(r) for r in reader if len(r) == _AUDIT_ROW_LEN)
    return rows


def run_tier_script(scripts_dir: Path, script: str, db_file: Path, *args: str) -> str:
    env = dict(
        os.environ,
        V4_DB=str(db_file),
        PYTHONPATH=os.pathsep.join(
            filter(None, [str(scripts_dir.parent), os.environ.get("PYTHONPATH", "")])
        ),
    )
    cmd = [sys.executable, str(scripts_dir / script), *args]
    logger.info("$ V4_DB=%s %s", db_file, " ".join(cmd))
    result = subprocess.run(  # noqa: S603
        cmd,
        cwd=scripts_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    sys.stdout.write(result.stdout)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        msg = f"{script} failed ({result.returncode})"
        raise RuntimeError(msg)
    return result.stdout


def detector_counts(scripts_dir: Path, db_file: Path) -> dict[str, int]:
    """Re-run each shipped detector; return what it still finds."""
    counts = {}
    out = run_tier_script(scripts_dir, "detect_h9_v4.py", db_file)
    counts["wrong_h9"] = _grab(out, "faulty (WA09 hESC mis-mapped to H9 lymphoma):")
    out = run_tier_script(scripts_dir, "build_badlabel_v4.py", db_file)
    counts["bad_label_samples"] = _grab(out, "v4 bad_label set:")
    counts["bad_label_ledger"] = _grab(out, "ledger rows:")
    out = run_tier_script(scripts_dir, "build_organism_truth_v4.py", db_file)
    counts["wrong_organism"] = _grab(out, "organism truth v4 (wrong human-default):")
    return counts


def _grab(text: str, prefix: str) -> int:
    for line in text.splitlines():
        if line.strip().startswith(prefix):
            tail = line.split(prefix, 1)[1].strip().split()[0]
            return int(tail.replace(",", "").split("->")[0])
    return -1


def run_shards(
    accessions: list[str], csv_dir: Path, workers: int, cli_args: list[str]
) -> None:
    """Fan stage 1 out over N processes, round-robin by accession."""
    csv_dir.mkdir(parents=True, exist_ok=True)
    entrypoint = Path(__file__).resolve().parents[1] / "main.py"
    procs = []
    for i in range(workers):
        shard = accessions[i::workers]
        if not shard:
            continue
        shard_file = csv_dir / f"_shard{i}.txt"
        shard_file.write_text("\n".join(shard) + "\n")
        cmd = [
            sys.executable,
            str(entrypoint),
            "enrich",
            "batch",
            "--accession-file",
            str(shard_file),
            *cli_args,
            "--stage1-only",
        ]
        env = dict(os.environ, ENRICH_BATCH_SHARD=str(i))
        log_file = (csv_dir / f"_shard{i}.log").open("w")
        procs.append(
            (
                i,
                # S603: cmd is this interpreter re-invoking this same CLI on a
                # shard file we just wrote. No shell.
                subprocess.Popen(  # noqa: S603
                    cmd,
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                ),
                log_file,
            )
        )
        logger.info(
            "shard %d: %d accessions -> %s", i, len(shard), csv_dir / f"_shard{i}.log"
        )
    for i, proc, log_file in procs:
        code = proc.wait()
        log_file.close()
        logger.info("shard %d exited %d", i, code)
