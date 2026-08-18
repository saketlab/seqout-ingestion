"""Run preflightx over a manifest, writing the annotated CSV and provenance JSON."""

from __future__ import annotations

import csv
import json
import logging
import subprocess
from collections import Counter, OrderedDict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SUBCMD = "check"
SEX_NOCALL = frozenset({"", "unknown", "undetermined", "ambiguous", "na"})
CALL_COLUMNS = (
    "preflightx_species",
    "preflightx_sex",
    "preflightx_assay",
    "preflightx_library",
    "preflightx_modality",
)
TIMEOUT_S = 900


def to_https(url: str) -> str:
    """Rewrite an ENA ftp path to https; the streamed reader only speaks https."""
    url = url.strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://")):
        return url
    return "https://" + url.removeprefix("ftp://")


def pick_r1_r2(fastq_ftp: str) -> tuple[str | None, str | None]:
    """From a ';'-joined fastq list return (R1, R2); prefer _1/_2, else first."""
    parts = [to_https(p) for p in (fastq_ftp or "").split(";") if p.strip()]
    if not parts:
        return None, None
    r1 = next((p for p in parts if p.endswith("_1.fastq.gz")), None)
    r2 = next((p for p in parts if p.endswith("_2.fastq.gz")), None)
    if r1:
        return r1, r2
    return parts[0], None


def call_preflightx(
    r1: str,
    r2: str | None,
    n_reads: int,
    exe: str,
    sex_reads: int | None = None,
) -> dict[str, Any] | None:
    """Return the parsed preflightx JSON for one run, or None if it failed."""
    cmd = [exe, "--n-reads", str(n_reads)]
    if sex_reads is not None:
        cmd += ["--sex-reads", str(sex_reads)]
    cmd += ["--json", SUBCMD, r1]
    if r2:
        cmd.append(r2)
    try:
        out = subprocess.run(  # noqa: S603
            cmd, capture_output=True, text=True, timeout=TIMEOUT_S, check=False
        )
    except (subprocess.TimeoutExpired, OSError):
        logger.debug("preflightx failed on %s", r1)
        return None
    if out.returncode != 0 or not out.stdout.strip():
        return None
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return None


def extract(report: dict[str, Any]) -> tuple[str, str, str, str, str]:
    """(species, sex, assay, library, modality) from one report; '' when absent."""
    chemistry = report.get("chemistry") or {}
    classification = report.get("classification") or {}
    return (
        (report.get("species") or {}).get("called") or "",
        (report.get("sex") or {}).get("verdict") or "",
        chemistry.get("assay_display") or chemistry.get("assay_id") or "",
        classification.get("library") or "",
        classification.get("modality") or "",
    )


def vote(values: list[str]) -> str:
    """Majority across an experiment's runs, blanks ignored, ties to first-seen."""
    counts = Counter(v for v in values if v)
    if not counts:
        return ""
    top = max(counts.values())
    return next((v for v in values if v and counts[v] == top), "")


def _deep_sex(
    runs: list[tuple[str, str | None]],
    deep_n_reads: int,
    deep_max_runs: int,
    exe: str,
    sink: list[dict[str, Any]],
) -> str:
    """Re-call sex only, scanning far more reads. Appends reports to sink."""
    verdicts = []
    for r1, r2 in runs[:deep_max_runs]:
        report = call_preflightx(r1, r2, deep_n_reads, exe, sex_reads=deep_n_reads)
        if report:
            verdicts.append(extract(report)[1])
            sink.append({"r1": r1, "r2": r2, "n_reads": deep_n_reads, "report": report})
    return vote(verdicts)


def _experiment_runs(
    rows: list[dict[str, str]],
) -> OrderedDict[str, list[tuple[str, str | None]]]:
    """Group scannable fastq pairs by experiment, preserving manifest order."""
    grouped: OrderedDict[str, list[tuple[str, str | None]]] = OrderedDict()
    for row in rows:
        if row.get("record_type") != "run":
            continue
        experiment = row.get("experiment_accession") or ""
        if not experiment:
            continue
        r1, r2 = pick_r1_r2(row.get("fastq_ftp", ""))
        if r1:
            grouped.setdefault(experiment, []).append((r1, r2))
    return grouped


def annotate(
    in_csv: Path,
    out_csv: Path,
    exe: str = "preflightx",
    n_reads: int = 20_000,
    max_runs: int = 1,
    deep_n_reads: int = 400_000,
    deep_max_runs: int = 2,
    provenance_path: Path | None = None,
) -> int:
    """
    Annotate one manifest CSV in place-shaped output. Returns experiments scanned.

    Calls are EXPERIMENT-level: every run row of an experiment carries that
    experiment's voted value, and sample rows are left blank because a sample is
    not what was scanned.
    """
    with in_csv.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        logger.warning("empty manifest, nothing to scan: %s", in_csv)
        return 0
    header = list(rows[0].keys()) + [c for c in CALL_COLUMNS if c not in rows[0]]

    provenance: OrderedDict[str, dict[str, Any]] = OrderedDict()
    calls: dict[str, tuple[str, str, str, str, str]] = {}
    for experiment, runs in _experiment_runs(rows).items():
        reports: list[dict[str, Any]] = []
        scanned: list[tuple[str, str, str, str, str]] = []
        for r1, r2 in runs[:max_runs]:
            report = call_preflightx(r1, r2, n_reads, exe, sex_reads=n_reads)
            if report:
                scanned.append(extract(report))
                reports.append(
                    {"r1": r1, "r2": r2, "n_reads": n_reads, "report": report}
                )
        voted = [vote([s[i] for s in scanned]) for i in range(5)]
        deep_reports: list[dict[str, Any]] = []
        if deep_n_reads > 0 and voted[1].strip().lower() in SEX_NOCALL:
            deeper = _deep_sex(runs, deep_n_reads, deep_max_runs, exe, deep_reports)
            if deeper:
                voted[1] = deeper
        calls[experiment] = (voted[0], voted[1], voted[2], voted[3], voted[4])
        provenance[experiment] = {
            "vote": dict(
                zip(
                    ("species", "sex", "assay", "library", "modality"),
                    voted,
                    strict=True,
                )
            ),
            "runs": reports,
            "deep_runs": deep_reports,
        }

    blank = ("", "", "", "", "")
    with out_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            call = (
                calls.get(row.get("experiment_accession") or "", blank)
                if row.get("record_type") == "run"
                else blank
            )
            row.update(dict(zip(CALL_COLUMNS, call, strict=True)))
            writer.writerow(row)

    if provenance_path:
        provenance_path.write_text(
            json.dumps(
                {
                    "accession": rows[0].get("canonical_accession", ""),
                    "subcmd": SUBCMD,
                    "params": {
                        "n_reads": n_reads,
                        "max_runs": max_runs,
                        "deep_n_reads": deep_n_reads,
                        "deep_max_runs": deep_max_runs,
                    },
                    "experiments": provenance,
                }
            )
        )
    return len(calls)


def annotate_one(
    manifest: Path,
    out_dir: Path,
    exe: str = "preflightx",
    n_reads: int = 20_000,
    max_runs: int = 1,
    deep_n_reads: int = 400_000,
    deep_max_runs: int = 2,
) -> bool:
    """
    Annotate ONE manifest. Returns False when its output already exists.

    The `.tmp`-then-rename is what makes the whole thing resumable: a present
    `<acc>.csv` always means a finished scan, so an interrupted host resumes by
    simply skipping it. This is also exactly the contract yuj's watchdog uses --
    it tests for `<out_dir>/<item>.csv` before ever invoking the worker.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / manifest.name
    if out_csv.exists():
        return False
    tmp = out_csv.with_suffix(".csv.tmp")
    annotate(
        manifest,
        tmp,
        exe,
        n_reads,
        max_runs,
        deep_n_reads,
        deep_max_runs,
        out_dir / f"{manifest.stem}.provenance.json",
    )
    tmp.replace(out_csv)
    return True


def annotate_dir(
    in_dir: Path,
    out_dir: Path,
    exe: str = "preflightx",
    n_reads: int = 20_000,
    max_runs: int = 1,
    deep_n_reads: int = 400_000,
    deep_max_runs: int = 2,
    accession: str | None = None,
) -> dict[str, int]:
    """
    Annotate every manifest in in_dir, or just accession when given.

    The single-accession form is how a fleet runner drives this: yuj appends one
    item per invocation, so the worker scans exactly one study and exits.
    """
    manifests = (
        [in_dir / f"{accession}.csv"]
        if accession
        else sorted(p for p in in_dir.iterdir() if p.suffix == ".csv")
    )
    done = skipped = 0
    for path in manifests:
        if not path.is_file():
            logger.warning("no manifest for %s", path.stem)
            continue
        if annotate_one(
            path, out_dir, exe, n_reads, max_runs, deep_n_reads, deep_max_runs
        ):
            done += 1
            logger.info("preflight %s (%d done, %d skipped)", path.stem, done, skipped)
        else:
            skipped += 1
    return {"annotated": done, "skipped": skipped}
