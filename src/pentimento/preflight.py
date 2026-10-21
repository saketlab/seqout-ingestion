"""Run preflightx over a manifest, writing the annotated CSV and provenance JSON."""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import subprocess
import threading
import time
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

SUBCMD = "check"
SEX_NOCALL = frozenset({"", "unknown", "undetermined", "ambiguous", "na"})
# Every preflightx verdict with its confidence column. Vote confidence with
# its verdict so mixed-run experiments keep matching call/confidence pairs.
#
# (verdict column, report section, key within it, confidence column or None)
CALL_SPECS: tuple[tuple[str, str, str, str | None], ...] = (
    ("preflightx_species", "species", "called", "preflightx_species_confidence"),
    ("preflightx_sex", "sex", "verdict", "preflightx_sex_confidence"),
    # Cell/nucleus source from intron fraction and mito %. Study text can infer it,
    # but preflightx is the read-derived source.
    ("preflightx_modality", "nucleus", "verdict", "preflightx_modality_confidence"),
    (
        "preflightx_bulk_vs_sc",
        "bulk_vs_sc",
        "verdict",
        "preflightx_bulk_vs_sc_confidence",
    ),
    ("preflightx_end", "end", "end", "preflightx_end_confidence"),
)
ASSAY_COLUMN = "preflightx_assay"
# chemistry grades itself outside the CALL_SPECS (verdict, confidence) shape
ASSAY_CONFIDENCE_COLUMN = "preflightx_assay_confidence"
# a read whose length merely matches a barcode geometry scores nothing here
ASSAY_ONLIST_PCT_COLUMN = "preflightx_assay_onlist_pct"
# every barcode round of a fallback hit needs most reads on its onlist
_MIN_DETECT_ONLIST_PCT = 50.0
_DETECT_CONFIDENT = frozenset({"high"})
CALL_COLUMNS: tuple[str, ...] = (
    "preflightx_species",
    "preflightx_species_confidence",
    "preflightx_sex",
    "preflightx_sex_confidence",
    ASSAY_COLUMN,
    ASSAY_CONFIDENCE_COLUMN,
    ASSAY_ONLIST_PCT_COLUMN,
    "preflightx_modality",
    "preflightx_modality_confidence",
    "preflightx_bulk_vs_sc",
    "preflightx_bulk_vs_sc_confidence",
    "preflightx_end",
    "preflightx_end_confidence",
)
# preflightx emits no `classification` key, so a rewrite drops the column.
STALE_CALL_COLUMNS = frozenset({"preflightx_library"})
SEX_COLUMN = "preflightx_sex"
SEX_CONFIDENCE_COLUMN = "preflightx_sex_confidence"
TIMEOUT_S = 900
# ENA drops connections intermittently over both HTTPS and FTP (and a campus
# middlebox breaks some HTTPS handshakes), so a failed scan is retried with the
# other scheme before the run is given up: https, ftp, https, ...
RETRIES = int(os.environ.get("PREFLIGHT_RETRIES", "3"))
RETRY_BACKOFF_S = float(os.environ.get("PREFLIGHT_RETRY_BACKOFF_S", "20"))
PROGRESS_EVERY = 2000
# Plate-based studies deposit one experiment per cell (up to ~55k), but a study's
# assay is settled long before that: scan a seeded random sample of at most this
# many experiments per manifest; the rest are written blank. 0 scans everything.
MAX_EXPERIMENTS = int(os.environ.get("PREFLIGHT_MAX_EXPERIMENTS", "0"))


def _strip_prefix(text: str, prefix: str) -> str:
    """
    Strip a prefix with slicing for cross-version compatibility.
    """
    return text[len(prefix) :] if text.startswith(prefix) else text  # noqa: FURB188


def to_https(url: str) -> str:
    """Rewrite an ENA ftp path to https; the streamed reader only speaks https."""
    url = url.strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://")):
        return url
    return "https://" + _strip_prefix(url, "ftp://")


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


def _as_scheme(url: str | None, scheme: str) -> str | None:
    """Rewrite an ENA path to the given scheme (https or ftp)."""
    if not url:
        return url
    path = url.split("://", 1)[-1]
    return f"{scheme}://{path}"


def _run_preflightx(
    r1: str, r2: str | None, n_reads: int, exe: str, sex_reads: int | None
) -> tuple[dict[str, Any] | None, str]:
    """One preflightx call: (parsed JSON or None, short reason on failure)."""
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
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except OSError as e:
        return None, f"exec: {e}"
    if out.returncode != 0 or not out.stdout.strip():
        return None, " | ".join(out.stderr.strip().splitlines()[-3:])[:300]
    try:
        return json.loads(out.stdout), ""
    except json.JSONDecodeError:
        return None, "unparseable JSON"


def call_preflightx(
    r1: str,
    r2: str | None,
    n_reads: int,
    exe: str,
    sex_reads: int | None = None,
) -> dict[str, Any] | None:
    """Return parsed preflightx JSON for one run, or None once every attempt failed."""
    reason = ""
    for attempt in range(max(RETRIES, 1)):
        scheme = "https" if attempt % 2 == 0 else "ftp"
        a1, a2 = _as_scheme(r1, scheme) or r1, _as_scheme(r2, scheme)
        report, reason = _run_preflightx(a1, a2, n_reads, exe, sex_reads)
        if report is not None:
            return report
        if attempt + 1 < RETRIES:
            time.sleep(RETRY_BACKOFF_S * (attempt + 1))
    logger.warning("preflightx failed on %s after %d attempts: %s", r1, RETRIES, reason)
    return None


def extract(report: dict[str, Any]) -> dict[str, str]:
    """Every call column from one report, keyed by column name; '' when absent."""
    chemistry = report.get("chemistry") or {}
    called = dict.fromkeys(CALL_COLUMNS, "")
    called[ASSAY_COLUMN] = (
        chemistry.get("assay_display") or chemistry.get("assay_id") or ""
    )
    called[ASSAY_CONFIDENCE_COLUMN] = str(chemistry.get("confidence") or "")
    called[ASSAY_ONLIST_PCT_COLUMN] = _max_onlist_pct(chemistry)
    # chemistry knows only 10x/Drop-seq; check's fallback detector names the rest
    detected = _detected_assay(report) if not chemistry.get("is_single_cell") else None
    if detected:
        (
            called[ASSAY_COLUMN],
            called[ASSAY_CONFIDENCE_COLUMN],
            called[ASSAY_ONLIST_PCT_COLUMN],
        ) = detected
    for column, section, key, confidence_column in CALL_SPECS:
        block = report.get(section) or {}
        called[column] = str(block.get(key) or "")
        if confidence_column:
            called[confidence_column] = str(block.get("confidence") or "")
    return called


def _max_onlist_pct(chemistry: dict[str, Any]) -> str:
    """
    Best onlist overlap across the scored whitelists, as a string, '' when none.

    Empty and 0 differ: empty means no onlist was scored, 0 means one was and
    nothing matched.
    """
    scores = chemistry.get("whitelist_scores") or []
    pcts = [s.get("pct") for s in scores if isinstance(s, dict)]
    pcts = [p for p in pcts if isinstance(p, (int, float))]
    return str(max(pcts)) if pcts else ""


def _detected_assay(report: dict[str, Any]) -> tuple[str, str, str] | None:
    """
    Top assay_detection hit as (assay, confidence, onlist pct), or None.

    High-confidence hits only. Onlist assays need every barcode round matched;
    the rest need every fixed linker found.
    """
    detection = report.get("assay_detection") or {}
    confidence = str(detection.get("confidence") or "")
    hits = detection.get("hits") or []
    if confidence not in _DETECT_CONFIDENT or not hits or not isinstance(hits[0], dict):
        return None
    top = hits[0]
    name = str(top.get("display_name") or top.get("assay_id") or "")
    if not name:
        return None
    rates = [
        pair[-1]
        for pair in top.get("onlist_overlap") or []
        if isinstance(pair, (list, tuple)) and isinstance(pair[-1], (int, float))
    ]
    if rates:
        # every round must match; one matching list alone is chance
        # onlist_overlap is a fraction, the column a percentage
        worst_pct = round(min(rates) * 100, 2)
        ok = worst_pct >= _MIN_DETECT_ONLIST_PCT
        return (name, confidence, str(worst_pct)) if ok else None
    found, total = top.get("anchors_found"), top.get("anchors_total")
    all_anchors = isinstance(total, int) and total > 0 and found == total
    # empty, not 0: no onlist was scored
    return (name, confidence, "") if all_anchors else None


def _text(value: Any) -> str | None:
    """Strip to NULL. preflightx spells "did not call" as both '' and absent."""
    if value is None:
        return None
    return str(value).strip() or None


def structural_calls(report: dict[str, Any]) -> tuple[Any, ...]:
    """
    Return modality/read-geometry calls and supporting evidence.

    Ordered to match build._PFX_EXP_COLUMNS. Run-grained CSVs lack these
    numerics, so experiment-grained routes append them.
    """
    nucleus = report.get("nucleus") or {}
    bulk = report.get("bulk_vs_sc") or {}
    end = report.get("end") or {}
    return (
        _text(nucleus.get("verdict")),
        _text(nucleus.get("confidence")),
        nucleus.get("intron_frac"),
        nucleus.get("mito_pct"),
        nucleus.get("reads_scanned"),
        _text(bulk.get("verdict")),
        _text(bulk.get("confidence")),
        _text(end.get("end")),
        _text(end.get("confidence")),
    )


NO_STRUCTURAL_CALLS: tuple[Any, ...] = (None,) * 9


def vote(values: list[str]) -> str:
    """Majority across an experiment's runs, blanks ignored, ties to first-seen."""
    counts = Counter(v for v in values if v)
    if not counts:
        return ""
    top = max(counts.values())
    return next((v for v in values if v and counts[v] == top), "")


def vote_calls(scanned: list[dict[str, str]]) -> dict[str, str]:
    """
    Majority-vote call columns across an experiment's runs.

    Confidence travels with the run that supplied the winning verdict.
    """
    voted = dict.fromkeys(CALL_COLUMNS, "")
    voted[ASSAY_COLUMN] = vote([s.get(ASSAY_COLUMN, "") for s in scanned])
    for column, _section, _key, confidence_column in CALL_SPECS:
        winner = vote([s.get(column, "") for s in scanned])
        voted[column] = winner
        if confidence_column and winner:
            voted[confidence_column] = next(
                (
                    s.get(confidence_column, "")
                    for s in scanned
                    if s.get(column) == winner
                ),
                "",
            )
    return voted


def _call_header(first_row: dict[str, str]) -> list[str]:
    """Manifest columns, minus any stale call column, plus the current ones."""
    base = [
        c for c in first_row if c not in CALL_COLUMNS and c not in STALE_CALL_COLUMNS
    ]
    return base + list(CALL_COLUMNS)


def _stronger(new: str, old: str) -> str:
    """
    Return the better of a re-derived call and the existing row value.

    Blanks and abstentions lose to real calls; `unknown` is a word-spelled NULL.
    """
    new = new.strip()
    if not new:
        return old
    old = old.strip()
    if new.lower() in SEX_NOCALL and old and old.lower() not in SEX_NOCALL:
        return old
    return new


def _write_calls(
    out_csv: Path,
    header: list[str],
    rows: list[dict[str, str]],
    calls: dict[str, dict[str, str]],
    *,
    additive: bool = False,
) -> None:
    """
    Write rows carrying experiment-level voted calls.

    Sample rows are blanked because samples were not scanned. additive=True fills
    blanks and preserves stronger existing calls.
    """
    blank = dict.fromkeys(CALL_COLUMNS, "")
    with out_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            call = (
                calls.get(row.get("experiment_accession") or "", blank)
                if row.get("record_type") == "run"
                else blank
            )
            if additive:
                call = {
                    c: _stronger(v, (row.get(c) or "").strip()) for c, v in call.items()
                }
            row.update(call)
            writer.writerow(row)


def votes_from_provenance(provenance: dict[str, Any]) -> dict[str, dict[str, str]]:
    """
    Re-derive per-experiment votes from a stored provenance JSON.

    Stored reports let CALL_SPECS columns be recovered locally through
    annotate()'s voting path, including the deep-sex override.
    """
    calls: dict[str, dict[str, str]] = {}
    for experiment, entry in (provenance.get("experiments") or {}).items():
        scanned = [
            extract(r["report"]) for r in (entry.get("runs") or []) if r.get("report")
        ]
        voted = vote_calls(scanned)
        if voted[SEX_COLUMN].strip().lower() in SEX_NOCALL:
            deep = [
                extract(r["report"])
                for r in (entry.get("deep_runs") or [])
                if r.get("report")
            ]
            if deep:
                deep_voted = vote_calls(deep)
                if deep_voted[SEX_COLUMN]:
                    voted[SEX_COLUMN] = deep_voted[SEX_COLUMN]
                    voted[SEX_CONFIDENCE_COLUMN] = deep_voted[SEX_CONFIDENCE_COLUMN]
        calls[experiment] = voted
    return calls


def _is_current(csv_path: Path) -> bool:
    """Report whether the CSV header already carries every call column."""
    try:
        with csv_path.open(newline="", encoding="utf-8", errors="replace") as handle:
            header = next(csv.reader(handle), [])
    except OSError:
        return False
    return set(CALL_COLUMNS).issubset(header)


def backfill_one(csv_path: Path, provenance_path: Path) -> str:  # noqa: PLR0911
    """
    Rewrite one annotated CSV's call columns from provenance.

    CSVs with no report are left untouched; returns a refusal reason or "".
    """
    if not provenance_path.is_file():
        return "no_provenance"
    if _is_current(csv_path):
        # Current header makes a killed run resumable by rerunning it.
        return "current"
    try:
        provenance = json.loads(provenance_path.read_text())
    except (OSError, ValueError):
        return "unreadable_provenance"
    calls = votes_from_provenance(provenance)
    if not any(any(v.values()) for v in calls.values()):
        return "provenance_has_no_reports"
    try:
        with csv_path.open(newline="", encoding="utf-8", errors="replace") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return "unreadable_csv"
    if not rows:
        return "empty_csv"
    # Include PID and thread id in temp names. Concurrent backfills over one dir
    # can otherwise collide and make the loser rename a vanished file.
    tmp = csv_path.with_suffix(f".csv.backfill{os.getpid()}.{threading.get_ident()}")
    try:
        _write_calls(tmp, _call_header(rows[0]), rows, calls, additive=True)
        tmp.replace(csv_path)
    except OSError:
        tmp.unlink(missing_ok=True)
        return "write_failed"
    return ""


def backfill_dir(annotated_dir: Path, max_workers: int = 24) -> dict[str, int]:
    """
    Re-derive call columns for every annotated CSV in a directory.

    Stored reports make this a local CSV rewrite through the voting path.
    """
    counts: Counter[str] = Counter()
    # iterdir avoids glob argument-list limits in adjacent tooling.
    paths = [p for p in sorted(annotated_dir.iterdir()) if p.suffix == ".csv"]
    logger.info("backfill: %d csv files in %s", len(paths), annotated_dir)

    def _one(path: Path) -> str:
        return backfill_one(path, annotated_dir / f"{path.stem}.provenance.json")

    # Thread pool fits per-file NFS latency; each write uses a distinct temp name.
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for reason in pool.map(_one, paths):
            with lock:
                counts[reason or "rewritten"] += 1
                total = sum(counts.values())
            if total % PROGRESS_EVERY == 0:
                logger.info(
                    "backfill: %d/%d files, %s", total, len(paths), dict(counts)
                )
    logger.info("backfill %s: %s", annotated_dir, dict(counts))
    return dict(counts)


def _deep_sex(
    runs: list[tuple[str, str | None]],
    deep_n_reads: int,
    deep_max_runs: int,
    exe: str,
    sink: list[dict[str, Any]],
) -> tuple[str, str]:
    """Re-call sex only, scanning far more reads. Appends reports to sink."""
    scanned: list[dict[str, str]] = []
    for r1, r2 in runs[:deep_max_runs]:
        report = call_preflightx(r1, r2, deep_n_reads, exe, sex_reads=deep_n_reads)
        if report:
            scanned.append(extract(report))
            sink.append({"r1": r1, "r2": r2, "n_reads": deep_n_reads, "report": report})
    voted = vote_calls(scanned)
    return voted[SEX_COLUMN], voted[SEX_CONFIDENCE_COLUMN]


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
    threads: int = 1,
) -> int:
    """
    Annotate one manifest CSV. Returns experiments scanned.

    Calls are experiment-level; sample rows stay blank because samples were not
    scanned. Experiments are independent preflightx subprocesses, so `threads`
    scans them concurrently; output order follows the manifest either way.
    """
    with in_csv.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        logger.warning("empty manifest, nothing to scan: %s", in_csv)
        return 0
    header = _call_header(rows[0])

    def scan(
        item: tuple[str, list[tuple[str, str | None]]],
    ) -> tuple[dict[str, str], dict[str, Any]]:
        _, runs = item
        reports: list[dict[str, Any]] = []
        scanned: list[dict[str, str]] = []
        for r1, r2 in runs[:max_runs]:
            report = call_preflightx(r1, r2, n_reads, exe, sex_reads=n_reads)
            if report:
                scanned.append(extract(report))
                reports.append(
                    {"r1": r1, "r2": r2, "n_reads": n_reads, "report": report}
                )
        voted = vote_calls(scanned)
        deep_reports: list[dict[str, Any]] = []
        if deep_n_reads > 0 and voted[SEX_COLUMN].strip().lower() in SEX_NOCALL:
            deeper, deeper_confidence = _deep_sex(
                runs, deep_n_reads, deep_max_runs, exe, deep_reports
            )
            if deeper:
                voted[SEX_COLUMN] = deeper
                voted[SEX_CONFIDENCE_COLUMN] = deeper_confidence
        return voted, {
            # short keys, matching the shape
            # provenance_json._call_row already reads out of the stored files
            "vote": {_strip_prefix(k, "preflightx_"): v for k, v in voted.items()},
            "runs": reports,
            "deep_runs": deep_reports,
        }

    items = list(_experiment_runs(rows).items())
    n_total = len(items)
    if 0 < MAX_EXPERIMENTS < n_total:
        # seeded by the manifest name, so a rerun scans the same experiments
        keep = set(random.Random(in_csv.stem).sample(range(n_total), MAX_EXPERIMENTS))  # noqa: S311 (sampling, not crypto)
        items = [item for i, item in enumerate(items) if i in keep]  # manifest order
    if threads > 1 and len(items) > 1:
        with ThreadPoolExecutor(max_workers=min(threads, len(items))) as pool:
            results = list(pool.map(scan, items))  # map keeps manifest order
    else:
        results = [scan(item) for item in items]
    provenance: OrderedDict[str, dict[str, Any]] = OrderedDict()
    calls: dict[str, dict[str, str]] = {}
    for (experiment, _), (voted, prov) in zip(items, results, strict=True):
        calls[experiment] = voted
        provenance[experiment] = prov

    _write_calls(out_csv, header, rows, calls)

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
                        "max_experiments": MAX_EXPERIMENTS,
                        "n_experiments_total": n_total,
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
    threads: int = 1,
) -> bool:
    """
    Annotate one manifest unless finished output exists.

    The `.tmp`-then-rename pattern makes a present `<acc>.csv` a complete scan.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / manifest.name
    if out_csv.exists():
        return False
    tmp = out_csv.with_suffix(".csv.tmp")
    scanned = annotate(
        manifest,
        tmp,
        exe,
        n_reads,
        max_runs,
        deep_n_reads,
        deep_max_runs,
        out_dir / f"{manifest.stem}.provenance.json",
        threads,
    )
    if not scanned:
        # Unscannable manifests stay pending until links are available.
        tmp.unlink(missing_ok=True)
        logger.warning("no scannable run in %s, leaving unclaimed", manifest.name)
        return False
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
    threads: int = 1,
) -> dict[str, int]:
    """
    Annotate every manifest in in_dir, or one accession when given.

    Fleet workers call the single-accession form.
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
            path, out_dir, exe, n_reads, max_runs, deep_n_reads, deep_max_runs, threads
        ):
            done += 1
            logger.info("preflight %s (%d done, %d skipped)", path.stem, done, skipped)
        else:
            skipped += 1
    return {"annotated": done, "skipped": skipped}
