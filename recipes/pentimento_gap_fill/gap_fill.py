#!/usr/bin/env python3
"""
Find gapped single-cell studies, dispatch jobs, and collect results.

A gapped study is believed single-cell but missing ncount, preflight, or quant.
Ncount can use supplementary matrices with no reads; preflight and quant require
runs with FASTQ. The jobs use different grains: ncount/preflight take studies,
while quant takes runs.

Quant expands study -> run through download_links.db, because it resolves aliases
completely while `pentimento_run` links only part of its rows to counted samples.
Postgres access is read-only.

Usage:
  gap_fill.py scan
  gap_fill.py targets --out-dir recipes/pentimento_gap_fill/targets
  gap_fill.py targets --from-file suspected_sc.txt --out-dir /tmp/t
  gap_fill.py dispatch --out-dir ... --job ncount --job preflight
  gap_fill.py collect --pull-dir ~/gapfill_pull --out-db ~/gapfill.db
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "recipes"))

from pg_connect import connect  # noqa: E402

logger = logging.getLogger("gap_fill")

# the universe of things we think are single-cell
#
# Union text-derived calls with counted studies; either source can miss real
# single-cell data the other sees.
UNIVERSE_SQL = """
    SELECT canonical_accession AS acc FROM unified_metadata WHERE is_single_cell IS TRUE
    UNION
    SELECT study_accession FROM pentimento_study WHERE COALESCE(study_cells, 0) > 0
"""

# Studies whose own text suggests single-cell but neither ncount nor preflightx
# has measured. `single_cell_status` separates asserted from measured evidence,
# so these can be addressed as work items.
UNIVERSE_SUSPECTED = """
    SELECT canonical_accession AS acc FROM unified_metadata
     WHERE single_cell_status = 'text_indicates_single_cell'
"""

# the three gaps
#
# Each returns study accessions. `has_reads` is what separates the read-based jobs
# from ncount: n_runs_linked is pentimento's own count of resolvable runs, and a
# study with none of them cannot be preflighted or quantified no matter what.
GAP_SQL = {
    # Never counted, or counted with samples still missing a number. Reads are
    # irrelevant for this gap.
    "ncount": """
        SELECT u.acc FROM ({universe}) u
        LEFT JOIN pentimento_study p ON p.study_accession = u.acc
        WHERE (u.acc LIKE 'GSE%' OR u.acc LIKE 'E-%')
          AND (
            p.study_accession IS NULL
            OR (COALESCE(p.n_samples_detailed, 0) = 0
                AND COALESCE(p.study_cells, 0) = 0)
            OR EXISTS (
                 SELECT 1 FROM pentimento_sample s
                 WHERE s.study_accession = u.acc
                   AND (s.cells IS NULL OR s.genes IS NULL)
               )
          )
    """,
    # Has runs with FASTQ, but no run carries a preflightx call.
    #
    # LEFT JOIN, with read availability from sra_run_download_links rather than
    # p.n_runs_linked. Unscreened suspected studies are not yet in pentimento.
    #
    # A study with run rows but no fastq_ftp is skipped rather than dispatched:
    # those are usually BAM-only deposits, and preflight needs FASTQ, so sending
    # them to the fleet burns a slot to learn nothing (see CLAUDE.md).
    "preflight": """
        SELECT u.acc FROM ({universe}) u
        LEFT JOIN pentimento_study p ON p.study_accession = u.acc
        WHERE COALESCE(p.n_runs_preflightx, 0) = 0
          AND EXISTS (
                SELECT 1 FROM sra_run_download_links d
                WHERE d.study_accession = u.acc
                  AND COALESCE(d.fastq_ftp, '') <> ''
              )
    """,
    # Has runs, preflightx confirms single-cell, and quant has not measured any
    # runs. The preflightx gate keeps bulk studies out of the quant work list.
    #
    # Two independent confirmations, either of which counts: assay_is_single_cell
    # is a barcode-whitelist hit, bulk_vs_sc is R1 length structure. CLAUDE.md notes
    # they are independent and that disagreement is informative, so OR is right --
    # requiring both would drop real single-cell data on a technicality.
    #
    # NULL and 0 in n_runs_measurable both mean "nothing landed". A measured
    # negative is a non-zero count with empty flags, not a gap.
    "quant": """
        SELECT u.acc FROM ({universe}) u
        JOIN pentimento_study p ON p.study_accession = u.acc
        WHERE COALESCE(p.n_runs_linked, 0) > 0
          AND COALESCE(p.n_runs_measurable, 0) = 0
          AND COALESCE(p.n_runs_preflightx, 0) > 0
          AND EXISTS (
                SELECT 1 FROM pentimento_run r
                WHERE r.study_accession = p.study_accession
                  AND (r.assay_is_single_cell OR r.bulk_vs_sc = 'single_cell')
              )
    """,
    # Confirmed single-cell, but no run carries a real sex call. `unknown` is an
    # abstention, so it counts as a gap; the deep sex pass (--deep-n-reads)
    # exists to fill it.
    "sex": """
        SELECT u.acc FROM ({universe}) u
        JOIN pentimento_study p ON p.study_accession = u.acc
        WHERE COALESCE(p.n_runs_linked, 0) > 0
          AND EXISTS (
                SELECT 1 FROM pentimento_run r
                WHERE r.study_accession = p.study_accession
                  AND (r.assay_is_single_cell OR r.bulk_vs_sc = 'single_cell')
              )
          AND NOT EXISTS (
                SELECT 1 FROM pentimento_run r
                WHERE r.study_accession = p.study_accession
                  AND r.sex_verdict IN ('male', 'female')
              )
    """,
}

# quant and sex depend on preflightx results. `scan` prints the ordering;
# `targets --stage` enforces it.
STAGE_ORDER = ("ncount", "preflight", "quant", "sex")
DEPENDS_ON_PREFLIGHT = ("quant", "sex")

LOAD_BATCH = 5000

JOB_SPEC = {
    # job          yuj dir             input file       grain
    # ncount merges into accessions.txt, not work.txt: yuj regenerates work.txt
    # during scatter. preflight scatters studies.txt into itself.
    "ncount": ("yuj/ncount", "accessions.txt", "study"),
    "preflight": ("yuj/preflight", "studies.txt", "study"),
    "quant": ("yuj/quant", "accessions.csv", "run"),
    # the sex gap is filled by a preflightx DEEP pass, not a separate binary, so it
    # lands in the same job dir as preflight. dispatch MERGES, so the two coexist.
    "sex": ("yuj/preflight", "studies.txt", "study"),
}


def _universe_cte(
    from_file: Path | None, universe: str = "default"
) -> tuple[str, list[list[str]]]:
    """Return (sql, params). An explicit list REPLACES the pg universe, not adds."""
    if from_file:
        accs = [a.strip() for a in from_file.read_text().splitlines() if a.strip()]
        if not accs:
            msg = f"{from_file} is empty"
            raise SystemExit(msg)
        return "SELECT unnest(%s::text[]) AS acc", [accs]
    if universe == "suspected":
        return UNIVERSE_SUSPECTED, []
    return UNIVERSE_SQL, []


def gap_studies(
    need: str,
    from_file: Path | None,
    limit: int | None,
    universe_kind: str = "default",
) -> list[str]:
    universe, params = _universe_cte(from_file, universe_kind)
    sql = GAP_SQL[need].format(universe=universe) + " ORDER BY 1"
    if limit:
        sql += f" LIMIT {int(limit)}"  # int() cast, never caller text
    with connect() as conn:
        return [r[0] for r in conn.execute(sql, params or None)]


def cmd_scan(args: argparse.Namespace) -> int:
    universe, params = _universe_cte(
        args.from_file, getattr(args, "universe", "default")
    )
    with connect() as conn:
        total = conn.execute(
            f"SELECT count(*) FROM ({universe}) u",  # noqa: S608
            params or None,
        ).fetchone()[0]
        print(
            f"universe: {total} studies"
            f"{' (from ' + str(args.from_file) + ')' if args.from_file else ''}"
        )
        for need, gap_sql in GAP_SQL.items():
            sql = gap_sql.format(universe=universe)
            n = conn.execute(
                f"SELECT count(*) FROM ({sql}) t",  # noqa: S608
                params or None,
            ).fetchone()[0]
            note = "" if need == "ncount" else "  (requires reads)"
            if need in DEPENDS_ON_PREFLIGHT:
                note += "  [needs preflightx first]"
            print(f"  {need:10s} {n:>7}{note}")
    print(
        "\nncount needs no reads: a GEO/ArrayExpress study with no SRA"
        " link still counts."
    )
    print(
        "quant and sex are gated on preflightx CONFIRMING single-cell, so they\n"
        "shrink as preflight lands. Run the stages in order: ncount, preflight,\n"
        "then quant/sex."
    )
    return 0


def cmd_targets(args: argparse.Namespace) -> int:
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    caps = {
        "ncount": args.max_ncount,
        "preflight": args.max_preflight,
        "quant": args.max_quant,
        "sex": args.max_sex,
    }
    written: dict[str, int] = {}
    for need in args.job or list(GAP_SQL):
        studies = gap_studies(
            need, args.from_file, caps[need], getattr(args, "universe", "default")
        )
        path = out / f"{need}_studies.txt"
        path.write_text("\n".join(studies) + "\n" if studies else "")
        written[need] = len(studies)
        print(f"{need:10s} {len(studies):>7} studies -> {path.name}")

    # quant consumes RUN accessions, so expand here rather than letting the
    # fleet script guess. Capped per study: one atlas can resolve to orders of
    # magnitude more runs than the median study.
    if "quant" in written and written["quant"] and not args.no_expand:
        from pentimento.manifest import run_accessions

        studies = (out / "quant_studies.txt").read_text().split()
        runs = run_accessions(
            args.links_db_file,
            args.unified_db_file,
            studies,
            max_per_study=args.max_runs_per_study,
        )
        (out / "quant_runs.csv").write_text("\n".join(runs) + "\n")
        print(
            f"quant      {len(runs):>7} runs (cap {args.max_runs_per_study}/study)"
            f" -> quant_runs.csv"
        )
    return 0


def cmd_dispatch(args: argparse.Namespace) -> int:
    """
    Copy the work lists into the yuj job dirs.

    Does not launch. Run yuj scatter from the job dir so it reads the right
    fleet; host crons pick up the slices.
    """
    for need in args.job or list(JOB_SPEC):
        job_dir, infile, grain = JOB_SPEC[need]
        name = "quant_runs.csv" if grain == "run" else f"{need}_studies.txt"
        src = args.out_dir / name
        if not src.is_file():
            print(f"{need:10s} SKIP (no {src.name}; run `targets` first)")
            continue
        dst = REPO / job_dir / infile
        existing = (
            [x.strip() for x in dst.read_text().splitlines() if x.strip()]
            if dst.is_file()
            else []
        )
        incoming = [x.strip() for x in src.read_text().splitlines() if x.strip()]
        # Merge rather than overwrite. These files may already contain in-flight
        # work, and preflight and sex share yuj/preflight.
        merged = existing + [a for a in incoming if a not in set(existing)]
        added = len(merged) - len(existing)
        if args.apply:
            if dst.exists():
                shutil.copyfile(dst, dst.with_suffix(dst.suffix + ".bak"))
            dst.write_text("\n".join(merged) + "\n")
            print(
                f"{need:10s} +{added:<7} -> {job_dir}/{infile} "
                f"({len(existing)} -> {len(merged)}, backup kept)"
            )
        else:
            print(
                f"{need:10s} +{added:<7} would merge into {job_dir}/{infile} "
                f"({len(existing)} -> {len(merged)})"
            )
    if not args.apply:
        print("\nDRY RUN. Re-run with --apply to write the job dirs.")
    else:
        print(
            "\nWritten. Scatter with: cd <job dir> && yuj scatter"
            "  (cwd selects the fleet)"
        )
    return 0


# Where each job's results land, and which loader reads them. `collect` builds
# one SQLite with the tables needed for diffing and transfer.
COLLECT_SPEC = {
    "ncount": ("*.json", "study_ncount / sample_ncount"),
    "preflight": ("*.csv", "run_preflightx"),
    "quant": ("*.quant.csv", "microbe_quant / quant_provenance"),
}


def cmd_collect(args: argparse.Namespace) -> int:
    """
    Load pulled fleet results into one SQLite, ready to diff against Postgres.

    Not `pentimento build`: that path re-reads the full preflight corpus even
    when only one result dir changed. This loads only the dirs given.

    Scope transfers from partial collects. A quant-only db has NULL in every
    preflightx column, and the standard pentimento transfer may write NULLs.
    """
    import sqlite3

    from helpers import exec_query_from_file
    from pentimento import sources

    out = args.out_db
    out.unlink(missing_ok=True)
    conn = sqlite3.connect(out)
    exec_query_from_file("tables/pentimento.sql", conn=conn)

    def load_sql(table: str, sql: str, rows: object) -> int:
        """Stream rows through a caller-supplied INSERT (build.py owns the columns)."""
        n = 0
        cur = conn.cursor()
        batch: list = []
        for row in rows:
            batch.append(row)
            if len(batch) >= LOAD_BATCH:
                cur.executemany(sql, batch)
                conn.commit()
                n += len(batch)
                batch = []
        if batch:
            cur.executemany(sql, batch)
            conn.commit()
            n += len(batch)
        print(f"  {table:20s} {n} rows")
        return n

    def load(table: str, rows: object, cols: int) -> int:
        n = 0
        ph = ",".join("?" * cols)
        cur = conn.cursor()
        batch: list = []
        for row in rows:
            batch.append(row)
            if len(batch) >= LOAD_BATCH:
                cur.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({ph})", batch)  # noqa: S608
                conn.commit()
                n += len(batch)
                batch = []
        if batch:
            cur.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({ph})", batch)  # noqa: S608
            conn.commit()
            n += len(batch)
        print(f"  {table:20s} {n} rows")
        return n

    if args.ncount_dir:
        print("ncount:")
        load("study_ncount", sources.load_ncount_projects(args.ncount_dir), 14)
        load("sample_ncount", sources.load_ncount_samples(args.ncount_dir), 8)

    if args.quant_dir:
        print("quant:")
        load("microbe_quant", _quant_rows(sources, args.quant_dir), 16)
        prov = list(sources.load_quant_provenance(args.quant_dir))
        known = {r[0] for r in prov}
        prov += list(sources.load_status_only(args.quant_dir, known))
        load("quant_provenance", prov, 13)

    if args.preflight_dir:
        # run_preflightx is wider than the CSV loader's tuple: the CSVs land in
        # a staging table and are then PROJECTED into the real one. Reuse
        # build.py's staging DDL and projection; a hand-written copy drifts from
        # the column list it has to match.
        from pentimento import build as pbuild
        from pentimento import provenance_json

        print("preflight:")
        conn.executescript(pbuild._PFX_RUN_STAGING)  # noqa: SLF001
        # the provenance JSONs feed two experiment-grained staging tables
        for _t in pbuild._PFX_EXP_STAGING:  # noqa: SLF001
            conn.executescript(pbuild._PFX_STAGING.format(table=_t))  # noqa: SLF001
        load_sql(
            "_pfx_run",
            pbuild._INSERTS["_pfx_run"],  # noqa: SLF001
            sources.load_preflightx_csvs(args.preflight_dir),
        )
        # the .provenance.json sidecars carry the numeric evidence and the
        # experiment-grained vote; they feed different tables from the CSVs
        for table, row in provenance_json.load(args.preflight_dir):
            conn.execute(pbuild._INSERTS[table], row)  # noqa: SLF001
        conn.commit()
        pbuild._expand_to_runs(conn)  # noqa: SLF001
        conn.commit()
        n = conn.execute("SELECT count(*) FROM run_preflightx").fetchone()[0]
        sexed = conn.execute(
            "SELECT count(*) FROM run_preflightx WHERE sex_verdict IN ('male','female')"
        ).fetchone()[0]
        sc = conn.execute(
            "SELECT count(*) FROM run_preflightx WHERE bulk_vs_sc = 'single_cell'"
        ).fetchone()[0]
        print(f"  run_preflightx       {n} rows ({sexed} sexed, {sc} single_cell)")

    print(f"-> {out}")
    conn.close()
    return 0


def _quant_rows(sources: object, quant_dir: Path) -> object:
    """microbe_quant, with total_reads moved from last to 11th to match the table."""
    for r in sources.load_microbe_quant(quant_dir):
        # loader order: ...breadth, viral, viral_ev, bact, bact_valid, total_reads
        # table order:  ...breadth, total_reads, viral, viral_ev, bact, bact_valid,
        #               is_background
        yield (*r[:10], r[14], r[10], r[11], r[12], r[13], 0)


def main() -> int:
    ap = argparse.ArgumentParser(prog="pentimento-gap-fill")
    ap.add_argument(
        "--universe",
        choices=("default", "suspected"),
        default="default",
        help="suspected = studies whose own text says single-cell and that "
        "nothing has ever measured (single_cell_status = "
        "'text_indicates_single_cell'); 16,037 of them as of 2026-08-19",
    )
    ap.add_argument(
        "--from-file",
        type=Path,
        help="explicit accession list; REPLACES the Postgres universe",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("scan", help="read-only sizing report").set_defaults(func=cmd_scan)

    t = sub.add_parser("targets", help="write per-job work lists")
    t.add_argument("--out-dir", type=Path, required=True)
    t.add_argument("--job", action="append", choices=list(GAP_SQL))
    t.add_argument("--max-ncount", type=int)
    t.add_argument("--max-preflight", type=int)
    t.add_argument("--max-quant", type=int)
    t.add_argument("--max-sex", type=int)
    t.add_argument("--max-runs-per-study", type=int, default=500)
    t.add_argument(
        "--no-expand",
        action="store_true",
        help="skip the study->run expansion for quant",
    )
    t.add_argument(
        "--links-db-file",
        type=Path,
        default=Path("/data/share/seqout/download_links/download_links.db"),
    )
    t.add_argument(
        "--unified-db-file",
        type=Path,
        default=Path("/data/share/seqout/unified_metadata/unified_metadata.db"),
    )
    t.set_defaults(func=cmd_targets)

    d = sub.add_parser("dispatch", help="copy work lists into the yuj job dirs")
    d.add_argument("--out-dir", type=Path, required=True)
    d.add_argument("--job", action="append", choices=list(JOB_SPEC))  # includes sex
    d.add_argument("--apply", action="store_true")
    d.set_defaults(func=cmd_dispatch)

    c = sub.add_parser("collect", help="load pulled fleet results into one SQLite")
    c.add_argument("--out-db", type=Path, required=True)
    c.add_argument(
        "--ncount-dir",
        type=Path,
        action="append",
        default=[],
        help="repeatable; dirs of ncount *.json",
    )
    c.add_argument(
        "--quant-dir", type=Path, help="dir of <RUN>.quant.csv plus .status sidecars"
    )
    c.add_argument(
        "--preflight-dir",
        type=Path,
        help="annotated dir; reported, not loaded (see --help)",
    )
    c.set_defaults(func=cmd_collect)

    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
