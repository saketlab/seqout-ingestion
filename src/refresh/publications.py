"""
Weekly publication refresh, run on the VPS straight against production Postgres.

Every other stage buffers through SQLite on the lab box and pushes over a tunnel
(see READ-THIS.md). This one deliberately does not. Publication linkage is the
one enrichment that goes stale on its own -- a dataset released in 2024 gets its
paper in 2026 -- so it has to run where the data lives, on its own cadence, with
no ``DATA_DIR`` and no per-source SQLite window slices to be reconciled against.

Three steps, in order:

  1. resolve   studies carrying no publication at all -> pysradb (NCBI elink /
               literature search), writing ``pubmed_id_pysradb``. Never-checked
               studies go first, then the least-recently-checked.
  2. metadata  pmids a study references but ``pubmed_metadata`` does not hold ->
               ``pysradb.pmid_info`` (NCBI efetch + OpenAlex journal metrics).
  3. tsv       ``pub_tsv_transfer``, which folds the paper titles into each
               study's search ``tsv`` so the new papers are searchable.

Both API steps are budgeted (``--limit-per-source``, ``--meta-limit``) because
the unlinked backlog is ~1.5M studies, most of it ENA (measured 2026-08-08: ENA
1,016,796, SRA 454,880, GEO 32,265, GSA 25,002, AE 65,551, GEA 735, DRA 1,178
DDBJ-exclusive). A weekly run chips at that forever; it is not meant to finish.
``pubmed_last_checked`` is the queue cursor -- stamped on every accession tried,
whether or not a pmid came back, so the next run moves on instead of retrying
the same misses. ``--recheck-after`` decides when a study becomes eligible
again, which is what picks up papers published after we first looked.

Nothing here deletes, and nothing here is lost on the next lab-box transfer:
``postgres.py`` upserts through ``non_nulling_update()``, so an incoming NULL
``pubmed_id_pysradb`` from a fresh SQLite window loses to what we wrote.

Progress is the database itself -- ``refresh status`` reads it back, and the
numbers move while the run is in flight.
"""

import logging
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple

import psycopg
from pysradb.sraweb import SRAweb
from tqdm import tqdm

from backfill.backfill_utils import _chunked, _parse_pmid
from backfill.pubmed_ids import _resolve_via_sraweb
from backfill.pubmed_meta import UPSERT_SQL, _pmid_row_to_tuple
from pipeline_types import Datasource
from transfer.pub_tsv import pub_tsv_transfer

logger = logging.getLogger(__name__)


class _Source(NamedTuple):
    datasource: Datasource
    table: str
    acc_col: str
    # "this study has no publication linked from any column". Every table also
    # carries pubmed_last_checked, which is the queue cursor, not a linkage.
    unlinked: str
    # the rows this job is responsible for at all. DRA is the only source where
    # that is not the whole table; keeping it separate from `unlinked` is what
    # stops `refresh status` from counting out-of-scope rows as "linked".
    scope: str = "TRUE"


# GSA-Human has no table of its own in postgres; its studies live in gsa_studies.
# gsa_projects.pubmed_id (jsonb) is left out: it has no join back to a study, and
# GSA is deliberately outside pub_tsv anyway (see transfer/pub_tsv.py).
_SOURCES = (
    _Source(
        Datasource.Sra,
        "sra_studies",
        "accession",
        "coalesce(pubmed_id_pysradb, '') = ''"
        " AND links->'XREF_LINK'->>'DB' IS DISTINCT FROM 'pubmed'",
    ),
    _Source(
        Datasource.Geo,
        "geo_series",
        "accession",
        "coalesce(pubmed_id_pysradb, '') = ''"
        " AND coalesce(array_length(pubmed_id, 1), 0) = 0"
        " AND coalesce(array_length(pubmed_id_citation, 1), 0) = 0",
    ),
    _Source(
        Datasource.Ena,
        "ena_studies",
        "study_accession",
        "coalesce(pubmed_id_pysradb, '') = ''",
    ),
    _Source(
        Datasource.ArrayExpress,
        "arrayexpress_experiments",
        "accession",
        "coalesce(pubmed_id_pysradb, '') = '' AND coalesce(pubmed_id, '') = ''",
    ),
    _Source(
        Datasource.Gea,
        "gea_experiments",
        "accession",
        "coalesce(pubmed_id_pysradb, '') = '' AND coalesce(pubmed_id, '') = ''",
    ),
    _Source(
        Datasource.Gsa,
        "gsa_studies",
        "study_accession",
        "coalesce(pubmed_id_pysradb, '') = ''",
    ),
    # DDBJ-exclusive only: a mirrored DRP's SRA/ENA twin is canonical and already
    # carries the pmid, so the DDBJ copy would cost ~17k NCBI calls nothing reads.
    # is_only_ddbj is int2 here, not boolean.
    _Source(
        Datasource.Dra,
        "dra_studies",
        "accession",
        "coalesce(pubmed_id_pysradb, '') = ''"
        " AND links->'XREF_LINK'->>'DB' IS DISTINCT FROM 'pubmed'",
        scope="is_only_ddbj = 1",
    ),
)

# Every pmid any study points at, from every column that can hold one. The regex
# doubles as the NULL/''/garbage filter; geo's arrays and the scalar text
# columns all reduce to the same shape. '0' is in there and is not a pmid, hence
# the leading [1-9].
#
# Known residue: a pmid pubmed and OpenAlex both refuse to resolve never lands in
# pubmed_metadata, so it stays in this backlog and is retried every week. That is
# bounded and visible in `refresh status`; a "tried and failed" marker would fix
# it, at the cost of never retrying after a transient NCBI outage.
_REFERENCED_PMIDS = """
    SELECT DISTINCT p FROM (
        SELECT pubmed_id_pysradb AS p FROM sra_studies
        UNION ALL SELECT pubmed_id_pysradb FROM ena_studies
        UNION ALL SELECT pubmed_id_pysradb FROM arrayexpress_experiments
        UNION ALL SELECT pubmed_id_pysradb FROM dra_studies
        UNION ALL SELECT pubmed_id_pysradb FROM gea_experiments
        UNION ALL SELECT pubmed_id_pysradb FROM gsa_studies
        UNION ALL SELECT pubmed_id_pysradb FROM geo_series
        UNION ALL SELECT unnest(pubmed_id) FROM geo_series
        UNION ALL SELECT unnest(pubmed_id_citation) FROM geo_series
        UNION ALL SELECT pubmed_id FROM arrayexpress_experiments
        UNION ALL SELECT pubmed_id FROM gea_experiments
    ) refs
    WHERE p ~ '^[1-9][0-9]*$'
"""

_MISSING_PMIDS = f"""
    SELECT x.p FROM ({_REFERENCED_PMIDS}) x
    LEFT JOIN pubmed_metadata m ON m.pmid = x.p
    WHERE m.pmid IS NULL
    LIMIT %s
"""  # noqa: S608

# Titles fetched before sraweb.py switched to itertext() stop at the first inline
# tag: "... through P2Y<sub>12</sub> receptor." was stored as "... through P2Y".
# _MISSING_PMIDS never revisits a row that already exists, so they need their own
# queue. PubMed titles end in punctuation; a cut one ends mid-word. The heuristic
# over-matches (an epmc-sourced title with no full stop is re-fetched for
# nothing), which costs efetch quota and nothing else; the upsert is idempotent.
_TRUNCATED_TITLE_PMIDS = r"""
    SELECT pmid FROM pubmed_metadata
    WHERE title IS NOT NULL AND title <> '' AND title !~ '[.?!)\]]$'
    ORDER BY pmid
    LIMIT %s
"""

# Same columns and same conflict handling as the sqlite twin; only the
# placeholder style differs. Derived rather than copied so the two cannot drift.
_PG_UPSERT = UPSERT_SQL.replace("?", "%s")


def refresh_publications(
    pg_url: str,
    ncbi_keys_file: Path,
    openalex_keys_file: Path,
    limit_per_source: int,
    meta_limit: int,
    recheck_after_days: int,
    batch_size: int,
    retries: int,
    timeout: int,
    skip_journal_metrics: bool,
    skip_resolve: bool,
    skip_meta: bool,
    skip_tsv: bool,
    is_dry_run: bool,
) -> None:
    """Resolve missing publications, fetch their metadata, refresh ``pub_tsv``."""
    ncbi_keys = _read_keys(ncbi_keys_file)
    openalex_keys = _read_keys(openalex_keys_file)

    if not skip_resolve:
        _resolve_step(
            pg_url,
            ncbi_keys,
            limit_per_source,
            recheck_after_days,
            batch_size,
            retries,
            timeout,
            is_dry_run,
        )

    if not skip_meta:
        _metadata_step(
            pg_url,
            ncbi_keys,
            openalex_keys,
            meta_limit,
            batch_size,
            skip_journal_metrics,
            retries,
            timeout,
            is_dry_run,
        )

    if skip_tsv or is_dry_run:
        logger.info("skipping pub_tsv refresh")
    else:
        logger.info("refreshing pub_tsv ...")
        pub_tsv_transfer(pg_url)

    logger.info("publication refresh done")


def repair_truncated_titles(
    pg_url: str,
    ncbi_keys_file: Path | None,
    meta_limit: int,
    batch_size: int,
    retries: int,
    timeout: int,
    skip_tsv: bool,
    is_dry_run: bool,
) -> None:
    """One-off: re-fetch titles the pre-``itertext()`` efetch parser cut short.

    Titles only. Deliberately not ``_metadata_step``: ``pmid_info`` re-fetches a
    citation count per pmid from OpenAlex (17k singleton requests for data we
    already hold) and returns a whole row, which ``UPSERT_SQL`` writes wholesale
    -- with journal metrics skipped that would blank ``journal_h_index`` and
    friends on every row it touched. Only ``title`` was ever wrong, so only
    ``title`` is written, and only when efetch gives back something non-empty.
    """
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute(_TRUNCATED_TITLE_PMIDS, (meta_limit,))
        pmids = [row[0] for row in cur.fetchall()]

    logger.info("titles | %d pmids queued (cap %d)", len(pmids), meta_limit)
    if is_dry_run or not pmids:
        return

    # keys are optional here, unlike the weekly run: this is ~175 efetch batches,
    # about a minute even at NCBI's unkeyed 3/s, and the keys file lives on the
    # VPS while the backlog is small enough to clear from a laptop.
    ncbi_keys = _read_keys(ncbi_keys_file) if ncbi_keys_file else [None]
    pbar = _pbar(len(pmids), "pubmed titles", "pmid")
    written = sum(
        _run_workers(
            _title_worker,
            pmids,
            ncbi_keys,
            pbar,
            pg_url=pg_url,
            batch_size=batch_size,
            retries=retries,
            timeout=timeout,
        ),
    )
    pbar.close()
    logger.info("titles | %d/%d rows updated", written, len(pmids))

    logger.info("syncing study_publications titles ...")
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute(_SYNC_STUDY_PUBLICATION_TITLES)
        logger.info("  %s rows updated", cur.rowcount)
        conn.commit()

    if skip_tsv:
        logger.info("skipping pub_tsv refresh")
    else:
        logger.info("refreshing pub_tsv ...")
        pub_tsv_transfer(pg_url)

    logger.info("title repair done")


_TITLE_UPDATE = """
    UPDATE pubmed_metadata SET title = %s WHERE pmid = %s AND title <> %s
"""

# study_publications.publications is a denormalized copy of pubmed_metadata rows
# (postgres.py::_transfer_study_publications builds it on the lab box), and it is
# what the API returns and what pub_tsv's main path indexes; repairing
# pubmed_metadata alone changes nothing a user can see. Titles only: every other
# key in those objects is still whatever the transfer wrote.
#
# Non-array publications would make jsonb_array_elements throw, hence the
# typeof guard; the outer IS DISTINCT FROM keeps this to the rows that move.
_SYNC_STUDY_PUBLICATION_TITLES = """
    UPDATE study_publications sp
    SET publications = q.pubs
    FROM (
        SELECT s.accession, s.source, jsonb_agg(
                   CASE
                       WHEN m.title IS NOT NULL AND m.title <> (e ->> 'title')
                       THEN jsonb_set(e, '{title}', to_jsonb(m.title))
                       ELSE e
                   END ORDER BY ord
               ) AS pubs
        FROM study_publications s
        CROSS JOIN LATERAL jsonb_array_elements(s.publications)
                   WITH ORDINALITY AS a(e, ord)
        LEFT JOIN pubmed_metadata m ON m.pmid = a.e ->> 'pmid'
        WHERE jsonb_typeof(s.publications) = 'array'
        GROUP BY s.accession, s.source
    ) q
    WHERE sp.accession = q.accession
      AND sp.source = q.source
      AND sp.publications IS DISTINCT FROM q.pubs
"""


def _title_worker(
    data: list[str],
    key: str | None,
    pbar: tqdm,
    pg_url: str,
    batch_size: int,
    retries: int,
    timeout: int,
) -> int:
    """efetch a batch, keep the title, drop the rest of the row on the floor."""
    sra = SRAweb(api_key=key, retries=retries, timeout=timeout, verbose=False)
    updated = 0

    with psycopg.connect(pg_url) as conn:
        for batch in _chunked(data, batch_size):
            results: dict[str, dict] = {}
            try:
                # private, but it is the efetch half of pmid_info without the
                # OpenAlex half; the public entry point cannot skip that.
                sra._fetch_pmid_metadata_ncbi(batch, results)  # noqa: SLF001
            except Exception:
                logger.exception("efetch failed for a batch of %d", len(batch))
                results = {}

            rows = [
                (entry["title"], pmid, entry["title"])
                for pmid, entry in results.items()
                if entry.get("title")
            ]
            if rows:
                with conn.cursor() as cur:
                    cur.executemany(_TITLE_UPDATE, rows)
                    updated += cur.rowcount if cur.rowcount > 0 else 0
                conn.commit()

            pbar.update(len(batch))

    return updated


# --------------------------------------------------------------------------
# step 1: resolve studies that have no publication at all
# --------------------------------------------------------------------------


def _resolve_step(
    pg_url: str,
    ncbi_keys: list[str],
    limit_per_source: int,
    recheck_after_days: int,
    batch_size: int,
    retries: int,
    timeout: int,
    is_dry_run: bool,
) -> None:
    targets: dict[_Source, list[str]] = {}
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        for source in _SOURCES:
            targets[source] = _unlinked_accessions(
                cur,
                source,
                recheck_after_days,
                limit_per_source,
            )

    for source, accessions in targets.items():
        logger.info(
            "resolve | %-4s | %d queued (cap %d)",
            source.datasource,
            len(accessions),
            limit_per_source,
        )

    if is_dry_run:
        return

    for source, accessions in targets.items():
        if not accessions:
            continue

        pbar = _pbar(len(accessions), f"resolve {source.datasource}", "acc")
        found = sum(
            _run_workers(
                _resolve_worker,
                accessions,
                ncbi_keys,
                pbar,
                pg_url=pg_url,
                source=source,
                batch_size=batch_size,
                retries=retries,
                timeout=timeout,
            ),
        )
        pbar.close()
        logger.info(
            "resolve | %-4s | %d/%d resolved to a pmid",
            source.datasource,
            found,
            len(accessions),
        )


def _unlinked_accessions(
    cur: psycopg.Cursor,
    source: _Source,
    recheck_after_days: int,
    limit: int,
) -> list[str]:
    """
    Return the oldest-checked unlinked studies, never-checked ones first.

    Seq-scans the study table once per run. Deliberately un-indexed: a partial
    index on (pubmed_last_checked) would have to be maintained on every transfer
    to save a few seconds a week.
    """
    cur.execute(
        f"""
        SELECT {source.acc_col} FROM {source.table}
        WHERE ({source.scope})
          AND ({source.unlinked})
          AND (pubmed_last_checked IS NULL
               OR pubmed_last_checked < current_date - %(days)s::int)
        ORDER BY pubmed_last_checked NULLS FIRST
        LIMIT %(limit)s
        """,  # noqa: S608
        {"days": recheck_after_days, "limit": limit},
    )
    return [row[0] for row in cur.fetchall()]


def _resolve_worker(
    data: list[str],
    key: str,
    pbar: tqdm,
    pg_url: str,
    source: _Source,
    batch_size: int,
    retries: int,
    timeout: int,
) -> int:
    sra = SRAweb(key, retries=retries, timeout=timeout, verbose=False)
    found = 0

    with psycopg.connect(pg_url) as conn:
        for batch in _chunked(data, batch_size):
            try:
                if source.datasource is Datasource.Dra:
                    updates = _resolve_dra(sra, conn, batch)
                else:
                    updates = _resolve_via_sraweb(sra, source.datasource, batch)
            except Exception:
                logger.exception(
                    "%s: failed to resolve a batch of %d",
                    source.datasource,
                    len(batch),
                )
                updates = []

            with conn.cursor() as cur:
                # stamped whether or not a pmid came back, so a miss is not
                # retried next week, which makes the queue advance.
                cur.execute(
                    f"UPDATE {source.table} SET pubmed_last_checked = current_date"  # noqa: S608
                    f" WHERE {source.acc_col} = ANY(%s)",
                    (batch,),
                )
                if updates:
                    cur.executemany(
                        f"UPDATE {source.table} SET pubmed_id_pysradb = %s"  # noqa: S608
                        f" WHERE {source.acc_col} = %s",
                        updates,
                    )
            conn.commit()

            found += len(updates)
            pbar.update(len(batch))

    return found


def _resolve_dra(
    sra: SRAweb,
    conn: psycopg.Connection,
    batch: list[str],
) -> list[tuple[str, str]]:
    """
    Resolve a DRP to a pmid via its BioProject -- DRA carries no pubmed link.

    Must go through fetch_bioproject_pmids, which resolves to an NCBI UID first:
    efetch by accession reads only the digits, so PRJDB13786 returns PRJNA13786.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT accession, bioproject_id FROM dra_studies"
            " WHERE accession = ANY(%s) AND bioproject_id IS NOT NULL",
            (batch,),
        )
        wanted: dict[str, str] = dict(cur.fetchall())

    if not wanted:
        return []

    found = sra.fetch_bioproject_pmids(sorted(set(wanted.values())))

    updates: list[tuple[str, str]] = []
    for accession, bioproject in wanted.items():
        pmids = found.get(bioproject) or []
        pmid = _parse_pmid(pmids[0]) if pmids else None
        if pmid:
            updates.append((pmid, accession))
    return updates


# --------------------------------------------------------------------------
# step 2: fetch metadata for pmids no one has looked up yet
# --------------------------------------------------------------------------


def _metadata_step(
    pg_url: str,
    ncbi_keys: list[str],
    openalex_keys: list[str],
    meta_limit: int,
    batch_size: int,
    skip_journal_metrics: bool,
    retries: int,
    timeout: int,
    is_dry_run: bool,
) -> None:
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute(_MISSING_PMIDS, (meta_limit,))
        pmids = [row[0] for row in cur.fetchall()]

    logger.info("metadata | %d pmids queued (cap %d)", len(pmids), meta_limit)
    if is_dry_run or not pmids:
        return

    # one worker per NCBI key, each paired with an OpenAlex key (they are rate
    # limited independently, and there are usually fewer OpenAlex ones).
    keys = [
        (ncbi, openalex_keys[i % len(openalex_keys)])
        for i, ncbi in enumerate(ncbi_keys)
    ]

    pbar = _pbar(len(pmids), "pubmed metadata", "pmid")
    written = sum(
        _run_workers(
            _meta_worker,
            pmids,
            keys,
            pbar,
            pg_url=pg_url,
            batch_size=batch_size,
            skip_journal_metrics=skip_journal_metrics,
            retries=retries,
            timeout=timeout,
        ),
    )
    pbar.close()
    logger.info("metadata | %d/%d rows written", written, len(pmids))


def _meta_worker(
    data: list[str],
    key: tuple[str, str],
    pbar: tqdm,
    pg_url: str,
    batch_size: int,
    skip_journal_metrics: bool,
    retries: int,
    timeout: int,
) -> int:
    ncbi_key, openalex_key = key
    sra = SRAweb(
        api_key=ncbi_key,
        openalex_api_key=openalex_key,
        retries=retries,
        timeout=timeout,
        verbose=False,
    )
    written = 0

    with psycopg.connect(pg_url) as conn:
        for batch in _chunked(data, batch_size):
            try:
                info = sra.pmid_info(batch, skip_journal_metrics)
                rows = [
                    _pg_row(row.to_dict())
                    for _, row in info.iterrows()
                    if _parse_pmid(row.get("pmid"))
                ]
            except Exception:
                logger.exception("pmid_info failed for a batch of %d", len(batch))
                rows = []

            if rows:
                with conn.cursor() as cur:
                    cur.executemany(_PG_UPSERT, rows)
                conn.commit()
                written += len(rows)

            pbar.update(len(batch))

    return written


# The formats transfer/postgres.py's _parse_date accepts, minus its import: that
# module pulls the whole ingest stack (openpyxl, libarchive, h5py) in behind it,
# and this one has to run on a box that holds nothing but postgres and the API.
_PUB_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d")


def _pg_row(row: dict) -> tuple:
    """Build the sqlite tuple, coercing pub_date -- DATE here, TEXT there."""
    values = list(_pmid_row_to_tuple(row))
    values[4] = _parse_pub_date(values[4])
    return tuple(values)


def _parse_pub_date(value: str | None) -> date | None:
    """Parse a pub_date to NULL rather than failing the whole batch."""
    if not value or value.strip() in {"", "-"}:
        return None
    for fmt in _PUB_DATE_FORMATS:
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=UTC).date()
        except ValueError:
            continue
    logger.debug("unparseable pub_date %r, storing NULL", value)
    return None


# --------------------------------------------------------------------------
# status
# --------------------------------------------------------------------------

_STATUS_ROW = """
    SELECT count(*) FILTER (WHERE {scope}),
           count(*) FILTER (WHERE ({scope}) AND NOT ({unlinked})),
           count(*) FILTER (WHERE ({scope}) AND ({unlinked})
                              AND pubmed_last_checked IS NULL),
           max(pubmed_last_checked) FILTER (WHERE {scope})
    FROM {table}
"""


def publication_status(pg_url: str) -> str:
    """Render a snapshot of publication coverage as a table."""
    lines = [
        f"{'source':<6} {'in scope':>10} {'linked':>10} {'unchecked':>10}"
        f" {'linked %':>9}  last checked",
    ]

    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")

        for source in _SOURCES:
            cur.execute(
                _STATUS_ROW.format(
                    table=source.table,
                    unlinked=source.unlinked,
                    scope=source.scope,
                ),
            )
            total, linked, unchecked, last_checked = cur.fetchone()
            pct = (linked * 100 / total) if total else 0
            lines.append(
                f"{source.datasource:<6} {total:>10,} {linked:>10,}"
                f" {unchecked:>10,} {pct:>8.1f}%  {last_checked or '-'}",
            )

        cur.execute(f"SELECT count(*) FROM ({_REFERENCED_PMIDS}) x")  # noqa: S608
        referenced = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM pubmed_metadata")
        stored = cur.fetchone()[0]
        # LIMIT NULL is "no limit", so this counts the whole backlog.
        cur.execute(f"SELECT count(*) FROM ({_MISSING_PMIDS}) q", (None,))  # noqa: S608
        missing = cur.fetchone()[0]

        lines.append("")
        lines.append(
            f"pmids: {referenced:,} referenced by a study,"
            f" {stored:,} in pubmed_metadata, {missing:,} still to fetch",
        )

        for table in ("geo_series", "sra_studies", "arrayexpress_experiments"):
            cur.execute(
                "SELECT count(*) FILTER (WHERE pub_tsv IS NOT NULL), count(*)"  # noqa: S608
                f" FROM {table}",
            )
            with_tsv, rows = cur.fetchone()
            lines.append(f"pub_tsv: {table} {with_tsv:,}/{rows:,} searchable by paper")

    return "\n".join(lines)


# --------------------------------------------------------------------------
# plumbing
# --------------------------------------------------------------------------


def _read_keys(keys_file: Path) -> list[str]:
    keys = [line.strip() for line in keys_file.read_text().splitlines() if line.strip()]
    if not keys:
        msg = f"{keys_file} holds no keys"
        raise ValueError(msg)
    return keys


def _pbar(total: int, desc: str, unit: str) -> tqdm:
    """Interactive when attached to a terminal; one line a minute under systemd."""
    is_tty = sys.stderr.isatty()
    return tqdm(total=total, desc=desc, unit=unit, mininterval=1 if is_tty else 60)


def _run_workers(
    worker: Callable[..., int],
    data: list[str],
    keys: list,
    pbar: tqdm,
    **kwargs: object,
) -> list[int]:
    """One thread per API key -- NCBI rate-limits per key, so more is not faster."""
    num_workers = min(len(keys), len(data))
    shards: list[list[str]] = [[] for _ in range(num_workers)]
    for i, item in enumerate(data):
        shards[i % num_workers].append(item)

    with ThreadPoolExecutor(num_workers) as pool:
        futures = [
            pool.submit(worker, data=shards[i], key=keys[i], pbar=pbar, **kwargs)
            for i in range(num_workers)
        ]
        return [f.result() for f in as_completed(futures)]
