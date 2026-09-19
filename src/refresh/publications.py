"""
Refresh publication links and metadata directly in production Postgres.

Run weekly on the VPS, independently of SQLite ingest windows: papers can
appear years after their data. No DATA_DIR is required.

Steps:
1. Resolve unlinked studies via pysradb into pubmed_id_pysradb, prioritizing
   never-checked studies, then the oldest checks.
2. Fetch missing pubmed_metadata via NCBI efetch and OpenAlex.
3. Fold publication titles into study search vectors via pub_tsv_transfer.
4. Refresh the indexed publication-date view via refresh_study_pub_dates.

--limit-per-source and --meta-limit bound API work. Every attempted study
gets pubmed_last_checked, including misses; --recheck-after controls when
it becomes eligible again. `refresh status` reports live database coverage.

No rows are deleted. Non-nulling transfer upserts preserve resolved PMIDs
when a later SQLite window supplies NULL.
"""

import logging
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple

import psycopg
from pysradb.sraweb import SRAweb, TransientAPIError
from tqdm import tqdm

from backfill.backfill_utils import _chunked, _parse_pmid
from backfill.pubmed_ids import _resolve_via_sraweb
from backfill.pubmed_meta import UPSERT_SQL, _pmid_row_to_tuple
from pipeline_types import Datasource
from transfer.pub_dates import refresh_study_pub_dates
from transfer.pub_tsv import pub_tsv_transfer

logger = logging.getLogger(__name__)


class _Source(NamedTuple):
    datasource: Datasource
    table: str
    acc_col: str
    # No publication linked from any column; pubmed_last_checked is only a cursor.
    unlinked: str
    # Scope is separate from linkage so status excludes out-of-scope DRA rows.
    scope: str = "TRUE"


# GSA-Human shares gsa_studies. Exclude gsa_projects.pubmed_id: it has no study
# join, and GSA is outside pub_tsv (see transfer/pub_tsv.py).
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
    # Only DDBJ-exclusive studies need lookups; mirrors use SRA/ENA PMIDs.
    # is_only_ddbj is int2, not boolean.
    _Source(
        Datasource.Dra,
        "dra_studies",
        "accession",
        "coalesce(pubmed_id_pysradb, '') = ''"
        " AND links->'XREF_LINK'->>'DB' IS DISTINCT FROM 'pubmed'",
        scope="is_only_ddbj = 1",
    ),
)

# Collect referenced PMIDs across arrays and scalar columns; the regex rejects
# NULL, empty, malformed and zero values. Unresolvable PMIDs stay in the backlog
# and retry weekly, allowing recovery from transient API failures.
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

# Older efetch parsing truncated titles at inline tags. Existing metadata is
# absent from _MISSING_PMIDS, so repair it separately using missing terminal
# punctuation as a heuristic. False positives cost quota; upserts are idempotent.
_TRUNCATED_TITLE_PMIDS = r"""
    SELECT pmid FROM pubmed_metadata
    WHERE title IS NOT NULL AND title <> '' AND title !~ '[.?!)\]]$'
    ORDER BY pmid
    LIMIT %s
"""

# Derive the Postgres upsert from SQLite SQL to keep columns and conflicts aligned.
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
    sources: list[str] | None = None,
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
            sources=sources,
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

    if not is_dry_run:
        logger.info("refreshing study_pub_dates ...")
        refresh_study_pub_dates(pg_url)

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
    """
    Repair titles truncated by the pre-itertext() efetch parser.

    Update only non-empty titles. Avoid pmid_info and full-row upserts, which
    would refetch citation counts and could erase existing journal metrics.
    """
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute(_TRUNCATED_TITLE_PMIDS, (meta_limit,))
        pmids = [row[0] for row in cur.fetchall()]

    logger.info("titles | %d pmids queued (cap %d)", len(pmids), meta_limit)
    if is_dry_run or not pmids:
        return

    # API keys are optional for this small, one-off efetch backlog.
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

# Refresh titles in the denormalized study_publications copy used by the API
# and pub_tsv. Preserve other keys, guard against non-array JSON, and update
# only changed rows.
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
    """Fetch a PMID batch and retain only titles."""
    sra = SRAweb(api_key=key, retries=retries, timeout=timeout, verbose=False)
    updated = 0

    with psycopg.connect(pg_url) as conn:
        for batch in _chunked(data, batch_size):
            results: dict[str, dict] = {}
            try:
                # Use the private efetch helper to skip pmid_info's OpenAlex requests.
                sra._fetch_pmid_metadata_ncbi(batch, results)  # noqa: SLF001
            except (Exception, TransientAPIError):
                # pysradb's TransientAPIError inherits BaseException; catch it explicitly
                # so one failed batch does not abort the run.
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
    sources: list[str] | None = None,
) -> None:
    wanted_sources = (
        [s for s in _SOURCES if s.datasource in sources] if sources else _SOURCES
    )
    targets: dict[_Source, list[str]] = {}
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        for source in wanted_sources:
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
    Return unlinked studies by oldest check, with never-checked studies first.

    Scan once per run; avoid maintaining a timestamp index on every transfer
    for this weekly query.
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
            except (Exception, TransientAPIError):
                # Catch TransientAPIError explicitly because it inherits BaseException.
                logger.exception(
                    "%s: failed to resolve a batch of %d",
                    source.datasource,
                    len(batch),
                )
                updates = []

            with conn.cursor() as cur:
                # Stamp misses too, so the queue advances.
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
    Resolve a DRP's PMID via BioProject; DRA has no direct publication link.

    Resolve the NCBI UID first: efetch by accession reads only the digits,
    so PRJDB13786 would return PRJNA13786.
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

    # Pair one worker per NCBI key with independently rate-limited OpenAlex keys.
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
            except (Exception, TransientAPIError):
                # Catch TransientAPIError explicitly because it inherits BaseException.
                logger.exception("pmid_info failed for a batch of %d", len(batch))
                rows = []

            if rows:
                with conn.cursor() as cur:
                    cur.executemany(_PG_UPSERT, rows)
                conn.commit()
                written += len(rows)

            pbar.update(len(batch))

    return written


# Match transfer/postgres.py date formats without importing its ingest dependencies.
_PUB_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d")


def _pg_row(row: dict) -> tuple:
    """Build the sqlite tuple, coercing pub_date: DATE here, TEXT there."""
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
    """One thread per API key: NCBI rate-limits per key, not by thread count."""
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
