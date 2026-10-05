"""
Read-only Postgres feeds for the sc_enrich.recall_rules passes.

Used by recipes/recall_fix/build.py. Every large reader pages on an indexed key
(keyset pagination): no statement scans more than one page or holds a long
snapshot. A whole-table regex scan over sra_experiments/geo_samples ran for 45+
minutes in the 2026-09-29 coverage analysis, shortly before production went into
crash recovery; short, restartable statements keep that load bounded.

The SQL prefilter (PROTOCOL_PREFILTER) is broad; the call is made in Python by
recall_rules.protocol_says_single_cell.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sc_enrich import recall_rules as rr

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from psycopg import Connection

logger = logging.getLogger(__name__)

PAGE = 200_000

PROTOCOL_PREFILTER = (
    r"10x|chromium|single[- ]?(cell|nucle)|rhapsody|evercode|parse bio|scale ?bio"
    r"|singleron|gexscope|dnbelab|mobidrop|seekone|pip[- ]?seq|icell8|fluidigm"
    r"|drop[- ]?seq|indrop|seq[- ]well|split[- ]?seq|sci[- ]?(rna|atac)|cel[- ]?seq2"
    r"|mars[- ]?seq|smart[- ]?seq3|strt[- ]?seq|quartz[- ]?seq2|vasa[- ]?seq"
    r"|flash[- ]?seq|microwell[- ]?seq|honeycomb|cell ?ranger|starsolo|visium"
    r"|slide[- ]?seq|stereo[- ]?seq|(sc|sn)atac"
)

TWINS_SQL = """
SELECT accession AS a, alias AS b FROM sra_studies WHERE alias ~ '^GSE[0-9]+$'
UNION ALL
SELECT study_accession, secondary_study_accession FROM ena_studies
 WHERE secondary_study_accession IS NOT NULL
"""

_LIBSOURCE_SQL = (
    "SELECT study, COUNT(*) FILTER"
    " (WHERE upper(library_source) LIKE '%%SINGLE CELL%%'), COUNT(*)"
    " FROM sra_experiments WHERE study = ANY(%s) GROUP BY study",
    "SELECT study_accession, COUNT(DISTINCT experiment_accession) FILTER"
    " (WHERE upper(library_source) LIKE '%%SINGLE CELL%%'),"
    " COUNT(DISTINCT experiment_accession)"
    " FROM ena_experiments WHERE study_accession = ANY(%s) GROUP BY study_accession",
)

# (table, key column, study column, text expression). ena_experiments is
# run-grained, so run_accession is its unique key.
_PROTOCOL_SOURCES = (
    (
        "sra_experiments",
        "accession",
        "study",
        "concat_ws(' | ', title, library_name, design_description)",
    ),
    (
        "ena_experiments",
        "run_accession",
        "study_accession",
        "concat_ws(' | ', experiment_title, library_name, sample_description)",
    ),
    (
        "geo_samples",
        "accession",
        "NULL",
        "concat_ws(' | ', title, description, channels::text)",
    ),
)


def _page_sql(table: str, key: str, study: str, text: str) -> str:
    # The key range is fixed first and the prefilter applied inside it, so each
    # statement is an index range scan over one page.
    return (
        f"SELECT {key}, {study}, txt FROM ("  # noqa: S608 - module constants only
        f"  SELECT {key}, {study}, {text} AS txt FROM {table}"
        f"   WHERE {key} > %(after)s ORDER BY {key} LIMIT %(page)s"
        f") p WHERE txt ~* %(rx)s"
    )


def _last_key_sql(table: str, key: str) -> str:
    return (
        f"SELECT max({key}) FROM (SELECT {key} FROM {table}"  # noqa: S608
        f" WHERE {key} > %(after)s ORDER BY {key} LIMIT %(page)s) p"
    )


def iter_protocol_texts(
    pg: Connection, page: int = PAGE
) -> Iterator[tuple[str, str, str]]:
    """Yield (table, study or GSM, text) for records passing the prefilter."""
    cur = pg.cursor()
    for table, key, study, text in _PROTOCOL_SOURCES:
        after, n_pages, n_rows = "", 0, 0
        while True:
            params = {"after": after, "page": page, "rx": PROTOCOL_PREFILTER}
            cur.execute(_last_key_sql(table, key), params)
            last = cur.fetchone()[0]
            if last is None:
                break
            cur.execute(_page_sql(table, key, study, text), params)
            rows = cur.fetchall()
            pg.commit()  # end the snapshot between pages
            n_rows += len(rows)
            for k, st, txt in rows:
                yield table, (k if table == "geo_samples" else st), txt
            after, n_pages = last, n_pages + 1
        logger.info("%s: %d pages, %d prefiltered records", table, n_pages, n_rows)


def gsm_to_series(pg: Connection, page: int = 20_000) -> dict[str, str]:
    """GSM -> GSE from geo_series.samples_ref (first series by accession wins)."""
    out: dict[str, str] = {}
    after = ""
    cur = pg.cursor()
    while True:
        cur.execute(
            "SELECT accession, samples_ref FROM geo_series WHERE accession > %s"
            " ORDER BY accession LIMIT %s",
            (after, page),
        )
        rows = cur.fetchall()
        pg.commit()
        if not rows:
            return out
        for gse, gsms in rows:
            for gsm in gsms or ():
                out.setdefault(gsm, gse)
        after = rows[-1][0]


def protocol_hits_by_study(pg: Connection) -> dict[str, int]:
    """Archive study (SRP/ERP/PRJEB/GSE) -> records naming a platform."""
    g2s: dict[str, str] | None = None
    hits: dict[str, int] = {}
    for table, rec, txt in iter_protocol_texts(pg):
        if not rr.protocol_says_single_cell(txt):
            continue
        study = rec
        if table == "geo_samples":
            g2s = g2s if g2s is not None else gsm_to_series(pg)
            study = g2s.get(rec)
        if study:
            hits[study] = hits.get(study, 0) + 1
    return hits


def fetch_all(pg: Connection, sql: str) -> list[tuple]:
    cur = pg.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    pg.commit()
    return rows


def alias_resolver(
    pg: Connection, page: int = 100_000
) -> tuple[Callable[[str], str | None], dict[str, str]]:
    """
    Build resolve(acc) -> canonical from unified_metadata aliases and twins.

    Same semantics as the matview's alias_map (min canonical wins). Paged on
    canonical_accession.
    """
    amap: dict[str, str] = {}
    canonicals: set[str] = set()
    after = ""
    cur = pg.cursor()
    while True:
        cur.execute(
            "SELECT canonical_accession, aliases FROM unified_metadata"
            " WHERE canonical_accession > %s ORDER BY canonical_accession LIMIT %s",
            (after, page),
        )
        rows = cur.fetchall()
        pg.commit()
        if not rows:
            break
        for canon, aliases in rows:
            canonicals.add(canon)
            for a in [canon, *(aliases or {}).values()]:
                if a and (a not in amap or canon < amap[a]):
                    amap[a] = canon
        after = rows[-1][0]
    twins: dict[str, str] = {}
    for a, b in fetch_all(pg, TWINS_SQL):
        twins.setdefault(a, b)
        twins.setdefault(b, a)
    logger.info("alias map: %d aliases, %d twin links", len(amap), len(twins))
    return (lambda acc: rr.resolve_canonical(acc, amap, twins, canonicals)), amap


def study_meta(
    pg: Connection, accs: list[str], chunk: int = 20_000
) -> dict[str, tuple]:
    """Canonical -> (title, organism, library strategies), for the veto."""
    out: dict[str, tuple] = {}
    cur = pg.cursor()
    for i in range(0, len(accs), chunk):
        cur.execute(
            "SELECT canonical_accession, title, dominant_scientific_name,"
            " library_strategy_counts FROM unified_metadata"
            " WHERE canonical_accession = ANY(%s)",
            (accs[i : i + chunk],),
        )
        for acc, title, org, strat in cur.fetchall():
            out[acc] = (title, org, list((strat or {}).keys()))
        pg.commit()
    return out


def libsource_counts(
    pg: Connection, archive_accs: list[str]
) -> dict[str, tuple[int, int]]:
    """(n declared SINGLE CELL, n experiments) per archive study, targeted list."""
    if not archive_accs:
        return {}
    out: dict[str, tuple[int, int]] = {}
    cur = pg.cursor()
    for sql in _LIBSOURCE_SQL:
        cur.execute(sql, (archive_accs,))
        for acc, n_sc, n in cur.fetchall():
            prev = out.get(acc, (0, 0))
            out[acc] = (max(prev[0], n_sc), max(prev[1], n))
        pg.commit()
    return out
