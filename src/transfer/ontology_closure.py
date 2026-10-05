"""
Load the ontology closure, term labels and Beacon filtering-terms cache into Postgres.

TRUNCATE+COPY, not DROP+CREATE: the schema is static and
sql/tables/ontology_closure_pg.sql is applied idempotently on every run, so a
re-run can't drop the indexes the way a DROP TABLE would. All three tables are
rebuilt whole every run -- the same "no partial state ever persists" exception
documented on transfer/pentimento.py's `_upsert`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, LiteralString, cast

import psycopg

from helpers import exec_query_from_file, sqlite_connect
from ontology_closure.build import ONTOLOGY_OF

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

ONTOLOGY_DISPLAY = {
    "mondo": "Mondo Disease Ontology",
    "uberon": "Uberon multi-species anatomy ontology",
    "cl": "Cell Ontology",
    "efo": "Experimental Factor Ontology",
}


def ontology_closure_transfer(pg_url: str, db_file: Path) -> None:
    """Load ontology_closure.db's tables into Postgres, replacing their contents."""
    con = sqlite_connect(db_file)
    closure = con.execute(
        "SELECT ontology, ancestor_id, descendant_id FROM ontology_closure"
    ).fetchall()
    terms = con.execute("SELECT ontology, id, label FROM ontology_terms").fetchall()
    con.close()

    with psycopg.connect(pg_url) as pg, pg.cursor() as cur:
        exec_query_from_file("tables/ontology_closure_pg.sql", conn=pg)
        cur.execute("TRUNCATE ontology_closure, ontology_terms")
        with cur.copy(
            "COPY ontology_closure (ontology, ancestor_id, descendant_id) FROM STDIN"
        ) as cp:
            for row in closure:
                cp.write_row(row)
        with cur.copy("COPY ontology_terms (ontology, id, label) FROM STDIN") as cp:
            for row in terms:
                cp.write_row(row)
        cur.execute("ANALYZE ontology_closure")
        cur.execute("ANALYZE ontology_terms")
        counts = cur.execute(
            "SELECT ontology, count(*) FROM ontology_closure "
            "GROUP BY ontology ORDER BY ontology"
        ).fetchall()
        (n_terms,) = cur.execute("SELECT count(*) FROM ontology_terms").fetchone() or (
            0,
        )
    logger.info("ontology_closure transfer: closure=%s terms=%d", counts, n_terms)


def ontology_closure_filtering_terms(pg_url: str) -> None:
    """
    Precompute the /filtering_terms payload into `beacon_filtering_terms`.

    Present ontology ids (per field) union their ancestors (from ontology_closure),
    labelled via ontology_terms, plus the organism/sex alphanumeric filters. Done
    offline so the endpoint is a trivial SELECT (the DISTINCT scans are slow live).
    """
    with psycopg.connect(pg_url) as pg, pg.cursor() as cur:
        exec_query_from_file("tables/ontology_closure_pg.sql", conn=pg)
        cur.execute("TRUNCATE beacon_filtering_terms")
        for field, onto in ONTOLOGY_OF.items():
            col = f"{field}_ontology_id"
            display = ONTOLOGY_DISPLAY[onto]
            cur.execute(
                cast(
                    "LiteralString",
                    f"""
                    INSERT INTO beacon_filtering_terms (term_type, id, label, scopes)
                    WITH present AS (
                        SELECT DISTINCT {col} AS id FROM ontology_samples_v5
                        WHERE {col} IS NOT NULL
                    ),
                    expanded AS (
                        SELECT id FROM present
                        UNION
                        SELECT DISTINCT c.ancestor_id FROM ontology_closure c
                        JOIN present p ON p.id = c.descendant_id WHERE c.ontology = %s
                    )
                    SELECT %s, e.id, t.label, ARRAY['biosample','individual']
                    FROM expanded e
                    LEFT JOIN ontology_terms t ON t.ontology = %s AND t.id = e.id
                    ON CONFLICT (id) DO NOTHING
                    """,  # noqa: S608 -- col/onto come from the fixed ONTOLOGY_OF dict
                ),
                (onto, display, onto),
            )
        cur.execute(
            "SELECT array_agg(DISTINCT sex) FROM (SELECT sex FROM ontology_samples_v5 "
            "TABLESAMPLE SYSTEM (1) WHERE sex IS NOT NULL AND sex <> '' LIMIT 5000) s"
        )
        sex_row = cur.fetchone()
        sex_vals = (sex_row[0] if sex_row else None) or []
        cur.execute(
            "INSERT INTO beacon_filtering_terms (term_type, id, label, scopes, vals) "
            "VALUES ('alphanumeric','sex','sex',ARRAY['biosample','individual'],%s), "
            "('alphanumeric','organism','organism',ARRAY['biosample','individual'],NULL)",
            (sex_vals,),
        )
        cur.execute("ANALYZE beacon_filtering_terms")
        (n,) = cur.execute(
            "SELECT count(*) FROM beacon_filtering_terms"
        ).fetchone() or (0,)
    logger.info("filtering-terms rows: %d", n)


def ontology_closure_verify_pg(pg_url: str) -> None:
    """Read-only: confirm the closure landed and the descendant filter uses an index."""
    with psycopg.connect(pg_url) as pg, pg.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ontology_closure WHERE ontology='mondo' "
            "AND ancestor_id='MONDO:0005070' AND descendant_id='MONDO:0004992'"
        )
        if not cur.fetchone():
            msg = "PG closure missing neoplasm -> breast-cancer edge"
            raise AssertionError(msg)
        cur.execute(
            "SELECT count(*) FROM ontology_samples_v5 "
            "WHERE disease_ontology_id='MONDO:0005070'"
        )
        (exact,) = cur.fetchone() or (0,)
        cur.execute(
            "SELECT count(*) FROM ontology_samples_v5 "
            "WHERE disease_ontology_id='MONDO:0005070' "
            "OR disease_ontology_id IN (SELECT descendant_id FROM ontology_closure "
            "WHERE ontology='mondo' AND ancestor_id='MONDO:0005070')"
        )
        (expanded,) = cur.fetchone() or (0,)
        if expanded < exact:
            msg = f"descendant set {expanded} < exact {exact}"
            raise AssertionError(msg)
        cur.execute(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT count(*) FROM ontology_samples_v5 "
            "WHERE disease_ontology_id IN (SELECT descendant_id FROM ontology_closure "
            "WHERE ontology='mondo' AND ancestor_id='MONDO:0005070')"
        )
        plan = "\n".join(r[0] for r in cur.fetchall())
    logger.info("%s", plan)
    if "Seq Scan on ontology_samples_v5" in plan:
        msg = "descendant filter fell back to seq scan"
        raise AssertionError(msg)
    logger.info("verify-pg OK: exact=%d expanded=%d", exact, expanded)
