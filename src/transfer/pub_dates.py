"""
study_pub_dates, the indexed twin of the publication days in study_publications.

Every path that rewrites study_publications refreshes it.
"""

import logging

import psycopg

from helpers import exec_query_from_file

logger = logging.getLogger(__name__)

_DDL = "tables/study_pub_dates_pg.sql"


def refresh_study_pub_dates(pg_url: str) -> None:
    """Rebuild the view, creating it if it is missing."""
    exec_query_from_file(_DDL, pg_dsn=pg_url)
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        # CONCURRENTLY keeps /search/structured readable through the rebuild.
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY study_pub_dates")
        conn.commit()
        cur.execute("SELECT count(*) FROM study_pub_dates")
        row = cur.fetchone()
    logger.info("study_pub_dates | %s rows", row[0] if row else "?")
