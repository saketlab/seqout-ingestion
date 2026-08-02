"""The upsert clause must update a column but never null one out."""

from transfer.pg_fields import non_nulling_update


def test_coalesces_every_payload_column():
    sql = non_nulling_update("sra_studies", ["title", "abstract"])
    # incoming NULL must lose to what postgres already holds
    assert "title = COALESCE(EXCLUDED.title, sra_studies.title)" in sql
    assert "abstract = COALESCE(EXCLUDED.abstract, sra_studies.abstract)" in sql
    # a bare assignment anywhere would reintroduce the nulling bug
    assert "title = EXCLUDED.title" not in sql
    assert "abstract = EXCLUDED.abstract" not in sql


def test_guard_skips_rows_that_would_not_change():
    sql = non_nulling_update("geo_series", ["summary"])
    where = sql.split(" WHERE ", 1)[1]
    # only rewrite when the incoming value is real AND actually different --
    # without this every conflicting row is rewritten into a dead tuple
    assert "EXCLUDED.summary IS NOT NULL" in where
    assert "geo_series.summary IS DISTINCT FROM EXCLUDED.summary" in where


def test_multiple_columns_are_or_ed_in_the_guard():
    sql = non_nulling_update("t", ["a", "b"])
    where = sql.split(" WHERE ", 1)[1]
    # any one changed column is enough to write the row
    assert " OR " in where
    assert where.count("IS DISTINCT FROM") == 2


def test_no_payload_yields_empty_so_caller_uses_do_nothing():
    assert non_nulling_update("t", []) == ""
