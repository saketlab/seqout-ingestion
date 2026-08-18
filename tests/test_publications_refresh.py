"""The bits of the weekly publication refresh that are not just SQL."""

from datetime import date

from refresh.publications import (
    _PG_UPSERT,
    _SOURCES,
    _TITLE_UPDATE,
    _parse_pub_date,
    _pg_row,
    _run_workers,
)


def test_upsert_uses_psycopg_placeholders_not_sqlite_ones():
    # derived from the sqlite twin by replacing '?' -- a leftover would make
    # psycopg raise on the first batch of every run
    assert "?" not in _PG_UPSERT
    assert _PG_UPSERT.count("%s") == 13


def test_pg_row_has_one_value_per_upsert_placeholder():
    row = _pg_row({"pmid": "12345", "title": "a paper", "pub_date": "2024-05-01"})
    assert len(row) == _PG_UPSERT.count("%s")
    assert row[0] == "12345"
    # pub_date is DATE in postgres and TEXT in the sqlite twin
    assert row[4] == date(2024, 5, 1)


def test_title_repair_touches_no_column_but_title():
    # the repair runs against rows that already hold good citation counts and
    # journal metrics; going through _PG_UPSERT instead would blank them
    assert _TITLE_UPDATE.count("%s") == 3  # new title, pmid, no-op guard
    for col in ("citation_count", "journal_h_index", "issn", "doi", "authors"):
        assert col not in _TITLE_UPDATE


def test_unparseable_pub_date_becomes_null_not_an_exception():
    # one bad date must not take the whole batch down with it
    assert _parse_pub_date("not a date") is None
    assert _parse_pub_date("-") is None
    assert _parse_pub_date("") is None
    assert _parse_pub_date(None) is None


def test_every_pub_date_format_postgres_transfer_accepts_still_parses():
    assert _parse_pub_date("2024-05-01") == date(2024, 5, 1)
    assert _parse_pub_date("2024/05/01") == date(2024, 5, 1)
    assert _parse_pub_date("20240501") == date(2024, 5, 1)


def test_sharding_covers_every_item_exactly_once():
    # a dropped or duplicated accession would silently skip a study, or burn an
    # NCBI call on one already done
    seen: list[str] = []

    def worker(data, key, pbar, **_):  # noqa: ARG001
        seen.extend(data)
        return len(data)

    data = [f"acc{i}" for i in range(17)]
    counts = _run_workers(worker, data, ["k1", "k2", "k3"], pbar=None)

    assert sorted(seen) == sorted(data)
    assert sum(counts) == len(data)


def test_worker_count_never_exceeds_the_work():
    # min(keys, data): an empty shard would open a pointless postgres connection
    counts = _run_workers(
        lambda data, key, pbar, **_: len(data),  # noqa: ARG005
        ["only-one"],
        ["k1", "k2", "k3"],
        pbar=None,
    )
    assert counts == [1]


def test_dra_is_the_only_source_scoped_to_part_of_its_table():
    # scope is kept out of `unlinked` so status does not count the ~17.6k
    # mirrored DRA studies as "linked"
    scoped = {s.datasource for s in _SOURCES if s.scope != "TRUE"}
    assert scoped == {"dra"}
