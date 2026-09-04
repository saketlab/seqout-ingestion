"""sc_stats staging: the shapes the sweep actually emits."""

import json
import sqlite3

from sc_stats.ingest import ingest_dirs

# Trimmed from real sweep output. pct_mito is a min/mean/median/max block when the
# study ships a features file and null when it does not, and staging it as a single
# REAL silently failed on every study that had one.
_WITH_MITO = {
    "accession": "GSE1",
    "matrices": [
        {
            "file": "GSM1_matrix.mtx.gz",
            "n_cells": 10,
            "n_barcodes": 20,
            "is_raw_barcode_space": False,
            "species": "Homo sapiens",
            "species_method": "gene_symbols",
            "n_mito_genes": 13,
            "nCount_RNA": {"min": 1.0, "mean": 2.0, "median": 3.0, "max": 4.0},
            "nFeature_RNA": {"min": 5.0, "mean": 6.0, "median": 7.0, "max": 8.0},
            "pct_mito": {"min": 0.0, "mean": 0.09, "median": 0.03, "max": 35.1},
            "min_counts": 500,
            "n_cells_filtered": 4,
            "nCount_RNA_filtered": {
                "min": 9.0,
                "mean": 10.0,
                "median": 11.0,
                "max": 12.0,
            },
            "nFeature_RNA_filtered": {
                "min": 13.0,
                "mean": 14.0,
                "median": 15.0,
                "max": 16.0,
            },
            "pct_mito_filtered": {"min": 0.1, "mean": 0.2, "median": 0.3, "max": 0.4},
        }
    ],
    "failures": [],
}

_NO_FEATURES = {
    "accession": "GSE2",
    "matrices": [
        {
            "file": "GSM2_matrix.mtx.gz",
            "n_cells": 7,
            "n_barcodes": 7,
            "is_raw_barcode_space": True,
            "species": None,
            "species_method": "none",
            "n_mito_genes": 0,
            "nCount_RNA": {"min": 1.0, "mean": 2.0, "median": 3.0, "max": 4.0},
            "nFeature_RNA": {"min": 1.0, "mean": 2.0, "median": 3.0, "max": 4.0},
            "pct_mito": None,
            "min_counts": 500,
            "n_cells_filtered": 2,
            "nCount_RNA_filtered": {"min": 1.0, "mean": 2.0, "median": 3.0, "max": 4.0},
            "nFeature_RNA_filtered": {
                "min": 1.0,
                "mean": 2.0,
                "median": 3.0,
                "max": 4.0,
            },
            "pct_mito_filtered": None,
        }
    ],
    "failures": ["GSM2_bad.mtx.gz"],
}


def _write(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    for payload in (_WITH_MITO, _NO_FEATURES):
        acc = payload["accession"]
        (tmp_path / f"{acc}.stats.json").write_text(json.dumps(payload))
        (tmp_path / f"{acc}.status").write_text("ok\n1 matrices\n")
    # no_matrix writes a status and no JSON; it must still be staged, or the
    # study is indistinguishable from one the sweep never reached
    (tmp_path / "GSE3.status").write_text("no_matrix\n")
    return tmp_path


def test_stages_pct_mito_quad_and_nulls(tmp_path):
    results = _write(tmp_path / "results")
    db = tmp_path / "sc_stats.db"
    counts = ingest_dirs(db, [results])

    assert counts["studies"] == 2
    assert counts["matrices"] == 2
    assert counts["status_only"] == 1

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    got = conn.execute(
        "SELECT * FROM sc_stats_matrix WHERE accession='GSE1'"
    ).fetchone()
    assert (got["pct_mito_min"], got["pct_mito_max"]) == (0.0, 35.1)
    assert got["pct_mito_f_median"] == 0.3
    assert got["ncount_median"] == 3.0
    assert got["nfeature_f_max"] == 16.0
    assert got["species"] == "Homo sapiens"

    # no features file: every mito column stays NULL rather than becoming 0
    bare = conn.execute(
        "SELECT * FROM sc_stats_matrix WHERE accession='GSE2'"
    ).fetchone()
    assert bare["pct_mito_mean"] is None
    assert bare["pct_mito_f_mean"] is None
    assert bare["is_raw_barcode_space"] == 1


def test_status_only_study_is_kept(tmp_path):
    results = _write(tmp_path / "results")
    db = tmp_path / "sc_stats.db"
    ingest_dirs(db, [results])
    conn = sqlite3.connect(db)
    state = conn.execute(
        "SELECT state, n_matrices FROM sc_stats_study WHERE accession='GSE3'"
    ).fetchone()
    assert state == ("no_matrix", 0)
    assert (
        conn.execute(
            "SELECT n_failures FROM sc_stats_study WHERE accession='GSE2'"
        ).fetchone()[0]
        == 1
    )


def test_reingest_is_idempotent(tmp_path):
    results = _write(tmp_path / "results")
    db = tmp_path / "sc_stats.db"
    ingest_dirs(db, [results])
    ingest_dirs(db, [results])
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM sc_stats_matrix").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM sc_stats_study").fetchone()[0] == 3


def test_study_rollups(tmp_path):
    """Rollups cover only what pentimento lacks; cells and genes are joined, not copied."""
    results = tmp_path / "roll"
    results.mkdir()
    payload = {
        "accession": "GSE9",
        "failures": [],
        "matrices": [
            {
                "file": f"m{i}.mtx.gz",
                "n_cells": 100 * i,
                "n_barcodes": 500 * i,
                "n_genes": 20000,
                "is_raw_barcode_space": False,
                "species": "Homo sapiens",
                "species_method": "gene_symbols",
                "n_mito_genes": 13,
                "nCount_RNA": {"min": 1.0, "mean": 2.0, "median": 3.0, "max": 4.0},
                "nFeature_RNA": {"min": 1.0, "mean": 2.0, "median": 3.0, "max": 4.0},
                "pct_mito": None,
                "min_counts": 500,
                "n_cells_filtered": 10 * i,
                "nCount_RNA_filtered": {
                    "min": 1.0,
                    "mean": 2.0,
                    "median": 3.0,
                    "max": 4.0,
                },
                "nFeature_RNA_filtered": {
                    "min": 1.0,
                    "mean": 2.0,
                    "median": 3.0,
                    "max": 4.0,
                },
                "pct_mito_filtered": None,
            }
            for i in (1, 2)
        ],
    }
    (results / "GSE9.stats.json").write_text(json.dumps(payload))
    (results / "GSE9.status").write_text("ok\n")

    db = tmp_path / "roll.db"
    ingest_dirs(db, [results])
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM sc_stats_study WHERE accession='GSE9'").fetchone()
    assert row["species"] == "Homo sapiens"
    assert row["any_raw_barcode_space"] == 0
    # cells and genes are pentimento's; sc_stats keeps only the join key
    cols = set(row.keys())
    assert "ncount" not in cols
    assert "ngene" not in cols
    got = conn.execute(
        "SELECT sample_accession FROM sc_stats_matrix WHERE accession='GSE9'"
    ).fetchall()
    assert {r[0] for r in got} == {None}  # m1.mtx.gz / m2.mtx.gz carry no GSM


def test_transfer_upsert_is_non_nulling_by_default():
    """A partial sweep must not blank columns an earlier, larger pass filled."""
    from transfer.sc_stats import MATRIX_COLS, _upsert

    sql = _upsert(
        "sc_stats_matrix", MATRIX_COLS, ("accession", "file"), overwrite=False
    )
    assert "COALESCE(EXCLUDED.n_cells, sc_stats_matrix.n_cells)" in sql
    assert "ON CONFLICT (accession, file)" in sql
    # the key columns are never in the SET list
    assert "SET accession =" not in sql

    over = _upsert(
        "sc_stats_matrix", MATRIX_COLS, ("accession", "file"), overwrite=True
    )
    assert "n_cells = EXCLUDED.n_cells" in over


def test_transfer_widens_sqlite_ints_into_booleans(tmp_path):
    """is_raw_barcode_space is INTEGER in sqlite and BOOLEAN in pg."""
    from transfer.sc_stats import MATRIX_COLS, _rows

    results = _write(tmp_path / "results")
    db = tmp_path / "sc_stats.db"
    ingest_dirs(db, [results])
    flag = MATRIX_COLS.index("is_raw_barcode_space")
    conn = sqlite3.connect(db)
    vals = {r[flag] for r in _rows(conn, "sc_stats_matrix", MATRIX_COLS)}
    assert vals == {True, False}
    assert all(isinstance(v, bool) for v in vals)


def test_sample_accession_is_parsed_for_the_pentimento_join(tmp_path):
    """The GSM prefix is the join key to pentimento_sample.cells/genes."""
    from sc_stats.ingest import _sample_accession

    assert _sample_accession("GSM2671415_E16_matrix.mtx.gz") == "GSM2671415"
    assert _sample_accession("GSM123.h5ad") == "GSM123"
    # series-scope files carry no sample identity; NULL beats a guess
    assert _sample_accession("matrix.mtx") is None
    assert _sample_accession("GSE1_all_matrix.mtx.gz") is None
    assert _sample_accession(None) is None

    results = _write(tmp_path / "results")
    db = tmp_path / "sc.db"
    ingest_dirs(db, [results])
    conn = sqlite3.connect(db)
    got = dict(conn.execute("SELECT file, sample_accession FROM sc_stats_matrix"))
    assert got["GSM1_matrix.mtx.gz"] == "GSM1"
    assert got["GSM2_matrix.mtx.gz"] == "GSM2"
