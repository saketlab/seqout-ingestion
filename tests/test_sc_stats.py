"""Statistics on a matrix whose answers are known by hand."""

from pathlib import Path

import h5py
import numpy as np
import pytest

from sc_stats.mito import detect_species, mito_mask
from sc_stats.stats import summarise_matrix

# 3 cells x 4 genes, cell-major. MT-CO1 is gene 1, MT-ND1 gene 3.
#   cell0: [10, 5, 0, 5]   total 20, detected 3, mito 10 -> 50%
#   cell1: [ 0, 1, 3, 0]   total  4, detected 2, mito  1 -> 25%
#   cell2: [ 2, 0, 0, 0]   total  2, detected 1, mito  0 -> 0%
FEATURES = ["GAPDH", "MT-CO1", "ACTB", "MT-ND1"]
DENSE = np.array([[10, 5, 0, 5], [0, 1, 3, 0], [2, 0, 0, 0]], dtype="float32")


def _write_h5ad(path: Path) -> None:
    rows, cols, vals, indptr = [], [], [], [0]
    for r in range(DENSE.shape[0]):
        for c in range(DENSE.shape[1]):
            if DENSE[r, c]:
                cols.append(c)
                vals.append(DENSE[r, c])
        indptr.append(len(vals))
        rows.append(r)
    with h5py.File(path, "w") as f:
        x = f.create_group("X")
        x.attrs["encoding-type"] = "csr_matrix"
        x.create_dataset("data", data=np.array(vals, dtype="float32"))
        x.create_dataset("indices", data=np.array(cols, dtype="int32"))
        x.create_dataset("indptr", data=np.array(indptr, dtype="int32"))
        var = f.create_group("var")
        var.create_dataset("_index", data=np.array(FEATURES, dtype="S16"))


@pytest.fixture
def h5ad(tmp_path: Path) -> Path:
    p = tmp_path / "toy.h5ad"
    _write_h5ad(p)
    return p


def test_counts_and_features(h5ad: Path) -> None:
    out = summarise_matrix(h5ad)
    assert out["n_cells"] == 3
    assert out["nCount_RNA"] == {"min": 2.0, "mean": 26 / 3, "median": 4.0, "max": 20.0}
    assert out["nFeature_RNA"]["min"] == 1.0
    assert out["nFeature_RNA"]["max"] == 3.0
    assert out["nFeature_RNA"]["median"] == 2.0


def test_pct_mito(h5ad: Path) -> None:
    out = summarise_matrix(h5ad)
    assert out["n_mito_genes"] == 2
    assert out["pct_mito"]["max"] == pytest.approx(50.0)
    assert out["pct_mito"]["min"] == pytest.approx(0.0)
    assert out["pct_mito"]["median"] == pytest.approx(25.0)


def test_empty_barcodes_are_not_cells(tmp_path: Path) -> None:
    """A zero-count barcode is an empty droplet; counting it drags every mean."""
    dense = np.vstack([DENSE, np.zeros((2, 4), dtype="float32")])
    vals, cols, indptr = [], [], [0]
    for r in range(dense.shape[0]):
        for c in range(dense.shape[1]):
            if dense[r, c]:
                cols.append(c)
                vals.append(dense[r, c])
        indptr.append(len(vals))
    p = tmp_path / "empty.h5ad"
    with h5py.File(p, "w") as f:
        x = f.create_group("X")
        x.attrs["encoding-type"] = "csr_matrix"
        x.create_dataset("data", data=np.array(vals, dtype="float32"))
        x.create_dataset("indices", data=np.array(cols, dtype="int32"))
        x.create_dataset("indptr", data=np.array(indptr, dtype="int32"))
        var = f.create_group("var")
        var.create_dataset("_index", data=np.array(FEATURES, dtype="S16"))
    out = summarise_matrix(p)
    assert out["n_barcodes"] == 5
    assert out["n_cells"] == 3


def test_mito_mask_does_not_match_nuclear_lookalikes() -> None:
    """MTOR and MTRNR2L8 are nuclear; a bare MT prefix would take both."""
    mask = mito_mask(["MT-CO1", "mt-nd1", "MTOR", "MTRNR2L8", "GAPDH", "MTATP6"])
    assert mask == [True, True, False, False, False, True]


def test_species_from_id_prefix() -> None:
    assert (
        detect_species(["ENSG00000229807.13", "ENSG00000141510"])[0] == "Homo sapiens"
    )
    assert detect_species(["ENSMUSG00000051951"])[0] == "Mus musculus"
    assert detect_species(["GAPDH", "ACTB"]) == (None, "none")


def test_raw_barcode_space_is_flagged_and_filtered_stats_separate():
    """A 10x whitelist-sized barcode axis is ambient space, not a cell set."""
    from sc_stats.stats import MIN_COUNTS, TENX_WHITELIST_SIZES, _summary

    assert 6_794_880 in TENX_WHITELIST_SIZES  # 10x v3
    assert 737_280 in TENX_WHITELIST_SIZES  # 10x v2
    # nothing clears the cutoff -> None, never a summary over an empty slice
    assert _summary(np.array([], dtype="float64")) is None
    below = np.array([1.0, 4.0, 20.0])
    assert _summary(below[below >= MIN_COUNTS]) is None


def _tar_gz(dest: Path, member: Path) -> None:
    import tarfile

    with tarfile.open(dest, "w:gz") as tf:
        tf.add(member, arcname=member.name)


def test_unpack_peels_a_doubly_gzipped_archive(tmp_path: Path) -> None:
    """GEO serves some .tar.gz gzipped twice; tarfile alone cannot open those."""
    import gzip
    import shutil

    from sc_stats.fetch import unpack

    (tmp_path / "matrix.mtx").write_text("%%MatrixMarket\n1 1 1\n1 1 5\n")
    plain = tmp_path / "study.tar.gz"
    _tar_gz(plain, tmp_path / "matrix.mtx")

    doubled = tmp_path / "doubled.tar.gz"
    with plain.open("rb") as src, gzip.open(doubled, "wb") as dst:
        shutil.copyfileobj(src, dst)

    found = unpack(doubled, tmp_path / "out")
    assert found is not None
    assert [f.name for f in found] == ["matrix.mtx"]


def test_unpack_reports_none_when_the_archive_will_not_open(tmp_path: Path) -> None:
    """None separates an unreadable archive from one holding no matrix."""
    from sc_stats.fetch import unpack

    broken = tmp_path / "broken.tar.gz"
    broken.write_bytes(b"not an archive at all")
    assert unpack(broken, tmp_path / "out") is None
