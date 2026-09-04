"""
Per-cell QC statistics for a single-cell count matrix.

Computes nCount (counts per cell), nFeature (genes detected per cell) and the
mitochondrial fraction, then reduces each to min/mean/median/max. The matrix is
read a slice at a time: these files reach tens of GB and the fleet hosts have no
scientific stack beyond h5py and numpy.

Orientation is decided per format rather than guessed. Getting it backwards
silently swaps cells and genes, and the result still looks like a plausible QC
table.
"""

from __future__ import annotations

import gzip
import logging
import re
from typing import TYPE_CHECKING, Any

import h5py
import numpy as np

from sc_stats.fetch import DENSE_SUFFIXES
from sc_stats.mito import detect_species, has_id_prefix, is_mito, mito_mask

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_CHUNK = 2_000_000  # nonzeros per slice

#: 10x barcode whitelist sizes; an axis of exactly this length is raw droplet
#: space, so its unfiltered quantiles describe ambient noise.
TENX_WHITELIST_SIZES = frozenset({737_280, 6_794_880, 6_684_672, 4_194_304})

#: counts a barcode must carry to enter the filtered summary; the unfiltered
#: summary is reported beside it, so either can be recomputed.
MIN_COUNTS = 500


def _summary(values: np.ndarray) -> dict[str, float] | None:
    if values.size == 0:
        return None
    return {
        "min": float(np.min(values)),
        "mean": float(np.mean(values)),
        "median": float(np.median(values)),
        "max": float(np.max(values)),
    }


def _accumulate_csr(
    data: h5py.Dataset,
    indices: h5py.Dataset,
    indptr: np.ndarray,
    is_mito: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Per-row totals, detected features and mito counts, one row block at a time."""
    n_rows = len(indptr) - 1
    total = np.zeros(n_rows, dtype="float64")
    detected = np.zeros(n_rows, dtype="int64")
    mito = np.zeros(n_rows, dtype="float64")

    start_row = 0
    while start_row < n_rows:
        end_row = start_row
        # grow the block until it covers roughly _CHUNK nonzeros
        while end_row < n_rows and indptr[end_row + 1] - indptr[start_row] < _CHUNK:
            end_row += 1
        end_row = max(end_row, start_row + 1)
        lo, hi = int(indptr[start_row]), int(indptr[end_row])
        if hi > lo:
            block_data = np.asarray(data[lo:hi], dtype="float64")
            block_idx = np.asarray(indices[lo:hi])
            offsets = indptr[start_row : end_row + 1] - lo
            for i in range(end_row - start_row):
                a, b = int(offsets[i]), int(offsets[i + 1])
                if b <= a:
                    continue
                vals = block_data[a:b]
                cols = block_idx[a:b]
                total[start_row + i] = vals.sum()
                detected[start_row + i] = int((vals > 0).sum())
                if is_mito.size:
                    mito[start_row + i] = vals[is_mito[cols]].sum()
        start_row = end_row
    return total, detected, mito


def _decode(values: Any) -> list[str]:
    return [v.decode() if isinstance(v, bytes) else str(v) for v in values]


def _read_h5ad(path: Path) -> dict[str, Any]:
    with h5py.File(path, "r") as f:
        if "X" not in f:
            msg = "no X matrix"
            raise ValueError(msg)
        x = f["X"]
        if not isinstance(x, h5py.Group):
            msg = "dense X is not supported"
            raise TypeError(msg)
        # h5ad is cells x genes, so CSR rows are cells
        encoding = x.attrs.get("encoding-type", "csr_matrix")
        if isinstance(encoding, bytes):
            encoding = encoding.decode()
        if encoding != "csr_matrix":
            msg = f"unsupported X encoding {encoding}"
            raise ValueError(msg)
        features = _feature_names_h5ad(f)
        is_mito = np.array(mito_mask(features), dtype=bool)
        indptr = np.asarray(x["indptr"])
        total, detected, mito = _accumulate_csr(
            x["data"], x["indices"], indptr, is_mito
        )
    return {
        "features": features,
        "n_cells": int(len(indptr) - 1),
        "total": total,
        "detected": detected,
        "mito": mito,
        "mito_genes": int(is_mito.sum()),
    }


def _feature_names_h5ad(f: h5py.File) -> list[str]:
    var = f.get("var")
    if var is None:
        return []
    for key in ("gene_symbols", "feature_name", "_index", "gene_ids"):
        if key in var:
            return _decode(var[key][:])
    index_attr = var.attrs.get("_index")
    if isinstance(index_attr, bytes):
        index_attr = index_attr.decode()
    if index_attr and index_attr in var:
        return _decode(var[index_attr][:])
    return []


def _read_10x_h5(path: Path) -> dict[str, Any]:
    with h5py.File(path, "r") as f:
        root = "matrix" if "matrix" in f else next(iter(f.keys()))
        g = f[root]
        shape = np.asarray(g["shape"])  # (genes, cells)
        n_genes, n_cells = int(shape[0]), int(shape[1])
        features = []
        if "features" in g and "name" in g["features"]:
            features = _decode(g["features"]["name"][:])
        elif "gene_names" in g:
            features = _decode(g["gene_names"][:])
        is_mito = np.array(mito_mask(features), dtype=bool)
        # 10x h5 is CSC over genes x cells, so each indptr span is one cell
        indptr = np.asarray(g["indptr"])
        total, detected, mito = _accumulate_csr(
            g["data"], g["indices"], indptr, is_mito
        )
    return {
        "features": features,
        "n_cells": n_cells,
        "n_genes": n_genes,
        "total": total,
        "detected": detected,
        "mito": mito,
        "mito_genes": int(is_mito.sum()),
    }


def _open_maybe_gz(path: Path):  # noqa: ANN202
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return path.open()


def _read_mtx(path: Path, features: list[str]) -> dict[str, Any]:
    """
    Matrix Market, streamed.

    The header gives rows x cols; 10x writes genes x cells. Which axis is a cell
    is decided by matching the feature count, not assumed.
    """
    is_mito = np.array(mito_mask(features), dtype=bool)
    with _open_maybe_gz(path) as fh:
        line = fh.readline()
        while line.startswith("%"):
            line = fh.readline()
        rows, cols, _nnz = (int(v) for v in line.split())
        genes_are_rows = len(features) == rows if features else rows < cols
        n_cells = cols if genes_are_rows else rows
        total = np.zeros(n_cells, dtype="float64")
        detected = np.zeros(n_cells, dtype="int64")
        mito = np.zeros(n_cells, dtype="float64")
        for line in fh:
            if not line or line[0] == "%":
                continue
            parts = line.split()
            if len(parts) < 3:  # noqa: PLR2004
                continue
            r, c, v = int(parts[0]) - 1, int(parts[1]) - 1, float(parts[2])
            cell = c if genes_are_rows else r
            gene = r if genes_are_rows else c
            total[cell] += v
            if v > 0:
                detected[cell] += 1
            if is_mito.size and gene < is_mito.size and is_mito[gene]:
                mito[cell] += v
    return {
        "features": features,
        "n_cells": n_cells,
        "total": total,
        "detected": detected,
        "mito": mito,
        "mito_genes": int(is_mito.sum()),
    }


_DENSE_DELIMS = ("\t", ",", ";", " ")

#: below this an axis is a handful of columns, not a count matrix
_MIN_DENSE_AXIS = 20

#: values parsed before the file costs more than the statistic is worth
_MAX_DENSE_VALUES = 300_000_000


def _clean(label: str) -> str:
    return label.strip().strip('"').strip("'")


def _is_number(value: str) -> bool:
    try:
        float(_clean(value))
    except ValueError:
        return False
    return True


def _spread(labels: list[str], n: int = 5000) -> list[str]:
    """
    Up to n labels spread across the axis; the head alone is not a sample.

    Gene lists arrive sorted, which puts every mt- symbol past a head window.
    """
    step = max(1, len(labels) // n)
    return [_clean(v) for v in labels[::step][:n]]


#: a cell barcode is never a gene; ID_PREFIX's Arabidopsis AT[1-5MC]G matches
#: any barcode beginning ATCG, which scores enough to flip the orientation.
_PURE_DNA_RX = re.compile(r"^[ACGTN]{12,}(?:[-_]\d+)?$", re.IGNORECASE)


def _gene_score(sample: list[str]) -> int:
    """Count labels that are recognisably genes: a stable ID prefix or MT symbol."""
    return sum(
        1
        for f in sample
        if not _PURE_DNA_RX.match(f) and (is_mito(f) or has_id_prefix(f))
    )


_BARCODE_RX = re.compile(r"[ACGTN]{12,}", re.IGNORECASE)

#: a pipeline that renames cells still marks the axis; sc_enrich/deep's
#: _cell_index_type recognises the same shape. "-1" is 10x's lane suffix.
_RENAMED_CELL_RX = re.compile(r"(?:cell[\s_.\-]?\d+|.*-\d+)", re.IGNORECASE)

#: share of an axis that must look like cells before it decides the orientation
_CELL_AXIS_SHARE = 0.8


def _cell_share(sample: list[str]) -> float:
    """Share of labels that are a barcode or a bare index, never a gene."""
    if not sample:
        return 0.0
    hits = sum(
        1
        for v in sample
        if v.isdigit() or _BARCODE_RX.search(v) or _RENAMED_CELL_RX.fullmatch(v)
    )
    return hits / len(sample)


#: corner-cell labels that name the row axis outright, e.g. a header starting
#: "Gene\t..." or BD Rhapsody's "Cell_Index,...". Checked before any heuristic.
_CORNER_GENE = (
    "gene",
    "genes",
    "geneid",
    "gene_id",
    "gene_name",
    "genename",
    "symbol",
    "gene_symbol",
    "feature",
    "features",
    "feature_id",
    "ensembl",
    "ensembl_id",
    "transcript",
    "probe",
)
_CORNER_CELL = (
    "cell",
    "cells",
    "cellid",
    "cell_id",
    "cell_index",
    "barcode",
    "barcodes",
    "well",
    "well_id",
    "sample",
    "sample_id",
    "spot",
)


#: copied from sc_enrich/deep/_process.CELL_MAJOR_HINTS, not imported: that
#: module needs httpx, absent on the fleet, and would stop every worker.
_CELL_MAJOR_HINTS = (
    "cell_by_gene",
    "cellxgene",
    "cell_x_gene",
    "cells_x_genes",
    "cellsxgenes",
    "cell_by_feature",
)


def _is_cell_major(name: str) -> bool:
    low = name.lower()
    return any(h in low for h in _CELL_MAJOR_HINTS)


def _corner_says(corner: str | None) -> bool | None:
    """Whether the corner cell names the rows as genes (True) or cells (False)."""
    if not corner:
        return None
    key = _clean(corner).lower().lstrip("#").strip()
    if key in _CORNER_GENE:
        return True
    if key in _CORNER_CELL:
        return False
    return None


def _dense_orientation(
    rows: list[str], cols: list[str], name: str, corner: str | None
) -> bool:
    """Say whether the rows are genes; raise when neither axis is recognisable."""
    stated = _corner_says(corner)
    if stated is not None:
        return stated
    # the counting fleet already trusts these filenames to mean cells-as-rows
    if _is_cell_major(name):
        return False
    row_sample, col_sample = _spread(rows), _spread(cols)
    row_score, col_score = _gene_score(row_sample), _gene_score(col_sample)
    if row_score != col_score:
        return row_score > col_score
    # a targeted panel carries neither stable IDs nor an MT gene, so fall back on
    # the axis that cannot be genes: barcodes and bare cell indices
    row_cells, col_cells = _cell_share(row_sample), _cell_share(col_sample)
    if row_cells >= _CELL_AXIS_SHARE > col_cells:
        return False
    if col_cells >= _CELL_AXIS_SHARE > row_cells:
        return True
    msg = f"neither axis of {name} is recognisably genes"
    raise ValueError(msg)


def _dense_header(
    fh: Any, path_name: str
) -> tuple[str, list[str], list[str], int, str | None]:
    """Delimiter, column labels, first body row, label columns, corner cell."""
    header = fh.readline()
    # pipeline preambles (BD Rhapsody writes seven ## lines) sit above the header
    while header.startswith("#"):
        header = fh.readline()
    if not header:
        msg = "empty file"
        raise ValueError(msg)
    delim = max(_DENSE_DELIMS, key=header.count)
    head = [_clean(v) for v in header.rstrip("\n\r").split(delim)]

    first = fh.readline()
    if not first:
        msg = "no data rows"
        raise ValueError(msg)
    body = first.rstrip("\n\r").split(delim)

    # the corner cell is optional: with one the header matches the body width,
    # without it the header is short by exactly the row-label column
    n_label_cols = len(body) - len(head)
    corner = None
    if n_label_cols == 0:
        corner, head, n_label_cols = head[0], head[1:], 1
    if n_label_cols < 1:
        msg = f"{path_name}: header wider than its rows"
        raise ValueError(msg)
    # gene_id and gene_name ahead of the counts is common enough to cost a check
    while n_label_cols < len(body) and not _is_number(body[n_label_cols]):
        head = head[1:]
        n_label_cols += 1
    if len(head) < _MIN_DENSE_AXIS and len(body) - n_label_cols < _MIN_DENSE_AXIS:
        msg = f"{path_name}: too few columns for a count matrix"
        raise ValueError(msg)
    return delim, head, body, n_label_cols, corner


def _read_dense(path: Path) -> dict[str, Any]:
    """
    Read a dense gene x cell table in one pass, accumulating both axes.

    Plate and combinatorial pipelines deposit these where 10x deposits an mtx
    triple, so the format does not fix the orientation the way h5ad's does. It is
    read off the labels afterwards -- whichever axis is recognisably genes is the
    gene axis -- and an undecidable file raises rather than guessing, because a
    transposed summary still looks like a plausible QC table.
    """
    with _open_maybe_gz(path) as fh:
        delim, col_labels, body, n_label_cols, corner = _dense_header(fh, path.name)
        n_cols = len(col_labels)
        col_is_mito = np.array(mito_mask(col_labels), dtype=bool)
        # index once: a boolean mask rescans every column for the ~13-37 that hit
        col_mito_idx = np.flatnonzero(col_is_mito)
        col_total = np.zeros(n_cols, dtype="float64")
        col_detected = np.zeros(n_cols, dtype="int64")
        col_mito = np.zeros(n_cols, dtype="float64")
        row_labels: list[str] = []
        row_is_mito: list[bool] = []
        row_total: list[float] = []
        row_detected: list[int] = []
        row_mito: list[float] = []

        n_values = 0
        for parts in _dense_rows(body, fh, delim, n_cols + n_label_cols):
            values = np.array(parts[n_label_cols:], dtype="float64")
            n_values += n_cols
            if n_values > _MAX_DENSE_VALUES:
                msg = f"{path.name}: larger than the sweep will parse"
                raise ValueError(msg)
            label = _clean(parts[0])
            nonzero = values > 0
            col_total += values
            col_detected += nonzero
            row_labels.append(label)
            row_total.append(float(values.sum()))
            row_detected.append(int(nonzero.sum()))
            row_mito.append(float(values[col_mito_idx].sum()))
            label_is_mito = is_mito(label)
            row_is_mito.append(label_is_mito)
            if label_is_mito:
                col_mito += values

    if _dense_orientation(row_labels, col_labels, path.name, corner):
        features = row_labels
        row_mask = np.array(row_is_mito, dtype=bool)
        total, detected, mito = col_total, col_detected, col_mito
    else:
        features, row_mask = col_labels, col_is_mito
        total = np.array(row_total, dtype="float64")
        detected = np.array(row_detected, dtype="int64")
        mito = np.array(row_mito, dtype="float64")
    return {
        "features": features,
        "n_cells": int(total.size),
        "total": total,
        "detected": detected,
        "mito": mito,
        "mito_genes": int(row_mask.sum()),
    }


def _dense_rows(first: list[str], fh, delim: str, width: int):  # noqa: ANN001, ANN202
    """Body rows of the expected width; a ragged line is dropped, not fatal."""
    if len(first) == width:
        yield first
    for raw in fh:
        parts = raw.rstrip("\n\r").split(delim)
        if len(parts) == width:
            yield parts


def summarise_matrix(path: Path, features_file: Path | None = None) -> dict[str, Any]:
    """Summary statistics for one matrix, or a row explaining why there are none."""
    name = path.name.lower()
    if ".h5ad" in name:
        raw = _read_h5ad(path)
    elif name.endswith((".h5", ".hdf5")):
        raw = _read_10x_h5(path)
    elif name.removesuffix(".gz").endswith(DENSE_SUFFIXES):
        # dispatch on the suffix alone; is_dense_table's denylist gates the
        # download, and reusing it here would reject a badly named local matrix
        raw = _read_dense(path)
    elif ".mtx" in name:
        features: list[str] = []
        if features_file and features_file.exists():
            with _open_maybe_gz(features_file) as fh:
                for line in fh:
                    parts = line.rstrip("\n").split("\t")
                    features.append(parts[1] if len(parts) > 1 else parts[0])
        raw = _read_mtx(path, features)
    else:
        msg = f"unsupported matrix format: {path.name}"
        raise ValueError(msg)

    total = raw["total"]
    keep = total > 0  # a zero-count barcode is empty droplet, not a cell
    strict = total >= MIN_COUNTS
    species, method = detect_species(raw["features"])
    detected = raw["detected"].astype("float64")

    pct = None
    if raw["mito_genes"] > 0:
        with np.errstate(divide="ignore", invalid="ignore"):
            pct = np.where(total > 0, raw["mito"] / np.maximum(total, 1) * 100.0, 0.0)

    return {
        "file": path.name,
        "n_cells": int(keep.sum()),
        "n_barcodes": int(raw["n_cells"]),
        # a raw-space matrix's unfiltered quantiles describe ambient droplets;
        # read the _filtered fields instead
        "is_raw_barcode_space": int(raw["n_cells"]) in TENX_WHITELIST_SIZES,
        "species": species,
        "species_method": method,
        # total feature-axis length, not genes per cell; an antibody matrix runs
        # two orders shorter, which nFeature_RNA cannot distinguish
        "n_features": len(raw["features"]),
        "n_mito_genes": raw["mito_genes"],
        "nCount_RNA": _summary(total[keep]),
        "nFeature_RNA": _summary(detected[keep]),
        "pct_mito": _summary(pct[keep]) if pct is not None else None,
        "min_counts": MIN_COUNTS,
        "n_cells_filtered": int(strict.sum()),
        "nCount_RNA_filtered": _summary(total[strict]),
        "nFeature_RNA_filtered": _summary(detected[strict]),
        "pct_mito_filtered": _summary(pct[strict]) if pct is not None else None,
    }
