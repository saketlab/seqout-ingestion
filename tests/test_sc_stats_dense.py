"""A dense table is summarised the same either way up, or not at all."""

import gzip

import pytest

from sc_stats.fetch import is_dense_table, is_wanted
from sc_stats.stats import summarise_matrix

GENES = ["ENSG00000001", "ENSG00000002", "MT-CO1", *[f"ENSG{i:08d}" for i in range(30)]]
CELLS = [f"AAACCTGAGAAACCA{i:02d}-1" for i in range(25)]

# cell j holds j+1 counts of every gene, so nCount is len(GENES) * (j + 1) and
# every gene is detected
COUNTS = [[j + 1 for j in range(len(CELLS))] for _ in GENES]


def _write(path, rows, row_labels, col_labels, sep=",", *, corner=True):
    head = ([""] if corner else []) + col_labels
    lines = [sep.join(head)]
    lines += [
        sep.join([label, *(str(v) for v in row)])
        for label, row in zip(row_labels, rows, strict=True)
    ]
    path.write_text("\n".join(lines) + "\n")
    return path


def _transposed(rows):
    return [list(col) for col in zip(*rows, strict=True)]


def test_genes_as_rows(tmp_path):
    f = _write(tmp_path / "expr_matrix.csv", COUNTS, GENES, CELLS)
    out = summarise_matrix(f)
    assert out["n_cells"] == len(CELLS)
    assert out["nFeature_RNA"]["max"] == len(GENES)
    assert out["nCount_RNA"]["max"] == len(GENES) * len(CELLS)
    assert out["n_mito_genes"] == 1


def test_cells_as_rows_gives_the_same_summary(tmp_path):
    a = summarise_matrix(_write(tmp_path / "a.csv", COUNTS, GENES, CELLS))
    b = summarise_matrix(_write(tmp_path / "b.csv", _transposed(COUNTS), CELLS, GENES))
    for key in ("n_cells", "n_mito_genes"):
        assert a[key] == b[key]
    for key in ("nCount_RNA", "nFeature_RNA", "pct_mito"):
        assert a[key] == b[key]


def test_tab_separated_and_gzipped(tmp_path):
    plain = _write(tmp_path / "counts.tsv", COUNTS, GENES, CELLS, "\t")
    gz = tmp_path / "counts.tsv.gz"
    with gzip.open(gz, "wt") as fh:
        fh.write(plain.read_text())
    assert summarise_matrix(gz) == {**summarise_matrix(plain), "file": gz.name}


def test_header_without_a_corner_cell(tmp_path):
    f = _write(tmp_path / "nocorner.csv", COUNTS, GENES, CELLS, corner=False)
    assert summarise_matrix(f)["n_cells"] == len(CELLS)


def test_a_gene_name_column_ahead_of_the_counts(tmp_path):
    rows = [[f"sym{i}", *row] for i, row in enumerate(COUNTS)]
    f = _write(tmp_path / "twolabel.csv", rows, GENES, CELLS)
    assert summarise_matrix(f)["n_cells"] == len(CELLS)


def test_a_pipeline_preamble_above_the_header_is_skipped(tmp_path):
    f = tmp_path / "rhapsody.csv"
    body = _write(tmp_path / "plain.csv", COUNTS, GENES, CELLS).read_text()
    f.write_text("####\n## BD Rhapsody Targeted Analysis Pipeline\n####\n" + body)
    assert summarise_matrix(f)["n_cells"] == len(CELLS)


def test_a_targeted_panel_orients_off_the_cell_axis(tmp_path):
    # symbols only: no stable IDs and no MT gene, so neither axis scores as genes
    panel = [f"CCR{i}" for i in range(len(GENES))]
    index = [str(900000 + i) for i in range(len(CELLS))]
    f = _write(tmp_path / "panel.csv", _transposed(COUNTS), index, panel)
    out = summarise_matrix(f)
    assert out["n_cells"] == len(index)
    assert out["nFeature_RNA"]["max"] == len(panel)


def test_a_gene_corner_cell_decides_the_orientation(tmp_path):
    # sorted symbols with every mt- gene past the sample window, and cell names
    # that are neither barcodes nor indices: only the corner cell "Gene" decides
    symbols = sorted([f"{i:07d}Rik" for i in range(6000)] + ["mt-Nd1", "mt-Co1"])
    wells = [f"SpSC0_A1_sc.{i}" for i in range(30)]
    f = tmp_path / "umi.tsv"
    f.write_text(
        "\t".join(["Gene", *wells])
        + "\n"
        + "\n".join("\t".join([g, *(["1"] * len(wells))]) for g in symbols)
        + "\n"
    )
    out = summarise_matrix(f)
    assert out["n_cells"] == len(wells)
    assert out["nFeature_RNA"]["max"] == len(symbols)


def test_a_cell_corner_cell_decides_the_other_way(tmp_path):
    f = _write(tmp_path / "mols.csv", _transposed(COUNTS), CELLS, GENES)
    head, rest = f.read_text().split("\n", 1)
    f.write_text("Cell_Index" + head + "\n" + rest)
    assert summarise_matrix(f)["n_cells"] == len(CELLS)


def test_a_barcode_axis_never_scores_as_genes(tmp_path):
    # ID_PREFIX's Arabidopsis AT[1-5MC]G matches any barcode starting ATCG, so
    # without a guard a barcode axis outscores an antibody axis and the matrix
    # is read transposed (GSM3596096: 52 antibodies x 6,500 cells)
    antibodies = ["B220 (CD45R)", "CD11b", "CCR7", *[f"CD{i}" for i in range(40)]]
    # real 16bp DNA, every one starting ATCG so every one is a false gene hit
    alphabet = "ACGT"
    barcodes = [
        "ATCG" + "".join(alphabet[(i >> (2 * k)) & 3] for k in range(12))
        for i in range(30)
    ]
    rows = [[j + 1 for j in range(len(barcodes))] for _ in antibodies]
    f = _write(tmp_path / "ctrl-ADT-count.csv", rows, antibodies, barcodes)
    out = summarise_matrix(f)
    assert out["n_cells"] == len(barcodes)
    assert out["n_features"] == len(antibodies)


def test_an_unrecognisable_axis_raises_rather_than_transposing(tmp_path):
    anon = [f"row{i}" for i in range(len(GENES))]
    vague = [f"col{i}" for i in range(len(CELLS))]
    f = _write(tmp_path / "anon.csv", COUNTS, anon, vague)
    with pytest.raises(ValueError, match="recognisably genes"):
        summarise_matrix(f)


def test_a_narrow_table_is_not_a_count_matrix(tmp_path):
    f = _write(tmp_path / "small.csv", [[1, 2]] * 3, GENES[:3], CELLS[:2])
    with pytest.raises(ValueError, match="too few columns"):
        summarise_matrix(f)


@pytest.mark.parametrize(
    ("name", "wanted"),
    [
        ("GSM1_expr_matrix.csv.gz", True),
        ("GSM1_processed_data.txt.gz", True),
        ("GSE1_annotated_sample_sheet.csv.gz", False),
        ("filelist.txt", False),
        ("GSM1_cell_metadata.csv", False),
        ("barcodes.tsv.gz", False),
        ("GSE1_reannotation.gtf.gz", False),
    ],
)
def test_which_supplementary_names_are_taken_as_dense(name, wanted):
    assert is_dense_table(name) is wanted


def test_a_dense_table_is_wanted_by_the_fetcher():
    assert is_wanted("ftp://ftp.ncbi.nlm.nih.gov/x/GSM1_expr_matrix.csv.gz")
