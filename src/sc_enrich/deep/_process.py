import csv
import functools
import gzip
import logging
import shutil
import tarfile
import tempfile
import zlib
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import h5py

# Import the two halves of the 'rds' extra independently. pyreadr reads most
# .rds files unaided; rpy2 handles S4/Seurat objects through system R.
try:
    import pyreadr
    from pyreadr.custom_errors import LibrdataError
except ImportError:
    _PYREADR_HINT = (
        "reading .rds files needs pyreadr. install with: uv pip install pyreadr"
    )

    class LibrdataError(Exception):  # placeholder so except LibrdataError resolves
        pass

    class _MissingPyreadr:
        def __getattr__(self, _name: str) -> object:
            raise ImportError(_PYREADR_HINT)

    pyreadr = _MissingPyreadr()  # type: ignore[assignment]

try:  # fallback for S4/Seurat objects; needs system R
    import rpy2.robjects as ro
except ImportError:
    _R_HINT = (
        "this .rds needs the rpy2 fallback, which requires a system R install. "
        "install with: uv sync --extra rds"
    )

    class _MissingR:  # any attribute access raises the friendly hint
        def __getattr__(self, _name: str) -> object:
            raise ImportError(_R_HINT)

    ro = _MissingR()  # type: ignore[assignment]

from helpers import ftp_data_stream, open_http_range
from sc_enrich.deep.counter_common import FileType, RdsReadError

logger = logging.getLogger(__name__)

# last rpy2-read .rds class; one worker per process, so a module holder is safe
_rds_state: dict[str, str] = {"class": ""}

_rds_genes: dict[str, int] = {"genes": -1}

# nnz of the last .mtx counted; -1 means unknown for raw/unfiltered checks.
_mtx_nnz: dict[str, int] = {"nnz": -1}


def last_rds_class() -> str:
    return _rds_state["class"]


def last_mtx_nnz() -> int:
    return _mtx_nnz["nnz"]


def last_rds_genes() -> int:
    return _rds_genes["genes"]


MIN_DISK_SPACE_BYTES = 5 * 1024**3
POSSIBLE_MISNOMER_FILE_TYPES = [FileType.Csv, FileType.Tsv, FileType.Txt]

FILE_TYPE_PRIORITY = [
    FileType.Mtx,
    FileType.Barcodes,
    FileType.H5,
    FileType.H5ad,
    FileType.Tsv,
    FileType.Csv,
    FileType.Txt,
    FileType.SpaceDelimited,
    FileType.SemicolonDelimited,
    FileType.Tar,
    FileType.Rds,
]

HEADER_STREAMABLE = frozenset(
    {
        FileType.Mtx,
        FileType.Barcodes,
        FileType.Txt,
        FileType.Tsv,
        FileType.Csv,
        FileType.SpaceDelimited,
        FileType.SemicolonDelimited,
    },
)

RELIABLE_TYPES = frozenset(
    {FileType.Mtx, FileType.Barcodes, FileType.H5, FileType.H5ad, FileType.Rds},
)

# cell-by-gene matrices have cells as rows, so these are kept despite the "gene"
# in the name and counted by row
CELL_MAJOR_HINTS = (
    "cell_by_gene",
    "cellxgene",
    "cell_x_gene",
    "cells_x_genes",
    "cellsxgenes",
    "cell_by_feature",
)

SKIP_SUBSTRINGS = (
    "fragments.tsv",
    ".bam",
    ".fastq",
    ".fq.gz",
    "_denovo_",
    ".sra",
    ".bigwig",
    ".bw",
    ".bedgraph",
)


def _classify_file(path: str) -> FileType:  # noqa: PLR0911
    ignore_words = [
        "barcode",
        "feature",
        "gene",
        "readme",
        "annotation",
        "metadata",
        "md5sum",
    ]

    def _is_type(ext: str) -> bool:
        if ext not in path:
            return False
        if is_cell_major(path):
            return True
        return not _contains_any(path, ignore_words)

    if _contains_any(path, list(SKIP_SUBSTRINGS)):
        return FileType.Unknown
    if "barcodes.tsv" in path:
        return FileType.Barcodes
    if ".h5ad" in path:
        return FileType.H5ad
    if _contains_any(path, [".h5", ".hdf5"]):
        return FileType.H5
    if "matrix.mtx" in path or _contains_any(path, [".mtx", ".mtx.gz"]):
        return FileType.Mtx
    if _is_type(".txt"):
        return FileType.Txt
    if _is_type(".csv"):
        return FileType.Csv
    if _is_type(".tsv"):
        return FileType.Tsv
    if ".rds" in path:
        return FileType.Rds
    if _contains_any(path, [".tar", ".tgz"]):
        return FileType.Tar
    return FileType.Unknown


def _contains_any(s: str, substrings: list[str]) -> bool:
    return any(sub in s for sub in substrings)


def is_cell_major(name: str) -> bool:
    return _contains_any(name.lower(), list(CELL_MAJOR_HINTS))


def _is_gzipped(path: Path) -> bool:
    gzip_magic = b"\x1f\x8b"
    try:
        with path.open("rb") as f:
            return f.read(2) == gzip_magic
    except OSError:
        return False


def _resolve_file_type(
    path: Path,
    file_type: FileType,
    chunk_size: int,
) -> tuple[FileType, Path] | None:
    if path.suffix == ".gz":
        if not _is_gzipped(path):
            stripped = path.with_suffix("")
            if not stripped.exists():
                path.rename(stripped)
            return _resolve_file_type(stripped, file_type, chunk_size)

        tmp_path = path.with_suffix("")
        if not tmp_path.exists():
            with gzip.open(path, "rb") as f_in, tmp_path.open("wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        path = tmp_path

    if file_type not in POSSIBLE_MISNOMER_FILE_TYPES:
        return file_type, path

    resolved_type, resolved_path = _file_type_sniffer(path, chunk_size)
    if resolved_type == FileType.Unknown:
        return None

    return resolved_type, resolved_path


def _file_type_sniffer(path: Path, chunk_size: int) -> tuple[FileType, Path]:  # noqa: PLR0911
    try:
        with path.open() as f:
            sample = f.read(chunk_size)
    except UnicodeDecodeError as e:
        logger.debug("failed to read %s: %s", path, e)
        return FileType.Unknown, path

    try:
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.reader(sample.splitlines(), dialect)
        row = next(reader)
    except csv.Error:
        row = sample.splitlines()[0].split("\t")
        dialect = None

    row = [r for r in row if r.strip()]

    if dialect is None:
        return FileType.Unknown, path
    if len(row) == 1:
        return FileType.Txt, path
    if dialect.delimiter == "\t":
        return FileType.Tsv, path
    if dialect.delimiter == ",":
        return FileType.Csv, path
    if dialect.delimiter == " ":
        return FileType.SpaceDelimited, path
    if dialect.delimiter == ";":
        return FileType.SemicolonDelimited, path
    return FileType.Txt, path


def safe_process(
    func: Callable[[Path, int], int],
) -> Callable[[Path, int], int]:
    @functools.wraps(func)
    def wrapper(path: Path, chunk_size: int, *args: Any, **kwargs: Any) -> int:
        try:
            return func(path, chunk_size, *args, **kwargs)
        except RdsReadError:
            raise  # let the caller log which accession was unreadable
        except Exception:
            logger.exception("failed to process %s", path)
            return -1

    return wrapper


def _get_sc_count(  # noqa: PLR0911
    dest_path: Path,
    file_type: FileType,
    chunk_size: int,
    tmp_dir: Path | None = None,
) -> int:
    if file_type == FileType.Barcodes:
        return _process_barcodes(dest_path, chunk_size)
    if file_type == FileType.H5:
        return _process_h5(dest_path, chunk_size)
    if file_type == FileType.H5ad:
        return _process_h5ad(dest_path, chunk_size)
    if file_type == FileType.Tar:
        return _process_tar(dest_path, chunk_size, tmp_dir=tmp_dir)
    if file_type == FileType.Txt:
        return _process_txt(dest_path, chunk_size)
    if file_type == FileType.Mtx:
        return _process_mtx(dest_path, chunk_size)
    if file_type == FileType.Tsv:
        return _process_delimiter_file_types("\t")(dest_path, chunk_size)
    if file_type == FileType.Csv:
        return _process_delimiter_file_types(",")(dest_path, chunk_size)
    if file_type == FileType.SpaceDelimited:
        return _process_delimiter_file_types(" ")(dest_path, chunk_size)
    if file_type == FileType.SemicolonDelimited:
        return _process_delimiter_file_types(";")(dest_path, chunk_size)
    if file_type == FileType.Rds:
        return _process_rds(dest_path, chunk_size)

    return 0


def _process_tar(path: Path, chunk_size: int, tmp_dir: Path | None = None) -> int:
    try:
        with tarfile.open(path, "r:*") as tar:
            max_count = 0
            extract_dir = Path(tempfile.mkdtemp(dir=tmp_dir))

            for member in tar.getmembers():
                name = member.name.lower()
                file_type = _classify_file(name)
                if file_type == FileType.Unknown:
                    continue

                f = tar.extractfile(member)
                if f is None:
                    continue

                member_path = Path(member.name)
                suffix = "".join(member_path.suffixes) or ".tmp"
                with tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=suffix,
                    dir=extract_dir,
                ) as tmp:
                    shutil.copyfileobj(f, tmp)
                    tmp_path = Path(tmp.name)

                result = _resolve_file_type(tmp_path, file_type, chunk_size)
                if result is None:
                    Path(tmp_path).unlink(missing_ok=True)
                    decompressed = tmp_path.with_suffix("")
                    if decompressed != tmp_path:
                        decompressed.unlink(missing_ok=True)
                    continue

                file_type, sniffed_path = result
                single_cell_count = _get_sc_count(
                    sniffed_path,
                    file_type,
                    chunk_size,
                    tmp_dir=extract_dir,
                )
                Path(tmp_path).unlink(missing_ok=True)

                if sniffed_path != tmp_path:
                    sniffed_path.unlink(missing_ok=True)
                max_count = max(max_count, single_cell_count)

            shutil.rmtree(extract_dir, ignore_errors=True)

    except Exception:
        logger.exception("failed to process %s", path)
        return -1
    else:
        return max_count


def _member_head(f: Any, name: str, max_bytes: int) -> bytes:
    """Decompressed head of a tar member, gunzipping a .gz member on the fly."""
    if name.lower().endswith(".gz"):
        dec = zlib.decompressobj(zlib.MAX_WBITS | 16)
        out = bytearray()
        while len(out) < max_bytes:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            out += dec.decompress(chunk)
        return bytes(out[:max_bytes])
    return f.read(max_bytes)


def count_tar_via_stream(
    url: str,
    chunk_size: int,
    *,
    timeout: int,
    socks_proxy: str | None,
    tmp_dir: Path | None = None,
) -> int | None:
    """Stream a tar, stopping at the first reliable member; None = full download."""
    ftp_url = url.replace("https://", "ftp://", 1)
    extract_dir = Path(tempfile.mkdtemp(dir=tmp_dir))
    max_count = 0
    try:
        with (
            ftp_data_stream(
                ftp_url, timeout=timeout, socks_proxy=socks_proxy
            ) as stream,
            tarfile.open(fileobj=stream, mode="r|*") as tar,
        ):
            for member in tar:
                if not member.isfile():
                    continue
                file_type = _classify_file(member.name.lower())
                if file_type == FileType.Unknown:
                    continue
                f = tar.extractfile(member)
                if f is None:
                    continue
                name = Path(member.name).name.removesuffix(".gz") or "member"
                tmp_path = extract_dir / name
                tmp_path.write_bytes(_member_head(f, member.name, chunk_size))
                result = _resolve_file_type(tmp_path, file_type, chunk_size)
                if result is None:
                    tmp_path.unlink(missing_ok=True)
                    continue
                resolved, sniffed = result
                count = _get_sc_count(
                    sniffed, resolved, chunk_size, tmp_dir=extract_dir
                )
                if sniffed != tmp_path:
                    sniffed.unlink(missing_ok=True)
                tmp_path.unlink(missing_ok=True)
                max_count = max(max_count, count)
                if count > 0 and resolved in RELIABLE_TYPES:
                    break
    except Exception:
        logger.debug("tar stream failed for %s, full download", url)
        return None
    else:
        return max_count
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)


def count_tar_members_via_stream(
    url: str,
    chunk_size: int,
    *,
    timeout: int,
    socks_proxy: str | None,
    max_members: int = 4000,
    tmp_dir: Path | None = None,
) -> list[tuple[str, int, int, str, int]] | None:
    """
    Count every matrix member of a streamed tar.

    Returns [(name, cells, genes, format, nnz)] for per-GSM RAW tars. None means
    stream failure.
    """
    ftp_url = url.replace("https://", "ftp://", 1)
    extract_dir = Path(tempfile.mkdtemp(dir=tmp_dir))
    out: list[tuple[str, int, int, str, int]] = []
    seen = 0
    try:
        with (
            ftp_data_stream(
                ftp_url, timeout=timeout, socks_proxy=socks_proxy
            ) as stream,
            tarfile.open(fileobj=stream, mode="r|*") as tar,
        ):
            for member in tar:
                if seen >= max_members:
                    break
                if not member.isfile():
                    continue
                file_type = _classify_file(member.name.lower())
                if file_type == FileType.Unknown:
                    continue
                seen += 1
                f = tar.extractfile(member)
                if f is None:
                    continue
                name = Path(member.name).name.removesuffix(".gz") or "member"
                tmp_path = extract_dir / name
                tmp_path.write_bytes(_member_head(f, member.name, chunk_size))
                result = _resolve_file_type(tmp_path, file_type, chunk_size)
                if result is None:
                    tmp_path.unlink(missing_ok=True)
                    continue
                resolved, sniffed = result
                count = _get_sc_count(
                    sniffed, resolved, chunk_size, tmp_dir=extract_dir
                )
                genes = (
                    gene_count_for(sniffed, resolved, chunk_size) if count > 0 else -1
                )
                nnz = last_mtx_nnz() if resolved == FileType.Mtx else -1
                if sniffed != tmp_path:
                    sniffed.unlink(missing_ok=True)
                tmp_path.unlink(missing_ok=True)
                if count > 0:
                    out.append((member.name, count, genes, resolved.value, nnz))
    except Exception:
        logger.debug("tar member stream failed for %s", url)
        return None
    else:
        return out
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)


@safe_process
def _process_barcodes(path: Path, chunk_size: int) -> int:
    with path.open("rb") as f:
        chunk = f.read(chunk_size)
    n = len(chunk.splitlines())
    return n if n > 1 else 0


def _h5_count(f: Any) -> int:
    n_cells: int | None = None

    def find_barcodes(name: str, obj: Any) -> None:
        nonlocal n_cells
        if n_cells is not None:
            return
        if obj is not None and name.rsplit("/", 1)[-1] == "barcodes":
            n_cells = len(obj)

    f.visititems(find_barcodes)
    return n_cells if n_cells is not None and n_cells > 1 else 0


def _h5ad_count(f: Any) -> int:
    n_cells: int | None = None
    if "obs/_index" in f:
        n_cells = len(f["obs/_index"])
    elif "X" in f:
        x = f["X"]
        if hasattr(x, "shape"):
            n_cells = x.shape[0]
        elif "shape" in x:
            n_cells = x["shape"][0]
    return n_cells if n_cells is not None and n_cells > 1 else 0


@safe_process
def _process_h5(path: Path, _: int) -> int:
    with h5py.File(path, "r") as f:
        return _h5_count(f)


@safe_process
def _process_h5ad(path: Path, _: int) -> int:
    with h5py.File(path, "r") as f:
        return _h5ad_count(f)


def count_h5_via_http(url: str, file_type: FileType) -> int | None:
    """Cell count of an .h5/.h5ad over HTTP range reads; None = full download."""
    reader = open_http_range(url.replace("ftp://", "https://", 1))
    if reader is None:
        return None
    try:
        with h5py.File(reader, "r") as f:
            return _h5_count(f) if file_type == FileType.H5 else _h5ad_count(f)
    except (OSError, ValueError, KeyError):
        return None
    finally:
        reader.close()


# feature/gene axis datasets in a 10x-style .h5 (CellRanger v2 "genes", v3
# "features") plus h5ad-in-h5 "var" indexes; matched by suffix on the h5 path
_H5_GENE_SUFFIXES = (
    "/features/id",
    "/features/name",
    "/genes",
    "/gene_names",
    "/var_names",
    "/var/_index",
)


_H5_GENE_LEAVES = ("genes", "gene_names", "var_names")


def _h5_genes(f: Any) -> int:
    """Gene-axis length of a 10x-style .h5; -1 means unknown."""
    n_genes: int | None = None

    def find_features(name: str, obj: Any) -> None:
        nonlocal n_genes
        if n_genes is not None or obj is None:
            return
        low = name.lower()
        leaf = low.rsplit("/", 1)[-1]
        if (low.endswith(_H5_GENE_SUFFIXES) or leaf in _H5_GENE_LEAVES) and hasattr(
            obj, "__len__"
        ):
            try:
                n_genes = len(obj)
            except TypeError:
                n_genes = None

    f.visititems(find_features)
    return n_genes if n_genes is not None and n_genes > 1 else -1


def _h5ad_genes(f: Any) -> int:
    """Gene-axis length of an .h5ad; -1 means unknown."""
    if "var/_index" in f:
        n = len(f["var/_index"])
        return n if n > 1 else -1
    if "X" in f:
        x = f["X"]
        if hasattr(x, "shape") and len(x.shape) == 2:  # noqa: PLR2004
            n = x.shape[1]
            return n if n > 1 else -1
        if "shape" in x:
            n = x["shape"][1]
            return n if n > 1 else -1
    return -1


def count_h5_dims_via_http(url: str, file_type: FileType) -> tuple[int, int] | None:
    """(cells, genes) of an .h5/.h5ad over HTTP range reads; None to fall back."""
    reader = open_http_range(url.replace("ftp://", "https://", 1))
    if reader is None:
        return None
    try:
        with h5py.File(reader, "r") as f:
            if file_type == FileType.H5ad:
                return _h5ad_count(f), _h5ad_genes(f)
            return _h5_count(f), _h5_genes(f)
    except (OSError, ValueError, KeyError):
        return None
    finally:
        reader.close()


def _mtx_genes(path: Path, chunk_size: int) -> int:
    """Gene axis (rows) from an .mtx header (-1 if unparseable)."""
    try:
        with path.open("rb") as f:
            header = f.read(chunk_size)
    except OSError:
        return -1
    _min_parts = 2
    for line in header.splitlines():
        if line.startswith(b"%"):
            continue
        parts = line.split()
        if len(parts) >= _min_parts:
            try:
                n_genes = int(parts[0])
            except (ValueError, IndexError):
                return -1
            return n_genes if n_genes > 1 else -1
    return -1


def gene_count_for(path: Path, file_type: FileType, chunk_size: int) -> int:
    """
    Genes for an already-sniffed file where cheaply derivable, else -1.

    File types lacking a gene axis reuse the local file from the cell-count pass.
    """
    try:
        if file_type == FileType.Mtx:
            return _mtx_genes(path, chunk_size)
        if file_type == FileType.H5:
            with h5py.File(path, "r") as f:
                return _h5_genes(f)
        if file_type == FileType.H5ad:
            with h5py.File(path, "r") as f:
                return _h5ad_genes(f)
        if file_type == FileType.Rds:
            return last_rds_genes()
    except Exception:
        logger.debug("gene count failed for %s", path)
        return -1
    return -1


@safe_process
def _process_txt(path: Path, chunk_size: int) -> int:
    with path.open("rb") as f:
        chunk = f.read(chunk_size)
    lines = chunk.strip().splitlines()
    _min_sc_lines = 2
    return len(lines) if len(lines) > _min_sc_lines else 0


@safe_process
def _process_mtx(path: Path, chunk_size: int) -> int:
    _mtx_nnz["nnz"] = -1
    with path.open("rb") as f:
        header = f.read(chunk_size)

    _dims_parts = 3
    _min_parts = 2
    for line in header.splitlines():
        if line.startswith(b"%"):
            continue
        parts = line.split()
        if len(parts) >= _min_parts:
            try:
                n_cells = int(parts[1])
                if len(parts) >= _dims_parts:
                    _mtx_nnz["nnz"] = int(parts[2])
            except (ValueError, IndexError):
                return 0
            else:
                return n_cells if n_cells > 1 else 0

    return 0


def _count_data_rows(path: Path) -> int:
    with path.open("rb") as f:
        n = sum(1 for _ in f)
    return n - 1 if n - 1 > 1 else 0


def _process_delimiter_file_types(delimiter: str) -> Callable[[Path, int], int]:
    @safe_process
    def _wrapper(path: Path, chunk_size: int) -> int:
        if is_cell_major(path.name):
            return _count_data_rows(path)

        with path.open("rb") as f:
            chunk = f.read(chunk_size)

        first_newline = chunk.find(b"\n")
        header = (
            chunk[:first_newline].decode().split(delimiter)
            if first_newline != -1
            else chunk.decode().split(delimiter)
        )
        _min_sc_cols = 3
        n_cols = len(header)
        return n_cols if n_cols > _min_sc_cols else 0

    return _wrapper


@safe_process
def _process_rds(path: Path, _: int) -> int:
    # Reset both holders so stale class or gene counts stay isolated by link.
    _rds_state["class"] = ""
    _rds_genes["genes"] = -1
    try:
        result = pyreadr.read_r(path)
    except LibrdataError:
        return _process_rds_rpy2(path)

    if not result:
        logger.error("no objects found in rds %s", path)
        return 0

    rds_df = next(iter(result.values()))
    _expected_ndim = 2
    if not hasattr(rds_df, "shape") or len(rds_df.shape) != _expected_ndim:
        logger.error("unexpected object type in rds %s: %s", path, type(rds_df))
        return 0

    n_genes = rds_df.shape[0]
    _rds_genes["genes"] = n_genes if n_genes > 1 else -1
    n_cells = rds_df.shape[1]
    return n_cells if n_cells > 1 else 0


def _process_rds_rpy2(path: Path) -> int:
    _rds_state["class"] = ""
    try:
        ro.r(f'obj <- readRDS("{path}")')
    except Exception as exc:
        # S4 class is often undefined, e.g. monocle3 missing for a cell_data_set
        raise RdsReadError(str(path), f"readRDS failed: {exc}") from exc

    try:
        _rds_state["class"] = str(cast("Any", ro.r("class(obj)[1]"))[0])
    except Exception:
        _rds_state["class"] = ""

    try:
        vec = cast(
            "Any",
            ro.r("""
            n_cells <- 0

            if (inherits(obj, "Seurat")) {
                counts <- tryCatch(
                    Seurat::GetAssayData(obj, layer = "counts"),
                    error = function(e) tryCatch(
                        Seurat::GetAssayData(obj, slot = "counts"),
                        error = function(e2) NULL
                    )
                )
                if (!is.null(counts)) {
                    n_cells <- ncol(counts)
                }
            } else if (inherits(obj, "SeuratObject")) {
                counts <- tryCatch(
                    SeuratObject::GetAssayData(obj, layer = "counts"),
                    error = function(e) NULL
                )
                if (!is.null(counts)) {
                    n_cells <- ncol(counts)
                }
            } else if (inherits(obj, "cell_data_set")) {
                n_cells <- ncol(obj)
            } else if (inherits(obj, "SingleCellExperiment")) {
                counts <- tryCatch(
                    SingleCellExperiment::counts(obj),
                    error = function(e) NULL
                )
                if (!is.null(counts)) {
                    n_cells <- ncol(counts)
                }
            } else if (inherits(obj, "ExpressionSet")) {
                exprs_mat <- tryCatch(
                    Biobase::exprs(obj),
                    error = function(e) NULL
                )
                if (!is.null(exprs_mat)) {
                    n_cells <- ncol(exprs_mat)
                }
            } else if (inherits(obj, "liger")) {
                raw_data <- tryCatch(
                    obj@raw.data,
                    error = function(e) NULL
                )
                if (!is.null(raw_data)) {
                    n_cells <- sum(sapply(raw_data, ncol))
                }
            } else if (inherits(obj, "Pagoda2")) {
                n_cells <- nrow(obj$counts)
            } else if (is.data.frame(obj) || is.matrix(obj)) {
                n_cells <- ncol(obj)
            } else {
                n_cells <- tryCatch(ncol(obj), error = function(e) 0)
                if (is.null(n_cells) || is.na(n_cells)) n_cells <- 0
            }
            n_cells
        """),
        )
        n_cells = int(vec[0])

    except Exception:
        logger.exception("rpy2 fallback failed for %s", path)
        return 0
    else:
        return n_cells if n_cells > 1 else 0


def _check_disk_space(path: Path, min_bytes: int = MIN_DISK_SPACE_BYTES) -> bool:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return False
    else:
        return usage.free >= min_bytes
