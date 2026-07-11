import csv
import functools
import gzip
import logging
import shutil
import tarfile
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import h5py
import pyreadr
import rpy2.robjects as ro
from pyreadr.custom_errors import LibrdataError

from sc_enrich.counter.counter_common import FileType, RdsReadError

logger = logging.getLogger(__name__)

_rds_state: dict[str, str] = {"class": ""}


def last_rds_class() -> str:
    """R class of the most recently rpy2-read .rds object ("" if none/plain)."""
    return _rds_state["class"]


MIN_DISK_SPACE_BYTES = 5 * 1024**3
POSSIBLE_MISNOMER_FILE_TYPES = [FileType.Csv, FileType.Tsv, FileType.Txt]

FILE_TYPE_PRIORITY = [
    FileType.Barcodes,
    FileType.H5,
    FileType.H5ad,
    FileType.Tar,
    FileType.Txt,
    FileType.Mtx,
    FileType.Tsv,
    FileType.Csv,
    FileType.SpaceDelimited,
    FileType.SemicolonDelimited,
    FileType.Rds,
]


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
        return ext in path and not _contains_any(path, ignore_words)

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


@safe_process
def _process_barcodes(path: Path, chunk_size: int) -> int:
    with path.open("rb") as f:
        chunk = f.read(chunk_size)
    idx = chunk.find(b"\n")
    if idx == -1 or len(chunk) <= idx + 1:
        return 0
    return len(chunk[idx + 1 :].splitlines())


@safe_process
def _process_h5(path: Path, _: int) -> int:
    n_cells = None

    with h5py.File(path, "r") as f:

        def find_barcodes(name: str, obj: Any) -> None:
            nonlocal n_cells
            if n_cells is not None:
                return
            if obj is not None and name.endswith("/barcodes"):
                n_cells = len(obj)

        f.visititems(find_barcodes)

    return n_cells if n_cells is not None and n_cells > 1 else 0


@safe_process
def _process_h5ad(path: Path, _: int) -> int:
    n_cells = None

    with h5py.File(path, "r") as f:
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
def _process_txt(path: Path, chunk_size: int) -> int:
    with path.open("rb") as f:
        chunk = f.read(chunk_size)
    lines = chunk.strip().splitlines()
    _min_sc_lines = 2
    return len(lines) if len(lines) > _min_sc_lines else 0


@safe_process
def _process_mtx(path: Path, chunk_size: int) -> int:
    with path.open("rb") as f:
        header = f.read(chunk_size)

    _min_parts = 2
    for line in header.splitlines():
        if line.startswith(b"%"):
            continue
        parts = line.split()
        if len(parts) >= _min_parts:
            try:
                n_cells = int(parts[1])
            except (ValueError, IndexError):
                return 0
            else:
                return n_cells if n_cells > 1 else 0

    return 0


def _process_delimiter_file_types(delimiter: str) -> Callable[[Path, int], int]:
    @safe_process
    def _wrapper(path: Path, chunk_size: int) -> int:
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

    n_cells = rds_df.shape[1]
    return n_cells if n_cells > 1 else 0


def _process_rds_rpy2(path: Path) -> int:
    _rds_state["class"] = ""
    try:
        ro.r(f'obj <- readRDS("{path}")')
    except Exception as exc:
        # S4 class often undefined (e.g. monocle3 missing for a cell_data_set)
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
