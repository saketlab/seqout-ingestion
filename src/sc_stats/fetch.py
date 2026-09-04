"""
Fetch one accession's matrix files to local scratch.

The counting fleet keeps nothing: it streams headers and deletes. Summary
statistics need the values, so every study is downloaded in full and removed
once summarised.
"""

from __future__ import annotations

import gzip
import logging
import shutil
import tarfile
from pathlib import Path
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

#: suffixes that hold a countable matrix, or an archive that may contain one
MATRIX_HINTS = (".h5ad", ".h5", ".hdf5", ".mtx", ".mtx.gz")
FEATURE_HINTS = ("features.tsv", "genes.tsv", "barcodes.tsv")
ARCHIVE_HINTS = (".tar", ".tar.gz", ".tgz")

#: dense gene x cell tables; plate and combinatorial pipelines deposit these
#: where 10x deposits an mtx triple.
DENSE_SUFFIXES = (".csv", ".tsv", ".txt")

#: dense suffixes that are never a count matrix; kept short, since a wrong
#: name here drops a study while a wrong keep costs one cheap sniff.
_NOT_A_MATRIX = (
    "readme",
    "filelist",
    "metadata",
    "annotation",
    "sample_sheet",
    "samplesheet",
    "well_id",
    "barcode",
    "feature",
    "cluster",
    "umap",
    "tsne",
    "peaks.",
    "manifest",
)


def is_dense_table(name: str) -> bool:
    lower = name.lower()
    stem = lower.removesuffix(".gz")
    if not stem.endswith(DENSE_SUFFIXES):
        return False
    return not any(token in lower for token in _NOT_A_MATRIX)


_CHUNK = 1 << 20
_TIMEOUT = 300


def is_archive(name: str) -> bool:
    return name.lower().endswith(ARCHIVE_HINTS)


def is_wanted(url: str) -> bool:
    name = url.rsplit("/", 1)[-1].lower()
    if any(h in name for h in MATRIX_HINTS + FEATURE_HINTS + ARCHIVE_HINTS):
        return True
    return is_dense_table(name)


def to_https(url: str) -> str:
    # GEO records every supplementary file as ftp://, and the lab subnets
    # intercept port 21 with a proxy that answers 403
    return "https://" + url[len("ftp://") :] if url.startswith("ftp://") else url


def download(url: str, dest: Path, max_bytes: int) -> Path | None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    # resume by output existence, the way the quant sweep does: a retried study
    # must not re-pull hundreds of MB it already has
    if dest.exists() and dest.stat().st_size > 0:
        logger.info("have %s", dest.name)
        return dest
    tmp = dest.with_suffix(dest.suffix + ".partial")
    tmp.unlink(missing_ok=True)
    logger.info("fetching %s", url)
    req = Request(to_https(url), headers={"User-Agent": "seqout-sc-stats"})  # noqa: S310
    written = 0
    try:
        with urlopen(req, timeout=_TIMEOUT) as r, tmp.open("wb") as fh:  # noqa: S310
            while chunk := r.read(_CHUNK):
                written += len(chunk)
                if written > max_bytes:
                    tmp.unlink(missing_ok=True)
                    logger.warning("%s exceeds the size cap", url)
                    return None
                fh.write(chunk)
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        logger.warning("download failed %s: %s", url, exc)
        return None
    tmp.replace(dest)
    return dest


def _peel_gzip(path: Path) -> Path | None:
    """Return a copy with one gzip layer removed, or None if it was the last."""
    try:
        with gzip.open(path, "rb") as fh:
            if fh.read(2) != b"\x1f\x8b":
                return None
        out = path.with_name(path.name + ".peeled")
        with gzip.open(path, "rb") as src, out.open("wb") as dst:
            shutil.copyfileobj(src, dst)
    except (OSError, EOFError):
        return None
    return out


def unpack(path: Path, into: Path) -> list[Path] | None:
    """Matrix files the archive held, or None if it could not be opened."""
    name = path.name.lower()
    if not any(name.endswith(h) for h in ARCHIVE_HINTS):
        return []
    into.mkdir(parents=True, exist_ok=True)
    archive, peeled = path, None
    try:
        tarfile.open(archive).close()
    except (tarfile.ReadError, OSError):
        # GEO serves some archives gzipped twice, so the outer layer yields
        # another gzip stream rather than the tar
        peeled = _peel_gzip(path)
        if peeled is not None:
            archive = peeled
    try:
        with tarfile.open(archive) as tf:
            members = [
                m
                for m in tf.getmembers()
                if m.isfile()
                and (
                    any(h in m.name.lower() for h in MATRIX_HINTS + FEATURE_HINTS)
                    or is_dense_table(Path(m.name).name)
                )
            ]
            for m in members:
                # a member path is attacker-controlled; keep the basename only
                src = tf.extractfile(m)
                if src is None:
                    continue
                with src, (into / Path(m.name).name).open("wb") as fh:
                    shutil.copyfileobj(src, fh)
    except (tarfile.TarError, OSError) as exc:
        logger.warning("unpack failed %s: %s", path, exc)
        return None
    finally:
        if peeled is not None:
            peeled.unlink(missing_ok=True)
    return [
        p
        for p in into.iterdir()
        if any(h in p.name.lower() for h in MATRIX_HINTS) or is_dense_table(p.name)
    ]
