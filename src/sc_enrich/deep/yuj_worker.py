#!/usr/bin/env python3
"""
Yuj work item: count cells for one single-cell accession.

Reads download links from yuj_setup/accessions/<accession>.json, sniffs the
supplementary files, writes results/<accession>.json, and stays idempotent.
"""

import functools
import json
import logging
import os
import re
import shutil
import signal
import socket
import sys
import tarfile
from pathlib import Path
from typing import Any

from helpers import (
    FileTooLargeError,
    download_file,
    download_file_ftp,
    download_ftp_head,
    download_http_head,
    ftp_data_stream,
)
from sc_enrich.deep._process import (
    FILE_TYPE_PRIORITY,
    HEADER_STREAMABLE,
    _check_disk_space,
    _get_sc_count,
    _member_head,
    _resolve_file_type,
    count_h5_dims_via_http,
    count_tar_members_via_stream,
    count_tar_via_stream,
    gene_count_for,
    is_cell_major,
    last_mtx_nnz,
    last_rds_class,
)
from sc_enrich.deep.counter_common import DownloadLink, FileType, RdsReadError

logger = logging.getLogger(__name__)

_FAILURES_LOG = "rds_read_failures.jsonl"


def _record_rds_failure(
    output_dir: Path, accession: str, url: str, reason: str
) -> None:
    # one worker per host, so the append is race-free; per-host logs merge on yuj pull
    rec = {
        "host": socket.gethostname(),
        "accession": accession,
        "url": url,
        "reason": reason,
    }
    with (output_dir / _FAILURES_LOG).open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


_CHUNK_SIZE = 10 * 1024 * 1024
_RETRIES = 5
_TIMEOUT = 30
_MAX_TIMEOUT = 300

# bounds only the whole-file path
_MAX_FILE_BYTES = 30 * 1024**3

# mtx count is on the first line
_MTX_HEAD_BYTES = 1 * 1024 * 1024

# per-barcode annotation tables; detected by filename, confirmed by header row only
_META_HEAD_BYTES = 256 * 1024
_MAX_META_COLS = 40
_META_NAME_HINTS = (
    "metadata",
    "meta_data",
    "meta-data",
    "_meta.",
    "cell_meta",
    "cellmeta",
    "annotation",
    "annot",
    "celltype",
    "cell_type",
    "cell.type",
    "cell_label",
    "phenotype",
    "pdata",
    "obs.csv",
    "obs.tsv",
)
# matrix/index files that are never per-cell metadata
_META_EXCLUDE = (
    ".mtx",
    "barcodes.tsv",
    "features.tsv",
    "genes.tsv",
    ".h5",
    ".hdf5",
)
_META_EXTS = (".csv", ".tsv", ".txt")
# rds/h5ad embed per-cell metadata internally; assume metadata present
_EMBEDDED_METADATA_FORMATS = ("rds", "h5ad")

# raw/unfiltered 10x matrices report the whole barcode whitelist as columns, not
# real cells; flag (not drop) via exact whitelist sizes or nnz/cells far too low
_WHITELIST_SIZES = frozenset(
    {737280, 6794880, 4526976, 3571200, 6390720, 1000000, 900000},
)
_MIN_NNZ_PER_CELL = 20
# streaming a tar has no random access, so bound how much we scan for metadata
_MAX_TARS_SCANNED = 2
_MAX_TAR_META_MEMBERS = 50
_MAX_TAR_MEMBERS_WALKED = 4000
_BARCODE_COL_HINTS = (
    "barcode",
    "cell_id",
    "cellid",
    "cell.id",
    "cell_name",
    "cellname",
    "cell_barcode",
)
# named biological annotations only; numeric clusters (ident/leiden/louvain) have
# their own category and would wrongly inflate has_celltype
_CELLTYPE_COL_HINTS = (
    "celltype",
    "cell_type",
    "cell.type",
    "cell_class",
    "annotation",
    "annot",
    "subtype",
    "cell_subtype",
    "cell_label",
    "majortype",
    "major_type",
    "minortype",
    "minor_type",
    "lineage",
    "population",
    "cell_ontology",
    "cellontology",
)

# annotation categories matched against column names (token-aware, see _match_field);
# demographic ones (has_demographics rollup) come first
_DEMOGRAPHIC_CATEGORIES = ("sex", "age", "race_ancestry", "bmi", "smoking")
_META_FIELD_HINTS: dict[str, tuple[str, ...]] = {
    "subject": (
        "subject",
        "individual",
        "patient",
        "participant",
        "donor",
        "orig.ident",
        "mouse",
        "animal",
    ),
    "sample_id": (
        "sample",
        "sample_id",
        "sampleid",
        "biosample",
        "specimen",
        "aliquot",
        "gsm",
    ),
    "celltype": _CELLTYPE_COL_HINTS,
    "cluster": (
        "cluster",
        "seurat_clusters",
        "leiden",
        "louvain",
        "ident",
        "subcluster",
        "resolution",
    ),
    "sex": ("sex", "gender"),
    "age": ("age",),
    "race_ancestry": ("race", "ancestry", "ethnicity", "ethnic", "population"),
    "bmi": ("bmi", "body_mass_index", "obese", "obesity"),
    "smoking": ("smoking", "smoker", "smoke", "tobacco", "pack_years", "packyears"),
    "tissue": (
        "tissue",
        "organ",
        "region",
        "anatomical",
        "site",
        "compartment",
        "source_name",
        "biopsy",
        "isolation_source",
        "organism_part",
    ),
    "disease": ("disease", "diagnosis", "phenotype", "pathology", "lesion"),
    "disease_stage": (
        "stage",
        "grade",
        "severity",
        "tnm",
        "metastatic",
        "metastasis",
        "remission",
        "relapse",
        "progression",
    ),
    "condition": (
        "condition",
        "group",
        "status",
        "case",
        "control",
        "healthy",
        "normal",
        "disease_status",
        "case_control",
    ),
    "treatment": (
        "treatment",
        "treated",
        "untreated",
        "drug",
        "therapy",
        "intervention",
        "exposure",
        "stim",
        "stimulation",
        "infection",
        "infected",
        "dose",
        "compound",
        "vehicle",
        "perturbation",
    ),
    "timepoint": (
        "timepoint",
        "time_point",
        "time",
        "day",
        "week",
        "month",
        "hour",
        "baseline",
        "followup",
        "follow_up",
        "visit",
        "longitudinal",
    ),
    "genotype": (
        "genotype",
        "mutation",
        "variant",
        "allele",
        "strain",
        "knockout",
        "wildtype",
        "transgenic",
        "ko",
        "wt",
        "tg",
        "carrier",
    ),
    "medication": (
        "medication",
        "steroid",
        "immunosuppression",
        "chemo",
        "antibiotic",
        "biologic",
    ),
    "comorbidity": (
        "comorbidity",
        "comorbid",
        "diabetes",
        "hypertension",
        "asthma",
        "copd",
        "autoimmune",
    ),
    "outcome": (
        "response",
        "responder",
        "nonresponder",
        "outcome",
        "survival",
        "pfs",
        "os",
        "recurrence",
        "mortality",
    ),
    "pregnancy": (
        "pregnancy",
        "pregnant",
        "trimester",
        "gestational",
        "gestation",
        "parity",
        "menopause",
    ),
    "assay_modality": (
        "assay",
        "modality",
        "atac",
        "adt",
        "cite",
        "vdj",
        "tcr",
        "bcr",
        "multiome",
        "spatial",
    ),
    "technology": (
        "platform",
        "technology",
        "chemistry",
        "kit",
        "chromium",
        "smartseq",
        "smart_seq",
        "dropseq",
        "drop_seq",
        "indrop",
    ),
    "batch": (
        "batch",
        "library",
        "lane",
        "flowcell",
        "run",
        "pool",
        "hashtag",
        "hto",
        "multiplex",
        "demux",
    ),
    "qc": (
        "ncount",
        "n_count",
        "nfeature",
        "n_feature",
        "percent_mt",
        "pct_mt",
        "mito",
        "doublet",
        "scrublet",
        "umis",
    ),
}

_SIDECAR_DIR = Path("yuj_setup/accessions")
_OUT_DIR = Path("results")

# fallback when no per-host socks_proxy file
_DEFAULT_SOCKS = "10.195.102.16:1080"

# hard per-accession wall-clock; download paths have no idle-stall timeout, so on
# fire we record a 0 (item marked done, run converges); override via SC_ITEM_DEADLINE
_PER_ITEM_DEADLINE = int(os.environ.get("SC_ITEM_DEADLINE", "1800"))


class _StallTimeoutError(BaseException):
    """BaseException so the broad `except Exception` paths can't swallow the alarm."""


def _on_alarm(signum: int, frame: object) -> None:  # noqa: ARG001
    raise _StallTimeoutError


def _read_proxy() -> str | None:
    pf = Path("socks_proxy")
    if not pf.exists():
        return _DEFAULT_SOCKS
    val = pf.read_text().strip()
    return None if not val or val.lower() == "direct" else val


def main() -> None:
    accession = sys.argv[1]
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUT_DIR / f"{accession}.json"
    if out_path.exists():
        return

    meta = json.loads((_SIDECAR_DIR / f"{accession}.json").read_text())

    signal.signal(signal.SIGALRM, _on_alarm)
    signal.alarm(_PER_ITEM_DEADLINE)
    try:
        result = process_accession(
            accession=meta["accession"],
            source=meta.get("source", ""),
            aliases=meta.get("aliases", ""),
            links=meta.get("links", []),
            output_dir=_OUT_DIR,
            chunk_size=_CHUNK_SIZE,
            retries=_RETRIES,
            timeout=_TIMEOUT,
            max_timeout=_MAX_TIMEOUT,
        )
    except _StallTimeoutError:
        logger.warning("stall-timeout (%ss) on %s", _PER_ITEM_DEADLINE, accession)
        result = {
            "accession": accession,
            "source": meta.get("source", ""),
            "aliases": meta.get("aliases", ""),
            "single_cell_count": 0,
            "single_cell_file_format": "",
            "stall_timeout": True,
        }
    except Exception:
        logger.exception("failed accession %s", accession)
        result = {"accession": accession, "error": True}
    finally:
        signal.alarm(0)

    # write via temp file so a crash mid-write can't leave a half-written output
    # that yuj's resume would treat as completed
    tmp = out_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(result, indent=2))
    tmp.replace(out_path)


def process_accession(
    accession: str,
    source: str,
    aliases: str,
    links: list[tuple[str, str]],
    output_dir: Path,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> dict[str, Any]:
    dl_links = [DownloadLink(type=FileType(t), url=u) for t, u in links]
    dl_links.sort(key=lambda x: FILE_TYPE_PRIORITY.index(x.type))

    acc_dir = output_dir / accession
    acc_dir.mkdir(parents=True, exist_ok=True)

    full_download = functools.partial(
        download_file_ftp,
        chunk_size=chunk_size,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
        with_pbar=False,
        socks_proxy=_read_proxy(),
        max_bytes=_MAX_FILE_BYTES,
    )

    def count_link(link: DownloadLink) -> tuple[int, int, str, str]:  # noqa: PLR0911
        # fetch and count one link -> (n_cells, n_genes, format, rds_class);
        # n_genes is -1 when not cheaply derivable (barcodes/txt/tsv/csv/tar)
        # EBI serves supplementary files over HTTPS only; ftp:// there times out
        use_http = "ebi.ac.uk" in link.url
        ftp_url = link.url.replace("https://", "ftp://", 1)

        # range-read the h5 shape (cells + genes), no full download
        if link.type in {FileType.H5, FileType.H5ad}:
            dims = count_h5_dims_via_http(link.url, link.type)
            if dims is not None:
                n, g = dims
                return (n, g, link.type.value, "") if n > 0 else (0, -1, "", "")

        # stream tar members over ftp; EBI tars fall through to a full http download
        if link.type == FileType.Tar and not use_http:
            n = count_tar_via_stream(
                link.url, chunk_size, timeout=timeout, socks_proxy=_read_proxy()
            )
            if n is not None:
                return (n, -1, link.type.value, "") if n > 0 else (0, -1, "", "")

        dest_path = acc_dir / link.extract_filename()
        try:
            if link.type in HEADER_STREAMABLE and not is_cell_major(link.url):
                # counted from the first chunk, so fetch only that
                head = _MTX_HEAD_BYTES if link.type == FileType.Mtx else chunk_size
                if use_http:
                    dest_path = download_http_head(
                        link.url,
                        dest_path,
                        max_decompressed=head,
                        num_retries=retries,
                        timeout=timeout,
                        max_wait=max_timeout,
                    )
                else:
                    dest_path = download_ftp_head(
                        ftp_url,
                        dest_path,
                        max_decompressed=head,
                        num_retries=retries,
                        timeout=timeout,
                        max_wait=max_timeout,
                        socks_proxy=_read_proxy(),
                    )
            else:
                if not _check_disk_space(output_dir):
                    logger.warning("skipping %s: insufficient disk space", link.url)
                    return (0, -1, "", "")
                if use_http:
                    dest_path = download_file(
                        link.url,
                        dest_path,
                        chunk_size=chunk_size,
                        num_workers=1,
                        num_retries=retries,
                        timeout=timeout,
                        max_wait=max_timeout,
                        with_pbar=False,
                        validate_content_length=False,
                    )
                else:
                    dest_path = full_download(url=ftp_url, dest_path=dest_path)
        except FileTooLargeError as exc:
            logger.info("skipping oversized file: %s", exc)
            return (0, -1, "", "")
        except Exception:
            logger.exception("failed to download %s", link.url)
            return (0, -1, "", "")

        try:
            result = _resolve_file_type(dest_path, link.type, chunk_size)
            if result is None:
                logger.debug("unable to find file type for %s", dest_path)
                return (0, -1, "", "")
            file_type, sniffed_path = result
            n = _get_sc_count(sniffed_path, file_type, chunk_size)
            g = gene_count_for(sniffed_path, file_type, chunk_size) if n > 0 else -1
            if sniffed_path != dest_path:
                sniffed_path.unlink(missing_ok=True)
            rds = last_rds_class() if file_type == FileType.Rds else ""
            return (n, g, file_type.value, rds) if n > 0 else (0, -1, "", "")  # noqa: TRY300
        except RdsReadError as exc:
            _record_rds_failure(output_dir, accession, link.url, exc.reason)
            return (0, -1, "", "")
        except Exception:
            logger.exception("failed to process %s", link.url)
            return (0, -1, "", "")
        finally:
            dest_path.unlink(missing_ok=True)
            dest_path.with_suffix("").unlink(missing_ok=True)

    # group links by sample (GSM in the URL, else study accession); no early-exit so
    # every sample is captured, keeping the best cell and best gene count per sample
    samples: dict[str, dict[str, Any]] = {}

    def _merge(
        key: str,
        n_cells: int,
        n_genes: int,
        fmt: str,
        rds_class: str,
        nnz: int,
        src: str,
    ) -> None:
        if n_cells <= 0:
            return
        s = samples.setdefault(
            key,
            {
                "sample": key,
                "cells": 0,
                "genes": -1,
                "nnz": -1,
                "cells_format": "",
                "genes_format": "",
                "rds_class": "",
                "unfiltered": False,
            },
        )
        if n_cells > s["cells"]:
            s["cells"] = n_cells
            s["cells_format"] = fmt
            # unfiltered describes the reported count, so it tracks the cells-winner
            s["unfiltered"] = _is_unfiltered(n_cells, nnz, src)
        if n_genes > s["genes"]:
            # genes only come from a real expression matrix, so its format is the
            # authoritative one to report for the sample
            s["genes"] = n_genes
            s["genes_format"] = fmt
        # nnz is a matrix property, independent of which file won the cell count
        if nnz > 0:
            s["nnz"] = nnz
        if rds_class:
            s["rds_class"] = rds_class

    for link in dl_links:
        # a per-GSM tar holds one matrix per sample; stream member-by-member so each
        # attributes to its own GSM instead of collapsing to a study max
        if link.type == FileType.Tar and "ebi.ac.uk" not in link.url:
            members = count_tar_members_via_stream(
                link.url, chunk_size, timeout=timeout, socks_proxy=_read_proxy()
            )
            if members is not None:
                for mname, m_cells, m_genes, m_fmt, m_nnz in members:
                    # GSM may be in the member name (multi-sample tar) or only in the
                    # tar filename (per-GSM 10x triplet tar)
                    key = _sample_key(mname, accession)
                    if key == accession:
                        key = _sample_key(link.url, accession)
                    _merge(key, m_cells, m_genes, m_fmt, "", m_nnz, mname)
                continue
            # stream failed; fall back to the scalar count_link path below
        n_cells, n_genes, fmt, rds_class = count_link(link)
        nnz = last_mtx_nnz() if fmt == FileType.Mtx.value else -1
        _merge(
            _sample_key(link.url, accession),
            n_cells,
            n_genes,
            fmt,
            rds_class,
            nnz,
            link.url,
        )

    # metadata scan is best-effort: if it trips the deadline, keep the counts rather
    # than letting main() discard the whole item as a stall
    try:
        metadata = _scan_metadata(
            links,
            accession,
            acc_dir,
            retries=retries,
            timeout=timeout,
            max_timeout=max_timeout,
        )
    except _StallTimeoutError:
        logger.warning("metadata scan hit deadline on %s; counts kept", accession)
        metadata = {}

    shutil.rmtree(acc_dir, ignore_errors=True)

    return _build_result(accession, source, aliases, samples, metadata)


def _is_unfiltered(cells: int, nnz: int, src: str) -> bool:
    # matrix looks raw/unfiltered: whitelist cols, not cells
    if cells in _WHITELIST_SIZES:
        return True
    if nnz > 0 and cells > 0 and nnz < cells * _MIN_NNZ_PER_CELL:
        return True
    low = src.rsplit("/", 1)[-1].lower()
    return "raw" in low and "filtered" not in low and cells > 500_000  # noqa: PLR2004


def _sample_key(url: str, accession: str) -> str:
    # GSM id from the basename (not whole URL): GEO FTP paths carry a GSMxxxxnnn dir
    # stub whose literal `nnn` would truncate a full-URL match to a colliding id
    m = re.search(r"GSM\d+", url.rsplit("/", 1)[-1])
    if m:
        return m.group(0)
    # fall back to an all-digit GSM dir segment (the GSMxxxxnnn stub has letters so
    # \d+ rejects it) for basenames like .../GSM1234567/suppl/matrix.mtx.gz
    for seg in url.split("/"):
        if re.fullmatch(r"GSM\d+", seg):
            return seg
    return accession


def _is_metadata_link(url: str) -> bool:
    # filename looks like a per-cell metadata table
    name = url.rsplit("/", 1)[-1].lower().removesuffix(".gz")
    if any(x in name for x in _META_EXCLUDE):
        return False
    if not name.endswith(_META_EXTS):
        return False
    return any(h in name for h in _META_NAME_HINTS)


def _parse_header(line: str, name: str) -> list[str]:
    # split a header row into column names, guessing the delimiter
    line = line.rstrip("\r\n")
    low = name.lower().removesuffix(".gz")
    if low.endswith(".csv"):
        delim = ","
    elif low.endswith(".tsv"):
        delim = "\t"
    else:
        # .txt is not reliably tab-delimited (R write.table defaults to space); pick
        # the most frequent
        delim = max(("\t", ",", ";", " "), key=line.count)
    return [c.strip().strip('"').strip("'") for c in line.split(delim)]


def _match_field(col: str, hints: tuple[str, ...]) -> bool:
    # token-aware hint match: exact token (`age` in `age_years`, not `percentage`)
    # plus a substring fallback for distinctive multi-part hints (`orig.ident`)
    low = col.lower()
    toks = set(re.split(r"[^a-z0-9]+", low))
    for h in hints:
        if h in toks:
            return True
        if ("_" in h or "." in h or len(h) >= 8) and h in low:  # noqa: PLR2004
            return True
    return False


def _detect_field_map(cols: list[str]) -> dict[str, list[str]]:
    # map each annotation category -> the exact columns matched; keeping source
    # columns (not just presence) matters: a `condition` from `disease_status` vs
    # `treatment` is analytically different downstream
    out: dict[str, list[str]] = {}
    for cat, hints in _META_FIELD_HINTS.items():
        matched = [c for c in cols if _match_field(c, hints)]
        if matched:
            out[cat] = matched
    return out


def _cell_index_type(value: str) -> str:
    # classify a per-cell row-index value: barcode (ACGTN run), renamed (cell1/Cell_2),
    # or id (anything else)
    v = value.strip().strip('"').strip("'")
    if re.search(r"[ACGTNacgtn]{10,}", v):
        return "barcode"
    if re.fullmatch(r"(?i)cell[\s_.\-]?\d+", v):
        return "renamed"
    return "id"


def _header_info(first: str, second: str, name: str) -> dict[str, Any]:
    # build a metadata info dict from a file's first two lines
    cols = _parse_header(first, name)
    low = [c.lower() for c in cols]
    row = _parse_header(second, name) if second.strip() else []
    named_barcode = any(h in c for c in low for h in _BARCODE_COL_HINTS)
    # unnamed row index shows up two ways: an explicit empty header cell, or the R
    # write.table default (no rowname header) where the data row is one field wider
    has_unnamed_index = bool(row) and (
        (bool(cols) and cols[0] == "") or len(row) > len(cols)
    )
    cell_index = _cell_index_type(row[0]) if has_unnamed_index else None
    fmap = _detect_field_map(cols)
    fields = sorted(fmap)
    has_barcode = named_barcode or cell_index == "barcode"
    return {
        "file": name,
        "columns": cols[:_MAX_META_COLS],
        "n_columns": len(cols),
        "has_barcode": has_barcode,
        "cell_index": cell_index,
        "has_celltype": "celltype" in fmap,
        "has_donor": "subject" in fmap,
        "has_demographics": any(c in fmap for c in _DEMOGRAPHIC_CATEGORIES),
        "fields": fields,
        "category_to_columns": {k: v[:6] for k, v in fmap.items()},
        # a per-cell table has a cell key and at least one annotation field; filters out
        # sample-level description tables that merely share a metadata-ish name
        "per_cell": (has_barcode or cell_index is not None) and bool(fields),
    }


def _probe_metadata(
    url: str,
    acc_dir: Path,
    *,
    retries: int,
    timeout: int,
    max_timeout: int,
    proxy: str | None,
) -> dict[str, Any] | None:
    # head-read a metadata file and report its columns / annotation fields
    name = url.rsplit("/", 1)[-1]
    dest = acc_dir / f"meta_{name}"
    use_http = "ebi.ac.uk" in url
    try:
        if use_http:
            path = download_http_head(
                url,
                dest,
                max_decompressed=_META_HEAD_BYTES,
                num_retries=retries,
                timeout=timeout,
                max_wait=max_timeout,
            )
        else:
            path = download_ftp_head(
                url.replace("https://", "ftp://", 1),
                dest,
                max_decompressed=_META_HEAD_BYTES,
                num_retries=retries,
                timeout=timeout,
                max_wait=max_timeout,
                socks_proxy=proxy,
            )
        with path.open("rb") as f:
            first = f.readline(1 << 20).decode("utf-8", "replace")
            second = f.readline(1 << 20).decode("utf-8", "replace")
    except Exception:
        logger.debug("metadata probe failed for %s", url)
        return None
    finally:
        dest.unlink(missing_ok=True)
        dest.with_suffix("").unlink(missing_ok=True)

    info = _header_info(first, second, name)
    info["url"] = url
    return info


def _scan_tar_metadata(
    tar_url: str,
    accession: str,
    *,
    timeout: int,
    proxy: str | None,
) -> dict[str, dict[str, Any]]:
    # find per-cell metadata members inside a streamed tar, keyed by GSM/study;
    # records tar url + member path so the file stays retrievable (tar -xOf), bounded
    # by the per-item deadline and a member cap since a tar has no random access
    ftp_url = tar_url.replace("https://", "ftp://", 1)
    by_key: dict[str, dict[str, Any]] = {}
    probed = 0
    seen = 0
    try:
        with (
            ftp_data_stream(ftp_url, timeout=timeout, socks_proxy=proxy) as stream,
            tarfile.open(fileobj=stream, mode="r|*") as tar,
        ):
            for member in tar:
                # cap files probed and members walked, so a huge archive with no
                # candidates can't stream unbounded
                if probed >= _MAX_TAR_META_MEMBERS or seen >= _MAX_TAR_MEMBERS_WALKED:
                    break
                seen += 1
                if not member.isfile() or not _is_metadata_link(member.name):
                    continue
                f = tar.extractfile(member)
                if f is None:
                    continue
                head = _member_head(f, member.name, _META_HEAD_BYTES)
                text = head.decode("utf-8", "replace")
                nl = text.find("\n")
                first, second = (text, "") if nl == -1 else (text[:nl], text[nl + 1 :])
                base = member.name.rsplit("/", 1)[-1]
                info = _header_info(first, second, base)
                if not info["per_cell"]:
                    probed += 1
                    continue
                info["url"] = tar_url
                info["tar_member"] = member.name
                key = _sample_key(member.name, accession)
                prev = by_key.get(key)
                if prev is None or len(info["fields"]) > len(prev["fields"]):
                    by_key[key] = info
                probed += 1
    except Exception:
        logger.debug("tar metadata scan failed for %s", tar_url)
    return by_key


def _scan_metadata(
    links: list[tuple[str, str]],
    accession: str,
    acc_dir: Path,
    *,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> dict[str, dict[str, Any]]:
    # map sample key (GSM or accession) -> metadata-file info, if any
    proxy = _read_proxy()
    by_key: dict[str, dict[str, Any]] = {}

    def _keep(key: str, info: dict[str, Any]) -> None:
        # only per-cell tables count as reusable metadata; keep the richest
        if not info["per_cell"]:
            return
        prev = by_key.get(key)
        if prev is None or len(info["fields"]) > len(prev["fields"]):
            by_key[key] = info

    tar_urls: list[str] = []
    for t, url in links:
        if t == FileType.Tar.value:
            tar_urls.append(url)
        if not _is_metadata_link(url):
            continue
        info = _probe_metadata(
            url,
            acc_dir,
            retries=retries,
            timeout=timeout,
            max_timeout=max_timeout,
            proxy=proxy,
        )
        if info is not None:
            _keep(_sample_key(url, accession), info)

    # only stream tars when nothing external was found; a tar reads whole (no random
    # access); bounded by tar/member caps, per-item deadline is the backstop
    if not by_key:
        for tar_url in tar_urls[:_MAX_TARS_SCANNED]:
            found = _scan_tar_metadata(tar_url, accession, timeout=timeout, proxy=proxy)
            for key, info in found.items():
                _keep(key, info)

    return by_key


def _build_result(
    accession: str,
    source: str,
    aliases: str,
    samples: dict[str, dict[str, Any]],
    metadata: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    # assemble the per-accession result; single_cell_count = max(per-GSM sum,
    # largest study-level file) so combined + per-sample matrices don't double count
    gsm = [s for s in samples.values() if s["sample"].startswith("GSM")]
    study = [s for s in samples.values() if not s["sample"].startswith("GSM")]
    gsm_sum = sum(s["cells"] for s in gsm)
    study_max = max((s["cells"] for s in study), default=0)

    # GSMs are the per-sample breakdown; also surface study-level file(s) when there
    # are no GSMs or a combined matrix exceeds the per-GSM sum, else it's the
    # combined superset, so drop it
    rows = sorted(gsm, key=lambda s: s["cells"], reverse=True)
    if not gsm or study_max > gsm_sum:
        rows += sorted(study, key=lambda s: s["cells"], reverse=True)

    proj_meta = metadata.get(accession)

    def _fmt(s: dict[str, Any]) -> str:
        # prefer the expression-matrix format (the one that yielded genes) over a
        # metadata txt/tsv that happened to tie/edge the cell count
        return s["genes_format"] if s["genes"] > 0 else s["cells_format"]

    def _sample_meta(s: dict[str, Any]) -> dict[str, Any] | None:
        # a metadata file keyed to this exact sample wins
        ext = metadata.get(s["sample"])
        if ext is not None:
            return ext
        # rds/h5ad containers embed per-cell metadata
        if _fmt(s) in _EMBEDDED_METADATA_FORMATS:
            m: dict[str, Any] = {
                "embedded": True,
                "format": _fmt(s),
                "has_celltype": True,
            }
            if s["rds_class"]:
                m["rds_class"] = s["rds_class"]
            return m
        # a study-wide metadata file covers every GSM (tag the borrowed scope)
        if s["sample"].startswith("GSM") and proj_meta is not None:
            return {**proj_meta, "scope": "study"}
        return None

    def _scope(s: dict[str, Any]) -> str:
        return "sample" if s["sample"].startswith("GSM") else "study"

    per_sample = [
        {
            "sample": s["sample"],
            "scope": _scope(s),
            "cells": s["cells"],
            "genes": s["genes"] if s["genes"] > 0 else None,
            "nnz": s["nnz"] if s["nnz"] > 0 else None,
            "file_format": _fmt(s),
            "unfiltered": s["unfiltered"],
            "metadata": _sample_meta(s),
        }
        for s in rows
    ]

    # union of annotation categories available across every metadata source
    all_meta = [p["metadata"] for p in per_sample if p["metadata"]]
    if proj_meta:
        all_meta.append(proj_meta)
    meta_fields = sorted({f for m in all_meta for f in m.get("fields", [])})

    out: dict[str, Any] = {
        "accession": accession,
        "source": source,
        "aliases": aliases,
        "single_cell_count": max(gsm_sum, study_max),
        "single_cell_file_format": _fmt(rows[0]) if rows else "",
        "n_samples": len(per_sample),
        "n_gsm_samples": sum(1 for p in per_sample if p["scope"] == "sample"),
        "unfiltered": any(p["unfiltered"] for p in per_sample),
        "has_metadata": bool(metadata) or any(p["metadata"] for p in per_sample),
        "has_celltype": any(m.get("has_celltype") for m in all_meta),
        "has_donor": any(m.get("has_donor") for m in all_meta),
        "has_demographics": any(m.get("has_demographics") for m in all_meta),
        "metadata_fields": meta_fields,
        "project_metadata": proj_meta,
        "per_sample": per_sample,
    }
    if rows and rows[0]["rds_class"]:
        out["rds_class"] = rows[0]["rds_class"]
    return out


if __name__ == "__main__":
    main()
