#!/usr/bin/env python3
"""Yuj work item: count cells for one single-cell accession."""

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

_MAX_FILE_BYTES = 30 * 1024**3

_MTX_HEAD_BYTES = 1 * 1024 * 1024

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
_META_EXCLUDE = (
    ".mtx",
    "barcodes.tsv",
    "features.tsv",
    "genes.tsv",
    ".h5",
    ".hdf5",
)
_META_EXTS = (".csv", ".tsv", ".txt")
_EMBEDDED_METADATA_FORMATS = ("rds", "h5ad")

# Raw/unfiltered 10x matrices report whole barcode whitelists as columns;
# detect exact whitelist sizes or low nnz/cells.
_WHITELIST_SIZES = frozenset(
    {737280, 6794880, 4526976, 3571200, 6390720, 1000000, 900000},
)
_MIN_NNZ_PER_CELL = 20
# For matrices with fewer features than _MIN_NNZ_PER_CELL, judge filtered-vs-raw
# by density relative to the feature count. 0.5 sits in the empty band between
# the filtered cluster (>=0.8) and the raw tail (<0.2).
_MIN_LOW_FEATURE_DENSITY = 0.5
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

_DEFAULT_SOCKS = "10.195.102.16:1080"

# Opt-in: fetch NCBI supplementary files over HTTPS. GEO records links as ftp://;
# some subnets intercept port 21 with a proxy whose 403 ftplib reports as a
# protocol error. The worker then records a matrix-free study.
# ftp.ncbi.nlm.nih.gov serves the same paths over HTTPS. Off by default to
# preserve existing job behaviour.
_FORCE_HTTP = os.environ.get("SC_FORCE_HTTP", "") == "1"

# hard per-accession wall-clock: download paths lack idle-stall timeouts; timed-out
# items record 0 and count as done, letting the run converge
_PER_ITEM_DEADLINE = int(os.environ.get("SC_ITEM_DEADLINE", "1800"))


class _StallTimeoutError(BaseException):
    """BaseException bypasses broad except Exception paths."""


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

    # Write via temp file so resume sees only complete outputs.
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

    def count_link(  # noqa: PLR0911
        link: DownloadLink, *, force_http: bool = False
    ) -> tuple[int, int, str, str]:
        # EBI serves supplementary files over HTTPS only; ftp:// there times out
        use_http = force_http or _FORCE_HTTP or "ebi.ac.uk" in link.url
        ftp_url = link.url.replace("https://", "ftp://", 1)
        # ftp.ncbi.nlm.nih.gov serves the same paths over HTTPS; HTTP branches receive the converted URL.
        http_url = link.url.replace("ftp://", "https://", 1)

        if link.type in {FileType.H5, FileType.H5ad}:
            dims = count_h5_dims_via_http(link.url, link.type)
            if dims is not None:
                n, g = dims
                return (n, g, link.type.value, "") if n > 0 else (0, -1, "", "")

        if link.type == FileType.Tar and not use_http:
            n = count_tar_via_stream(
                link.url, chunk_size, timeout=timeout, socks_proxy=_read_proxy()
            )
            if n is not None:
                return (n, -1, link.type.value, "") if n > 0 else (0, -1, "", "")

        dest_path = acc_dir / link.extract_filename()
        try:
            if link.type in HEADER_STREAMABLE and not is_cell_major(link.url):
                head = _MTX_HEAD_BYTES if link.type == FileType.Mtx else chunk_size
                if use_http:
                    dest_path = download_http_head(
                        http_url,
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
                        http_url,
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
            s["unfiltered"] = _is_unfiltered(n_cells, nnz, n_genes, src)
        if n_genes > s["genes"]:
            s["genes"] = n_genes
            s["genes_format"] = fmt
        if nnz > 0:
            s["nnz"] = nnz
        if rds_class:
            s["rds_class"] = rds_class

    for link in dl_links:
        # A per-GSM tar holds one matrix per sample; stream members separately so
        # each attributes to its own GSM.
        # Under SC_FORCE_HTTP this FTP streamer is skipped; HTTPS handling below does
        # the same work on hosts where port 21 is intercepted.
        if (
            link.type == FileType.Tar
            and "ebi.ac.uk" not in link.url
            and not _FORCE_HTTP
        ):
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
        # A blocked ftp download and a genuinely empty file both return no cells, so
        # ftp zeroes are retried over https before being trusted. The except path
        # matters because intercepted port 21 raises before a return-value check.
        retryable = not _FORCE_HTTP and "ebi.ac.uk" not in link.url
        try:
            n_cells, n_genes, fmt, rds_class = count_link(link)
        except Exception:
            if not retryable:
                raise
            logger.debug("ftp path raised for %s; retrying over https", link.url)
            n_cells, n_genes, fmt, rds_class = 0, -1, "", ""
        if n_cells <= 0 and retryable:
            try:
                n_cells, n_genes, fmt, rds_class = count_link(link, force_http=True)
            except Exception:
                logger.debug("https retry also failed for %s", link.url)
                n_cells, n_genes, fmt, rds_class = 0, -1, "", ""
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


def _is_unfiltered(cells: int, nnz: int, genes: int, src: str) -> bool:
    if cells in _WHITELIST_SIZES:
        return True
    if 0 < genes < _MIN_NNZ_PER_CELL:
        # nnz/cells is bounded by feature count. HTO and ADT libraries carry 2-14
        # features, so compare against achievable density: filtered low-feature
        # matrices are near-dense, raw ones are mostly empty droplets.
        return nnz > 0 and cells > 0 and nnz < cells * genes * _MIN_LOW_FEATURE_DENSITY
    if nnz > 0 and cells > 0 and nnz < cells * _MIN_NNZ_PER_CELL:
        return True
    low = src.rsplit("/", 1)[-1].lower()
    return "raw" in low and "filtered" not in low and cells > 500_000  # noqa: PLR2004


def _sample_key(url: str, accession: str) -> str:
    # match the basename only: GEO FTP paths carry a GSMxxxxnnn directory stub
    # whose literal nnn truncates a full-URL match into a colliding id
    m = re.search(r"GSM\d+", url.rsplit("/", 1)[-1])
    if m:
        return m.group(0)
    for seg in url.split("/"):
        if re.fullmatch(r"GSM\d+", seg):
            return seg
    return accession


def _is_metadata_link(url: str) -> bool:
    name = url.rsplit("/", 1)[-1].lower().removesuffix(".gz")
    if any(x in name for x in _META_EXCLUDE):
        return False
    if not name.endswith(_META_EXTS):
        return False
    return any(h in name for h in _META_NAME_HINTS)


def _parse_header(line: str, name: str) -> list[str]:
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
    # exact token match, so age hits age_years but never percentage; distinctive
    # multi-part hints like orig.ident also get a substring fallback
    low = col.lower()
    toks = set(re.split(r"[^a-z0-9]+", low))
    for h in hints:
        if h in toks:
            return True
        if ("_" in h or "." in h or len(h) >= 8) and h in low:  # noqa: PLR2004
            return True
    return False


def _detect_field_map(cols: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for cat, hints in _META_FIELD_HINTS.items():
        matched = [c for c in cols if _match_field(c, hints)]
        if matched:
            out[cat] = matched
    return out


def _cell_index_type(value: str) -> str:
    v = value.strip().strip('"').strip("'")
    if re.search(r"[ACGTNacgtn]{10,}", v):
        return "barcode"
    if re.fullmatch(r"(?i)cell[\s_.\-]?\d+", v):
        return "renamed"
    return "id"


def _header_info(first: str, second: str, name: str) -> dict[str, Any]:
    cols = _parse_header(first, name)
    low = [c.lower() for c in cols]
    row = _parse_header(second, name) if second.strip() else []
    named_barcode = any(h in c for c in low for h in _BARCODE_COL_HINTS)
    # an unnamed row index is either an empty header cell or the R write.table
    # default, which omits the rowname header and leaves the data row one wider
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
    # records tar url + member path so the file stays retrievable via tar -xOf;
    # a streamed tar has no random access, hence the member cap
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
    proxy = _read_proxy()
    by_key: dict[str, dict[str, Any]] = {}

    def _keep(key: str, info: dict[str, Any]) -> None:
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
    # single_cell_count = max(per-GSM sum, largest study-level file), avoiding
    # double counts when combined and per-sample matrices coexist.
    gsm = [s for s in samples.values() if s["sample"].startswith("GSM")]
    study = [s for s in samples.values() if not s["sample"].startswith("GSM")]
    gsm_sum = sum(s["cells"] for s in gsm)
    study_max = max((s["cells"] for s in study), default=0)

    rows = sorted(gsm, key=lambda s: s["cells"], reverse=True)
    if not gsm or study_max > gsm_sum:
        rows += sorted(study, key=lambda s: s["cells"], reverse=True)

    proj_meta = metadata.get(accession)

    def _fmt(s: dict[str, Any]) -> str:
        return s["genes_format"] if s["genes"] > 0 else s["cells_format"]

    def _sample_meta(s: dict[str, Any]) -> dict[str, Any] | None:
        ext = metadata.get(s["sample"])
        if ext is not None:
            return ext
        if _fmt(s) in _EMBEDDED_METADATA_FORMATS:
            m: dict[str, Any] = {
                "embedded": True,
                "format": _fmt(s),
                "has_celltype": True,
            }
            if s["rds_class"]:
                m["rds_class"] = s["rds_class"]
            return m
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
