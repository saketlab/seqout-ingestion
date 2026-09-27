"""
Fleet worker: scan one study's tabular supplementary files for Excel gene-name errors.

yuj appends the accession as argv[-1]. Stdlib only: the hosts have bare python3.
Reads files.tsv (accession, url) beside this file, streams each url, and writes
results/<accession>.json.

A hit is a cell in the first GENE_COLS columns or in the header row that is
either (a) a date whose (month, day) is one Excel is known to make from a gene
symbol (dates.txt, from Abeysooriya et al. 2021 S1 Table) in any common
rendering, or (b) scientific notation of the shape Excel makes from RIKEN
clone ids (2310009E13 -> 2.31E+13).

Agilent Feature Extraction files (first row literally "TYPE") get their
FEPARAMS/STATS metadata rows (single-row run parameters like Protocol_date,
Scan_Date) excluded from scanning; see _AGILENT_META_BLOCKS in scan_text.
"""

from __future__ import annotations

import collections
import gzip
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

HERE = Path(__file__).resolve().parent
GENE_COLS = 3
MAX_BYTES = int(os.environ.get("XG_MAX_BYTES", 200 * 1024 * 1024))
MAX_ROWS = int(os.environ.get("XG_MAX_ROWS", 3_000_000))
MAX_HEADER_FIELDS = 200_000
TIMEOUT = 120
UA = {"User-Agent": "seqout-excel-gene-scan/1 (contact: seqout@gmail.com)"}

MONTHS = {
    m: i + 1
    for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split())
}
MONTHS.update(
    {
        m: i + 1
        for i, m in enumerate(
            "january february march april may june july august september october november december".split()
        )
    }
)
# locale spellings from the 2021 paper's error corpus (Dutch, French, Spanish/Portuguese, German); not full i18n
MONTHS.update(
    {
        "sept": 9,
        "mrt": 3,
        "maart": 3,
        "mars": 3,
        "mei": 5,
        "mai": 5,
        "ago": 8,
        "agosto": 8,
        "okt": 10,
        "oktober": 10,
        "dez": 12,
        "dic": 12,
    }
)
_MON = "(" + "|".join(re.escape(m) for m in sorted(MONTHS, key=len, reverse=True)) + ")"
_TIME = (
    r"(?:[ t](?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,9})?)?"
    r"(?: ?[ap]m)?(?: ?(?:z|[+-](?:[01]\d|2[0-3]):?[0-5]\d))?)?"
)
RE_DMON = re.compile(
    rf"^(\d{{1,2}})[-/ .]{_MON}(?:[-/ .](\d{{2}}|\d{{4}}))?{_TIME}$", re.I
)
RE_MOND = re.compile(
    rf"^{_MON}[-/ .](\d{{1,2}})(?:[-/ .,]+(\d{{4}}|\d{{2}}))?{_TIME}$", re.I
)
RE_NUM = re.compile(
    rf"^(\d{{1,2}})[-/.](\d{{1,2}})[-/.](\d{{2}}|\d{{4}}){_TIME}$", re.I
)
RE_ISO = re.compile(rf"^(\d{{4}})[-/.](\d{{1,2}})[-/.](\d{{1,2}}){_TIME}$", re.I)
# RIKEN ids are 7 digits+letter+2 digits; Excel's exponent = suffix+6, so 2310009E13 -> 2.31E+19 (range 7-30)
RE_SCI = re.compile(r"^[1-9](?:\.\d{1,14})?e\+(?:0?[7-9]|[12]\d|30)$", re.I)


def load_dates(path: Path = HERE / "dates.txt") -> set[tuple[int, int]]:
    out = set()
    for line in path.read_text().split():
        m, d = line.split("-")
        out.add((int(m), int(d)))
    return out


def date_key(cell: str) -> tuple[int, int] | None:
    """(month, day) if the cell reads as a date, else None."""
    s = cell.strip().strip("\"'").strip()
    if not s or len(s) > 40:
        return None
    m = RE_DMON.match(s)
    if m:
        return MONTHS[m.group(2).lower()], int(m.group(1))
    m = RE_MOND.match(s)
    if m:
        # lowercase unpadded day (sep-1) is a C. elegans symbol spelling, not an Excel date (Excel pads: Sep-01)
        if m.group(1).islower() and len(m.group(2)) == 1 and not m.group(3):
            return None
        return MONTHS[m.group(1).lower()], int(m.group(2))
    m = RE_ISO.match(s)
    if m:
        return int(m.group(2)), int(m.group(3))
    m = RE_NUM.match(s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        # US (m/d) and EU (d/m) readings; caller checks either against the set.
        return ("ambig", a, b)  # type: ignore[return-value]
    return None


def classify(
    cell: str,
    dates: set[tuple[int, int]],
    col: int = 0,
    *,
    enable_date_serial_text: bool = False,
) -> str | None:
    """'date' / 'sci' / 'date_serial_text' / None for one cell; sci and
    date_serial_text count only in the first column.

    date_serial_text is off by default: text/CSV/TSV carries no cell-format
    metadata to confirm a bare serial was ever a date, unlike scan_xlsx's
    date_serial. Confirmed false positive: TP53's Entrez id "7157" decodes to
    (8, 5), which the real dates.txt contains. No caller separates it from
    n_hits, so enabling it would inflate the verified count directly; don't
    flip the default without also changing every call site.
    """
    k = date_key(cell)
    if k is not None:
        if k[0] == "ambig":
            _, a, b = k
            if (a, b) in dates or (b, a) in dates:
                return "date"
            return None
        return "date" if k in dates else None
    s = cell.strip().strip("\"'")
    if col == 0 and RE_SCI.match(s):
        return "sci"
    if col == 0 and enable_date_serial_text:
        sk = _serial_to_key(s)
        if sk is not None and sk in dates:
            return "date_serial_text"
    return None


RETRY_SLEEP = (15, 60, 180)


class _FtpStream(io.RawIOBase):
    """Streaming RETR over ftplib; closes the control connection with the data socket."""

    def __init__(self, url: str, offset: int = 0):
        from ftplib import FTP  # noqa: PLC0415
        from urllib.parse import unquote, urlparse  # noqa: PLC0415

        u = urlparse(url)
        self.url, self.offset = url, offset
        self.sock = self.ftp = None
        self.ftp = FTP(u.hostname, timeout=TIMEOUT)  # noqa: S321
        self.ftp.login()
        self.ftp.voidcmd("TYPE I")
        self.sock = self.ftp.transfercmd(f"RETR {unquote(u.path)}", rest=offset or None)
        self.sock.settimeout(TIMEOUT)

    def reopen(self, offset: int) -> "_FtpStream":
        """Fresh connection at offset: the cheap way to skip a big tar member."""
        self.close()
        return _FtpStream(self.url, offset)

    def readable(self):
        return True

    def readinto(self, b):
        n = self.sock.recv_into(b)
        return n

    def read(self, n=-1):
        chunks, got = [], 0
        while n is None or n < 0 or got < n:
            want = 1 << 20 if n is None or n < 0 else min(1 << 20, n - got)
            c = self.sock.recv(want)
            if not c:
                break
            chunks.append(c)
            got += len(c)
        return b"".join(chunks)

    def close(self):
        for o in (self.sock, self.ftp):
            try:
                if o is not None:
                    o.close()
            except OSError:
                pass
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class _HttpsRangeStream(io.RawIOBase):
    """Same shape as _FtpStream over https with Range, for hosts blocking port 21; reopen() just requests the new offset."""

    def __init__(self, url: str, offset: int = 0):
        self.url, self.offset = url, offset
        https = re.sub(
            r"^ftp://ftp\.ncbi\.nlm\.nih\.gov/", "https://ftp.ncbi.nlm.nih.gov/", url
        )
        https = urllib.parse.quote(https, safe=":/?&=%+@")
        headers = dict(UA)
        if offset:
            headers["Range"] = f"bytes={offset}-"
        self.resp = None
        for wait in (*RETRY_SLEEP, None):
            try:
                self.resp = urllib.request.urlopen(  # noqa: S310
                    urllib.request.Request(https, headers=headers), timeout=TIMEOUT
                )
                break
            except urllib.error.HTTPError as e:
                if e.code not in (429, 500, 502, 503, 504) or wait is None:
                    raise
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                if wait is None:
                    raise
            time.sleep(wait)
        if offset and self.resp.status != 206:
            msg = f"server ignored Range at offset {offset}"
            raise OSError(msg)

    def readable(self):
        return True

    def readinto(self, b):
        chunk = self.resp.read(len(b))
        b[: len(chunk)] = chunk
        return len(chunk)

    def read(self, n=-1):
        return self.resp.read(None if n is None or n < 0 else n)

    def reopen(self, offset: int) -> "_HttpsRangeStream":
        self.close()
        return _HttpsRangeStream(self.url, offset)

    def close(self):
        try:
            if self.resp is not None:
                self.resp.close()
        except OSError:
            pass
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


_FTP_OK: list[bool | None] = [None]


def _ftp_reachable() -> bool:
    """Probed once per process; retrying FTP per file would cost minutes on networks that drop port 21."""
    if _FTP_OK[0] is None:
        if os.environ.get("XG_FORCE_HTTPS"):
            _FTP_OK[0] = False
        else:
            # a real login, not just a connect: some networks proxy port 21 and accept the socket before saying 403
            from ftplib import FTP  # noqa: PLC0415

            try:
                f = FTP("ftp.ncbi.nlm.nih.gov", timeout=10)  # noqa: S321
                f.login()
                f.close()
                _FTP_OK[0] = True
            except Exception:  # noqa: BLE001
                _FTP_OK[0] = False
    return _FTP_OK[0]


def _open_url(url: str):
    """Backoff open: NCBI 503s a busy client, then blocks its IP. ftp:// is
    tried first when port 21 is open (NCBI throttles https harder); otherwise https.
    """
    if url.startswith("ftp://") and _ftp_reachable():
        for wait in (*RETRY_SLEEP, None):
            try:
                return _FtpStream(url)
            except Exception as e:  # noqa: BLE001
                # 550 and protocol errors aren't transient; fall through to https for the real status
                if wait is None or type(e).__name__ in ("error_perm", "error_proto"):
                    break
            time.sleep(wait)
    url = re.sub(
        r"^ftp://ftp\.ncbi\.nlm\.nih\.gov/", "https://ftp.ncbi.nlm.nih.gov/", url
    )
    url = urllib.parse.quote(url, safe=":/?&=%+@")
    req = urllib.request.Request(url, headers=UA)
    for wait in (*RETRY_SLEEP, None):
        try:
            return urllib.request.urlopen(req, timeout=TIMEOUT)  # noqa: S310
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or wait is None:
                raise
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            if wait is None:
                raise
        time.sleep(wait)
    raise RuntimeError("unreachable")


class _Capped(io.RawIOBase):
    def __init__(self, raw, cap):
        self.raw, self.left = raw, cap

    def readable(self):
        return True

    def readinto(self, b):
        if self.left <= 0:
            return 0
        chunk = self.raw.read(min(len(b), self.left))
        self.left -= len(chunk)
        b[: len(chunk)] = chunk
        return len(chunk)


def _sniff_delim(line: str) -> str:
    """Tab wins whenever present: free-text sample names can embed more spaces
    than there are tab columns (E-SMDB-2231), which broke space-count sniffing
    and shredded names into gene-like tokens. Comma/semicolon/space are tried
    only when there's no tab at all.
    """
    if "\t" in line:
        return "\t"
    return max(",; ", key=line.count)


def _lines(fh):
    """Yield lines; a gzip member cut short by the byte cap ends the scan quietly.

    Splits on \\r\\n, \\r, or bare \\n (plain iteration only catches \\n), since a
    bare-CR file like GSE10633_allnormalcalls.txt would otherwise read as one
    line, skip all column filtering, and misclassify its embedded dates as hits.
    """
    buf = b""
    try:
        while True:
            # small reads avoid gzip's read(n) raising EOFError with zero bytes on a truncated trailer
            chunk = fh.read(4096)
            if not chunk:
                break
            buf += chunk
            parts = buf.splitlines(keepends=True)
            if parts and not parts[-1].endswith((b"\n", b"\r")):
                buf = parts.pop()
            else:
                buf = b""
            yield from parts
    except EOFError:
        pass
    if buf:
        yield buf


_DEADLINE = [float("inf")]  # wall-clock cap shared by every member of one bundle

# Agilent and Illumina raw exports both carry real dates in a metadata
# preamble that reads as gene corruption. Each wraps it in a distinctive
# marker no ordinary matrix uses: Agilent's TYPE/FEPARAMS-or-STATS/DATA
# blocks (gated on row 1 == "TYPE"), and Illumina's [Heading]/[Assay]
# INI sections (unconditional, since the brackets alone are distinctive).
_AGILENT_META_BLOCKS = {"FEPARAMS", "STATS"}
# Agilent FEPARAMS field names for files whose first block has no TYPE tag (line 0 is the bare names row)
_AGILENT_FEPARAMS_SIGNATURE = {
    "FeatureExtractor_ScanFileName",
    "FeatureExtractor_Barcode",
    "Protocol_Name",
    "Grid_Name",
}
_INI_META_SECTIONS = {"[heading]", "[header]"}
_INI_DATA_SECTIONS = {"[assay]", "[data]", "[probes]", "[controls]"}

# CodeLink/Amersham exports (GSE8941) repeat a per-run timestamp in an early
# column of every row instead of once per file. No real gene symbol is ever
# the majority value across a matrix's rows, so a hit above DOMINANT_FRACTION
# is dropped as a repeated-constant artifact, once there are enough rows for
# "majority" to be meaningful.
DOMINANT_FRACTION = 0.5
MIN_ROWS_FOR_DOMINANCE = 50

# GENE_COLS checks the first 3 columns regardless of what they are, so a
# header itself can name a non-gene field, like single-cell metadata's
# "date" column (GSE223626), which never trips DOMINANT_FRACTION.
_NON_GENE_COL_NAMES = {
    "date",
    "time",
    "timestamp",
    "datetime",
    "age",
    "sex",
    "gender",
    "genotype",
    "batch",
    "sample",
    "sample_id",
    "sampleid",
    "cell",
    "cell_id",
    "cellid",
    "barcode",
    "collection_date",
    "scan_date",
    "protocol_date",
    "processing_date",
    "run_date",
    "extract_date",
    "hyb_date",
    "assay_name",
    "assay_date",
    # chromosome/scaffold ids also get mangled into sci notation, but that's a coordinate, not a gene (GSE59075's "chromosome" col held "1.10E+12")
    "chromosome",
    "chr",
    "chrom",
    "contig",
    "scaffold",
    "bp start",
    "bp stop",
    "bp_start",
    "bp_stop",
    "position",
    "start",
    "stop",
}

_COL_SEP_RE = re.compile(r"[.\-\s]+")


def _is_non_gene_col(name: str) -> bool:
    """True if a header cell names an obviously non-gene field.

    Normalizes '.'/'-'/whitespace to '_' first: R/Seurat spells fields with dots
    ("Collection.Date") where this repo uses underscores, which let GSE266500's
    per-cell date column slip past an exact-string check and get scanned as genes.
    """
    normalized = _COL_SEP_RE.sub("_", name.strip().strip("'\"").lower())
    return normalized in _NON_GENE_COL_NAMES


def _drop_dominant(
    hits: collections.Counter,
    kinds: collections.Counter,
    hit_kind: dict,
    rows: int,
) -> None:
    if rows < MIN_ROWS_FOR_DOMINANCE:
        return
    threshold = rows * DOMINANT_FRACTION
    for value, count in list(hits.items()):
        if count > threshold:
            kinds[hit_kind[value]] -= count
            if kinds[hit_kind[value]] <= 0:
                del kinds[hit_kind[value]]
            del hits[value]


def scan_text(fh, dates, max_rows: int = MAX_ROWS) -> dict:
    hits = collections.Counter()
    kinds = collections.Counter()
    hit_kind: dict = {}
    rows = 0
    header = None
    delim = "\t"
    truncated = False
    is_agilent = False
    skip_data = False
    skip_cols: set = set()
    for raw in _lines(fh):
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if not line:
            continue
        if line.startswith("#"):
            if header is not None:
                continue
            # BED/MACS files prefix the real header with "#" (GSE225970);
            # treating it as a comment hid the header, so "chrom" never hit
            # the _NON_GENE_COL_NAMES check. Only line 1 gets this, and only
            # if it still looks delimited after stripping "#".
            candidate = line.lstrip("#").strip()
            if not candidate or max(candidate.count(d) for d in "\t,;") == 0:
                continue
            line = candidate
        rows += 1
        if rows > max_rows or (rows % 20000 == 0 and time.time() > _DEADLINE[0]):
            truncated = True
            break
        if header is None:
            delim = _sniff_delim(line)
            header = line.split(delim)[:MAX_HEADER_FIELDS]
            is_agilent = header[:1] == ["TYPE"]
            if not is_agilent and _AGILENT_FEPARAMS_SIGNATURE & set(header):
                # GSE9651 variant: the first FEPARAMS block has no TYPE tag at
                # all (line 0 is the bare names row), so gate on the field-name
                # signature instead and skip the immediately following DATA row.
                is_agilent = True
                skip_data = True
            skip_cols = {
                i for i, c in enumerate(header[:GENE_COLS]) if _is_non_gene_col(c)
            }
            for cell in header:
                k = classify(cell, dates)
                if k:
                    v = cell.strip().strip('"')
                    hits[v] += 1
                    kinds[k] += 1
                    hit_kind[v] = k
            continue
        tag = line.split(delim, 1)[0].strip()
        if is_agilent:
            if tag == "TYPE":
                continue
            if tag in _AGILENT_META_BLOCKS:
                skip_data = True
                continue
            if tag == "FEATURES":
                skip_data = False
                continue
            if tag == "DATA" and skip_data:
                continue
        tag_lower = tag.lower()
        if tag_lower in _INI_META_SECTIONS:
            skip_data = True
            continue
        if tag_lower in _INI_DATA_SECTIONS:
            skip_data = False
            continue
        if skip_data:
            continue
        for col, cell in enumerate(line.split(delim, GENE_COLS)[:GENE_COLS]):
            if col in skip_cols:
                continue
            k = classify(cell, dates, col)
            if k:
                v = cell.strip().strip('"')
                hits[v] += 1
                kinds[k] += 1
                hit_kind[v] = k
    _drop_dominant(hits, kinds, hit_kind, rows)
    return {
        "rows": rows,
        "truncated": truncated,
        "header": (header or [])[:6],
        "n_hits": sum(hits.values()),
        "kinds": dict(kinds),
        "examples": hits.most_common(30),
    }


# Built-in calendar-date formats only; 18-21 and 45-47 are clocks and durations.
_BUILTIN_DATE_FMTS = {14, 15, 16, 17, 22} | set(range(27, 37)) | set(range(50, 59))
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def _date_styles(z: zipfile.ZipFile) -> set[int]:
    try:
        root = ET.fromstring(z.read("xl/styles.xml"))
    except KeyError:
        return set()
    custom = {}
    for nf in root.iter(f"{_NS}numFmt"):
        code = re.sub(
            r'"[^"]*"|\[[^\]]*\]|\\.|[_*].', "", nf.get("formatCode", "")
        ).lower()
        # y or d marks a calendar date; a lone m next to h or s is minutes.
        custom[int(nf.get("numFmtId"))] = bool(re.search(r"y|d|mmm", code))
    styles = set()
    xfs = root.find(f"{_NS}cellXfs")
    if xfs is None:
        return styles
    for i, xf in enumerate(xfs):
        fid = int(xf.get("numFmtId", "0"))
        if fid in _BUILTIN_DATE_FMTS or custom.get(fid):
            styles.add(i)
    return styles


def _serial_to_key(v: str, date1904: bool = False) -> tuple[int, int] | None:
    try:
        n = int(float(v))
    except (ValueError, OverflowError):
        return None
    if not (0 if date1904 else 1) <= n <= 80000:
        return None
    if date1904:
        d = date(1904, 1, 1) + timedelta(days=n)
    elif n < 60:  # before Excel's fictitious 1900-02-29
        d = date(1899, 12, 31) + timedelta(days=n)
    elif n == 60:
        return None
    else:
        d = date(1899, 12, 30) + timedelta(days=n)
    return d.month, d.day


_COLRE = re.compile(r"^([A-Z]+)(\d+)$")


def scan_xlsx(data: bytes, dates) -> dict:
    z = zipfile.ZipFile(io.BytesIO(data))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).iter(f"{_NS}si"):
            shared.append("".join(t.text or "" for t in si.iter(f"{_NS}t")))
    dstyles = _date_styles(z)
    date1904 = False
    if "xl/workbook.xml" in z.namelist():
        wb = ET.fromstring(z.read("xl/workbook.xml"))
        pr = wb.find(f"{_NS}workbookPr")
        date1904 = pr is not None and pr.get("date1904", "0").lower() in ("1", "true")
    hits = collections.Counter()
    kinds = collections.Counter()
    hit_kind: dict = {}
    cells = 0
    header_names: dict = {}
    sheets = [n for n in z.namelist() if n.startswith("xl/worksheets/sheet")]
    for name in sheets:
        with z.open(name) as fh:
            for _, el in ET.iterparse(fh):
                if el.tag != f"{_NS}c":
                    continue
                m = _COLRE.match(el.get("r", ""))
                if not m:
                    el.clear()
                    continue
                col, row = m.group(1), int(m.group(2))
                if row != 1 and (len(col) > 1 or col > "C"):
                    el.clear()
                    continue
                cells += 1
                if cells > MAX_ROWS:
                    break
                t = el.get("t")
                v = el.find(f"{_NS}v")
                text = None
                if t == "s" and v is not None and v.text:
                    text = shared[int(v.text)] if int(v.text) < len(shared) else ""
                elif t == "d" and v is not None:  # native ISO date cell
                    text = v.text
                elif t in ("inlineStr", "str"):
                    text = "".join(x.text or "" for x in el.iter(f"{_NS}t")) or (
                        v.text if v is not None else ""
                    )
                elif v is not None and v.text and int(el.get("s", "0")) in dstyles:
                    if row != 1 and _is_non_gene_col(header_names.get(col, "")):
                        el.clear()
                        continue
                    k = _serial_to_key(v.text, date1904)
                    if k and k in dates:
                        label = f"{k[1]}-{list(MONTHS)[k[0] - 1].title()}"
                        hits[label] += 1
                        kinds["date_serial"] += 1
                        hit_kind[label] = "date_serial"
                    el.clear()
                    continue
                if row == 1 and text:
                    header_names[col] = text
                if text:
                    if row != 1 and _is_non_gene_col(header_names.get(col, "")):
                        el.clear()
                        continue
                    k = classify(text, dates, 0 if col == "A" else 1)
                    if k:
                        v = text.strip()
                        hits[v] += 1
                        kinds[k] += 1
                        hit_kind[v] = k
                el.clear()
    _drop_dominant(hits, kinds, hit_kind, cells)
    return {
        "rows": cells,
        "truncated": cells > MAX_ROWS,
        "sheets": len(sheets),
        "n_hits": sum(hits.values()),
        "kinds": dict(kinds),
        "examples": hits.most_common(30),
    }


TABULAR = (".txt", ".tsv", ".csv", ".tab", ".xlsx")
SKIP_MEMBER_READ = (
    8 << 20
)  # non-tabular members above this are skipped by reconnecting at the next header


def _is_tabular(name: str) -> bool:
    n = name.lower()
    if n.endswith(".gz"):
        n = n[:-3]
    return n.endswith(TABULAR)


def _scan_bytes(name: str, data: bytes, dates) -> dict:
    low = name.lower()
    if low.endswith(".gz"):
        data = gzip.decompress(data)
        low = low[:-3]
    if low.endswith(".xlsx"):
        return scan_xlsx(data, dates)
    return scan_text(io.BytesIO(data), dates, MAX_ROWS // 6)


def _read_exact(stream, n: int) -> bytes:
    out, got = [], 0
    while got < n:
        c = stream.read(min(1 << 20, n - got))
        if not c:
            break
        out.append(c)
        got += len(c)
    return b"".join(out)


def _member_summary(name: str, r: dict) -> dict:
    return {
        "member": name,
        "rows": r.get("rows"),
        "n_hits": r["n_hits"],
        "kinds": r["kinds"],
        "examples": r["examples"][:10],
    }


def scan_tar(url: str, dates, budget: int = MAX_BYTES) -> dict:
    """Walk a tar over FTP, scanning tabular members, seeking past the rest.

    Tar over a socket cannot seek, but FTP can resume and https takes a Range,
    so a large non-tabular member (CEL, bigWig, ...) costs one reconnect
    instead of a full read.
    """
    stream = None
    if url.startswith("ftp://") and _ftp_reachable():
        try:
            stream = _FtpStream(url)
        except Exception:  # noqa: BLE001 (any FTP failure falls through to https)
            stream = None
    if stream is None:
        stream = _HttpsRangeStream(url)
    members, pos, spent, longname = [], 0, 0, None
    n_members = n_scanned = 0
    truncated = False
    t0 = time.time()
    _DEADLINE[0] = t0 + 600
    try:
        while True:
            if time.time() > _DEADLINE[0]:
                truncated = True
                break
            hdr = _read_exact(stream, 512)
            pos += 512
            if len(hdr) < 512 or hdr == b"\0" * 512:
                break
            name = hdr[:100].rstrip(b"\0").decode("utf-8", "replace")
            try:
                size = int(hdr[124:136].strip(b"\0 ") or b"0", 8)
            except ValueError:
                break
            typ = hdr[156:157]
            padded = (size + 511) // 512 * 512
            if typ == b"L":  # GNU long name: applies to the next header
                longname = (
                    _read_exact(stream, padded)[:size]
                    .rstrip(b"\0")
                    .decode("utf-8", "replace")
                )
                pos += padded
                continue
            if longname:
                name, longname = longname, None
            if typ in (b"x", b"g"):  # pax headers, small; just consume
                _read_exact(stream, padded)
                pos += padded
                continue
            n_members += 1
            if typ in (b"0", b"\0") and _is_tabular(name) and size <= budget - spent:
                data = _read_exact(stream, padded)[:size]
                pos += padded
                spent += padded
                try:
                    r = _scan_bytes(name, data, dates)
                    n_scanned += 1
                    if r["n_hits"]:
                        members.append(_member_summary(name, r))
                except Exception as e:  # noqa: BLE001
                    members.append(
                        {"member": name, "error": f"{type(e).__name__}: {str(e)[:100]}"}
                    )
            elif padded <= SKIP_MEMBER_READ:
                _read_exact(stream, padded)
                pos += padded
                spent += padded
            else:
                pos += padded
                try:
                    stream = stream.reopen(pos)
                except Exception:  # noqa: BLE001 (finishes over https on a mid-scan failure)
                    stream = _HttpsRangeStream(url, pos)
            if spent >= budget:
                truncated = True
                break
    finally:
        _DEADLINE[0] = float("inf")
        stream.close()
    return {
        "fmt": "tar",
        "members": n_members,
        "members_scanned": n_scanned,
        "truncated": truncated,
        "n_hits": sum(m.get("n_hits", 0) for m in members),
        "hits": members,
        "kinds": {
            k: sum(m.get("kinds", {}).get(k, 0) for m in members)
            for k in ("date", "date_serial", "sci")
        },
        "examples": sorted(
            ((v, n) for m in members for v, n in m.get("examples", [])),
            key=lambda t: -t[1],
        )[:30],
    }


def scan_zip(data: bytes, dates) -> dict:
    z = zipfile.ZipFile(io.BytesIO(data))
    members = []
    n_members = n_scanned = 0
    for info in z.infolist():
        if info.is_dir():
            continue
        n_members += 1
        if not _is_tabular(info.filename) or info.file_size > MAX_BYTES:
            continue
        try:
            r = _scan_bytes(info.filename, z.read(info), dates)
            n_scanned += 1
            if r["n_hits"]:
                members.append(_member_summary(info.filename, r))
        except Exception as e:  # noqa: BLE001
            members.append(
                {
                    "member": info.filename,
                    "error": f"{type(e).__name__}: {str(e)[:100]}",
                }
            )
    return {
        "fmt": "zip",
        "members": n_members,
        "members_scanned": n_scanned,
        "n_hits": sum(m.get("n_hits", 0) for m in members),
        "hits": members,
        "kinds": {
            k: sum(m.get("kinds", {}).get(k, 0) for m in members)
            for k in ("date", "date_serial", "sci")
        },
        "examples": sorted(
            ((v, n) for m in members for v, n in m.get("examples", [])),
            key=lambda t: -t[1],
        )[:30],
    }


def scan_url(url: str, dates) -> dict:
    t0 = time.time()
    rec = {"url": url}
    low = url.lower()
    try:
        if low.endswith(".tar"):
            rec.update(scan_tar(url, dates))
            rec["status"] = rec.get("status", "ok")
            rec["secs"] = round(time.time() - t0, 1)
            return rec
        with _open_url(url) as resp:
            if low.endswith((".xlsx", ".xlsx.gz")):
                data = resp.read(MAX_BYTES)
                if low.endswith(".gz"):
                    data = gzip.decompress(data)
                rec.update(scan_xlsx(data, dates))
                rec["fmt"] = "xlsx"
            elif low.endswith(".zip"):
                data = resp.read(MAX_BYTES + 1)
                if len(data) > MAX_BYTES:
                    rec["status"] = "skipped_size"
                    return rec
                rec.update(scan_zip(data, dates))
            elif low.endswith((".xls", ".tar.gz", ".tgz")):
                rec["status"] = "skipped_format"
                return rec
            else:
                src = io.BufferedReader(_Capped(resp, MAX_BYTES), 1 << 20)
                fh = gzip.GzipFile(fileobj=src) if low.endswith(".gz") else src
                rec.update(scan_text(fh, dates))
                rec["fmt"] = "text"
        rec["status"] = "ok"
    except Exception as e:  # noqa: BLE001 (one bad file must not sink the whole study)
        rec["status"] = "error"
        rec["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    rec["secs"] = round(time.time() - t0, 1)
    return rec


def main() -> int:
    if len(sys.argv) < 2:
        sys.stderr.write("usage: yuj_worker.py <ACCESSION>\n")
        return 2
    acc = sys.argv[-1]
    out_dir = Path(os.environ.get("XG_OUT_DIR", HERE / "results"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{acc}.json"
    if out.exists():
        return 0
    dates = load_dates()
    urls = []
    with open(os.environ.get("XG_MANIFEST", HERE / "files.tsv")) as fh:
        for line in fh:
            a, _, u = line.rstrip("\n").partition("\t")
            if a == acc:
                urls.append(u)
    files = [scan_url(u, dates) for u in urls]
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"accession": acc, "files": files}))
    tmp.rename(out)
    sys.stderr.write(
        f"{acc}: {len(files)} files, {sum(f.get('n_hits', 0) for f in files)} hits\n"
    )
    return 0


def demo() -> None:
    dates = {(3, 1), (9, 2), (12, 1), (3, 8)}
    assert classify("1-Mar", dates) == "date"
    assert classify("Mar-01", dates) == "date"
    assert classify("2016-03-01", dates) == "date"
    assert classify("3/1/16", dates) == "date"
    assert classify("1-Mar-2021 00:00:00", dates) == "date"
    assert classify("2-Sep", dates) == "date"
    assert classify("2-Jul", dates) is None
    assert classify("2021-03-01T00:00:00Z", dates) == "date"
    assert classify("Mar-01-21", dates) == "date"
    assert classify("1-March-2021", dates) == "date"
    assert classify("1-mrt", dates) == "date"
    assert classify("sep-2", dates) is None and classify("Sep-02", dates) == "date"
    assert classify("0E+20", dates) is None
    assert classify("5-Mar", dates) is None
    assert classify("2.31E+19", dates) == "sci"
    assert classify("1.11E+28", dates) == "sci"
    assert classify("1E+07", dates) == "sci"
    assert classify("1E+06", dates) is None
    assert classify("2.31E+13", dates, col=1) is None
    assert classify("2.03E+01", dates) is None
    assert classify("8E12", dates) is None
    assert classify("1.5E+40", dates) is None
    assert (
        _serial_to_key("44256") == (3, 1)
        and _serial_to_key("1") == (1, 1)
        and _serial_to_key("60") is None
    )
    assert _serial_to_key("42794", date1904=True) == (3, 1)
    assert classify("MARCH1", dates) is None
    assert classify("ENSG00000000003", dates) is None
    assert classify("12.5", dates) is None
    # off by default: a bare Entrez ID in col 0 must stay silent unless a caller opts in
    assert classify("41334", dates) is None
    assert classify("41334", dates, enable_date_serial_text=True) == "date_serial_text"
    assert classify("41334", dates, col=1, enable_date_serial_text=True) is None
    assert classify("41470", dates, enable_date_serial_text=True) is None
    # real false positive (Codex review): TP53's Entrez id 7157 decodes to (8, 5), which the real dates.txt contains
    real_dates = load_dates()
    assert _serial_to_key("7157") == (8, 5)
    assert (8, 5) in real_dates
    assert (
        classify("7157", real_dates, enable_date_serial_text=True) == "date_serial_text"
    )
    assert classify("7157", real_dates) is None  # default stays off
    fh = io.BytesIO(b'gene\ts1\ts2\nA2M\t1\t2\n8-Mar\t3\t4\n"1-Dec"\t0\t0\n')
    r = scan_text(fh, dates)
    assert r["n_hits"] == 2 and r["examples"][0][0] in ("8-Mar", "1-Dec"), r
    # padded past several _lines() read chunks so the missing gzip trailer
    # lands deep in the padding, not on the row being asserted on
    padded = b"gene\ts1\n2310009E13\t1\n1-Mar\t2\n" + b"".join(
        f"GENE{i}\t{i}\n".encode() for i in range(1000)
    )
    fh = io.BytesIO(gzip.compress(padded)[:-8])
    r = scan_text(gzip.GzipFile(fileobj=fh), dates)
    assert r["kinds"].get("date") == 1, r
    zb = io.BytesIO()
    with zipfile.ZipFile(zb, "w") as z:
        z.writestr("a/counts.txt", "gene\ts1\n2-Sep\t1\nTP53\t2\n")
        z.writestr("a/reads.fastq", "@r1\nACGT\n")
        z.writestr("b/m.csv.gz", gzip.compress(b"gene,s1\n1-Dec,1\n"))
    r = scan_zip(zb.getvalue(), dates)
    assert r["n_hits"] == 2 and r["members"] == 3 and r["members_scanned"] == 2, r
    agilent = (
        b"TYPE\ttext\ttext\n"
        b"FEPARAMS\tProtocol_Name\tProtocol_date\n"
        b"DATA\tGE2_11kx2_1005\t07-Sep-2005 10:30\n"
        b"TYPE\ttext\tfloat\n"
        b"STATS\tStatName\tStatValue\n"
        b"DATA\tFoo\t2-Sep\n"
        b"TYPE\ttext\ttext\ttext\n"
        b"FEATURES\tProbeName\tGeneName\tOther\n"
        b"DATA\t1\t8-Mar\tx\n"
    )
    r = scan_text(io.BytesIO(agilent), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "8-Mar", r
    illumina = (
        b"Illumina, Inc.\n"
        b"[Heading]\n"
        b"Descriptor File Name,GSAMD-24v1-0_20011747_A1.bpm\n"
        b"Date Manufactured,9/2/2016\n"
        b"[Assay]\n"
        b"IlmnID,Name,Chr\n"
        b"rs123,2-Sep,1\n"
    )
    r = scan_text(io.BytesIO(illumina), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "2-Sep", r
    codelink_header = b"ASSAY_ID\tASSAY_NAME\tGENE_ID\n"
    codelink_rows = (
        b"".join(f"{i}\tSep 8, 2005 11:44:58 AM\tGENE{i}\n".encode() for i in range(60))
        + b"1\t9\t8-Mar\n"
    )
    r = scan_text(io.BytesIO(codelink_header + codelink_rows), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "8-Mar", r
    sc_meta = (
        b"gene\tdate\tage\n"
        + b"".join(f"GENE{i}\tnotadate{i}\t8w\n".encode() for i in range(29))
        + b"1-Mar\t2-Sep\t8w\n"
    )
    r = scan_text(io.BytesIO(sc_meta), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "1-Mar", r
    agilent_headerless = (
        b"Protocol_Name\tProtocol_date\n"
        b"DATA\tGE2_44k_1005\t07-Sep-2005 10:30\n"
        b"TYPE\ttext\tfloat\n"
        b"STATS\tStatName\tStatValue\n"
        b"DATA\tFoo\t2-Sep\n"
        b"TYPE\ttext\ttext\ttext\n"
        b"FEATURES\tProbeName\tGeneName\tOther\n"
        b"DATA\t1\t8-Mar\tx\n"
    )
    r = scan_text(io.BytesIO(agilent_headerless), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "8-Mar", r
    bed_style = (
        b"#chrom\tchromStart\tname\n"
        + b"".join(f"chr{i % 5}\t{i}\tGENE{i}\n".encode() for i in range(60))
        + b"chr1\t99\t1-Mar\n"
    )
    r = scan_text(io.BytesIO(bed_style), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "1-Mar", r
    # a genuine multi-line comment block (no consistent delimiter) must still be skipped as a header
    free_text_comments = (
        b"# generated by some pipeline\n# see README for details\ngene\ts1\n2-Sep\t1\n"
    )
    r = scan_text(io.BytesIO(free_text_comments), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "2-Sep", r
    # GSE10633 shape: bare \r, no \n; without splitting on \r the blob reads
    # as one header line and classifies every token unconditionally
    bare_cr = b"\r".join(
        [b"gene\tval\tid\tsrcdb"]
        + [f"GENE{i}\t{i}\tID{i}\t8/8/05".encode() for i in range(60)]
        + [b"1-Mar\t1\tID60\t8/8/05"]
    )
    r = scan_text(io.BytesIO(bare_cr), dates)
    assert r["rows"] == 62, r
    assert r["n_hits"] == 1 and r["examples"][0][0] == "1-Mar", r
    # E-SMDB-2231 shape: free-text column names with more spaces than tabs
    space_heavy = (
        b"\t".join(f"RUS R5NG 2/9/03 {i} h (2).MBA".encode() for i in range(60))
        + b"\n"
        + b"\t".join(f"{i}.1".encode() for i in range(60))
        + b"\n"
        + b"\t".join([b"1-Mar"] + [b"0"] * 59)
    )
    r = scan_text(io.BytesIO(space_heavy), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "1-Mar", r
    # GSE266500's "Collection.Date" (this repo spells it collection_date) slipped past an exact-string blocklist check
    dotted_header = (
        b"gene\tval\tCollection.Date\n"
        + b"".join(f"GENE{i}\t1\t11/9/2020\n".encode() for i in range(60))
        + b"1-Mar\t1\t11/9/2020\n"
    )
    r = scan_text(io.BytesIO(dotted_header), dates)
    assert r["n_hits"] == 1 and r["examples"][0][0] == "1-Mar", r
    print("ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--demo"]:
        demo()
    else:
        raise SystemExit(main())
