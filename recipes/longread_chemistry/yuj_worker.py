"""
Fleet worker: pull one long-read run's chemistry evidence from its submitted
BAM header, if it has one.

yuj appends the run accession as argv[-1]. Stdlib only -- the hosts have bare
python3, same constraint as recipes/excel_genes/yuj_worker.py. Two network
calls: ENA filereport for submitted_ftp (find a .bam), then a Range GET on
the BAM's own header. No samtools dependency -- the BAM header is a plain
gzip/BGZF stream (concatenated gzip members), so the stdlib gzip module reads
it directly; see _bam_header_text.

Writes results/<run_accession>.json: {"run_accession", "status", ...fields}.
status is "exact" (chemistry tag found), "no_bam" (no submitted BAM),
"no_tag" (BAM header has neither vendor tag), or "error" (network exhausted
retries -- NOT the same as no_bam, and must not be read as one downstream).
"""

from __future__ import annotations

import gzip
import io
import json
import os
import re
import struct
import sys
import time
import urllib.error
import urllib.request
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
FILEREPORT = "https://www.ebi.ac.uk/ena/portal/api/filereport"
BAM_HEADER_BYTE_CAP = int(os.environ.get("LRC_HEADER_CAP", 512_000))
TIMEOUT = 30
UA = {"User-Agent": "seqout-longread-chem/1 (contact: seqout@gmail.com)"}
RETRY_SLEEP = (5, 20, 60)

ONT_MODEL_RE = re.compile(r"basecall_model=(\S+)")
ONT_MODEL_SPLIT_RE = re.compile(
    r"^(?P<moltype>dna|rna)_r(?P<pore>[\d.]+)_(?P<kit>e[\d.]+)_"
    r"(?:(?P<speed>\d+bps)_)?(?P<tier>\w+)@(?P<basecall_model_version>v[\d.]+)$"
)
ONT_PU_RE = re.compile(r"[\t ]PU:(\S+)")
ONT_PG_RE = re.compile(r"@PG\t.*\bPN:(dorado|guppy|bonito)\b.*?\bVN:(\S+)")

PB_TAGS_RE = re.compile(
    r"(BINDINGKIT|SEQUENCINGKIT|BASECALLERVERSION|SMRTCELLKIT)=([^;]+)"
)
PB_PM_RE = re.compile(r"[\t ]PM:(\S+)")
PB_CM_RE = re.compile(r"[\t ]CM:(\S+)")
PB_PG_RE = re.compile(r"@PG\t.*\bPN:(ccs|bam2bam|baz2bam)\b.*?\bVN:(\S+)")


def _get(url: str, headers: dict | None = None) -> bytes:
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    last: Exception | None = None
    for wait in (*RETRY_SLEEP, None):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:  # noqa: S310
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or wait is None:
                raise
            last = e
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as e:
            if wait is None:
                raise
            last = e
        time.sleep(wait)
    raise last or RuntimeError("unreachable")  # pragma: no cover


def submitted_bam_url(run_acc: str) -> str | None:
    body = _get(
        f"{FILEREPORT}?accession={run_acc}&result=read_run&fields=submitted_ftp"
    ).decode("utf-8", "replace")
    lines = body.strip().splitlines()
    if len(lines) < 2:
        return None
    field = lines[1].split("\t", 1)[-1] if "\t" in lines[1] else ""
    bams = [f for f in field.split(";") if f.lower().endswith(".bam")]
    return bams[0] if bams else None


def _bam_header_text(raw: bytes, max_text: int = 2_000_000) -> str | None:
    """BAM is BGZF (concatenated gzip members) wrapping BAM\\1 + l_text + text.
    None if the byte cap cut the stream before the header text was fully read.
    """
    gz = gzip.GzipFile(fileobj=io.BytesIO(raw))
    try:
        magic = gz.read(4)
        if magic != b"BAM\x01":
            return None
        (l_text,) = struct.unpack("<i", gz.read(4))
        text = gz.read(min(l_text, max_text))
        if len(text) < min(l_text, max_text):
            return None  # truncated by our own byte cap
        return text.decode("utf-8", "replace")
    except (EOFError, OSError, struct.error, zlib.error):
        return None


def parse_chemistry(header: str) -> dict[str, str] | None:
    """Every field the header's @RG/@PG lines yield, plus a 'chemistry' and
    'raw_evidence' summary. None if neither platform's tag is present."""
    ont = ONT_MODEL_RE.search(header)
    if ont:
        model_str = ont.group(1)
        fields: dict[str, str] = {}
        m = ONT_MODEL_SPLIT_RE.match(model_str)
        if m:
            fields["ont_pore"] = f"r{m.group('pore')}"
            fields["ont_kit"] = m.group("kit")
            if m.group("speed"):
                fields["ont_speed_bps"] = m.group("speed").removesuffix("bps")
            fields["ont_model_tier"] = m.group("tier")
            fields["ont_basecall_model_version"] = m.group("basecall_model_version")
        pu = ONT_PU_RE.search(header)
        if pu:
            fields["flow_cell_id"] = pu.group(1)
        pg = ONT_PG_RE.search(header)
        if pg:
            fields["basecaller_software"] = pg.group(1)
            fields["basecaller_software_version"] = pg.group(2)
        return {**fields, "chemistry": f"ONT {model_str}", "raw_evidence": model_str}

    pb = dict(PB_TAGS_RE.findall(header))
    if pb:
        fields = {}
        if "BINDINGKIT" in pb:
            fields["pacbio_binding_kit"] = pb["BINDINGKIT"]
        if "SEQUENCINGKIT" in pb:
            fields["pacbio_sequencing_kit"] = pb["SEQUENCINGKIT"]
        if "SMRTCELLKIT" in pb:
            fields["pacbio_smrtcell_kit"] = pb["SMRTCELLKIT"]
        if "BASECALLERVERSION" in pb:
            fields["pacbio_basecaller_version"] = pb["BASECALLERVERSION"]
        pm = PB_PM_RE.search(header)
        if pm:
            fields["pacbio_platform_model"] = pm.group(1)
        cm = PB_CM_RE.search(header)
        if cm:
            fields["pacbio_chemistry_code"] = cm.group(1)
        pg = PB_PG_RE.search(header)
        if pg:
            fields["basecaller_software"] = pg.group(1)
            fields["basecaller_software_version"] = pg.group(2)
        raw = ";".join(f"{k}={v}" for k, v in sorted(pb.items()))
        return {**fields, "chemistry": f"PacBio {raw}", "raw_evidence": raw}
    return None


def check_one(run_acc: str) -> dict:
    try:
        bam_url = submitted_bam_url(run_acc)
    except Exception as e:  # noqa: BLE001 -- network exhausted, not "no bam"
        return {
            "run_accession": run_acc,
            "status": "error",
            "error": f"{type(e).__name__}: {e}",
        }
    if not bam_url:
        return {"run_accession": run_acc, "status": "no_bam"}

    full_url = bam_url if bam_url.startswith("http") else f"https://{bam_url}"
    try:
        raw = _get(full_url, headers={"Range": f"bytes=0-{BAM_HEADER_BYTE_CAP}"})
    except Exception as e:  # noqa: BLE001
        return {
            "run_accession": run_acc,
            "status": "error",
            "error": f"{type(e).__name__}: {e}",
            "bam_url": bam_url,
        }

    header = _bam_header_text(raw)
    if header is None:
        return {
            "run_accession": run_acc,
            "status": "error",
            "error": "header truncated by byte cap",
            "bam_url": bam_url,
        }
    fields = parse_chemistry(header)
    if not fields:
        return {"run_accession": run_acc, "status": "no_tag", "bam_url": bam_url}
    return {"run_accession": run_acc, "status": "exact", "bam_url": bam_url, **fields}


def main() -> int:
    if len(sys.argv) < 2:
        sys.stderr.write("usage: yuj_worker.py <RUN_ACCESSION>\n")
        return 2
    run_acc = sys.argv[-1]
    out_dir = Path(os.environ.get("LRC_OUT_DIR", HERE / "results"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{run_acc}.json"
    if out.exists():
        return 0
    result = check_one(run_acc)
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(result))
    tmp.rename(out)
    sys.stderr.write(f"{run_acc}: {result['status']}\n")
    return 0


def demo() -> None:
    ont_header = (
        "@HD\tVN:1.6\n"
        "@RG\tID:x\tPU:FAY31496\tDS:basecall_model=dna_r10.4.1_e8.2_400bps_sup@v5.2.0\n"
        "@PG\tID:basecaller\tPN:dorado\tVN:1.1.1+e72f1492\n"
    )
    r = parse_chemistry(ont_header)
    assert r is not None
    assert r["ont_pore"] == "r10.4.1", r
    assert r["ont_kit"] == "e8.2", r
    assert r["ont_speed_bps"] == "400", r
    assert r["ont_model_tier"] == "sup", r
    assert r["flow_cell_id"] == "FAY31496", r
    assert r["basecaller_software"] == "dorado", r
    assert r["basecaller_software_version"] == "1.1.1+e72f1492", r

    pb_header = (
        "@HD\tVN:1.6\n"
        "@RG\tID:x\tPM:REVIO\tCM:R/P2-C2/5.0-25M\t"
        "DS:BINDINGKIT=103-496-900;SEQUENCINGKIT=103-496-700;"
        "SMRTCELLKIT=102-202-200;BASECALLERVERSION=5.0\n"
        "@PG\tID:ccs\tPN:ccs\tVN:8.7.0\n"
    )
    r = parse_chemistry(pb_header)
    assert r is not None
    assert r["pacbio_platform_model"] == "REVIO", r
    assert r["pacbio_chemistry_code"] == "R/P2-C2/5.0-25M", r
    assert r["pacbio_binding_kit"] == "103-496-900", r
    assert r["basecaller_software_version"] == "8.7.0", r

    assert parse_chemistry("@HD\tVN:1.6\n") is None

    # a real BAM: BGZF magic + "BAM\1" + l_text + text, gzip-compressed.
    payload = b"BAM\x01" + struct.pack("<i", len(ont_header)) + ont_header.encode()
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(payload)
    header = _bam_header_text(buf.getvalue())
    assert header == ont_header, header
    assert _bam_header_text(buf.getvalue()[: len(buf.getvalue()) // 2]) is None
    print("ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--demo"]:
        demo()
    else:
        raise SystemExit(main())
