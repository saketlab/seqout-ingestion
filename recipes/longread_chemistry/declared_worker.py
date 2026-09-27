"""
Fleet worker: mine an ONT run's ENA-declared library_construction_protocol /
experimental_protocol free text for a kit code or flow-cell/pore version.

yuj appends the run accession as argv[-1]. Stdlib only, same constraint as
yuj_worker.py. One network call: ENA filereport for the two protocol fields
-- no BAM download, this is author-declared text, not sequencer-measured
evidence, so it lands at chemistry_confidence='declared', never 'exact'.

Piloted 2026-09-09: submitted-BAM headers, FASTQ read comments, and MinKNOW
summary sidecars all measured 0% yield for ONT at scale (see memory
ont-chemistry-unrecoverable). This field is populated for ~20% of ONT runs
and, when present, is usually explicit ("SQK-LSK109", "R9.4.1 flowcells") --
authors write down what they used far more often than the archive retains
sequencer-embedded evidence.

Writes results/<run_accession>.json: {"run_accession", "status", ...fields}.
status is "declared" (kit or flow-cell parsed out), "no_match" (protocol
text present but nothing recognized), "empty" (ENA has no protocol text for
this run), or "error" (network exhausted retries).
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
FILEREPORT = "https://www.ebi.ac.uk/ena/portal/api/filereport"
TIMEOUT = 30
UA = {"User-Agent": "seqout-longread-chem/1 (contact: seqout@gmail.com)"}
RETRY_SLEEP = (5, 20, 60)

# Library prep / barcoding kits: SQK-xxxx###, EXP-xxx###, or the bare kit
# family some depositors write without the SQK- prefix ("LSK109|LIGATION").
KIT_RE = re.compile(
    r"\b(?:SQK|EXP)-[A-Z]{2,5}\d{2,4}\b"
    r"|\b(?:LSK|RBK|RAD|PCB|NBD|ULK|RPB|VSK)-?\d{2,4}\b",
    re.I,
)
# Flow-cell / pore chemistry: FLO-xxx### product code, or the pore version
# authors state directly ("R9.4.1 flowcells", "R10.4.1").
FLOWCELL_RE = re.compile(r"\bFLO-[A-Z]{2,5}\d{2,4}\b|\bR\d{1,2}\.\d(?:\.\d)?\b", re.I)
BASECALLER_RE = re.compile(
    r"\b(Guppy|Dorado|Bonito|MinKNOW)\b[^\d]{0,10}"
    r"\(?v?(\d+\.\d+(?:\.\d+)?(?:[+.]\w+)?)\)?",
    re.I,
)


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers=UA)
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


def fetch_protocol_text(run_acc: str) -> str:
    body = _get(
        f"{FILEREPORT}?accession={run_acc}&result=read_run"
        "&fields=library_construction_protocol,experimental_protocol"
    ).decode("utf-8", "replace")
    lines = body.strip().splitlines()
    if len(lines) < 2:
        return ""
    fields = lines[1].split("\t")
    return " ".join(f for f in fields[1:] if f).strip()


def parse_declared_chemistry(text: str) -> dict[str, str] | None:
    """Kit/flow-cell/basecaller mentions in author-written protocol text.

    None when nothing recognized -- absence here is not evidence of
    absence, the study may just not have named its kit in this field.
    """
    kit = KIT_RE.search(text)
    flowcell = FLOWCELL_RE.search(text)
    if not kit and not flowcell:
        return None

    fields: dict[str, str] = {}
    if kit:
        fields["ont_kit"] = kit.group(0).upper()
    if flowcell:
        fields["ont_pore"] = flowcell.group(0).upper()
    bc = BASECALLER_RE.search(text)
    if bc:
        fields["basecaller_software"] = bc.group(1).lower()
        fields["basecaller_software_version"] = bc.group(2)

    if kit and flowcell:
        chemistry = f"ONT {fields['ont_pore']} ({fields['ont_kit']})"
    elif flowcell:
        chemistry = f"ONT {fields['ont_pore']} (kit not stated)"
    else:
        chemistry = f"ONT kit {fields['ont_kit']} (flow cell not stated)"

    return {**fields, "chemistry": chemistry, "raw_evidence": text[:500]}


def check_one(run_acc: str) -> dict:
    try:
        text = fetch_protocol_text(run_acc)
    except Exception as e:  # network exhausted, not "empty"
        return {
            "run_accession": run_acc,
            "status": "error",
            "error": f"{type(e).__name__}: {e}",
        }
    if not text:
        return {"run_accession": run_acc, "status": "empty"}
    fields = parse_declared_chemistry(text)
    if not fields:
        return {"run_accession": run_acc, "status": "no_match"}
    return {"run_accession": run_acc, "status": "declared", **fields}


def main() -> int:
    if len(sys.argv) < 2:
        sys.stderr.write("usage: declared_worker.py <RUN_ACCESSION>\n")
        return 2
    run_acc = sys.argv[-1]
    out_dir = Path(os.environ.get("LRC_OUT_DIR", HERE / "declared_results"))
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
    r = parse_declared_chemistry("LSK109|LIGATION")
    assert r is not None
    assert r["ont_kit"] == "LSK109", r
    assert "ont_pore" not in r, r
    assert r["chemistry"] == "ONT kit LSK109 (flow cell not stated)", r

    r = parse_declared_chemistry(
        "RNA Libraries were constructed using the PCR_cDNA sequencing "
        "barcoding kit (SQK-PCB109, Oxford Nanopore) as per manufacturer "
        "instructions"
    )
    assert r is not None
    assert r["ont_kit"] == "SQK-PCB109", r

    r = parse_declared_chemistry(
        "Sequenced on two R9.4.1 flowcells using the MinION Mk1C, MinKNOW "
        "software (v 3.3.2). Base-calling was performed by Guppy (v 3.1.5)."
    )
    assert r is not None
    assert r["ont_pore"] == "R9.4.1", r
    assert "ont_kit" not in r, r
    assert r["chemistry"] == "ONT R9.4.1 (kit not stated)", r
    # first basecaller-shaped match wins; both are legitimate mentions here
    assert r["basecaller_software"] in ("minknow", "guppy"), r

    r = parse_declared_chemistry("LSK-109 EXP-NBD096|LIGATION")
    assert r is not None
    assert r["ont_kit"] in ("LSK-109", "EXP-NBD096"), r

    assert parse_declared_chemistry("") is None
    assert parse_declared_chemistry("NV-NSPHL-321304_01") is None
    print("ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--demo"]:
        demo()
    else:
        raise SystemExit(main())
