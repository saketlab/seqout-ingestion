"""
Fleet worker for `pentimento preflight run`, one study per invocation.

Runs on borrowed hosts with bare python3 plus the preflightx binary. yuj appends
the accession as argv[-1].
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from preflight import annotate_dir  # deployed flat, beside this file

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
_ARGV_MIN = 2


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.warning("%s is not an integer; using %d", name, default)
        return default


def main() -> int:
    if len(sys.argv) < _ARGV_MIN:
        sys.stderr.write("usage: yuj_worker.py <ACCESSION>\n")
        return 2
    accession = sys.argv[-1]
    exe = os.environ.get("PREFLIGHTX_EXE", str(HERE / "preflightx"))
    if not Path(exe).is_file():
        # Missing scanner is fatal; zero results remain a real scan outcome.
        sys.stderr.write(f"FATAL: preflightx not found at {exe}\n")
        return 3
    counts = annotate_dir(
        Path(os.environ.get("PREFLIGHT_IN_DIR", HERE / "manifests")),
        Path(os.environ.get("PREFLIGHT_OUT_DIR", HERE / "annotated")),
        exe,
        _int_env("PREFLIGHT_N_READS", 20_000),
        _int_env("PREFLIGHT_MAX_RUNS", 1),
        _int_env("PREFLIGHT_DEEP_N_READS", 400_000),
        _int_env("PREFLIGHT_DEEP_MAX_RUNS", 2),
        accession,
        _int_env("PREFLIGHT_THREADS", 1),
    )
    logger.info("preflight %s: %s", accession, json.dumps(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
