"""
Fleet worker for embedding, one shard of the work list per invocation.

Takes a JSONL shard of {accession, source, text} and writes {accession, vector}
to a .npz beside it. Knows nothing about which host it runs on: the fleet lives
in the gitignored yuj/ directory, so this module is the same whether it is
driven by yuj, by ssh, or run in place.

    python3 yuj_worker.py <shard.jsonl>

yuj appends the work item, so the shard is always argv[-1].
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_MODEL = os.environ.get("EMBED_MODEL", "abhinand/MedEmbed-large-v0.1")
BATCH = int(os.environ.get("EMBED_BATCH", "64"))

_EXIT_USAGE = 2
_EXIT_MISSING_DEPENDENCY = 3
_EXIT_NO_INPUT = 4


def _fatal(message: str, code: int) -> int:
    # loud and non-zero: a host that cannot embed must not report success
    sys.stderr.write(f"FATAL: {message}\n")
    return code


def main() -> int:
    if len(sys.argv) < 2:  # noqa: PLR2004
        sys.stderr.write("usage: yuj_worker.py <shard.jsonl>\n")
        return _EXIT_USAGE

    shard = Path(sys.argv[-1])
    if not shard.is_file():
        return _fatal(f"no such shard: {shard}", _EXIT_NO_INPUT)

    out = shard.with_suffix(".npz")
    if out.exists():
        logger.info("%s already done, skipping", out.name)
        return 0

    try:
        import numpy as np
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        return _fatal(f"embedding deps missing: {exc}", _EXIT_MISSING_DEPENDENCY)

    rows = [json.loads(line) for line in shard.read_text().splitlines() if line.strip()]
    if not rows:
        return _fatal(f"{shard} holds no rows", _EXIT_NO_INPUT)

    logger.info("loading %s", DEFAULT_MODEL)
    model = SentenceTransformer(DEFAULT_MODEL, device="cuda", trust_remote_code=True)

    logger.info("embedding %d texts from %s", len(rows), shard.name)
    vectors = model.encode(
        [r["text"] for r in rows],
        batch_size=BATCH,
        show_progress_bar=True,
        normalize_embeddings=True,
    )

    # write beside the shard under a temp name; a killed run must not leave a
    # short .npz that the skip-if-exists check above would trust
    tmp = out.with_suffix(".npz.partial")
    # savez_compressed appends .npz to a name that lacks it, so the temp path it
    # actually writes is not the one we rename; hand it an open file instead.
    with tmp.open("wb") as fh:
        np.savez_compressed(
            fh,
            accessions=np.array([r["accession"] for r in rows]),
            sources=np.array([r["source"] for r in rows]),
            vectors=np.asarray(vectors, dtype="float32"),
        )
    tmp.replace(out)
    logger.info("wrote %s", out.name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
