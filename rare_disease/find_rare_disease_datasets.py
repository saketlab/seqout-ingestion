#!/usr/bin/env python
"""
List the studies in the corpus annotated with a rare disease.

Reads the disease list fetch_rare_diseases.py writes and matches it against the
curated per-sample `disease` field in Postgres, then rolls the hits up to studies.

Why that field and not the study text. `enriched_samples_v4.disease` is a closed
vocabulary of ~22.6k values over 12.1M annotated samples, btree-indexed, so the
match is an equality join rather than a guess at prose. A full-text sweep of study
titles reaches studies the enrichment has not covered, but pays for it in false
positives ("... unlike cystic fibrosis, ...") and cannot say which samples.

Matching. Both sides are folded with fetch_rare_diseases.normalize, and each
disease brings its MONDO label and exact synonyms as alternate spellings, so
"alpha-galactosidase A deficiency" lands on Fabry disease. Synonyms shorter than
--min-chars are dropped: MONDO carries initialisms like "FD" that would otherwise
match half the corpus.

Connection. Postgres on seqoutdb wants a password over TCP, so the 15432 tunnel
cannot be used unattended. Forwarding to the server's *unix socket* instead makes
the connection peer-authenticated (the peer credential is the ssh user), which
needs no password. This opens that tunnel if it is not already up, and only reads.

    uv run python rare_disease/find_rare_disease_datasets.py
"""

from __future__ import annotations

import argparse
import csv
import logging
import socket
import subprocess
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch_rare_diseases import normalize

logger = logging.getLogger("rare_disease.datasets")

SSH_HOST = "sramap@167.114.208.177"
PG_SOCKET = "/var/run/postgresql/.s.PGSQL.5432"
# 15432 is the TCP-to-TCP tunnel and prompts for a password.
LOCAL_PORT = 15433
PG_DB = "pysradb"

VOCABULARY = """
SELECT disease, count(*) AS n
FROM enriched_samples_v4
WHERE nullif(btrim(disease), '') IS NOT NULL
GROUP BY 1
"""

STUDIES = """
SELECT s.disease,
       s.study_accession,
       count(*) AS n_samples_matched,
       st.title,
       st.n_samples,
       st.single_cell_modality
FROM enriched_samples_v4 s
LEFT JOIN enriched_studies_v4 st ON st.accession = s.study_accession
WHERE s.disease = ANY(%s)
GROUP BY 1, 2, 4, 5, 6
"""

HEADER = (
    "study_accession",
    "title",
    "n_samples",
    "n_samples_matched",
    "single_cell_modality",
    "disease",
    "mondo_id",
    "matched_value",
)


def ensure_tunnel(port: int, host: str, *, timeout: int) -> None:
    """Open the socket-forward if nothing is listening on port yet."""
    with socket.socket() as s:
        s.settimeout(2)
        if s.connect_ex(("127.0.0.1", port)) == 0:
            logger.info("tunnel already up on 127.0.0.1:%d", port)
            return

    logger.info("opening tunnel 127.0.0.1:%d -> %s:%s", port, host, PG_SOCKET)
    subprocess.run(  # noqa: S603
        [  # noqa: S607 -- ssh is resolved off PATH, on purpose
            "ssh",
            "-f",
            "-N",
            "-o",
            "BatchMode=yes",
            "-o",
            "ExitOnForwardFailure=yes",
            "-L",
            f"{port}:{PG_SOCKET}",
            host,
        ],
        check=True,
        timeout=timeout,
    )


def read_terms(
    path: Path,
    *,
    min_chars: int,
    rare_only: bool,
) -> dict[str, tuple[str, str]]:
    """
    Map each normalized search term to the (disease, mondo_id) that owns it.

    First writer wins, so a term shared by two diseases is credited to the one
    sorted first rather than duplicated across both.
    """
    terms: dict[str, tuple[str, str]] = {}
    with path.open() as fh:
        for row in csv.DictReader(fh):
            if rare_only and row["mondo_rare"] != "1":
                continue
            for term in row["search_terms"].split(";"):
                if len(term) >= min_chars:
                    terms.setdefault(term, (row["nord_name"], row["mondo_id"]))
    return terms


def main() -> int:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--diseases", type=Path, default=here / "rare_diseases.csv")
    p.add_argument("--out", type=Path, default=here / "rare_disease_datasets.csv")
    p.add_argument(
        "--unmatched-out",
        type=Path,
        default=here / "unmatched_disease_values.csv",
        help="vocabulary values no rare disease claimed, biggest first",
    )
    p.add_argument("--port", type=int, default=LOCAL_PORT)
    p.add_argument("--ssh-host", default=SSH_HOST)
    p.add_argument(
        "--min-chars",
        type=int,
        default=6,
        help="skip search terms shorter than this",
    )
    p.add_argument(
        "--rare-only",
        action="store_true",
        help="keep only diseases in MONDO's 'rare' subset",
    )
    p.add_argument("--log-level", default="info")
    args = p.parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.diseases.exists():
        logger.error("%s not found -- run fetch_rare_diseases.py first", args.diseases)
        return 1

    terms = read_terms(
        args.diseases,
        min_chars=args.min_chars,
        rare_only=args.rare_only,
    )
    logger.info("%d search terms from %s", len(terms), args.diseases)

    ensure_tunnel(args.port, args.ssh_host, timeout=60)
    dsn = f"postgresql://sramap@127.0.0.1:{args.port}/{PG_DB}"

    with psycopg.connect(dsn) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute(VOCABULARY)
            vocabulary = cur.fetchall()
            logger.info(
                "%d distinct disease values in enriched_samples_v4",
                len(vocabulary),
            )

            matched: dict[str, tuple[str, str]] = {}
            unmatched: list[tuple[str, int]] = []
            for value, n in vocabulary:
                hit = terms.get(normalize(value))
                if hit:
                    matched[value] = hit
                else:
                    unmatched.append((value, n))
            logger.info(
                "%d vocabulary values matched a rare disease, %d did not",
                len(matched),
                len(unmatched),
            )

            cur.execute(STUDIES, (list(matched),))
            hits = cur.fetchall()
            logger.info("%d (value, study) pairs", len(hits))

    # One row per study/disease: two spellings of the same disease on one study
    # are the same fact, so their sample counts add instead of duplicating.
    rows: dict[tuple[str, str], list] = {}
    for value, study, n_matched, title, n_samples, modality in hits:
        disease, mondo_id = matched[value]
        key = (study, disease)
        if key in rows:
            rows[key][3] += n_matched
            rows[key][7] += f";{value}"
            continue
        rows[key] = [
            study,
            title or "",
            n_samples,
            n_matched,
            modality or "",
            disease,
            mondo_id,
            value,
        ]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        w.writerows(sorted(rows.values()))

    with args.unmatched_out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(("disease_value", "n_samples"))
        w.writerows(sorted(unmatched, key=lambda r: -r[1]))

    studies = {r[0] for r in rows.values()}
    diseases = {r[5] for r in rows.values()}
    logger.info(
        "wrote %s: %d study/disease pairs, %d studies, %d rare diseases with data",
        args.out,
        len(rows),
        len(studies),
        len(diseases),
    )
    logger.info(
        "wrote %s: %d unclaimed vocabulary values",
        args.unmatched_out,
        len(unmatched),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
