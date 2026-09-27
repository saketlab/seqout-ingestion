"""
Compound lexicon for drug-perturbation detection, built from ChEBI.

A treatment value counts as a compound only if a token in it is a ChEBI name or
synonym of a compound that has a drug-like role (drug, pharmaceutical, enzyme
inhibitor, agonist, antagonist, immunosuppressant, antimicrobial). No model is
involved, so every hit traces to a ChEBI id.

Only single-token names are kept. The SQL tokenises a value on
`[^a-z0-9-]+`, so multi-word names could never match; `is_token` is the one
definition of what survives. Curated corrections live in lexicon_overrides.tsv
and always win over ChEBI.

Usage: python -m perturbation.lexicon chebi.obo out.tsv
"""

from __future__ import annotations

import csv
import logging
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

logger = logging.getLogger(__name__)
OVERRIDES_PATH = Path(__file__).with_name("lexicon_overrides.tsv")

__all__ = [
    "KINDS",
    "OVERRIDES_PATH",
    "ROLE_ROOTS",
    "build_rows",
    "is_token",
    "load_overrides",
    "parse_obo",
    "role_closure",
]

HAS_ROLE = "RO:0000087"
# pharmaceutical (covers drug and antineoplastic agent), enzyme inhibitor,
# agonist, antagonist, immunosuppressive agent, antimicrobial agent
ROLE_ROOTS = frozenset(
    {
        "CHEBI:52217",
        "CHEBI:23924",
        "CHEBI:48705",
        "CHEBI:48706",
        "CHEBI:35705",
        "CHEBI:33281",
    }
)
MIN_LEN = 5
KINDS = frozenset({"compound", "biologic", "inducer", "stimulus", "exclude"})
_TOKEN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SYN = re.compile(r'^synonym: "(?P<text>.*)" (?P<scope>EXACT|RELATED) (?P<rest>.*)$')
# RELATED synonyms are noise (brand names collide with words: "Pathway",
# "Integrin"); only WHO INN names are safe
_RELATED_OK = ("chebi/INN",)
# cytokines, LPS-like stimuli and checkpoint targets are not compounds
_NOT_COMPOUND = re.compile(
    r"^(il-?\d|ifn|interferon|tnf|tgf|[gm]-?csf|gm-?csf|egf|fgf|vegf|pd-?l?1|ctla"
    r"|lipopolysaccharide|ionomycin|phorbol|oncostatin|cytokine)"
)


def is_token(name: str) -> bool:
    return len(name) >= MIN_LEN and bool(_TOKEN.match(name)) and not name.isdigit()


def parse_obo(lines: Iterable[str]) -> dict[str, dict]:
    """Parse [Term] stanzas into id -> {name, is_a, roles, syns}."""
    terms: dict[str, dict] = {}
    cur: dict | None = None
    for raw in lines:
        line = raw.rstrip("\n")
        if line == "[Term]":
            cur = {"name": "", "is_a": set(), "roles": set(), "syns": [], "dead": False}
        elif line.startswith("[") or (not line and cur is None):
            cur = None
        elif cur is None:
            continue
        elif line.startswith("id: "):
            terms[line[4:]] = cur
        elif line.startswith("name: "):
            cur["name"] = line[6:]
        elif line.startswith("is_a: "):
            cur["is_a"].add(line[6:].split(" ")[0])
        elif line.startswith("relationship: " + HAS_ROLE):
            cur["roles"].add(line.split(" ")[2])
        elif line.startswith("is_obsolete: true"):
            cur["dead"] = True
        elif line.startswith("synonym: "):
            m = _SYN.match(line)
            if m:
                cur["syns"].append((m["text"], m["scope"], m["rest"]))
    return terms


def role_closure(terms: dict[str, dict], roots: Iterable[str] = ROLE_ROOTS) -> set[str]:
    """Every role id at or below a root, following is_a downwards."""
    children: dict[str, list[str]] = {}
    for tid, t in terms.items():
        for parent in t["is_a"]:
            children.setdefault(parent, []).append(tid)
    seen, stack = set(roots), list(roots)
    while stack:
        for child in children.get(stack.pop(), ()):
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen


def _names(term: dict) -> Iterator[tuple[str, str]]:
    yield term["name"], "name"
    for text, scope, rest in term["syns"]:
        if "IUPAC" in rest:
            continue
        if scope == "EXACT" or any(k in rest for k in _RELATED_OK):
            yield text, "synonym"


def build_rows(terms: dict[str, dict]) -> list[tuple[str, str, str, str]]:
    """
    (token, chebi_id, label, source) rows, one per token.

    A token claimed by several compounds goes to the direct name over a
    synonym, then to the lowest ChEBI number, so the output is reproducible.
    """
    roles = role_closure(terms)
    best: dict[str, tuple[int, int, str, str, str]] = {}
    for tid, t in terms.items():
        if t["dead"] or not t["roles"] & roles:
            continue
        for text, source in _names(t):
            token = text.lower().strip()
            if not is_token(token) or _NOT_COMPOUND.match(token):
                continue
            key = (source != "name", int(tid.split(":")[1]))
            if token not in best or key < best[token][:2]:
                best[token] = (*key, tid, t["name"], source)
    return sorted((tok, v[2], v[3], v[4]) for tok, v in best.items())


def load_overrides(path: Path) -> dict[str, str]:
    """
    Read curated token to kind rows.

    Unlike ChEBI names these may be short (chir, dapt), but must still be one
    SQL-tokenisable token of a known kind.
    """
    with path.open(newline="") as f:
        rows = csv.DictReader(
            (ln for ln in f if not ln.startswith("#")), delimiter="\t"
        )
        out = {r["token"].lower(): r["kind"] for r in rows}
    for token, kind in out.items():
        if not _TOKEN.match(token) or kind not in KINDS:
            msg = f"bad override {token!r} ({kind!r}) in {path.name}"
            raise ValueError(msg)
    return out


def main(argv: list[str]) -> None:
    src, dst = Path(argv[1]), Path(argv[2])
    with src.open() as f:
        rows = build_rows(parse_obo(f))
    overrides = load_overrides(OVERRIDES_PATH)
    out: dict[str, tuple[str, str, str, str]] = {
        tok: ("compound", cid, label, source) for tok, cid, label, source in rows
    }
    for tok, kind in overrides.items():
        _, cid, label, _ = out.get(tok, ("", "", tok, ""))
        out[tok] = (kind, cid, label, "override")
    with dst.open("w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["token", "kind", "chebi_id", "label", "source"])
        for tok in sorted(out):
            kind, cid, label, source = out[tok]
            w.writerow([tok, kind, cid, label, source])
    logger.info("%d tokens (%d overrides) -> %s", len(out), len(overrides), dst)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main(sys.argv)
