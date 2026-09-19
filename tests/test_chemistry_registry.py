"""Keep the generated chemistry ladder in sync with its source rules."""

import subprocess
import sys
from pathlib import Path

import pytest

from sc_evidence.rules import CHEMISTRIES, chemistry_case_sql

ROOT = Path(__file__).resolve().parents[1]
RENDER = ROOT / "recipes/sc_stats_gap/render_chemistry_case.py"


def test_the_checked_in_sql_matches_the_registry():
    # Detect rule changes without regeneration and manual edits to the ladder.
    r = subprocess.run(  # noqa: S603
        [sys.executable, str(RENDER), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, r.stderr


def test_labels_are_unique_per_series():
    labels = [c.label for c in CHEMISTRIES if c.label]
    assert len(labels) == len(set(labels))


def test_exclusions_carry_a_modality_and_no_label():
    for chem in CHEMISTRIES:
        if chem.modality != "transcriptome":
            assert chem.label is None, (
                f"{chem.modality} should be excluded, not plotted"
            )


def test_generated_sql_uses_the_guarded_regex_not_the_loose_like():
    sql = chemistry_case_sql()
    # Loose SQL LIKE gates also match cellseq; the regex must reject it.
    assert "cel(?!l)" in sql
    assert "bd[-_. ]?rhapsody" in sql
    assert "%cel_seq%" not in sql
    assert "ILIKE" not in sql


@pytest.mark.parametrize(
    ("value", "label"),
    [
        ("CEL-Seq2", "CEL-seq2"),
        ("CEL-Seq", None),
        ("Smart-Seq3", "Smart-seq3"),
        ("Smart-Seq3Xpress", "Smart-seq3"),
        ("Smart-Seq 3", "Smart-seq3"),
        ("Smartseq3 Rna-Seq", "Smart-seq3"),
        ("Smart-Seq2", None),
        ("Smart-Seq V4 Ultra Low Input Rna Kit", None),
        ("Quartz-Seq2", "Quartz-seq2"),
        ("Quartz-Seq", None),
        ("cellseq", None),
        ("BD Rhapsody", "BD Rhapsody"),
        ("Rhapsody cultivar", None),
        ("sci-RNA-Seq3", "sci-RNA-seq"),
        ("Scirna-Seq3", "sci-RNA-seq"),
        ("ascii-rna", None),
        ("Parse Biosciences Evercode WT", "SPLiT-seq/Parse"),
        ("parsed reads", None),
        ("SPLiT-Seq", "SPLiT-seq/Parse"),
        ("scATAC-seq", None),
        ("Visium HD", None),
        ("10x Multiome", None),
    ],
)
def test_first_matching_rule_wins(value, label):
    """Mirror the SQL CASE in Python: same order, same regexes."""
    import re

    got = None
    for chem in CHEMISTRIES:
        if any(re.search(t.regex, value, re.IGNORECASE) for t in chem.terms):
            got = chem.label
            break
    assert got == label
