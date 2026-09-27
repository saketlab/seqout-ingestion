"""The union-find in sql/matviews/build_study_canonical.py.

Offline. Guards the one thing a naive implementation gets wrong: a shared
BioProject id (whether stored under the bioproject or ena alias key) must
never merge two unrelated studies.
"""

import importlib.util
import pathlib

import pytest

_SCRIPT = (
    pathlib.Path(__file__).resolve().parent.parent
    / "sql"
    / "matviews"
    / "build_study_canonical.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("build_study_canonical", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover - psycopg absent in some envs
        pytest.skip(f"build_study_canonical not importable: {exc}")
    return mod


def test_build_mapping():
    mod = _load()
    rows = [
        ("GSE164471", "geo", {"ena": "PRJNA690800", "sra": "SRP300916"}),
        ("SRP300916", "sra", {"geo": "GSE164471"}),
        # two unrelated studies sharing one bioproject must NOT merge
        ("GSE1", "geo", {"bioproject": "PRJNA1"}),
        ("GSE2", "geo", {"bioproject": "PRJNA1"}),
        # unified_catalog.py also stores an ENA *project* accession under the
        # "ena" key (see the module docstring); sharing one there is the same
        # many-to-one risk and must NOT merge either
        ("GSE3", "geo", {"ena": "PRJNA2"}),
        ("GSE4", "geo", {"ena": "PRJNA2"}),
    ]
    mapping = mod.build_mapping(rows)
    assert mapping["GSE164471"] == "GSE164471"
    assert mapping["SRP300916"] == "GSE164471"  # geo outranks sra
    assert mapping["GSE1"] == "GSE1"
    assert mapping["GSE2"] == "GSE2"
    assert mapping["GSE3"] == "GSE3"
    assert mapping["GSE4"] == "GSE4"
