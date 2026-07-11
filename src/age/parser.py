"""
Parse free-text sample ``age`` into a canonical numeric ``age_days``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

DAYS_PER_YEAR = 365.25
DAYS_PER_MONTH = 365.25 / 12  # ~30.4375
DAYS_PER_WEEK = 7.0
HOURS_PER_DAY = 24.0

# A bare integer this small is implausible as an hour count (< ~1 day) and more
# likely a mis-encoded age; but 0 is the known junk placeholder. We still treat
# all non-zero bare numbers as hours (confirmed empirically) and only special-case 0.
_UNSPECIFIED = {
    "",
    "na",
    "n/a",
    "nan",
    "none",
    "null",
    "unknown",
    "unspecified",
    "age unspecified",
    "not collected",
    "not applicable",
    "not available",
    "missing",
    "not determined",
    "nd",
}

# Qualitative stage words
_QUALITATIVE = (
    ("neonate", "neonate"),
    ("newborn", "neonate"),
    ("infant", "infant"),
    ("juvenile", "child"),
    ("pediatric", "child"),
    ("paediatric", "child"),
    ("child", "child"),
    ("adolescent", "child"),
    ("elderly", "aged"),
    ("geriatric", "aged"),
    ("aged", "aged"),
    ("adult", "adult"),
)

# Prenatal stage words
_PRENATAL_WORDS = (
    "fetus",
    "foetus",
    "fetal",
    "foetal",
    "embryo",
    "embryonic",
    "blastocyst",
    "gastrula",
    "zygote",
    "morula",
    "prenatal",
    "oocyte",
)

_NUM = r"\d+(?:\.\d+)?"
_BARE_RE = re.compile(rf"^{_NUM}$")
_INEQ_RE = re.compile(r"^\s*(?:<=|>=|<|>|≤|≥|~)\s*(.+)$")
_RANGE_RE = re.compile(rf"^({_NUM})\s*[-]\s*({_NUM})\s*([A-Za-z]*)$")
_FW_RE = re.compile(rf"^({_NUM})\s*FW$", re.IGNORECASE)  # fetal weeks
_EDAY_RE = re.compile(r"^E\d+(?:\.\d+)?$", re.IGNORECASE)  # embryonic day, e.g. E14
_YM_RE = re.compile(
    rf"^({_NUM})\s*(?:Y|YR|YRS|YEARS?)(?:\s*({_NUM})\s*(?:M|MO|MONTHS?))?$",
    re.IGNORECASE,
)
_UNIT_RE = re.compile(
    rf"^({_NUM})\s*(Y|YR|YRS|YEARS?|M|MO|MONTHS?|W|WK|WKS|WEEKS?|D|DAYS?|H|HR|HRS|HOURS?)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class AgeNorm:
    """Normalized age; age_days is NULL for junk/qualitative/prenatal/unparseable."""

    age_days: float | None
    age_group: str | None
    prenatal: bool
    age_raw: str


def _unit_to_days(value: float, unit: str) -> float | None:
    u = unit.lower()
    if u.startswith("y"):
        return value * DAYS_PER_YEAR
    if u in {"m", "mo"} or u.startswith("month"):
        return value * DAYS_PER_MONTH
    if u.startswith(("w", "wk")):
        return value * DAYS_PER_WEEK
    if u.startswith("d"):
        return value
    if u.startswith(("h", "hr", "hour")):
        return value / HOURS_PER_DAY
    return None


def _num(text: str) -> float:
    return round(float(text), 3)


def parse_age(raw: str) -> AgeNorm:
    """Normalize a raw age string to :class:`AgeNorm`. Never raises."""
    if raw is None:  # defensive: callers pass str, but be total
        return AgeNorm(None, None, False, "")
    s = raw.strip()
    low = s.lower()

    if low in _UNSPECIFIED:
        return AgeNorm(None, "unspecified", False, raw)

    # Prenatal: stage words, embryonic-day (E14), or fetal-weeks (16FW).
    if any(w in low for w in _PRENATAL_WORDS) or _EDAY_RE.match(s) or _FW_RE.match(s):
        return AgeNorm(None, "fetal", True, raw)

    # Inequality (<1D, >90Y): parse the boundary value, keep its unit.
    m = _INEQ_RE.match(s)
    if m:
        inner = parse_age(m.group(1))
        return AgeNorm(inner.age_days, inner.age_group, inner.prenatal, raw)

    # Range (55-59Y, 5-12M, 30-39, 8-10FW): midpoint of the two ends, shared unit.
    m = _RANGE_RE.match(s)
    if m:
        lo, hi, unit = _num(m.group(1)), _num(m.group(2)), m.group(3)
        mid = (lo + hi) / 2
        if unit.lower() == "fw":
            return AgeNorm(None, "fetal", True, raw)
        if unit:
            days = _unit_to_days(mid, unit)
            out = round(days, 3) if days is not None else None
            return AgeNorm(out, None, False, raw)
        # bare range with no unit -> treat as years (age ranges like 30-39, 55-59)
        return AgeNorm(round(mid * DAYS_PER_YEAR, 3), None, False, raw)

    # Bare number -> hours (confirmed); 0 is the junk placeholder.
    if _BARE_RE.match(s):
        v = _num(s)
        if v == 0:
            return AgeNorm(None, None, False, raw)
        return AgeNorm(round(v / HOURS_PER_DAY, 3), None, False, raw)

    # Years (+ optional months): 30Y6M, 53Y, 1.5Y.
    m = _YM_RE.match(s)
    if m:
        days = _num(m.group(1)) * DAYS_PER_YEAR
        if m.group(2):
            days += _num(m.group(2)) * DAYS_PER_MONTH
        return AgeNorm(round(days, 3), None, False, raw)

    # Single unit: 3M, 10W, <1D already stripped, 8760H.
    m = _UNIT_RE.match(s)
    if m:
        days = _unit_to_days(_num(m.group(1)), m.group(2))
        return AgeNorm(round(days, 3) if days is not None else None, None, False, raw)

    # Qualitative stage words -> coarse group, no number.
    for word, group in _QUALITATIVE:
        if word in low:
            return AgeNorm(None, group, False, raw)

    # Unparseable: never guess.
    return AgeNorm(None, None, False, raw)


if __name__ == "__main__":
    cases = {
        "53Y": (19358.25, None, False),
        "30Y6M": (11140.125, None, False),
        "8760.0": (365.0, None, False),  # hours -> days
        "1344.0": (56.0, None, False),  # 8 weeks (mouse)
        "3M": (91.312, None, False),
        "10W": (70.0, None, False),
        "168.0": (7.0, None, False),
        "55-59Y": (20819.25, None, False),  # midpoint 57Y
        "5-12M": (258.719, None, False),  # midpoint 8.5 months
        "<1D": (1.0, None, False),
        ">90Y": (32872.5, None, False),
        "1.5Y": (547.875, None, False),
        "0.0": (None, None, False),  # junk placeholder
        "0": (None, None, False),
        "Age unspecified": (None, "unspecified", False),
        "unknown": (None, "unspecified", False),
        "Adult": (None, "adult", False),
        "Children": (None, "child", False),
        "Neonate": (None, "neonate", False),
        "Fetus": (None, "fetal", True),
        "16FW": (None, "fetal", True),
        "8-10FW": (None, "fetal", True),
        "E14": (None, "fetal", True),
        "Blastocyst stage": (None, "fetal", True),
        "gibberish!!": (None, None, False),
        "": (None, "unspecified", False),
    }
    for raw, (days, group, prenatal) in cases.items():
        r = parse_age(raw)
        assert r.prenatal == prenatal, (raw, r)
        assert r.age_group == group, (raw, r)
        if days is None:
            assert r.age_days is None, (raw, r)
        else:
            assert r.age_days is not None and abs(r.age_days - days) < 1.0, (raw, r)
    print("parse_age self-check OK")
