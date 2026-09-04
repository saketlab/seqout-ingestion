"""
Pick the mitochondrial genes out of a feature list.

Species comes from the features themselves: stable-ID prefixes are unambiguous,
symbol shape is the fallback. Same order of preference as sextimator's
DetectSpecies, reimplemented here because the fleet hosts get python and nothing
else.
"""

from __future__ import annotations

import re

# stable-ID prefix -> scientific name. A prefix match is definitive.
ID_PREFIX: dict[str, str] = {
    "ENSG": "Homo sapiens",
    "ENSMUSG": "Mus musculus",
    "ENSRNOG": "Rattus norvegicus",
    "ENSDARG": "Danio rerio",
    "ENSGALG": "Gallus gallus",
    "ENSSSCG": "Sus scrofa",
    "ENSBTAG": "Bos taurus",
    "ENSCAFG": "Canis lupus familiaris",
    "ENSMMUG": "Macaca mulatta",
    "ENSXETG": "Xenopus tropicalis",
    "FBgn": "Drosophila melanogaster",
    "WBGene": "Caenorhabditis elegans",
    "AT[1-5MC]G": "Arabidopsis thaliana",
}

# the trailing separator matters; a bare MT prefix pulls in nuclear MTOR and
# MTRNR.
_SYMBOL_RX = re.compile(r"^(MT-|MT_|MT\.)", re.IGNORECASE)

# Symbols that carry no separator but are mitochondrial in the standard
# nomenclature of at least one species we see.
_EXTRA = {
    "MTATP6",
    "MTATP8",
    "MTCO1",
    "MTCO2",
    "MTCO3",
    "MTCYB",
    "MTND1",
    "MTND2",
    "MTND3",
    "MTND4",
    "MTND4L",
    "MTND5",
    "MTND6",
    "MTRNR1",
    "MTRNR2",
}

_VERSION_RX = re.compile(r"\.[0-9]+$")

# built from ID_PREFIX so a new species stays a one-line edit; the values are
# patterns (AT[1-5MC]G), not literal prefixes, hence a regex.
_ID_PREFIX_RX = re.compile("^(?:" + "|".join(ID_PREFIX) + ")")


def has_id_prefix(feature: str) -> bool:
    """Say whether the label carries a known stable-ID prefix."""
    return bool(_ID_PREFIX_RX.match(strip_version(feature)))


def is_mito(feature: str) -> bool:
    """Whether one feature is mitochondrial. mito_mask is the vectorised form."""
    name = strip_version(feature).strip()
    return bool(_SYMBOL_RX.match(name)) or name.upper() in _EXTRA


def strip_version(feature: str) -> str:
    """ENSG00000229807.13 and ENSG00000229807 are the same gene."""
    return _VERSION_RX.sub("", feature)


def detect_species(features: list[str]) -> tuple[str | None, str]:
    """Return (scientific name, how it was decided)."""
    counts: dict[str, int] = {}
    for f in features[:5000]:
        stripped = strip_version(f)
        for prefix, name in ID_PREFIX.items():
            if re.match(prefix, stripped):
                counts[name] = counts.get(name, 0) + 1
                break
    if counts:
        best = max(counts, key=lambda k: counts[k])
        return best, "id_prefix"
    return None, "none"


def mito_mask(features: list[str]) -> list[bool]:
    """
    Mark each feature that is mitochondrial.

    Symbol-based, so it works whether or not the species resolved. An
    Ensembl-ID matrix with no symbol column yields no hits, which is reported
    as an absent measurement rather than zero percent.
    """
    return [is_mito(f) for f in features]
