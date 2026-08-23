"""
Fetch mode of inheritance for MONDO diseases.

HPO bridges OMIM and Orphanet bridges OrphaCode through MONDO xrefs. Labels can
mention a mode without declaring inheritance.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from helpers import exec_query_from_file, send_req, sqlite_connect
from ontology.extract import _load_obo_graph
from ontology.extract import download as download_ontology
from ontology.sources import SOURCES

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

#: Official HPO annotation release. purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa
#: redirects here; direct target works behind proxies that skip redirects.
#:
#: "latest" is unstable. The file preamble release date is stored per row;
#: --hpoa-url pins a repeat build.
HPOA_URL = (
    "https://github.com/obophenotype/human-phenotype-ontology/"
    "releases/latest/download/phenotype.hpoa"
)

#: Orphanet's natural history product. It covers diseases known through Orphanet
#: xrefs in addition to HPO's OMIM-keyed rows.
#:
#: The file has no versioned URL; store the JDBOR root date per row.
ORPHANET_URL = "https://www.orphadata.com/data/xml/en_product9_ages.xml"

MONDO = next(s for s in SOURCES if s.name == "mondo")

#: phenotype.hpoa column order. The '#' preamble is followed by positional data;
#: a shifted `aspect` column yields empty inheritance rows.
_DB_ID, _HPO_ID, _ASPECT = 0, 3, 10

#: HP inheritance terms mapped to UI labels. Penetrance terms under
#: HP:0000005 modify a mode and are excluded. Unmapped terms are logged as
#: warnings.
MODES = {
    "HP:0000006": "Autosomal dominant",
    "HP:0000007": "Autosomal recessive",
    "HP:0001417": "X-linked",
    "HP:0001419": "X-linked recessive",
    "HP:0001423": "X-linked dominant",
    "HP:0001450": "Y-linked",
    "HP:0001427": "Mitochondrial",
    "HP:0001426": "Multifactorial",
    "HP:0010982": "Polygenic",
    "HP:0010983": "Oligogenic",
    "HP:0010984": "Digenic",
    "HP:0003743": "Genetic anticipation",
    "HP:0003745": "Sporadic",
    "HP:0001442": "Somatic mosaicism",
    "HP:0001470": "Sex-limited autosomal dominant",
    "HP:0001475": "Male-limited autosomal dominant",
    "HP:0001466": "Contiguous gene syndrome",
    "HP:0025352": "Autosomal dominant germline de novo mutation",
    "HP:0012275": "Autosomal dominant somatic cell mutation",
    "HP:0032382": "Uniparental disomy",
    "HP:0032384": "Uniparental isodisomy",
    "HP:0034341": "Autosomal dominant with paternal imprinting",
    "HP:0034344": "Autosomal dominant with maternal imprinting",
}

#: Modifiers HPO files under the same aspect. Known and skipped on purpose.
_NOT_A_MODE = {"HP:0003829", "HP:0003744", "HP:0003831", "HP:4000158"}

#: Orphanet vocabulary mapped to the same labels HPO uses.
#:
#: "Not applicable" is a real answer for no Mendelian pattern.
#: "Unknown", "No data available" and "Not yet documented" are blanks.
ORPHA_MODES = {
    "Autosomal dominant": "Autosomal dominant",
    "Autosomal recessive": "Autosomal recessive",
    "X-linked dominant": "X-linked dominant",
    "X-linked recessive": "X-linked recessive",
    "Y-linked": "Y-linked",
    "Mitochondrial inheritance": "Mitochondrial",
    "Multigenic/multifactorial": "Multifactorial",
    "Oligogenic": "Oligogenic",
    "Semi-dominant": "Semi-dominant",
    "Not applicable": "Not applicable",
}
_ORPHA_BLANK = {"Unknown", "No data available", "Not yet documented"}

_INSERT = """
INSERT INTO disease_inheritance (
    mondo_id, modes, hpo_ids, omim_ids, orpha_ids, sources,
    hpoa_version, hpo_version, orphanet_version, fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(mondo_id) DO UPDATE SET
    modes            = excluded.modes,
    hpo_ids          = excluded.hpo_ids,
    omim_ids         = excluded.omim_ids,
    orpha_ids        = excluded.orpha_ids,
    sources          = excluded.sources,
    hpoa_version     = excluded.hpoa_version,
    hpo_version      = excluded.hpo_version,
    orphanet_version = excluded.orphanet_version,
    fetched_at       = excluded.fetched_at
"""


def hpoa_versions(text: str) -> tuple[str | None, str | None]:
    """Read the release stamps out of the file's own '#' preamble."""
    hpoa = hpo = None
    for line in text.splitlines():
        if not line.startswith("#"):
            break
        if line.startswith("#version:"):
            hpoa = line.split(":", 1)[1].strip()
        elif line.startswith("#hpo-version:"):
            hpo = line.split(":", 1)[1].strip()
    return hpoa, hpo


def inheritance_by_omim(text: str) -> dict[str, set[str]]:
    """OMIM id to the HP inheritance terms annotated against it."""
    out: dict[str, set[str]] = defaultdict(set)
    unmapped: set[str] = set()
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) <= _ASPECT or fields[_ASPECT] != "I":
            continue
        hpo = fields[_HPO_ID]
        if hpo in _NOT_A_MODE:
            continue
        if hpo not in MODES:
            unmapped.add(hpo)
            continue
        out[fields[_DB_ID]].add(hpo)
    if unmapped:
        logger.warning(
            "unmapped HPO inheritance terms (add to MODES or _NOT_A_MODE): %s",
            ", ".join(sorted(unmapped)),
        )
    return out


def inheritance_by_orpha(xml_text: str) -> tuple[dict[str, set[str]], str | None]:
    """OrphaCode to its inheritance labels, plus the release date on the root."""
    root = ET.fromstring(xml_text)  # noqa: S314 - orphadata, not user input
    out: dict[str, set[str]] = defaultdict(set)
    unmapped: set[str] = set()
    for disorder in root.iter("Disorder"):
        code = disorder.findtext("OrphaCode")
        if not code:
            continue
        for entry in disorder.iter("TypeOfInheritance"):
            name = (entry.findtext("Name") or "").strip()
            if not name or name in _ORPHA_BLANK:
                continue
            if name not in ORPHA_MODES:
                unmapped.add(name)
                continue
            out[f"Orphanet:{code}"].add(ORPHA_MODES[name])
    if unmapped:
        logger.warning(
            "unmapped Orphanet inheritance labels (add to ORPHA_MODES or "
            "_ORPHA_BLANK): %s",
            ", ".join(sorted(unmapped)),
        )
    return out, root.attrib.get("date")


def xrefs_by_mondo(
    ontology_dir: Path, prefixes: tuple[str, ...]
) -> dict[str, dict[str, set[str]]]:
    """
    MONDO curie to cross-references, bucketed by prefix.

    All requested prefixes share one ontology parse.
    """
    path = download_ontology(MONDO, ontology_dir, workers=4, force=False)
    out: dict[str, dict[str, set[str]]] = {p: defaultdict(set) for p in prefixes}
    for node in _load_obo_graph(path)["nodes"]:
        node_id = node.get("id", "")
        meta = node.get("meta") or {}
        if "/MONDO_" not in node_id or meta.get("deprecated"):
            continue
        curie = "MONDO:" + node_id.rsplit("MONDO_", 1)[1]
        for xref in meta.get("xrefs") or []:
            val = xref.get("val", "")
            for prefix in prefixes:
                if val.startswith(prefix):
                    out[prefix][curie].add(val)
    return out


def fetch_inheritance(
    output_db_file: Path,
    ontology_dir: Path,
    timeout: int = 120,
    num_retries: int = 5,
    hpoa_url: str = HPOA_URL,
    orphanet_url: str = ORPHANET_URL,
) -> None:
    """
    Land one inheritance row per MONDO term reached by HPO or Orphanet.

    HPO bridges OMIM; Orphanet bridges OrphaCodes. Some terms are reachable
    through only one bridge.
    """
    hpoa = _get(hpoa_url, timeout, num_retries)
    hpoa_version, hpo_version = hpoa_versions(hpoa)
    by_omim = inheritance_by_omim(hpoa)
    logger.info(
        "hpoa %s: %d OMIM diseases carry a mode of inheritance",
        hpoa_version,
        len(by_omim),
    )

    orpha_xml = _get(orphanet_url, timeout, num_retries)
    by_orpha, orpha_version = inheritance_by_orpha(orpha_xml)
    logger.info(
        "orphanet %s: %d disorders carry a mode of inheritance",
        orpha_version,
        len(by_orpha),
    )

    xrefs = xrefs_by_mondo(ontology_dir, ("OMIM:", "Orphanet:"))
    omim_xrefs, orpha_xrefs = xrefs["OMIM:"], xrefs["Orphanet:"]
    logger.info(
        "mondo: %d terms xref OMIM, %d xref Orphanet",
        len(omim_xrefs),
        len(orpha_xrefs),
    )

    fetched_at = datetime.now(UTC).isoformat()
    rows = []
    for mondo in set(omim_xrefs) | set(orpha_xrefs):
        hpo_ids = sorted(
            {h for o in omim_xrefs.get(mondo, ()) for h in by_omim.get(o, ())}
        )
        orpha_modes = {
            m for o in orpha_xrefs.get(mondo, ()) for m in by_orpha.get(o, ())
        }
        modes = {MODES[h] for h in hpo_ids} | orpha_modes
        if not modes:
            continue
        sources = []
        if hpo_ids:
            sources.append("hpoa")
        if orpha_modes:
            sources.append("orphanet")
        rows.append(
            (
                mondo,
                ";".join(sorted(modes)),
                ";".join(hpo_ids),
                ";".join(sorted(o for o in omim_xrefs.get(mondo, ()) if o in by_omim)),
                ";".join(
                    sorted(o for o in orpha_xrefs.get(mondo, ()) if o in by_orpha)
                ),
                ";".join(sources),
                hpoa_version,
                hpo_version,
                orpha_version,
                fetched_at,
            )
        )

    output_db_file.parent.mkdir(parents=True, exist_ok=True)
    with sqlite_connect(output_db_file) as conn:
        exec_query_from_file("tables/inheritance.sql", conn=conn)
        exec_query_from_file("indexes/inheritance.sql", conn=conn)
        conn.executemany(_INSERT, rows)
        conn.commit()
    both = sum(1 for r in rows if r[5] == "hpoa;orphanet")
    logger.info(
        "inheritance: staged %d MONDO terms (%d from both sources); "
        "hpoa %s, orphanet %s",
        len(rows),
        both,
        hpoa_version,
        orpha_version,
    )


def _get(url: str, timeout: int, num_retries: int) -> str:
    """One retrying GET. Both sources redirect, so redirects are followed."""
    return send_req(
        "GET",
        url,
        headers={"User-Agent": f"seqout-ingestion (+{url})"},
        timeout=timeout,
        num_retries=num_retries,
        max_wait=60,
        follow_redirects=True,
    ).text
