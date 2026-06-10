import csv
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SourceMaps:
    organism: dict[str, dict[str, int]] = field(default_factory=dict)
    library: dict[str, dict[str, int]] = field(default_factory=dict)
    n_exp: dict[str, int] = field(default_factory=dict)
    n_samp: dict[str, int] = field(default_factory=dict)
    center: dict[str, list[str]] = field(default_factory=dict)
    pmids: dict[str, set[str]] = field(default_factory=dict)
    published: dict[str, str] = field(default_factory=dict)


def build_unified_metadata(
    geo_db_file: Path,
    sra_db_file: Path,
    ena_db_file: Path,
    ae_db_file: Path,
    unified_catalog_db_file: Path,
    pubmed_metadata_db_file: Path,
    assay_mapping_csv_file: Path,
    taxonomy_mapping_csv_file: Path,
):
    unified_catalog = _load_unified_catalog(unified_catalog_db_file)
    pubmed_mapping = _load_pubmed_metadata(pubmed_metadata_db_file)
    assay_mapping = _load_assay_mapping(assay_mapping_csv_file)
    taxonomy_mapping = _load_taxonomy_mapping(taxonomy_mapping_csv_file)


def _load_unified_catalog(db_file: Path) -> list[tuple[str, str, str, str, dict]]:
    logging.info("loading unified catalog...")
    with sqlite3.Connection(db_file) as conn:
        catalog = conn.execute(
            "SELECT canonical_accession, source, title, description, aliases FROM unified_studies"
        ).fetchall()
    logging.info(f"\t{len(catalog)} entries loaded from unified catalog")

    return catalog


def _load_pubmed_metadata(db_file: Path) -> dict[str, tuple[str, int]]:
    logging.info("loading pubmed mapping...")
    with sqlite3.Connection(db_file) as conn:
        rows = conn.execute(
            "SELECT pmid, journal, citation_count FROM pubmed_metadata"
        ).fetchall()

    pubmed_mapping = {
        str(pmid): (journal, int(citation_count))
        for pmid, journal, citation_count in rows
    }
    return pubmed_mapping


def _load_assay_mapping(csv_file: Path) -> dict[str, tuple[str, str]]:
    logging.info("loading assay mapping...")
    assay_mapping = {}
    with open(csv_file) as f:
        for row in csv.DictReader(f):
            assay_mapping[row["raw_term"]] = (row["canonical_assay"], row["category"])
    logging.info(f"\t{len(assay_mapping)} entries loaded from assay mapping")
    return assay_mapping


def _load_taxonomy_mapping(csv_file: Path) -> dict[str, str]:
    logging.info("loading taxonomy common names mapping...")
    taxonomy_mapping = {}
    with open(csv_file) as f:
        for row in csv.DictReader(f):
            name = row["scientific_name"].strip().lower()
            common = row["common_name"].strip()
            if name and common:
                taxonomy_mapping[name] = common
    logging.info(
        f"\t{len(taxonomy_mapping)} entries loaded from taxonomy common names mapping"
    )
    return taxonomy_mapping


def _scan_geo(db_file: Path):
    maps = SourceMaps()
    sample_organisms: dict[str, set[str]] = {}

    with sqlite3.Connection(db_file) as conn:
        cursor = conn.execute(
            "SELECT accession, channels FROM geo_samples WHERE channels IS NOT NULL"
        )

        total_samples = 0

        for acc, channels_str in cursor:
            total_samples += 1
            channels = json.loads(channels_str)
            organisms: set[str] = set()

            for ch in channels:
                organism = ch.get("Organism")
                if isinstance(organism, dict):
                    text = organism.get("#text", "")
                    if text:
                        organisms.add(text.strip())
                elif isinstance(organism, str) and organism.strip():
                    organisms.add(organism.strip())

            if organisms:
                sample_organisms[acc] = organisms

        logging.info(
            "\t%d / %d samples have organism", len(sample_organisms), total_samples
        )
