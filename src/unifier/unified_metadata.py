import csv
import json
import logging
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SourceMap:
    organism: dict[str, dict[str, int]] = field(default_factory=dict)
    library: dict[str, dict[str, int]] = field(default_factory=dict)
    n_exp: dict[str, int] = field(default_factory=dict)
    n_samp: dict[str, int] = field(default_factory=dict)
    center: dict[str, set[str]] = field(default_factory=dict)
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

    geo_maps = _scan_geo(geo_db_file)


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

    logging.info(f"\t{len(pubmed_mapping)} entries loaded from pubmed mapping")
    return pubmed_mapping


def _load_assay_mapping(csv_file: Path) -> dict[str, tuple[str, str]]:
    logging.info("loading assay mapping...")
    with open(csv_file) as f:
        assay_mapping = {
            row["raw_term"]: (row["canonical_assay"], row["category"])
            for row in csv.DictReader(f)
        }

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


def _scan_geo(db_file: Path) -> SourceMap:
    map = SourceMap()
    conn = sqlite3.connect(db_file)

    sample_to_organisms: dict[str, set[str]] = defaultdict(set[str])
    samples_in_series_counter: Counter[str] = Counter()
    organisms_in_series_counter: dict[str, Counter[str]] = defaultdict(Counter[str])
    accession_to_organization: dict[str, set[str]] = defaultdict(set[str])

    # scanning geo_samples
    rows = conn.execute(
        "SELECT accession, channels FROM geo_samples WHERE channels IS NOT NULL"
    ).fetchall()

    for acc, channels_str in rows:
        channels = json.loads(channels_str)

        for ch in channels:
            organism = ch.get("Organism")
            if isinstance(organism, dict):
                text = organism.get("#text", "")
                if text:
                    sample_to_organisms[acc].add(text.strip())
            elif isinstance(organism, str) and organism.strip():
                sample_to_organisms[acc].add(organism.strip())

    logging.info("\t%d samples with organism", len(sample_to_organisms))

    # scanning geo_series
    rows = conn.execute(
        "SELECT accession, samples_ref, series_type, published_at, pubmed_id, pubmed_id_pysradb FROM geo_series WHERE samples_ref IS NOT NULL"
    ).fetchall()

    series_to_samples = {
        item[0]: [v["@ref"] for v in json.loads(item[1])] for item in rows
    }

    for series_acc, samples in series_to_samples.items():
        for sample_acc in samples:
            samples_in_series_counter[series_acc] += 1
            organisms = sample_to_organisms[sample_acc]

            for org in organisms:
                organisms_in_series_counter[series_acc][org] += 1

    for (
        accession,
        samples_ref,
        series_type,
        published_at,
        pubmed_id_json,
        pubmed_id_pysradb,
    ) in rows:
        if series_type:
            try:
                parsed_series_type = json.loads(series_type)
                if isinstance(parsed_series_type, list):
                    for st in parsed_series_type:
                        if not str(st):
                            continue
                        map.library[accession][str(st)] += 1
            except (json.JSONDecodeError, TypeError):
                map.library[accession][series_type] = 1

        if published_at:
            map.published[accession] = published_at

        if pubmed_id_json:
            try:
                pmids = json.loads(pubmed_id_json)
                if isinstance(pmids, list):
                    for p in pmids:
                        s = str(p).strip()
                        if s:
                            map.pmids[accession].add(s)
                elif pmids:
                    s = str(pmids).strip()
                    map.pmids[accession].add(s)
            except (json.JSONDecodeError, TypeError):
                pass

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                map.pmids[accession].add(s)

    logging.info(
        f"\t{len(map.library)} series with library type, {len(map.published)} series with date, {len(map.pmids)} series with pmids"
    )

    # scanning geo_contributors
    rows = conn.execute(
        "SELECT accession, organization FROM geo_contributors WHERE organization IS NOT NULL AND organization != '' AND organization != 'GEO' AND organization != 'NCBI/NLM/NIH' ORDER BY accession"
    ).fetchall()

    for accession, organization in rows:
        organization = organization.strip().lower()
        if not organization:
            continue

        accession_to_organization[accession].add(organization)

    # map.library and map.published populated in line within the above loop
    map.organism = {k: dict(v) for k, v in organisms_in_series_counter.items()}
    map.n_exp = dict(samples_in_series_counter)
    map.n_samp = dict(samples_in_series_counter)
    map.center = accession_to_organization

    conn.close()
    return map


def _scan_sra(db_file: Path) -> SourceMap:
    map = SourceMap()
    conn = sqlite3.connect(db_file)

    submission_to_organisms: dict[str, set[str]] = defaultdict(set[str])
    samples_in_submission_counter: Counter[str] = Counter()

    # scanning sra_samples

    conn.close()
    return map
