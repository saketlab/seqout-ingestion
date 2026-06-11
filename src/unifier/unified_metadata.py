import csv
import json
import logging
import sqlite3
from collections import Counter, defaultdict
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


def _scan_geo(db_file: Path) -> SourceMaps:
    maps = SourceMaps()

    # mapping each sample to a list of organisms
    samples_to_organisms: dict[str, set[str]] = defaultdict(set[str])
    # counting number of samples present in each series
    series_sample_cnt: Counter[str] = Counter()
    # mapping each each series' to a counter of organisms present in all of its samples
    series_organism_cnt: dict[str, Counter[str]] = defaultdict(Counter[str])

    # mapping series' accession id to organizations
    accession_to_org: dict[str, list[str]] = defaultdict(list[str])
    # for deduping organization names
    seen_orgs: dict[str, set[str]] = defaultdict(set[str])

    with sqlite3.Connection(db_file) as conn:
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
                        samples_to_organisms[acc].add(text.strip())
                elif isinstance(organism, str) and organism.strip():
                    samples_to_organisms[acc].add(organism.strip())

        logging.info("\t%d samples with organism", len(samples_to_organisms))

        # scanning geo_series
        rows = conn.execute(
            "SELECT accession, samples_ref, series_type, published_at, pubmed_id, pubmed_id_pysradb FROM geo_series WHERE samples_ref IS NOT NULL"
        ).fetchall()

        series_to_samples = {
            item[0]: [v["@ref"] for v in json.loads(item[1])] for item in rows
        }

        for series_acc, samples in series_to_samples.items():
            for sample_acc in samples:
                series_sample_cnt[series_acc] += 1
                organisms = samples_to_organisms[sample_acc]

                for org in organisms:
                    series_organism_cnt[series_acc][org] += 1

        for (
            _,
            _,
            series_type,
            published_at,
            pubmed_id_json,
            pubmed_id_pysradb,
        ) in rows:
            if series_type:
                try:
                    parsed = json.loads(series_type)
                    if isinstance(parsed, list):
                        maps.library[acc] = {str(t): 1 for t in parsed if t}
                    elif parsed:
                        maps.library[acc] = {str(parsed): 1}
                except (json.JSONDecodeError, TypeError):
                    maps.library[acc] = {series_type: 1}

            if published_at:
                maps.published[acc] = published_at

            if pubmed_id_json:
                try:
                    pmids = json.loads(pubmed_id_json)
                    if isinstance(pmids, list):
                        for p in pmids:
                            s = str(p).strip()
                            if s:
                                maps.pmids.setdefault(acc, set()).add(s)
                    elif pmids:
                        s = str(pmids).strip()
                        if s:
                            maps.pmids.setdefault(acc, set()).add(s)
                except (json.JSONDecodeError, TypeError):
                    pass

            if pubmed_id_pysradb:
                s = str(pubmed_id_pysradb).strip()
                if s:
                    maps.pmids.setdefault(acc, set()).add(s)

        logging.info(
            f"\t{len(maps.library)} series with library type, {len(maps.published)} with date, {len(maps.pmids)} with pmids"
        )

        maps.organism = {k: dict(v) for k, v in series_organism_cnt.items()}
        maps.n_exp = dict(series_sample_cnt)
        maps.n_samp = dict(series_sample_cnt)

        # scanning geo_contributors
        rows = conn.execute(
            "SELECT accession, organization FROM geo_contributors WHERE organization IS NOT NULL AND organization != '' AND organization != 'GEO' AND organization != 'NCBI/NLM/NIH' ORDER BY accession"
        ).fetchall()

        for accession, organization in rows:
            organization = organization.strip().lower()
            if not organization:
                continue

            if organization not in seen_orgs[accession]:
                seen_orgs[accession].add(organization)
                accession_to_org[accession].append(organization)

        maps.center = accession_to_org
        logging.info(f"\t{len(maps.center)} series with center")

    return maps


def _scan_sra(db_file: Path):
    maps = SourceMaps()

    submission_to_organisms: dict[str, Counter[str]] = defaultdict(Counter[str])
    submission_sample_cnt: dict[str, int] = defaultdict(int)
    study_to_library: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_exp_cnt: dict[str, int] = defaultdict(int)

    with sqlite3.Connection(db_file) as conn:
        # scanning sra_samples
        rows = conn.execute(
            "SELECT submission, scientific_name FROM sra_samples WHERE submission IS NOT NULL"
        ).fetchall()

        for submission, name in rows:
            submission_sample_cnt[submission] += 1

            if name and name.strip():
                submission_to_organisms[submission][name] += 1

        logging.info(
            "\t%d submissions with organism, %d submissions with samples",
            len(submission_to_organisms),
            len(submission_sample_cnt),
        )

        # scanning sra_experiments
        rows = conn.execute(
            "SELECT study, library_strategy FROM sra_experiments WHERE study IS NOT NULL"
        ).fetchall()

        for study, lib_strategy in rows:
            study_exp_cnt[study] += 1

            if lib_strategy and lib_strategy.strip():
                study_to_library[study][lib_strategy] += 1

        logging.info(
            "\t%d studies with experiments, %d studies with library strategies",
            len(study_exp_cnt),
            len(study_to_library),
        )

        # scanning sra_studies
