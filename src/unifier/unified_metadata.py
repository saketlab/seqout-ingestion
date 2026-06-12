import csv
import json
import logging
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from helpers import exec_query_from_file


@dataclass
class SourceMap:
    organism: dict[str, dict[str, int]] = field(default_factory=dict)
    library: dict[str, dict[str, int]] = field(default_factory=dict)
    n_exp: dict[str, int] = field(default_factory=dict)
    n_samp: dict[str, int] = field(default_factory=dict)
    center: dict[str, list[str]] = field(default_factory=dict)
    pmids: dict[str, set[str]] = field(default_factory=dict)
    published: dict[str, str] = field(default_factory=dict)


_UNIFIED_METADATA_OUTPUT_DIR = "unified_metadata"


def build_unified_metadata(
    base_output_dir: Path,
    db_file: Optional[Path],
    geo_db_file: Path,
    sra_db_file: Path,
    ena_db_file: Path,
    ae_db_file: Path,
    unified_catalog_db_file: Path,
    pubmed_metadata_db_file: Path,
    assay_mapping_csv_file: Path,
    taxonomy_mapping_csv_file: Path,
    batch_size: int,
) -> tuple[list[dict], list[tuple[str, str, int]]]:
    output_dir = base_output_dir / _UNIFIED_METADATA_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    db_file = db_file or (output_dir / "unified_metadata.db")

    catalog = _load_unified_catalog(unified_catalog_db_file)
    pubmed_map = _load_pubmed_metadata(pubmed_metadata_db_file)
    assay_map = _load_assay_mapping(assay_mapping_csv_file)
    taxonomy = _load_taxonomy_mapping(taxonomy_mapping_csv_file)

    geo = _scan_geo(geo_db_file)
    sra = _scan_sra(sra_db_file)
    ae = _scan_ae(ae_db_file)
    ena = _scan_ena(ena_db_file)

    sources = (geo, sra, ae, ena)
    pmid_map = _merge_pmid_map(sources)
    published_map = _merge_published_map(sources)

    source_data = {"geo": geo, "sra": sra, "ae": ae, "ena": ena}
    rows: list[dict] = []
    centers_rows: list[tuple[str, str, int]] = []
    stats: dict[str, int] = {
        "organism": 0,
        "library_strategy": 0,
        "assay_mapped": 0,
        "center": 0,
        "pmid": 0,
        "journal": 0,
    }

    for acc, source, title, description, aliases_json in catalog:
        sm = source_data[source]

        org_counts, common_counts, dominant_sci, dominant_common = _collapse_organism(
            sm.organism.get(acc), taxonomy
        )
        if org_counts:
            stats["organism"] += 1

        lib_counts, l1_counts, l2_counts, dominant_l1, dominant_l2, mapped = (
            _collapse_library(sm.library.get(acc), assay_map)
        )
        if lib_counts:
            stats["library_strategy"] += 1
        if mapped:
            stats["assay_mapped"] += 1

        n_experiments = sm.n_exp.get(acc)
        n_samples = sm.n_samp.get(acc)

        center_list = sm.center.get(acc)
        center = center_list[0] if center_list else None
        if center_list:
            stats["center"] += 1
            for pos, cn in enumerate(center_list):
                centers_rows.append((acc, cn, pos))

        pmids = pmid_map.get(acc)
        if pmids:
            stats["pmid"] += 1
        pmid, journal, citation_count = _pick_best_pmid(pmids, pubmed_map)
        if journal:
            stats["journal"] += 1

        row = {
            "canonical_accession": acc,
            "source": source,
            "title": title,
            "description": description,
            "aliases": aliases_json,
            "organism_counts": _json_or_none(org_counts),
            "common_name_counts": _json_or_none(common_counts),
            "dominant_scientific_name": dominant_sci,
            "dominant_common_name": dominant_common,
            "library_strategy_counts": _json_or_none(lib_counts),
            "assay_l1_counts": _json_or_none(l1_counts),
            "assay_l2_counts": _json_or_none(l2_counts),
            "dominant_assay_l1": dominant_l1,
            "dominant_assay_l2": dominant_l2,
            "n_experiments": n_experiments,
            "n_samples": n_samples,
            "center_name": center,
            "center_names": _json_or_none(center_list),
            "pmid": pmid,
            "journal": journal,
            "citation_count": citation_count,
            "first_published": published_map.get(acc),
        }
        rows.append(row)

    logging.info(f"enriched {len(rows)} rows")
    for k, v in stats.items():
        logging.info(f"  {k}: {v} ({100 * v // len(rows)}%)" if rows else f"  {k}: {v}")
    logging.info(f"center rows: {len(centers_rows)}")

    with sqlite3.Connection(db_file) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        exec_query_from_file("tables/unified_metadata_tables.sql", conn=conn)
        exec_query_from_file("indexes/unified_metadata_indexes.sql", conn=conn)

        cols = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)

        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            conn.executemany(
                f"INSERT OR IGNORE INTO unified_metadata ({col_names}) VALUES ({placeholders})",
                [tuple(r[c] for c in cols) for r in batch],
            )

        exec_query_from_file("tables/unified_metadata_centers_tables.sql", conn=conn)
        exec_query_from_file("indexes/unified_metadata_centers_indexes.sql", conn=conn)

        for start in range(0, len(centers_rows), batch_size):
            batch = centers_rows[start : start + batch_size]
            conn.executemany(
                "INSERT OR IGNORE INTO unified_metadata_centers "
                "(canonical_accession, center_name, position) VALUES (?, ?, ?)",
                batch,
            )

        conn.commit()

    return rows, centers_rows


def _load_unified_catalog(db_file: Path) -> list[tuple[str, str, str, str, dict]]:
    logging.info("loading unified catalog...")
    with sqlite3.Connection(db_file) as conn:
        catalog = conn.execute(
            "SELECT canonical_accession, source, title, description, aliases FROM unified_studies"
        ).fetchall()

    logging.info(f"\t{len(catalog)} entries loaded from unified catalog")
    return catalog


def _load_pubmed_metadata(db_file: Path) -> dict[str, tuple[str | None, int | None]]:
    logging.info("loading pubmed mapping...")
    with sqlite3.Connection(db_file) as conn:
        rows = conn.execute(
            "SELECT pmid, journal, citation_count FROM pubmed_metadata"
        ).fetchall()
        pubmed_mapping: dict[str, tuple[str | None, int | None]] = {}
        for pmid, journal, citation_count in rows:
            pmid_str = str(pmid)
            journal_val = str(journal) if journal else None
            cc_val = int(citation_count) if citation_count is not None else None
            pubmed_mapping[pmid_str] = (journal_val, cc_val)

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
    m = SourceMap()
    conn = sqlite3.connect(db_file)

    sample_organisms: dict[str, set[str]] = defaultdict(set[str])
    series_sample_count: Counter[str] = Counter()
    series_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])

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
                    sample_organisms[acc].add(text.strip())
            elif isinstance(organism, str) and organism.strip():
                sample_organisms[acc].add(organism.strip())

    logging.info(f"\t{len(sample_organisms)} samples with organism")

    # scanning geo_series
    rows = conn.execute(
        "SELECT accession, samples_ref, series_type, published_at, pubmed_id, pubmed_id_pysradb FROM geo_series WHERE samples_ref IS NOT NULL"
    ).fetchall()

    series_to_samples = {
        item[0]: [v["@ref"] for v in json.loads(item[1])] for item in rows
    }

    for series_acc, samples in series_to_samples.items():
        for sample_acc in samples:
            series_sample_count[series_acc] += 1
            organisms = sample_organisms.get(sample_acc)
            if organisms:
                for org in organisms:
                    series_organism_count[series_acc][org] += 1

    for (
        accession,
        _,
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
                        m.library[accession][str(st)] += 1
            except (json.JSONDecodeError, TypeError):
                m.library[accession][series_type] = 1

        if published_at:
            m.published[accession] = published_at

        if pubmed_id_json:
            try:
                pmids = json.loads(pubmed_id_json)
                if isinstance(pmids, list):
                    for p in pmids:
                        s = str(p).strip()
                        if s:
                            m.pmids[accession].add(s)
                elif pmids:
                    s = str(pmids).strip()
                    m.pmids[accession].add(s)
            except (json.JSONDecodeError, TypeError):
                pass

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                m.pmids[accession].add(s)

    # scanning geo_contributors
    rows = conn.execute(
        "SELECT accession, organization FROM geo_contributors WHERE organization IS NOT NULL AND organization != '' AND organization != 'GEO' AND organization != 'NCBI/NLM/NIH' ORDER BY accession"
    ).fetchall()

    center_orgs: dict[str, list[str]] = {}
    seen_orgs: dict[str, set[str]] = {}
    for accession, organization in rows:
        organization = organization.strip()
        if not organization:
            continue
        if accession not in center_orgs:
            center_orgs[accession] = []
            seen_orgs[accession] = set()
        org_lower = organization.lower()
        if org_lower not in seen_orgs[accession]:
            seen_orgs[accession].add(org_lower)
            center_orgs[accession].append(organization)

    m.organism = {k: dict(v) for k, v in series_organism_count.items()}
    m.n_exp = dict(series_sample_count)
    m.n_samp = dict(series_sample_count)
    m.center = center_orgs

    conn.close()
    return m


def _scan_sra(db_file: Path) -> SourceMap:
    m = SourceMap()
    conn = sqlite3.connect(db_file)

    submission_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    submission_sample_count: Counter[str] = Counter()
    study_experiment_count: Counter[str] = Counter()
    study_library_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_to_submission: dict[str, str] = defaultdict(str)
    study_to_center: dict[str, str] = defaultdict(str)

    # scanning sra_samples
    rows = conn.execute(
        "SELECT submission, scientific_name FROM sra_samples WHERE submission IS NOT NULL"
    ).fetchall()

    for submission, scientific_name in rows:
        submission_sample_count[submission] += 1
        if not scientific_name:
            continue

        scientific_name = str(scientific_name).strip()
        if not scientific_name:
            continue

        submission_organism_count[submission][scientific_name] += 1

    logging.info(
        f"\t{len(submission_organism_count)} submissions with organisms, {len(submission_sample_count)} submissions with samples"
    )

    # scanning sra_experiments
    rows = conn.execute(
        "SELECT study, library_strategy FROM sra_experiments WHERE study IS NOT NULL"
    ).fetchall()

    for study, strategy in rows:
        study_experiment_count[study] += 1
        if not strategy:
            continue

        strategy = str(strategy).strip()
        if not strategy:
            continue

        study_library_count[study][strategy] += 1

    logging.info(
        f"\t{len(study_library_count)} studies with library strategy, {len(study_experiment_count)} studies with experiments"
    )

    # scanning sra_studies
    rows = conn.execute(
        "SELECT accession, submission, links, published_at, pubmed_id_pysradb, center_name FROM sra_studies"
    ).fetchall()

    for (
        accession,
        submission,
        links_json,
        published_at,
        pubmed_id_pysradb,
        center_name,
    ) in rows:
        if submission:
            study_to_submission[accession] = submission
            org_counts = submission_organism_count.get(submission)
            if org_counts:
                m.organism[accession] = dict(org_counts)
            samp_count = submission_sample_count.get(submission)
            if samp_count:
                m.n_samp[accession] = samp_count

        if center_name:
            cn = center_name.strip()
            if cn:
                study_to_center[accession] = cn

        lib_counts = study_library_count.get(accession)
        if lib_counts:
            m.library[accession] = dict(lib_counts)

        exp_count = study_experiment_count.get(accession)
        if exp_count:
            m.n_exp[accession] = exp_count

        if published_at:
            m.published[accession] = published_at

        if links_json:
            try:
                links = json.loads(links_json)
                xref = links.get("XREF_LINK") if isinstance(links, dict) else None

                if isinstance(xref, dict) and xref.get("DB") == "pubmed":
                    pid = xref.get("ID", "")
                    if pid:
                        m.pmids[accession].add(str(pid).strip())
                elif isinstance(xref, list):
                    for x in xref:
                        if isinstance(x, dict) and x.get("DB") == "pubmed":
                            pid = x.get("ID", "")
                            if pid:
                                m.pmids[accession].add(str(pid).strip())
            except (json.JSONDecodeError, TypeError):
                pass

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                m.pmids[accession].add(s)

    # scanning sra_submissions for center (fallback to study center_name)
    sub_center: dict[str, str] = {}
    cursor = conn.execute(
        "SELECT accession, center_name FROM sra_submissions WHERE center_name IS NOT NULL AND center_name != ''"
    )
    for sub_acc, center_name in cursor:
        sub_center[sub_acc] = center_name.strip()

    for study_acc, submission in study_to_submission.items():
        center = sub_center.get(submission)
        study_cn_val = study_to_center.get(study_acc)
        centers: list[str] = []
        seen: set[str] = set()
        if center:
            centers.append(center)
            seen.add(center.lower())
        if study_cn_val and study_cn_val.lower() not in seen:
            centers.append(study_cn_val)
        if centers:
            m.center[study_acc] = centers
    for study_acc, cn in study_to_center.items():
        if study_acc not in m.center:
            m.center[study_acc] = [cn]

    conn.close()
    return m


def _scan_ae(db_file: Path) -> SourceMap:
    m = SourceMap()
    conn = sqlite3.connect(db_file)

    experiment_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    experiment_library_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    experiment_sample_count: Counter[str] = Counter()

    # scanning arrayexpress_samples
    rows = conn.execute(
        "SELECT experiment_accession, organism, library_strategy FROM arrayexpress_samples"
    ).fetchall()

    for accession, organism, library_strategy in rows:
        experiment_sample_count[accession] += 1

        if organism:
            organism = str(organism).strip()
            if not organism:
                continue

            experiment_organism_count[accession][organism] += 1

        if library_strategy:
            library_strategy = str(library_strategy).strip()
            if not library_strategy:
                continue

            experiment_library_count[accession][library_strategy] += 1

    # scanning arrayexpress_experiments
    rows = conn.execute(
        "SELECT accession, organizations, study_type, pubmed_id, pubmed_id_pysradb, release_date, contact_affiliation FROM arrayexpress_experiments"
    ).fetchall()

    for (
        accession,
        orgs_json,
        study_type,
        pubmed_id,
        pubmed_id_pysradb,
        release_date,
        contact_aff,
    ) in rows:
        centers: list[str] = []
        seen_lower: set[str] = set()

        if orgs_json:
            try:
                orgs = json.loads(orgs_json)
                if not isinstance(orgs, list):
                    continue

                for o in orgs:
                    name = o.get("name", "") if isinstance(o, dict) else ""
                    if not name:
                        continue

                    name = name.strip()
                    nl = name.lower()
                    if nl not in seen_lower:
                        seen_lower.add(nl)
                        centers.append(name)
            except (json.JSONDecodeError, TypeError):
                pass

        if not centers and contact_aff:
            aff = contact_aff.strip()
            if aff:
                centers.append(aff)

        if centers:
            m.center[accession] = centers

        if study_type and accession not in m.library:
            study_type = study_type.strip()
            if study_type:
                m.library[accession] = {study_type: 1}

        if release_date:
            m.published[accession] = release_date

        if pubmed_id:
            s = str(pubmed_id).strip()
            if s:
                m.pmids[accession].add(s)

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                m.pmids[accession].add(s)

    m.organism = {k: dict(v) for k, v in experiment_organism_count.items()}
    m.library = {k: dict(v) for k, v in experiment_library_count.items()}
    m.n_exp = dict(experiment_sample_count)
    m.n_samp = dict(experiment_sample_count)

    conn.close()
    return m


def _scan_ena(db_file: Path) -> SourceMap:
    m = SourceMap()
    conn = sqlite3.connect(db_file)

    study_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_library_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_center_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_sample_count: Counter[str] = Counter()
    study_experiment_count: Counter[str] = Counter()

    # scanning ena_samples
    rows = conn.execute(
        "SELECT study_accession, scientific_name FROM ena_samples WHERE study_accession IS NOT NULL"
    ).fetchall()

    for study_accession, name in rows:
        study_sample_count[study_accession] += 1
        if name:
            name = str(name).strip()
            if not name:
                continue
            study_organism_count[study_accession][name] += 1

    # scanning ena_experiments
    rows = conn.execute(
        "SELECT study_accession, library_strategy, center_name FROM ena_experiments WHERE study_accession IS NOT NULL"
    ).fetchall()

    for study_accession, library_strategy, center_name in rows:
        study_experiment_count[study_accession] += 1

        if library_strategy:
            library_strategy = str(library_strategy).strip()
            if library_strategy:
                study_library_count[study_accession][library_strategy] += 1

        if center_name:
            center_name = str(center_name).strip()
            if center_name:
                study_center_count[study_accession][center_name] += 1

    m.center = {
        acc: [name for name, _ in counts.most_common()]
        for acc, counts in study_center_count.items()
    }

    # scanning ena_studies
    rows = conn.execute(
        "SELECT study_accession, first_public, pubmed_id_pysradb, center_name FROM ena_studies"
    ).fetchall()

    for study_accession, first_public, pubmed_id_pysradb, center_name in rows:
        if first_public:
            m.published[study_accession] = first_public

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                m.pmids[study_accession].add(s)

        if center_name:
            cn = center_name.strip()
            if cn:
                existing = m.center.get(study_accession)
                if existing is None:
                    m.center[study_accession] = [cn]
                elif cn.lower() not in {c.lower() for c in existing}:
                    existing.append(cn)

    m.organism = {k: dict(v) for k, v in study_organism_count.items()}
    m.library = {k: dict(v) for k, v in study_library_count.items()}
    m.n_samp = dict(study_sample_count)
    m.n_exp = dict(study_experiment_count)

    conn.close()
    return m


def _json_or_none(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, dict) and not val:
        return None
    if isinstance(val, list) and not val:
        return None
    return json.dumps(val, ensure_ascii=False, separators=(",", ":"))


def _dominant_key(counts: dict[str, int]) -> str | None:
    if not counts:
        return None
    return max(counts, key=counts.__getitem__)


def _collapse_organism(
    org_counts: dict[str, int] | None,
    taxonomy: dict[str, str],
) -> tuple[dict[str, int] | None, dict[str, int] | None, str | None, str | None]:
    if not org_counts:
        return None, None, None, None
    common: dict[str, int] = {}
    for sci, c in org_counts.items():
        key = taxonomy.get(sci.lower(), sci)
        common[key] = common.get(key, 0) + c
    return org_counts, common, _dominant_key(org_counts), _dominant_key(common)


def _collapse_library(
    lib_counts: dict[str, int] | None,
    assay_map: dict[str, tuple[str, str]],
) -> tuple[
    dict[str, int] | None,
    dict[str, int] | None,
    dict[str, int] | None,
    str | None,
    str | None,
    bool,
]:
    if not lib_counts:
        return None, None, None, None, None, False
    l1: dict[str, int] = {}
    l2: dict[str, int] = {}
    for raw, c in lib_counts.items():
        mapped = assay_map.get(raw)
        if mapped:
            canonical, category = mapped
            l1[category] = l1.get(category, 0) + c
            l2[canonical] = l2.get(canonical, 0) + c
    return lib_counts, l1, l2, _dominant_key(l1), _dominant_key(l2), bool(l1)


def _pick_best_pmid(
    pmids: list[str] | None,
    pubmed_map: dict[str, tuple[str | None, int | None]],
) -> tuple[str | None, str | None, int | None]:
    if not pmids:
        return None, None, None
    best_pmid: str | None = None
    best_journal: str | None = None
    best_citations: int | None = None
    for p in pmids:
        entry = pubmed_map.get(p)
        if entry:
            j, cc = entry
            if best_pmid is None or (
                cc is not None and (best_citations is None or cc > best_citations)
            ):
                best_pmid = p
                best_journal = j
                best_citations = cc
    if best_pmid:
        return (
            best_pmid,
            best_journal,
            best_citations
            if best_citations is not None and best_citations >= 0
            else None,
        )
    return pmids[0], None, None


def _merge_pmid_map(sources: tuple[SourceMap, ...]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for sm in sources:
        for acc, pset in sm.pmids.items():
            merged[acc] = sorted(set(merged.get(acc, [])) | pset)
    return merged


def _merge_published_map(sources: tuple[SourceMap, ...]) -> dict[str, str]:
    merged: dict[str, str] = {}
    for sm in sources:
        merged.update(sm.published)
    return merged
