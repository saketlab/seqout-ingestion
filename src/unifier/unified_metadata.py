import csv
import json
import logging
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from tqdm import tqdm

from helpers import exec_query_from_file, sqlite_connect

logger = logging.getLogger(__name__)


@dataclass
class SourceMap:
    organism: dict[str, dict[str, int]] = field(default_factory=dict)
    library: dict[str, dict[str, int]] = field(default_factory=dict)
    num_experiments: dict[str, int] = field(default_factory=dict)
    num_samples: dict[str, int] = field(default_factory=dict)
    center: dict[str, list[str]] = field(default_factory=dict)
    pmids: dict[str, set[str]] = field(default_factory=dict)
    published: dict[str, str] = field(default_factory=dict)


UNIFIED_METADATA_OUTPUT_DIR = "unified_metadata"
SPLIT_INSTITUTES_PAT = re.compile(r"[,;]?\s*\d+[.\-)]\s*")
CLEAN_CENTER_PREFIX = re.compile(r"^[\s(,;]*\d+[.\-)]\s*")


def build_unified_metadata(
    base_output_dir: Path,
    db_file: Path | None,
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
    output_dir = base_output_dir / UNIFIED_METADATA_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    db_file = db_file or (output_dir / "unified_metadata.db")

    catalog = _load_unified_catalog(unified_catalog_db_file)
    pubmed_map = _load_pubmed_metadata(pubmed_metadata_db_file)
    assay_map = _load_assay_mapping(assay_mapping_csv_file)
    taxonomy = _load_taxonomy_mapping(taxonomy_mapping_csv_file)

    source_data: dict[str, SourceMap] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_scan_geo, geo_db_file): "geo",
            pool.submit(_scan_sra, sra_db_file): "sra",
            pool.submit(_scan_ae, ae_db_file): "ae",
            pool.submit(_scan_ena, ena_db_file): "ena",
        }

        for f in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="scanning sources",
            unit="src",
        ):
            label = futures[f]
            source_data[label] = f.result()

    geo, sra, ae, ena = (source_data[k] for k in ("geo", "sra", "ae", "ena"))
    sources = (geo, sra, ae, ena)
    pmid_map = _merge_pmid_map(sources)
    published_map = _merge_published_map(sources)

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
            sm.organism.get(acc),
            taxonomy,
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

        num_experiments = sm.num_experiments.get(acc)
        num_samples = sm.num_samples.get(acc)

        center_list = sm.center.get(acc)
        if center_list:
            if len(center_list) == 1:
                center_list = [
                    x.strip()
                    for x in re.split(SPLIT_INSTITUTES_PAT, center_list[0])
                    if x.strip()
                ]

            center_list = [
                CLEAN_CENTER_PREFIX.sub("", cn).strip()
                for cn in center_list
                if cn.strip()
            ]
            center_list = [
                cn for cn in center_list if cn and not cn.startswith(("(", ")", ","))
            ]
            center_list = [cn[0].upper() + cn[1:] for cn in center_list if cn]

            stats["center"] += len(center_list)
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
            "n_experiments": num_experiments,
            "n_samples": num_samples,
            "center_name": center_list[0] if center_list else None,
            "center_names": _json_or_none(center_list),
            "pmid": pmid,
            "journal": journal,
            "citation_count": citation_count,
            "first_published": published_map.get(acc),
        }
        rows.append(row)

    logger.info("enriched %d rows", len(rows))
    for k, v in stats.items():
        logger.info(f"  {k}: {v} ({100 * v // len(rows)}%)" if rows else f"  {k}: {v}")
    logger.info("center rows: %d", len(centers_rows))

    with sqlite_connect(db_file) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        exec_query_from_file("unified_metadata.sql", conn=conn)

        cols = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(c for c in cols)

        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            conn.executemany(
                f"INSERT OR IGNORE INTO unified_metadata ({col_names}) VALUES ({placeholders})",  # noqa: S608 E501
                [tuple(r[c] for c in cols) for r in batch],
            )

        for start in range(0, len(centers_rows), batch_size):
            batch = centers_rows[start : start + batch_size]
            conn.executemany(
                "INSERT OR IGNORE INTO unified_centers "
                "(canonical_accession, center_name, position) VALUES (?, ?, ?)",
                batch,
            )

        exec_query_from_file("indexes/unified_metadata.sql", conn=conn)
        conn.commit()

    return rows, centers_rows


def _load_unified_catalog(db_file: Path) -> list[tuple[str, str, str, str, dict]]:
    logger.info("loading unified catalog...")
    with sqlite_connect(db_file) as conn:
        catalog = conn.execute(
            """SELECT
                canonical_accession, source, title, description, aliases
            FROM unified_studies""",
        ).fetchall()

    logger.info("\t%d entries loaded from unified catalog", len(catalog))
    return catalog


def _load_pubmed_metadata(db_file: Path) -> dict[str, tuple[str | None, int | None]]:
    logger.info("loading pubmed mapping...")
    with sqlite_connect(db_file) as conn:
        rows = conn.execute(
            "SELECT pmid, journal, citation_count FROM pubmed_metadata",
        ).fetchall()
        pubmed_mapping: dict[str, tuple[str | None, int | None]] = {}
        for pmid, journal, citation_count in rows:
            pmid_str = str(pmid)
            journal_val = str(journal) if journal else None
            cc_val = int(citation_count) if citation_count is not None else None
            pubmed_mapping[pmid_str] = (journal_val, cc_val)

    logger.info("\t%d entries loaded from pubmed mapping", len(pubmed_mapping))
    return pubmed_mapping


def _load_assay_mapping(csv_file: Path) -> dict[str, tuple[str, str]]:
    logger.info("loading assay mapping...")
    with csv_file.open() as f:
        assay_mapping = {
            row["raw_term"]: (row["canonical_assay"], row["category"])
            for row in csv.DictReader(f)
        }

    logger.info("\t%d entries loaded from assay mapping", len(assay_mapping))
    return assay_mapping


def _load_taxonomy_mapping(csv_file: Path) -> dict[str, str]:
    logger.info("loading taxonomy common names mapping...")
    taxonomy_mapping = {}
    with csv_file.open() as f:
        for row in csv.DictReader(f):
            name = row["scientific_name"].strip().lower()
            common = row["common_name"].strip()
            if name and common:
                taxonomy_mapping[name] = common

    logger.info(
        "\t%d entries loaded from taxonomy common names mapping",
        len(taxonomy_mapping),
    )
    return taxonomy_mapping


def _scan_geo(db_file: Path) -> SourceMap:
    m = SourceMap()
    conn = sqlite_connect(db_file)

    sample_organisms: dict[str, set[str]] = defaultdict(set[str])
    series_sample_count: Counter[str] = Counter()
    series_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    series_library_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    pmids: dict[str, set[str]] = defaultdict(set)

    # scanning geo_samples
    rows = conn.execute(
        "SELECT accession, channels FROM geo_samples WHERE channels IS NOT NULL",
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

    logger.info("%d samples with organism", len(sample_organisms))

    # scanning geo_series
    rows = conn.execute(
        """SELECT accession, samples_ref, series_type, published_at, pubmed_id,
            pubmed_id_pysradb
        FROM geo_series WHERE samples_ref IS NOT NULL""",
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
                        series_library_count[accession][str(st)] += 1
            except (json.JSONDecodeError, TypeError):
                series_library_count[accession][series_type] += 1

        if published_at:
            m.published[accession] = published_at

        if pubmed_id_json:
            try:
                parsed = json.loads(pubmed_id_json)
                if isinstance(parsed, list):
                    for p in parsed:
                        s = str(p).strip()
                        if s:
                            pmids[accession].add(s)
                elif parsed:
                    s = str(parsed).strip()
                    if s:
                        pmids[accession].add(s)
            except (json.JSONDecodeError, TypeError):
                pass

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                pmids[accession].add(s)

    # scanning geo_contributors
    rows = conn.execute(
        """SELECT accession, organization
        FROM geo_contributors
        WHERE
            organization IS NOT NULL
            AND organization != ''
            AND organization != 'GEO'
            AND organization != 'NCBI/NLM/NIH'
            ORDER BY accession""",
    ).fetchall()

    center_orgs: dict[str, list[str]] = defaultdict(list)
    seen_orgs: dict[str, set[str]] = defaultdict(set)
    for accession, org_val in rows:
        organization = org_val.strip()
        if not organization:
            continue
        org_lower = organization.lower()
        if org_lower not in seen_orgs[accession]:
            seen_orgs[accession].add(org_lower)
            center_orgs[accession].append(organization)

    m.organism = {k: dict(v) for k, v in series_organism_count.items()}
    m.library = {k: dict(v) for k, v in series_library_count.items()}
    m.num_experiments = dict(series_sample_count)
    m.num_samples = dict(series_sample_count)
    m.center = dict(center_orgs)
    m.pmids = dict(pmids)

    conn.close()
    return m


def _scan_sra(db_file: Path) -> SourceMap:
    m = SourceMap()
    conn = sqlite_connect(db_file)

    submission_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    submission_sample_count: Counter[str] = Counter()
    study_experiment_count: Counter[str] = Counter()
    study_library_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_to_submission: dict[str, str] = defaultdict(str)
    study_to_center: dict[str, str] = defaultdict(str)
    pmids: dict[str, set[str]] = defaultdict(set)
    sub_center: dict[str, str] = {}

    # scanning sra_samples
    rows = conn.execute(
        """SELECT submission, scientific_name
            FROM sra_samples
            WHERE submission IS NOT NULL""",
    ).fetchall()

    for submission, sc_name_val in rows:
        submission_sample_count[submission] += 1
        scientific_name = str(sc_name_val).strip()
        if not scientific_name:
            continue
        submission_organism_count[submission][scientific_name] += 1

    logger.info(
        "%d submissions with organisms, %d submissions with samples",
        len(submission_organism_count),
        len(submission_sample_count),
    )

    # scanning sra_experiments
    rows = conn.execute(
        "SELECT study, library_strategy FROM sra_experiments WHERE study IS NOT NULL",
    ).fetchall()

    for study, strategy_val in rows:
        study_experiment_count[study] += 1
        strategy = str(strategy_val).strip()
        if not strategy:
            continue

        study_library_count[study][strategy] += 1

    logger.info(
        "%d studies with library strategy, %d studies with experiments",
        len(study_library_count),
        len(study_experiment_count),
    )

    # scanning sra_studies
    rows = conn.execute(
        """SELECT accession, submission, links, published_at,
            pubmed_id_pysradb, center_name
        FROM sra_studies""",
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
                m.num_samples[accession] = samp_count

        if center_name:
            cn = center_name.strip()
            if cn:
                study_to_center[accession] = cn

        lib_counts = study_library_count.get(accession)
        if lib_counts:
            m.library[accession] = dict(lib_counts)

        exp_count = study_experiment_count.get(accession)
        if exp_count:
            m.num_experiments[accession] = exp_count

        if published_at:
            m.published[accession] = published_at

        if links_json:
            try:
                links = json.loads(links_json)
                xref = links.get("XREF_LINK") if isinstance(links, dict) else None

                if isinstance(xref, dict) and xref.get("DB") == "pubmed":
                    pid = xref.get("ID", "")
                    if pid:
                        pmids[accession].add(str(pid).strip())
                elif isinstance(xref, list):
                    for x in xref:
                        if isinstance(x, dict) and x.get("DB") == "pubmed":
                            pid = x.get("ID", "")
                            if pid:
                                pmids[accession].add(str(pid).strip())
            except (json.JSONDecodeError, TypeError):
                pass

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                pmids[accession].add(s)

    m.pmids = dict(pmids)

    # scanning sra_submissions for center (fallback to study center_name)
    cursor = conn.execute(
        """SELECT accession, center_name
        FROM sra_submissions
        WHERE center_name IS NOT NULL AND center_name != ''""",
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
    conn = sqlite_connect(db_file)

    experiment_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    experiment_library_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    experiment_sample_count: Counter[str] = Counter()
    pmids: dict[str, set[str]] = defaultdict(set)

    # scanning arrayexpress_samples
    rows = conn.execute(
        """SELECT experiment_accession, organism, library_strategy
        FROM arrayexpress_samples""",
    ).fetchall()

    for accession, org_val, lib_strat_val in rows:
        experiment_sample_count[accession] += 1
        if org_val:
            organism = str(org_val).strip()
            if not organism:
                continue
            experiment_organism_count[accession][organism] += 1

        if lib_strat_val:
            library_strategy = str(lib_strat_val).strip()
            if not library_strategy:
                continue
            experiment_library_count[accession][library_strategy] += 1

    # scanning arrayexpress_experiments
    rows = conn.execute(
        """SELECT
            accession, organizations, study_type,
            pubmed_id, pubmed_id_pysradb, release_date,
            contact_affiliation
        FROM arrayexpress_experiments""",
    ).fetchall()

    for (
        accession,
        orgs_json,
        study_type_val,
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

        if study_type_val and accession not in m.library:
            study_type = study_type_val.strip()
            if study_type:
                m.library[accession] = {study_type: 1}

        if release_date:
            m.published[accession] = release_date

        if pubmed_id:
            s = str(pubmed_id).strip()
            if s:
                pmids[accession].add(s)

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                pmids[accession].add(s)

    m.organism = {k: dict(v) for k, v in experiment_organism_count.items()}
    m.library = {k: dict(v) for k, v in experiment_library_count.items()}
    m.num_experiments = dict(experiment_sample_count)
    m.num_samples = dict(experiment_sample_count)
    m.pmids = dict(pmids)

    conn.close()
    return m


def _scan_ena(db_file: Path) -> SourceMap:
    m = SourceMap()
    conn = sqlite_connect(db_file)

    study_organism_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_library_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_center_count: dict[str, Counter[str]] = defaultdict(Counter[str])
    study_sample_count: Counter[str] = Counter()
    study_experiment_count: Counter[str] = Counter()
    ena_center_orgs: dict[str, list[str]] = defaultdict(list)
    pmids: dict[str, set[str]] = defaultdict(set)

    # scanning ena_samples
    rows = conn.execute(
        """SELECT
            study_accession, scientific_name
        FROM ena_samples
        WHERE study_accession IS NOT NULL""",
    ).fetchall()

    for study_accession, name_val in rows:
        study_sample_count[study_accession] += 1
        if name_val:
            name = str(name_val).strip()
            if not name:
                continue
            study_organism_count[study_accession][name] += 1

    # scanning ena_experiments
    rows = conn.execute(
        """SELECT
            study_accession, library_strategy, center_name
        FROM ena_experiments
        WHERE study_accession IS NOT NULL""",
    ).fetchall()

    for study_accession, lib_strat_val, center_name_val in rows:
        study_experiment_count[study_accession] += 1

        if lib_strat_val:
            library_strategy = str(lib_strat_val).strip()
            if library_strategy:
                study_library_count[study_accession][library_strategy] += 1

        if center_name_val:
            center_name = str(center_name_val).strip()
            if center_name:
                study_center_count[study_accession][center_name] += 1

    # scanning ena_studies
    rows = conn.execute(
        """SELECT
            study_accession, first_public, pubmed_id_pysradb, center_name
        FROM ena_studies""",
    ).fetchall()

    for study_accession, first_public, pubmed_id_pysradb, center_name in rows:
        if first_public:
            m.published[study_accession] = first_public

        if pubmed_id_pysradb:
            s = str(pubmed_id_pysradb).strip()
            if s:
                pmids[study_accession].add(s)

        if center_name:
            cn = center_name.strip()
            if (
                cn
                and not cn.startswith("SUB")
                and cn.lower()
                not in {c.lower() for c in ena_center_orgs[study_accession]}
            ):
                ena_center_orgs[study_accession].append(cn)

    for acc, counts in study_center_count.items():
        for name, _ in counts.most_common():
            if name.startswith("SUB"):
                continue
            if name.lower() not in {c.lower() for c in ena_center_orgs[acc]}:
                ena_center_orgs[acc].append(name)

    m.organism = {k: dict(v) for k, v in study_organism_count.items()}
    m.library = {k: dict(v) for k, v in study_library_count.items()}
    m.num_experiments = dict(study_experiment_count)
    m.num_samples = dict(study_sample_count)
    m.center = dict(ena_center_orgs)
    m.pmids = dict(pmids)

    conn.close()
    return m


def _json_or_none(val: dict | list | None) -> str | None:
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
