import csv
import datetime
import json
import logging
import math
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
import humanfriendly
from tqdm import tqdm

from helpers import exec_query_from_file, send_req, sqlite_connect

logger = logging.getLogger(__name__)

AE_OUTPUT_DIR = "ae"
BIOSTUDIES_API_BASE_URL = "https://www.ebi.ac.uk/biostudies/api/v1"
BIOSTUDIES_FILES_BASE_URL = "https://www.ebi.ac.uk/biostudies/files"
PAGE_SIZE = 100


def ingest_ae(
    base_output_dir: Path,
    db_file: Path | None,
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    batch_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    output_dir = base_output_dir / AE_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    (output_dir / "db").mkdir(exist_ok=True, parents=True)
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"ae_metadata_{date_str}.db")

    logger.info(
        "starting ae ingest with following config:\n"
        "- num workers: %d\n"
        "- batch size: %d\n"
        "- retries: %d\n"
        "- timeout: %s\n"
        "- max timeout: %s",
        num_workers,
        batch_size,
        retries,
        humanfriendly.format_timespan(timeout),
        humanfriendly.format_timespan(max_timeout),
    )

    accessions = _fetch_accessions(
        start_date,
        end_date,
        num_workers,
        retries,
        timeout,
        max_timeout,
    )
    _download_study_metadatas(
        accessions,
        num_workers,
        data_dir,
        retries,
        timeout,
        max_timeout,
    )
    _download_magetab_files(data_dir, num_workers, retries, timeout, max_timeout)

    exec_query_from_file("tables/ae.sql", db_file)
    _process_accessions(accessions, db_file, data_dir, num_workers, batch_size)
    exec_query_from_file("indexes/ae.sql", db_file)


def _send_search_req(
    date: datetime.date,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> list[str] | None:
    r = send_req(
        method="GET",
        url=f"{BIOSTUDIES_API_BASE_URL}/search",
        params={
            "collection": "arrayexpress",
            "release_date": date,
            "page_size": 100,
        },
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
    )
    data = r.json()
    r.close()
    hits = data.get("hits")
    if hits is None or not isinstance(hits, list):
        return None

    return [hit["accession"] for hit in hits]


def _fetch_accessions(
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> list[str]:
    accessions: list[str] = []
    num_days = (end_date - start_date).days + 1
    num_workers = min(num_days, num_workers)

    chunk_size = math.ceil(num_days / num_workers)
    batches = [
        (
            start_date + datetime.timedelta(days=i),
            min(start_date + datetime.timedelta(days=i + chunk_size - 1), end_date),
        )
        for i in range(0, num_days, chunk_size)
    ]

    def _worker(batch: tuple[datetime.date, datetime.date]) -> tuple[list[str], int]:
        accessions: list[str] = []
        days_processed = 0
        start, end = batch
        current_date = start

        while current_date <= end:
            per_page_accessions = _send_search_req(
                current_date,
                retries,
                timeout,
                max_timeout,
            )
            if per_page_accessions is None or len(per_page_accessions) == 0:
                current_date += datetime.timedelta(1)
                days_processed += 1
                continue

            accessions.extend(per_page_accessions)

            if len(per_page_accessions) < PAGE_SIZE:
                current_date += datetime.timedelta(1)
                days_processed += 1
            else:
                curr_accessions = len(per_page_accessions)
                while curr_accessions >= PAGE_SIZE:
                    per_page_accessions = _send_search_req(
                        current_date,
                        retries,
                        timeout,
                        max_timeout,
                    )
                    if per_page_accessions is None:
                        break
                    if len(per_page_accessions) == 0:
                        break
                    accessions.extend(per_page_accessions)
                    curr_accessions = len(per_page_accessions)

                current_date += datetime.timedelta(1)
                days_processed += 1

        return accessions, days_processed

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=num_days, unit="day", desc="fetching accessions in range") as pbar,
    ):
        futures = [pool.submit(_worker, b) for b in batches]

        for f in as_completed(futures):
            batch_accessions, days_processed = f.result()
            accessions.extend(batch_accessions)
            pbar.update(days_processed)

    return accessions


def _study_metadata_worker(
    accession: str,
    output_dir: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    output_file = output_dir / f"{accession}.json"
    if output_file.exists():
        return

    r = send_req(
        method="GET",
        url=f"{BIOSTUDIES_API_BASE_URL}/studies/{accession}",
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
    )
    data = r.json()
    r.close()
    with output_file.open("w") as f:
        json.dump(data, f)


def _download_study_metadatas(
    accessions: list[str],
    num_workers: int,
    output_dir: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    output_dir.mkdir(exist_ok=True, parents=True)

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(
            total=len(accessions),
            unit="acc",
            desc="downloading study metadata",
        ) as pbar,
    ):
        futures = [
            pool.submit(
                _study_metadata_worker,
                acc,
                output_dir,
                retries,
                timeout,
                max_timeout,
            )
            for acc in accessions
        ]

        for f in as_completed(futures):
            f.result()
            pbar.update(1)


def _download_magetab_files(
    output_dir: Path,
    num_workers: int,
    retries: int,
    timeout: int,
    _max_timeout: int,
) -> None:
    metadata_json_files = output_dir.glob("*.json")
    files = []  # store (accession, filename) tuples

    for path in metadata_json_files:
        accession = str(path.name).split(".")[0]

        with path.open() as f:
            data = json.load(f)
            section = data.get("section", {})
            subsections = section.get("subsections", [])

            for subsection in subsections:
                item = subsection if isinstance(subsection, list) else [subsection]
                for sub in item:
                    if not isinstance(sub, dict):
                        continue

                    sub_type = sub.get("type")
                    if sub_type != "Assays and Data":
                        continue

                    files.append((accession, f"{accession}.idf.txt"))
                    files.append((accession, f"{accession}.sdrf.txt"))

    def _download_worker(accession: str, filename: str) -> None:
        dest_path = output_dir / filename
        if dest_path.exists():
            return

        url = f"{BIOSTUDIES_FILES_BASE_URL}/{accession}/{filename}"
        try:
            r = send_req(
                "GET",
                url,
                num_retries=retries,
                timeout=timeout,
                max_wait=timeout,
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == httpx.codes.NOT_FOUND:
                dest_path.touch()
                return

            logger.warning(
                "magetab download failed for %s: %s (response=%s, status=%s)",
                url,
                e,
                e.response,
                e.response.status_code,
            )
            raise

        with dest_path.open("wb") as f:
            f.write(r.content)
        r.close()

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=len(files), unit="file", desc="downloading magetab files") as pbar,
    ):
        futures = [
            pool.submit(_download_worker, accession, filename)
            for accession, filename in files
        ]

        for future in as_completed(futures):
            future.result()
            pbar.update(1)


def _parse_experiment_json(filepath: Path, accession: str) -> dict | None:
    with filepath.open() as f:
        data = json.load(f)

    result: dict[str, str | int | list | dict | None] = {
        "accession": accession,
        "overflow": {},
    }
    accession = data.get("accno")
    section = data.get("section", {})
    subsections = section.get("subsections", [])
    links = section.get("links", [])

    external_links = []
    processed_files = []
    exp_factors_detail = []
    authors = []
    orgs = []
    protocol_texts = []

    # processing subsections
    for it in subsections:
        items = _json_coerce_list(it)
        for sub in items:
            sub_type = sub.get("type")
            if not type:
                continue

            attrs = {a["name"]: a.get("value", "") for a in sub.get("attributes", [])}

            if sub_type == "Publication":
                result["pubmed_id"] = sub.get("accno")
                result["publication_title"] = attrs.get("Title")
                result["publication_authors"] = attrs.get("Authors")
                result["publication_doi"] = attrs.get("DOI")
            elif sub_type == "Author":
                name = attrs.get("Name", "").strip()
                authors.append(name)
            elif sub_type == "Organization":
                org = {
                    "id": sub.get("accno"),
                    "name": attrs.get("Name"),
                    "address": attrs.get("Address"),
                }

                if org["name"] or org["address"]:
                    orgs.append(org)
            elif sub_type == "Protocols":
                prot_type = attrs.get("Type", "Protocol")
                description = attrs.get("Description", "").strip()

                if description:
                    protocol_texts.append(f"{prot_type}: {description}")
            elif sub_type == "MINSEQE Score":
                result["minseqe_score"] = json.dumps(
                    {
                        "exp_design": attrs.get("Exp. Design"),
                        "protocols": attrs.get("Protocols"),
                        "processed": attrs.get("Processed"),
                        "raw": attrs.get("Raw"),
                        "variables": attrs.get("Variables"),
                    },
                )
            elif sub_type in {"Assays and Data", "Samples"}:
                if sub_type == "Assays and Data":
                    count_str = attrs.get("Assay count", "")
                    if count_str:
                        result["assay_count"] = int(count_str)
                    result["technology"] = attrs.get("Technology")
                    result["assay_by_molecule"] = attrs.get("Assay by Molecule")

                for inner in sub.get("subsections", []):
                    inners = _json_coerce_list(inner)
                    for s in inners:
                        inner_type = s.get("type")

                        # processed files
                        if (
                            sub_type == "Assays and Data"
                            and inner_type == "Processed Data"
                        ):
                            for file_group in s.get("files", []):
                                file_list = _json_coerce_list(file_group)
                                for f in file_list:
                                    path = f.get("path")
                                    processed_files.append(
                                        {
                                            "path": path,
                                            "size": f.get("size"),
                                            "url": (
                                                f"{BIOSTUDIES_FILES_BASE_URL}/"
                                                f"{accession}/{path}"
                                            ),
                                        },
                                    )
                        # experimental factor details
                        elif (
                            sub_type == "Samples"
                            and inner_type == "Experimental Factors"
                        ):
                            exp_factors_detail.extend(
                                {
                                    "name": attr.get("name"),
                                    "value": attr.get("value"),
                                }
                                for attr in s.get("attributes", [])
                            )

    # external links
    for link_group in links:
        link_list = link_group if isinstance(link_group, list) else [link_group]
        for link in link_list:
            if not isinstance(link, dict):
                continue

            url = link.get("url")
            link_type = None

            for attr in link.get("attributes", []):
                if attr.get("name") == "Type":
                    link_type = attr.get("value")

            if url:
                external_links.append({"url": url, "type": link_type})

    result["external_links"] = json.dumps(external_links)
    result["processed_data_files"] = json.dumps(processed_files)
    result["experimental_factors_detail"] = json.dumps(processed_files)
    result["authors"] = json.dumps(authors)
    result["organizations"] = json.dumps(orgs)
    result["protocols_description"] = json.dumps(protocol_texts)

    # building up metadata: title, release data, organism, description and study type
    for attr in data.get("attributes", []):
        name = attr.get("name", "")
        value = attr.get("value")
        if name == "ReleaseDate":
            result["release_date"] = value
        elif name == "Title":
            result["title"] = value

    for attr in section.get("attributes", []):
        name = attr.get("name")
        value = attr.get("value")

        if name == "Title":
            result["title"] = value
        elif name == "Organism":
            result["organism"] = value
        elif name == "Description":
            result["description"] = value
        elif name == "Study type":
            result["study_type"] = value
        elif name is not None and value is not None:
            result["overflow"][name] = value

    return result


def _parse_idf(filepath: Path) -> dict | None:
    idf_col_mapping = {
        "PubMed ID": ("pubmed_id", _idf_first_or_none),
        "Public Release Date": ("release_date_idf", _idf_first_or_none),
        "Person Last Name": ("contact_name", _idf_first_or_none),
        "Person Email": ("contact_email", _idf_first_or_none),
        "Person Affiliation": ("contact_affiliation", _idf_first_or_none),
        "Comment[ArrayExpressSubmissionDate]": ("submission_date", _idf_first_or_none),
        "Experimental Factor Name": ("experimental_factors", _idf_to_json_array),
        "Experimental Factor Type": ("experimental_factor_types", _idf_to_json_array),
        "Protocol Name": ("protocol_names", _idf_to_json_array),
        "Protocol Type": ("protocol_types", _idf_to_json_array),
    }

    result: dict[str, str | dict | None] = {"overflow": {}}
    rows = []

    with filepath.open() as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) == 0:
                continue
            rows.append(row)

    for row in rows:
        key = row[0].strip()
        values = [v.strip() for v in row[1:] if v.strip()]

        if key in idf_col_mapping:
            clean_col_name, value_extractor = idf_col_mapping[key]
            value = value_extractor(values)
            result[clean_col_name] = value

        if values:
            if len(values) == 1:
                result["overflow"][key] = values[0]
            else:
                result["overflow"][key] = values

    return result


def _merge_json_and_idf(json_data: dict | None, idf_data: dict | None) -> dict | None:
    if not json_data and not idf_data:
        return None

    result = {
        "accession": None,
        "title": None,
        "organism": None,
        "description": None,
        "study_type": None,
        "release_date": None,
        "submission_date": None,
        "experimental_factors": None,
        "experimental_factor_types": None,
        "contact_name": None,
        "contact_email": None,
        "contact_affiliation": None,
        "pubmed_id": None,
        "protocol_names": None,
        "protocol_types": None,
        "attributes_json": None,
        "publication_title": None,
        "publication_authors": None,
        "publication_doi": None,
        "external_links": None,
        "processed_data_files": None,
        "experimental_factors_detail": None,
        "authors": None,
        "organizations": None,
        "protocols_description": None,
        "assay_count": None,
        "technology": None,
        "assay_by_molecule": None,
        "minseqe_score": None,
    }

    overflow = {}

    if json_data:
        keys = [
            "accession",
            "title",
            "organism",
            "description",
            "study_type",
            "release_date",
            "publication_title",
            "publication_authors",
            "publication_doi",
            "external_links",
            "processed_data_files",
            "experimental_factors_detail",
            "authors",
            "organizations",
            "protocols_description",
            "assay_count",
            "technology",
            "assay_by_molecule",
            "minseqe_score",
        ]

        for k in keys:
            result[k] = json_data.get(k)

        overflow.update(json_data.get("overflow", {}))

    if idf_data:
        if not result["release_date"] and idf_data.get("release_date_idf"):
            result["release_date"] = idf_data.get("release_date_idf")
        if not result["pubmed_id"] and idf_data.get("pubmed_id"):
            result["pubmed_id"] = idf_data.get("pubmed_id")

        keys = [
            "submission_date",
            "experimental_factors",
            "experimental_factor_types",
            "contact_name",
            "contact_email",
            "contact_affiliation",
            "pubmed_id",
            "protocol_names",
            "protocol_types",
        ]

        for k in keys:
            result[k] = idf_data.get(k)

        overflow.update(idf_data.get("overflow", {}))

    result["attributes_json"] = json.dumps(overflow) if overflow else None
    return result


# credits: mr. sonnet 4.6
def normalize_sdrf_col(raw: str) -> str:
    # strip edges, lowercase, collapse whitespace
    # "  Source Name  " -> "source name"
    s = re.sub(r"\s+", " ", raw.strip().lower())

    # treat "factor value" as one token
    # "factor value[disease]" -> "factorvalue[disease]"
    s = s.replace("factor value", "factorvalue")

    # drop spaces around brackets
    # "characteristics[ organism part ]" -> "characteristics[organism part]"
    return re.sub(r"\s*\[|\]\s*", lambda m: m.group().strip(), s)


def _parse_sdrf(filepath: Path, accession: str) -> list | None:
    samples = []
    sample_col_mapping = {
        "source name": "source_name",
        "characteristics[organism]": "organism",
        "characteristics[organism part]": "organism_part",
        "characteristics[cell type]": "cell_type",
        "characteristics[genotype]": "genotype",
        "comment[sample_source_name]": "sample_source_name",
        "comment[sample_title]": "sample_title",
        "comment[sample_description]": "sample_description",
        "comment[library_strategy]": "library_strategy",
        "comment[library_source]": "library_source",
        "comment[library_selection]": "library_selection",
        "comment[ena_experiment]": "ena_experiment",
        "comment[ena_run]": "ena_run",
    }
    cols_to_skip = [
        "source name",
        "extract name",
        "labeled extract name",
        "label",
        "material type",
        "protocol ref",
        "term source ref",
        "term accession number",
        "assay name",
        "technology type",
        "array design ref",
        "array data file",
        "derived array data file",
        "scan name",
        "hybridization name",
        "normalization name",
        "performer",
        "provider",
        "unit",
    ]

    try:
        with filepath.open() as f:
            reader = csv.reader(f, delimiter="\t")
            raw_header = next(reader, None)
            if not raw_header:
                return None

            headers = [normalize_sdrf_col(h) for h in raw_header]

            for row in reader:
                if not row or not any(s.strip() for s in row):
                    continue

                sample: dict[str, str | None] = {"experiment_accession": accession}
                overflow = {}

                for db_col in sample_col_mapping.values():
                    sample[db_col] = None

                for i, clean_col in enumerate(headers):
                    if i >= len(row):
                        break

                    value = row[i].strip()
                    if not value:
                        continue

                    if clean_col in sample_col_mapping:
                        db_col = sample_col_mapping[clean_col]
                        sample[db_col] = value
                    else:
                        base = (
                            clean_col.split("[")[0].strip()
                            if "[" in clean_col
                            else clean_col
                        )

                        if base in cols_to_skip or clean_col.startswith("unit["):
                            continue

                        overflow[clean_col] = value

                sample["attributes_json"] = json.dumps(overflow)
                samples.append(sample)

    except FileNotFoundError:
        return None
    else:
        return samples or None


def _process_accession(accession: str, data_dir: Path) -> tuple | None:
    json_path = data_dir / f"{accession}.json"
    idf_path = data_dir / f"{accession}.idf.txt"
    sdrf_path = data_dir / f"{accession}.sdrf.txt"

    json_data = (
        _parse_experiment_json(json_path, accession) if json_path.exists() else None
    )
    idf_data = _parse_idf(idf_path) if idf_path.exists() else None
    merged = _merge_json_and_idf(json_data, idf_data)
    samples = _parse_sdrf(sdrf_path, accession)

    return merged, samples


def _process_accessions(
    accessions: list[str],
    db_file: Path,
    data_dir: Path,
    num_workers: int,
    batch_size: int,
) -> None:
    with (
        tqdm(
            total=len(accessions),
            unit="acc",
            desc="ingesting data to sqlite",
        ) as pbar,
        sqlite_connect(db_file) as conn,
    ):
        cursor = conn.cursor()

        for batch_start in range(0, len(accessions), batch_size):
            batch_accs = accessions[batch_start : batch_start + batch_size]
            result = {}

            with ThreadPoolExecutor(num_workers) as pool:
                futures = {
                    pool.submit(_process_accession, acc, data_dir): acc
                    for acc in batch_accs
                }

                for future in as_completed(futures):
                    acc = futures[future]
                    result[acc] = future.result()

            for acc in batch_accs:
                value = result.get(acc)
                if not value:
                    continue

                experiment, samples = value
                if experiment:
                    _insert_experiment(cursor, experiment)
                if samples:
                    _insert_samples(cursor, samples)

                pbar.update(1)

        conn.commit()


def _insert_experiment(cursor: sqlite3.Cursor, exp: dict) -> None:
    sql = """
        INSERT OR REPLACE INTO arrayexpress_experiments (
            accession, title, organism, description, study_type,
            release_date, submission_date,
            experimental_factors, experimental_factor_types,
            contact_name, contact_email, contact_affiliation,
            pubmed_id, protocol_names, protocol_types,
            attributes_json,
            publication_title, publication_authors, publication_doi,
            external_links, processed_data_files, experimental_factors_detail,
            authors, organizations, protocols_description,
            assay_count, technology, assay_by_molecule, minseqe_score
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        )
    """

    cursor.execute(
        sql,
        (
            exp["accession"],
            exp["title"],
            exp["organism"],
            exp["description"],
            exp["study_type"],
            exp["release_date"],
            exp["submission_date"],
            exp["experimental_factors"],
            exp["experimental_factor_types"],
            exp["contact_name"],
            exp["contact_email"],
            exp["contact_affiliation"],
            exp["pubmed_id"],
            exp["protocol_names"],
            exp["protocol_types"],
            exp["attributes_json"],
            exp["publication_title"],
            exp["publication_authors"],
            exp["publication_doi"],
            exp["external_links"],
            exp["processed_data_files"],
            exp["experimental_factors_detail"],
            exp["authors"],
            exp["organizations"],
            exp["protocols_description"],
            exp["assay_count"],
            exp["technology"],
            exp["assay_by_molecule"],
            exp["minseqe_score"],
        ),
    )


def _insert_samples(cursor: sqlite3.Cursor, samples: list[dict]) -> None:
    sql = """
        INSERT INTO arrayexpress_samples (
            experiment_accession, source_name,
            organism, organism_part, cell_type, genotype,
            sample_source_name, sample_title, sample_description,
            library_strategy, library_source, library_selection,
            ena_experiment, ena_run,
            attributes_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(
        sql,
        [
            (
                s["experiment_accession"],
                s["source_name"],
                s["organism"],
                s["organism_part"],
                s["cell_type"],
                s["genotype"],
                s["sample_source_name"],
                s["sample_title"],
                s["sample_description"],
                s["library_strategy"],
                s["library_source"],
                s["library_selection"],
                s["ena_experiment"],
                s["ena_run"],
                s["attributes_json"],
            )
            for s in samples
        ],
    )


def _json_coerce_list(elem: object) -> list:
    return elem if isinstance(elem, list) else [elem]


def _idf_first_or_none(values: list[str]) -> str | None:
    return values[0] if values else None


def _idf_to_json_array(values: list[str]) -> str | None:
    if not values:
        return None
    return json.dumps(values)
