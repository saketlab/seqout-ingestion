import datetime
import json
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from helpers import exec_query_from_file, send_req

logging.getLogger("urllib3.connectionpool").setLevel(
    logging.WARNING
)  # to disable debug messages by urllib

_ENA_OUTPUT_DIR = "ena"
_ENA_PORTAL_API_BASE_URL = "https://www.ebi.ac.uk/ena/portal/api"


_STUDY_FIELDS = [
    "study_accession",
    "secondary_study_accession",
    "secondary_study_alias",
    "secondary_study_center_name",
    "study_alias",
    "study_title",
    "study_description",
    "study_name",
    "center_name",
    "broker_name",
    "project_name",
    "tax_id",
    "tax_lineage",
    "tax_division",
    "scientific_name",
    "strain",
    "breed",
    "cultivar",
    "isolate",
    "first_public",
    "last_updated",
    "geo_accession",
    "keywords",
    "tag",
    "submission_tool",
    "parent_study_accession",
    "description",
    "status",
]
_EXPERIMENT_FIELDS = [
    "experiment_accession",
    "run_accession",
    "run_alias",
    "sample_accession",
    "study_accession",
    "experiment_title",
    "library_strategy",
    "library_source",
    "library_selection",
    "library_layout",
    "library_name",
    "instrument_platform",
    "instrument_model",
    "scientific_name",
    "tax_id",
    "center_name",
    "first_public",
    "last_updated",
    "sample_title",
    "sample_alias",
    "sample_description",
    "country",
    "host",
    "tissue_type",
    "cell_type",
    "disease",
    "dev_stage",
    "base_count",
    "read_count",
]
_SAMPLE_FIELDS = [
    "sample_accession",
    "secondary_sample_accession",
    "sample_alias",
    "sample_title",
    "sample_description",
    "tax_id",
    "scientific_name",
    "study_accession",
    "center_name",
    "broker_name",
    "first_public",
    "last_updated",
    "submission_accession",
    "submission_tool",
    "status",
    "project_name",
    "description",
    "checklist",
    "ncbi_reporting_standard",
    "keywords",
    "tag",
    "datahub",
    # organism details
    "strain",
    "cultivar",
    "ecotype",
    "isolate",
    "isolation_source",
    "bio_material",
    "culture_collection",
    "specimen_voucher",
    "sub_species",
    "sub_strain",
    "variety",
    "mating_type",
    "sex",
    "germline",
    # sampling / collection
    "collection_date",
    "collection_date_start",
    "collection_date_end",
    "collected_by",
    "identified_by",
    "country",
    "lat",
    "lon",
    "location",
    "location_start",
    "location_end",
    "altitude",
    "depth",
    "elevation",
    "marine_region",
    "sampling_campaign",
    "sampling_platform",
    "sampling_site",
    "sample_collection",
    "sample_material",
    "sample_capture_status",
    "protocol_label",
    # environmental
    "environmental_sample",
    "environment_biome",
    "environment_feature",
    "environment_material",
    "broad_scale_environmental_context",
    "local_environmental_context",
    "environmental_medium",
    "investigation_type",
    "ph",
    "salinity",
    "temperature",
    # host
    "host",
    "host_body_site",
    "host_genotype",
    "host_gravidity",
    "host_growth_conditions",
    "host_phenotype",
    "host_scientific_name",
    "host_sex",
    "host_status",
    "host_tax_id",
    "submitted_host_sex",
    # biological features
    "cell_line",
    "cell_type",
    "dev_stage",
    "disease",
    "tissue_lib",
    "tissue_type",
    "age",
    "experimental_factor",
    # surveillance / clinical
    "serotype",
    "serovar",
    "surveillance_target",
    "target_gene",
    # assembly / mag
    "assembly_quality",
    "assembly_software",
    "binning_software",
    "completeness_score",
    "contamination_score",
    "sequencing_method",
    # taxonomy
    "tax_lineage",
    "taxonomic_classification",
    "taxonomic_identity_marker",
    "related_sample_accession",
]


def ingest_ena(
    base_output_dir: Path,
    db_file: Optional[Path],
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    use_last_updated: bool,
    skip_experiments: bool,
    skip_samples: bool,
):
    output_dir = base_output_dir / _ENA_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"ena_metadata_{date_str}.db")

    logging.debug(
        "extracting ena file dumps in range (start_date=%s, end_date=%s)",
        start_date,
        end_date,
    )
    study_accessions = _fetch_accessions(start_date, end_date, use_last_updated)
    assert study_accessions is not None, "failed to fetch accessions"
    assert len(study_accessions) > 0, (
        f"no accessions found in {start_date} to {end_date} range"
    )

    _run_per_study_download(
        study_accessions=study_accessions,
        num_workers=num_workers,
        fields=_STUDY_FIELDS,
        output_dir=output_dir / "studies",
        result_type="study",
        desc="downloading per-study metadata",
    )

    if not skip_experiments:
        _run_per_study_download(
            study_accessions=study_accessions,
            num_workers=num_workers,
            fields=_EXPERIMENT_FIELDS,
            output_dir=output_dir / "experiments",
            result_type="read_experiment",
            desc="downloading experiments per study",
        )

    if not skip_samples:
        _run_per_study_download(
            study_accessions=study_accessions,
            num_workers=num_workers,
            fields=_SAMPLE_FIELDS,
            output_dir=output_dir / "samples",
            result_type="sample",
            desc="downloading samples per study",
        )

    exec_query_from_file("tables/ena_tables.sql", db_file)
    _ingest_data(db_file, output_dir)
    exec_query_from_file("indexes/ena_indexes.sql", db_file)


def _fetch_accessions(
    start_date: datetime.date, end_date: datetime.date, use_last_updated: bool
) -> list[str] | None:
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    field = "last_updated" if use_last_updated else "first_public"

    r = send_req(
        "GET",
        f"{_ENA_PORTAL_API_BASE_URL}/search",
        params={
            "result": "study",
            "query": f"{field} >= {start_date_str} AND {field} <= {end_date_str}",
            "fields": "study_accession",
            "format": "json",
        },
    )
    if r is None:
        return None

    data = r.json()
    r.close()
    study_accessions: list[str] = [
        s["study_accession"] for s in data if s.get("study_accession")
    ]
    return study_accessions


def _fetch_per_study(
    study_accession: str, result_type: str, fields: list[str]
) -> list[dict] | None:
    r = send_req(
        "GET",
        f"{_ENA_PORTAL_API_BASE_URL}/search",
        params={
            "result": result_type,
            "query": f"study_accession={study_accession}",
            "fields": ",".join(fields),
            "limit": 0,
            "format": "json",
        },
    )
    if r is None:
        return None
    if r.status_code == 204:
        return []

    data = r.json()
    r.close()
    return data


def _run_per_study_download(
    study_accessions: list[str],
    num_workers: int,
    fields: list[str],
    output_dir: Path,
    result_type: str,
    desc: str,
):
    output_dir.mkdir(exist_ok=True, parents=True)

    def _worker(study_accession: str):
        output_file = output_dir / f"{study_accession}.json"
        data = _fetch_per_study(study_accession, result_type, fields)
        if data is None:
            return

        with open(output_file, "w") as f:
            json.dump(data, f)

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(
            total=len(study_accessions),
            unit="accession",
            desc=desc,
        ) as pbar,
    ):
        futures = [pool.submit(_worker, acc) for acc in study_accessions]

        for f in as_completed(futures):
            f.result()
            pbar.update(1)


def _ingest_data(db_file: Path, output_dir: Path):
    with sqlite3.connect(db_file) as conn:
        ingest_data = [
            (
                "studies",
                "study_accession",
                _STUDY_FIELDS,
                "storing ena studies in sqlite",
            ),
            (
                "experiments",
                "run_accession",
                _EXPERIMENT_FIELDS,
                "storing ena experiments in sqlite",
            ),
            (
                "samples",
                "sample_accession",
                _SAMPLE_FIELDS,
                "storing ena samples in sqlite",
            ),
        ]

        for folder, key_col, fields, desc in ingest_data:
            _ingest_table_to_sqlite(conn, output_dir / folder, key_col, fields, desc)

        # rebuild fts
        conn.execute("INSERT INTO ena_studies_fts(ena_studies_fts) VALUES('rebuild')")
        conn.execute(
            "INSERT INTO ena_experiments_fts(ena_experiments_fts) VALUES('rebuild')"
        )
        conn.execute("INSERT INTO ena_samples_fts(ena_samples_fts) VALUES('rebuild')")
        conn.commit()


def _ingest_table_to_sqlite(
    conn: sqlite3.Connection,
    output_dir: Path,
    key_col: str,
    fields: list[str],
    desc: str,
):
    key_col_to_table = {
        "study_accession": "ena_studies",
        "run_accession": "ena_experiments",
        "sample_accession": "ena_samples",
    }

    if key_col not in key_col_to_table:
        raise ValueError(f"invalid key col: {key_col}")

    seen = set()
    total = 0
    for path in output_dir.glob("*.json"):
        try:
            with path.open() as f:
                rows = json.load(f)
        except Exception:
            continue

        for row in rows:
            acc = row.get(key_col)
            if acc and acc not in seen:
                seen.add(acc)
                total += 1
    seen.clear()

    with tqdm(total=total, unit=" accession", desc=desc) as pbar:
        cursor = conn.cursor()
        table = key_col_to_table[key_col]
        sql = f"INSERT OR IGNORE INTO {table} ({', '.join(fields)}) VALUES ({', '.join('?' * len(fields))})"

        for path in sorted(output_dir.glob("*.json")):
            try:
                with path.open() as f:
                    rows = json.load(f)
            except Exception:
                continue

            for row in rows:
                acc = row.get(key_col)
                if not acc or acc in seen:
                    continue

                seen.add(acc)
                cursor.execute(sql, _record_to_tuple(row, fields))
                pbar.update(1)

        conn.commit()


def _record_to_tuple(record: dict, cols: list[str]) -> tuple:
    values = []
    for col in cols:
        v = record.get(col, None)
        if v == "":
            v = None
        if col in ["tax_id", "base_count", "read_count"]:
            v = _coerce_int(v)
        values.append(v)
    return tuple(values)


def _coerce_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
