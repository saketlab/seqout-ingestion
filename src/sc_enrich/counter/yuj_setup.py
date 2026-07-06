import csv
import json
import logging
import math
from collections import defaultdict
from pathlib import Path

from helpers import batch_query_in, sqlite_connect
from sc_enrich.counter._process import _classify_file
from sc_enrich.counter.counter import DownloadLink, FileType

SC_ENRICH_OUTPUT_DIR = "sc_enrich"

logger = logging.getLogger(__name__)


def yuj_setup(
    base_output_dir: Path,
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    ae_db_file: Path,
    machines_csv_file: Path,
    batch_size: int,
) -> None:
    output_dir = base_output_dir / SC_ENRICH_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    conn = sqlite_connect(unified_metadata_db_file)
    geo_conn = sqlite_connect(geo_db_file)
    ae_conn = sqlite_connect(ae_db_file)

    def _db_flush() -> None:
        ae_conn.close()
        geo_conn.close()
        conn.close()

    accessions: list[str | tuple[str, str]] = []
    accessions_to_process: set[str] = set()
    gsm_to_gse: dict[str, str] = {}
    download_links_mapping: dict[str, list[DownloadLink]] = defaultdict(
        list[DownloadLink],
    )

    all_records = conn.execute(
        "SELECT canonical_accession, source, aliases "
        "FROM unified_metadata "
        "WHERE is_single_cell = '1' "
        "AND (single_cell_count IS NULL OR single_cell_count = 0)"
    ).fetchall()

    for accession, source, aliases_json in all_records:
        if source not in {"geo", "ae"}:
            aliases: dict[str, str] = json.loads(aliases_json)
            geo_alias = aliases.get("geo")
            arrayexpress_alias = aliases.get("arrayexpress")

            if geo_alias:
                accessions.append((accession, geo_alias))
            elif arrayexpress_alias:
                accessions.append((accession, arrayexpress_alias))
        else:
            accessions.append(accession)

    for v in accessions:
        if isinstance(v, str):
            accessions_to_process.add(v)
        elif isinstance(v, tuple):
            accessions_to_process.add(v[1])

    gse_list = [acc for acc in accessions_to_process if acc.startswith("GSE")]
    rows = batch_query_in(
        geo_conn,
        "SELECT accession, samples_ref FROM geo_series WHERE accession",
        gse_list,
        batch_size,
    )

    for accession, samples_ref_json in rows:
        if not samples_ref_json:
            continue

        samples_ref = json.loads(samples_ref_json)
        for v in samples_ref:
            if isinstance(v, dict):
                gsm_to_gse[v["@ref"]] = accession
            elif isinstance(v, str):
                gsm_to_gse[v] = accession

    all_gsms = list(gsm_to_gse.keys())
    if all_gsms:
        rows = batch_query_in(
            geo_conn,
            "SELECT accession, supplementary_data FROM geo_samples WHERE accession",
            all_gsms,
            batch_size,
        )

        for gsm_accession, supl_data_json in rows:
            if not supl_data_json:
                continue

            gse = gsm_to_gse.get(gsm_accession)
            if gse is None:
                continue

            supl_data: list[dict[str, str]] = json.loads(supl_data_json)
            for item in supl_data:
                url = item.get("#text")
                if not url:
                    continue

                file_type = _classify_file(url)
                if file_type == FileType.Unknown:
                    continue

                download_links_mapping[gse].append(
                    DownloadLink(type=file_type, url=url),
                )

    gse_accessions = {acc for acc in accessions_to_process if acc.startswith("GSE")}
    ae_accessions = accessions_to_process - gse_accessions

    geo_rows = batch_query_in(
        geo_conn,
        "SELECT accession, supplementary_data FROM geo_series WHERE accession",
        list(gse_accessions),
        batch_size,
    )
    ae_rows = batch_query_in(
        ae_conn,
        "SELECT accession, processed_data_files FROM arrayexpress_experiments WHERE accession",  # noqa: E501
        list(ae_accessions),
        batch_size,
    )

    for accession, supl_data_json in geo_rows:
        if not supl_data_json:
            continue

        if len(download_links_mapping.get(accession, [])) > 0:
            continue

        supl_data = json.loads(supl_data_json)
        for item in supl_data:
            url = item.get("#text")
            if url is None:
                continue

            file_type = _classify_file(url)
            if file_type == FileType.Tar:
                download_links_mapping[accession].append(
                    DownloadLink(type=file_type, url=url),
                )
                break

    for accession, processed_data_files_json in ae_rows:
        if not processed_data_files_json:
            continue

        processed_data_files = json.loads(processed_data_files_json)
        for item in processed_data_files:
            url = item.get("url")
            if url is None:
                continue

            file_type = _classify_file(url)
            if file_type == FileType.Unknown:
                continue

            download_links_mapping[accession].append(
                DownloadLink(type=file_type, url=url),
            )

    with Path.open(machines_csv_file, newline="") as f:
        num_machines = sum(1 for _ in csv.reader(f))

    if num_machines == 0:
        logger.error("machines CSV is empty")
        _db_flush()
        return

    data: list[tuple[str, str, str, list[tuple[str, str]]]] = []

    for r in all_records:
        accession = str(r[0])
        source = str(r[1])
        aliases: list[str] = list(dict(json.loads(r[2])).values())
        dl_items = download_links_mapping.get(r[0])
        if not dl_items:
            continue

        download_links = [(str(i.type), i.url) for i in dl_items]
        data.append((accession, source, "|".join(aliases), download_links))

    chunk_size = math.ceil(len(data) / num_machines)
    master_path = output_dir / "yuj_setup_master.txt"
    with Path.open(master_path, mode="w") as mf:
        for i in range(num_machines):
            chunk = data[i * chunk_size : (i + 1) * chunk_size]
            if not chunk:
                break

            csv_path = output_dir / f"yuj_setup_{i}.csv"
            with Path.open(csv_path, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["accession", "source", "aliases", "supplementary_data"],
                )
                writer.writerows(chunk)

            mf.write(f"{csv_path}\n")

    _db_flush()
