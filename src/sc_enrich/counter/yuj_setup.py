import json
import logging
from collections import defaultdict
from pathlib import Path

from helpers import batch_query_in, sqlite_connect
from sc_enrich.counter._process import _classify_file
from sc_enrich.counter.counter_common import DownloadLink, FileType

logger = logging.getLogger(__name__)


def _ro_connect(db_file: Path):  # noqa: ANN202
    """Open a source DB read-only (read-only share, WAL mode)."""
    return sqlite_connect(f"file:{db_file}?mode=ro&immutable=1", uri=True)


def _load_universe(
    conn,  # noqa: ANN001
    batch_size: int,
    *,
    accession_list_file: Path | None,
    check_all: bool,
) -> list[tuple[str, str, str]]:
    """Return (canonical_accession, source, aliases) rows for the universe."""
    if accession_list_file is not None:
        ids = [
            line.strip()
            for line in accession_list_file.read_text().splitlines()
            if line.strip()
        ]
        logger.info("universe from %s: %d ids", accession_list_file, len(ids))
        return batch_query_in(
            conn,
            "SELECT canonical_accession, source, aliases "
            "FROM unified_metadata WHERE canonical_accession",
            ids,
            batch_size,
        )
    where = "" if check_all else "AND sc_count_last_checked IS NULL"
    return conn.execute(
        "SELECT canonical_accession, source, aliases "  # noqa: S608 (where is a literal)
        f"FROM unified_metadata WHERE is_single_cell = '1' {where}",
    ).fetchall()


def yuj_setup(
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    ae_db_file: Path,
    output_dir: Path,
    batch_size: int,
    *,
    check_all: bool = False,
    accession_list_file: Path | None = None,
) -> None:
    output_dir.mkdir(exist_ok=True, parents=True)

    conn = _ro_connect(unified_metadata_db_file)
    geo_conn = _ro_connect(geo_db_file)
    ae_conn = _ro_connect(ae_db_file)

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

    all_records = _load_universe(
        conn,
        batch_size,
        accession_list_file=accession_list_file,
        check_all=check_all,
    )
    logger.info("found %d single-cell records to process", len(all_records))

    for accession, source, aliases_json in all_records:
        if source not in {"geo", "ae"}:
            aliases: dict[str, str] = json.loads(aliases_json or "{}")
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

    # download_links_mapping is keyed by the GEO/AE accession (the alias), but a
    # record's canonical_accession may be an SRA/ENA id. Map canonical -> the
    # key we actually stored links under, so the lookup below doesn't miss.
    canonical_to_key = {
        (v[0] if isinstance(v, tuple) else v): (v[1] if isinstance(v, tuple) else v)
        for v in accessions
    }

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

        # Series-level supplementary: capture every countable file, not just
        # tarballs (many studies put the matrix straight on the series).
        supl_data = json.loads(supl_data_json)
        for item in supl_data:
            url = item.get("#text")
            if url is None:
                continue

            file_type = _classify_file(url)
            if file_type == FileType.Unknown:
                continue

            download_links_mapping[accession].append(
                DownloadLink(type=file_type, url=url),
            )

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

    # One sidecar per id (its download links). accessions.txt lists ids with
    # links (the scatter unit); ids with none go to no_link_accessions.txt.
    acc_dir = output_dir / "accessions"
    acc_dir.mkdir(parents=True, exist_ok=True)
    master_path = output_dir / "accessions.txt"
    nolink_path = output_dir / "no_link_accessions.txt"

    n_with = 0
    n_without = 0
    with master_path.open("w") as mf, nolink_path.open("w") as nf:
        for r in all_records:
            accession = str(r[0])
            source = str(r[1])
            alias_values = list(dict(json.loads(r[2] or "{}")).values())
            link_key = canonical_to_key.get(accession, accession)
            dl_items = download_links_mapping.get(link_key) or []
            download_links = [(str(i.type), i.url) for i in dl_items]

            (acc_dir / f"{accession}.json").write_text(
                json.dumps(
                    {
                        "accession": accession,
                        "source": source,
                        "aliases": "|".join(alias_values),
                        "links": download_links,
                    },
                ),
            )
            if download_links:
                mf.write(f"{accession}\n")
                n_with += 1
            else:
                nf.write(f"{accession}\n")
                n_without += 1

    logger.info(
        "emitted %d accessions: %d with links -> %s, %d without -> %s",
        n_with + n_without,
        n_with,
        master_path,
        n_without,
        nolink_path,
    )
    _db_flush()
