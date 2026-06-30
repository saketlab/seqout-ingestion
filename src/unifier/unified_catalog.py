import contextlib
import json
import logging
import re
import sqlite3
from pathlib import Path

import pandas as pd

from helpers import exec_query_from_file

logger = logging.getLogger(__name__)

UNIFIED_CATALOG_DIR = "unified_catalog"


def build_unified_catalog(
    base_output_dir: Path,
    geo_db_file: Path,
    sra_db_file: Path,
    ena_db_file: Path,
    ae_db_file: Path,
    db_file: Path | None,
    dump_parquet: bool,
) -> None:
    output_dir = base_output_dir / UNIFIED_CATALOG_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    db_file = db_file or (output_dir / "unified_catalog.db")

    geo_dict = _load_geo(geo_db_file)
    sra_dict = _load_sra(sra_db_file)
    ena_dict = _load_ena(ena_db_file)
    ae_dict = _load_ae(ae_db_file)
    catalog = _build_catalog_tuples(geo_dict, sra_dict, ena_dict, ae_dict)

    with sqlite3.Connection(db_file) as conn:
        exec_query_from_file("tables/unified_studies.sql", conn=conn)
        conn.executemany(
            "INSERT OR REPLACE INTO unified_studies VALUES (?, ?, ?, ?, ?)",
            catalog,
        )
        conn.commit()

        if dump_parquet:
            table_df = pd.read_sql_query("SELECT * FROM unified_studies", conn)
            pq_path = output_dir / "unified_studies.parquet"
            table_df.to_parquet(pq_path)
            logger.info("saved data as parquet at %s", str(pq_path))

    size_mb = db_file.stat().st_size / 1024**2
    logger.info("inserted %d rows (%.2f MB)", len(catalog), size_mb)


def _load_geo(geo_db_file: Path) -> dict[str, tuple[str, str]]:
    with sqlite3.Connection(geo_db_file) as conn:
        rows = conn.execute(
            "SELECT accession, title, summary FROM geo_series",
        ).fetchall()

    result = {}
    for acc, title, summary in rows:
        result[acc] = (_clean_string(title), _clean_string(summary))

    return result


def _load_sra(sra_db_file: Path) -> dict[str, tuple[str, str, str]]:
    with sqlite3.Connection(sra_db_file) as conn:
        rows = conn.execute(
            "SELECT accession, alias, title, abstract FROM sra_studies",
        ).fetchall()

    result = {}
    for acc, alias, title, abstract in rows:
        result[acc] = (
            _clean_string(alias),
            _clean_string(title),
            _clean_string(abstract),
        )

    return result


def _load_ena(ena_db_file: Path) -> dict[str, tuple[str, str, str, str]]:
    with sqlite3.Connection(ena_db_file) as conn:
        rows = conn.execute(
            """SELECT
                study_accession, secondary_study_accession, geo_accession,
                study_title, study_description
            FROM ena_studies""",
        ).fetchall()

    result = {}
    for study_acc, sec_acc, geo_acc, title, description in rows:
        result[study_acc] = (
            _clean_string(sec_acc),
            _clean_string(geo_acc),
            _clean_string(title),
            _strip_html(description),
        )

    return result


def _load_ae(ae_db_file: Path) -> dict[str, tuple[str, str, list[dict]]]:
    with sqlite3.Connection(ae_db_file) as conn:
        rows = conn.execute(
            """SELECT accession, title, description, external_links
            FROM arrayexpress_experiments""",
        ).fetchall()

    result = {}
    for acc, title, desc, links_json in rows:
        links = []
        if links_json:
            with contextlib.suppress(json.JSONDecodeError, TypeError):
                links = json.loads(links_json)

        result[acc] = (_clean_string(title), _clean_string(desc), links)
    return result


# future reference: https://github.com/saketlab/pysradb-ingestion/blob/d117090430a6e2b095d5fda71c65e3275db10e6a/embed/build_unified_catalog.py#L142-L394
def _build_catalog_tuples(
    geo_dict: dict,
    sra_dict: dict,
    ena_dict: dict,
    ae_dict: dict,
) -> list[tuple[str, str, str, str, str]]:
    # sra alias -> sra accession(s) (reverse: given a gse or prjna, find srps)
    # multiple srps can map to the same alias (e.g. two srps for one gse)
    alias_to_srps: dict[str, list[str]] = {}
    srp_to_alias: dict[str, str] = {}
    for srp, (alias, _, _) in sra_dict.items():
        if alias:
            alias_to_srps.setdefault(alias, []).append(srp)
            srp_to_alias[srp] = alias

    # ena: geo_accession -> list of ena prj accessions
    geo_to_ena: dict[str, list[str]] = {}
    # ena: secondary (srp/erp/drp) -> ena prj accession
    srp_to_ena: dict[str, str] = {}
    for prj, (secondary, geo_acc, _, _) in ena_dict.items():
        if geo_acc:
            geo_to_ena.setdefault(geo_acc, []).append(prj)
        if secondary:
            srp_to_ena[secondary] = prj

    # ae: e-geod-xxx -> gsexxx
    ae_geod_to_gse: dict[str, str] = {}
    # ae: non-geod accession -> ena accessions (from external_links type=ena)
    ae_to_ena_links: dict[str, list[str]] = {}
    for ae_acc, (_, _, links) in ae_dict.items():
        if ae_acc.startswith("E-GEOD-"):
            gse_num = ae_acc[len("E-GEOD-") :]
            ae_geod_to_gse[ae_acc] = f"GSE{gse_num}"
        else:
            ena_accs = []
            for link in links:
                if isinstance(link, dict) and link.get("type") == "ENA":
                    url = link.get("url", "")
                    if url:
                        ena_accs.append(url)
            if ena_accs:
                ae_to_ena_links[ae_acc] = ena_accs

    # reverse: ena accession -> ae accession (for non-geod ae)
    ena_to_ae: dict[str, str] = {}
    for ae_acc, ena_accs in ae_to_ena_links.items():
        for ena_acc in ena_accs:
            ena_to_ae[ena_acc] = ae_acc

    claimed: set[str] = set()
    catalog: list[tuple[str, str, str, str, str]] = []

    # pre-claiming ae-linked sra entries
    # ae external_links contain erp accessions that also exist in sra.
    # since ae has richer metadata for these european studies, pre-claim
    # them so sra skips them. also handle sra entries whose alias is an
    # ae accession (e.g. erp152569 alias=e-mtab-13444).
    ae_preclaim: set[str] = set()
    for ae_acc in ae_dict:
        if ae_acc in claimed:
            continue
        if ae_acc.startswith("E-GEOD-"):
            continue  # these are handled by geo
        ena_accs = ae_to_ena_links.get(ae_acc, [])
        for ena_acc in ena_accs:
            # the ena link (e.g. erp123) may be in sra db
            if ena_acc in sra_dict:
                ae_preclaim.add(ena_acc)
            # the ena link's prj parent may have a secondary that's in sra
            if ena_acc in srp_to_ena:
                prj = srp_to_ena[ena_acc]
                if prj in ena_dict:
                    sec = ena_dict[prj][0]
                    if sec and sec in sra_dict:
                        ae_preclaim.add(sec)

    # also find sra entries whose alias is an ae accession
    for srp, (alias, _, _) in sra_dict.items():
        if alias and alias in ae_dict and not alias.startswith("E-GEOD-"):
            ae_preclaim.add(srp)

    claimed.update(ae_preclaim)
    if ae_preclaim:
        logger.info("pre-claimed %d sra entries for ae", len(ae_preclaim))

    # 1. processing geo (highest priority)
    logger.info("processing geo...")
    for gse, (title, desc) in geo_dict.items():
        aliases: dict[str, str] = {}

        # find sra alias(es) -- may be multiple srps for one gse
        srps = alias_to_srps.get(gse, [])
        for srp in srps:
            aliases.setdefault("sra", srp)  # store first srp
            claimed.add(srp)

            # sra entry alias may be a bioproject (prjna), geo (gse), or garbage
            bp_alias = srp_to_alias.get(srp, "")
            if bp_alias not in (srp, gse):
                if re.match(r"^PRJN[AEBDC]\d+$", bp_alias):
                    aliases.setdefault("bioproject", bp_alias)
                elif re.match(r"^GSE\d+$", bp_alias):
                    aliases.setdefault("geo", bp_alias)

            # if this srp maps to an ena prj, claim it
            if srp in srp_to_ena:
                prj = srp_to_ena[srp]
                aliases.setdefault("ena", prj)
                claimed.add(prj)

        # find ena alias(es) via geo_accession
        ena_prjs = geo_to_ena.get(gse, [])
        for prj in ena_prjs:
            aliases.setdefault("ena", prj)
            claimed.add(prj)

        # find arrayexpress e-geod alias
        ae_acc = f"E-GEOD-{gse[3:]}"
        if ae_acc in ae_dict:
            aliases["arrayexpress"] = ae_acc
            claimed.add(ae_acc)

        combined_text = f"{title}. {desc}".strip(". ")
        if combined_text:
            catalog.append(
                (gse, "geo", title, desc, json.dumps(aliases, separators=(",", ":"))),
            )

        claimed.add(gse)

    logger.info("\tgeo: %d canonical", len([r for r in catalog if r[1] == "geo"]))

    # 2. processing sra
    # skip gse-aliased, those are already claimed by geo
    logger.info("processing sra...")
    sra_count = 0
    for srp, (alias, title, abstract) in sra_dict.items():
        if srp in claimed:
            continue

        aliases = {}

        # sra alias may be a bioproject (prjna), geo (gse), or garbage
        # skip self-aliases (drp/erp entries where alias == accession)
        if alias and alias != srp:
            if re.match(r"^PRJN[AEBDC]\d+$", alias):
                aliases["bioproject"] = alias
            elif re.match(r"^GSE\d+$", alias):
                aliases["geo"] = alias

            # find ena entry via the srp's secondary accession reverse lookup
            ena_prj = srp_to_ena.get(srp)
            if ena_prj:
                aliases["ena"] = ena_prj
                claimed.add(ena_prj)
            # also check if alias itself (prjna) is an ena study_accession
            if alias in ena_dict:
                if "ena" not in aliases:
                    aliases["ena"] = alias
                claimed.add(alias)
        else:
            # Even without a useful alias, check reverse SRP->ENA lookup
            ena_prj = srp_to_ena.get(srp)
            if ena_prj:
                aliases["ena"] = ena_prj
                claimed.add(ena_prj)

        combined_text = f"{title}. {abstract}".strip(". ")
        if combined_text:
            catalog.append(
                (
                    srp,
                    "sra",
                    title,
                    abstract,
                    json.dumps(aliases, separators=(",", ":")),
                ),
            )
            sra_count += 1

        claimed.add(srp)

    logger.info("\tsra: %d canonical", sra_count)

    # 3. processing arrayexpress
    # note: ae external_links contain ena accessions (erp*). these erp*
    # accessions may also exist as study_accession in ena db (as prjeb*)
    # and as accessions in the sra db. we must claim all of them.
    logger.info("processing arrayexpress...")
    ae_count = 0
    for ae_acc, (title, desc, _) in ae_dict.items():
        if ae_acc in claimed:
            continue

        aliases = {}

        # claim linked ena accessions
        ena_accs = ae_to_ena_links.get(ae_acc, [])
        for ena_acc in ena_accs:
            aliases.setdefault("ena", ena_acc)
            claimed.add(ena_acc)
            # the ena external_link value (e.g. erp123456) might be:
            # a) a secondary_study_accession in ena db -> claim its prj parent
            # b) a study_accession in ena db directly
            # c) also present as an accession in the sra db
            if ena_acc in ena_dict:
                # it's an ena study_accession (prj*), claim its secondary too
                secondary = ena_dict[ena_acc][0]
                if secondary:
                    aliases.setdefault("sra", secondary)
                    claimed.add(secondary)
            # check if it's a secondary accession (erp*) that maps to a prj
            if ena_acc in srp_to_ena:
                prj = srp_to_ena[ena_acc]
                claimed.add(prj)
            # also claim from sra db if the erp exists there
            if ena_acc in sra_dict:
                claimed.add(ena_acc)
                aliases.setdefault("sra", ena_acc)

        combined_text = f"{title}. {desc}".strip(". ")
        if combined_text:
            catalog.append(
                (ae_acc, "ae", title, desc, json.dumps(aliases, separators=(",", ":"))),
            )
            ae_count += 1

        claimed.add(ae_acc)

    logger.info("\tae: %d canonical", ae_count)

    # 4. processing ena
    logger.info("processing ena...")
    ena_count = 0
    for prj, (secondary, geo_acc, title, desc) in ena_dict.items():
        if prj in claimed:
            continue

        aliases = {}
        if secondary:
            aliases["sra"] = secondary
            # if the secondary srp is in sra but wasn't claimed (edge case), claim it
            if secondary in sra_dict and secondary not in claimed:
                claimed.add(secondary)

        if geo_acc:
            if geo_acc.startswith("GSE"):
                aliases["geo"] = geo_acc
            elif geo_acc.startswith("E-"):
                aliases["arrayexpress"] = geo_acc

        combined_text = f"{title}. {desc}".strip(". ")
        if combined_text:
            catalog.append(
                (prj, "ena", title, desc, json.dumps(aliases, separators=(",", ":"))),
            )
            ena_count += 1

        claimed.add(prj)

    logger.info("\tena: %d canonical", ena_count)
    return catalog


def _strip_html(text: str) -> str:
    regexp = re.compile(r"<[^>]+>")
    return regexp.sub("", _clean_string(text)).strip()


def _clean_string(s: str) -> str:
    return (s or "").strip()
