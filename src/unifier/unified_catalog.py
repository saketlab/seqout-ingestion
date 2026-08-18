import contextlib
import json
import logging
import re
from pathlib import Path

import pandas as pd

from helpers import exec_query_from_file, sqlite_connect

logger = logging.getLogger(__name__)

UNIFIED_CATALOG_DIR = "unified_catalog"

# an allow-list, since an unrecognised prefix is likelier a parse error than a
# new archive
BIOPROJECT_RE = re.compile(r"^PRJ(?:NA|NB|NC|ND|NE|EB|EA|DB|DA|CA)\d+$")


def build_unified_catalog(
    base_output_dir: Path,
    geo_db_file: Path,
    sra_db_file: Path,
    ena_db_file: Path,
    ae_db_file: Path,
    gsa_db_file: Path,
    gsa_human_db_file: Path,
    dra_db_file: Path,
    gea_db_file: Path,
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
    gsa_dict = _load_gsa(gsa_db_file)
    gsa_human_dict = _load_gsa_human(gsa_human_db_file)
    dra_dict = _load_dra(dra_db_file)
    gea_dict = _load_gea(gea_db_file)
    catalog = _build_catalog_tuples(
        geo_dict,
        sra_dict,
        ena_dict,
        ae_dict,
        dra_dict,
        gea_dict,
    )
    catalog.extend(_build_gsa_tuples(gsa_dict, gsa_human_dict))

    with sqlite_connect(db_file) as conn:
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
    with sqlite_connect(geo_db_file) as conn:
        rows = conn.execute(
            "SELECT accession, title, summary FROM geo_series",
        ).fetchall()

    result = {}
    for acc, title, summary in rows:
        result[acc] = (_clean_string(title), _clean_string(summary))

    return result


def _bioproject_of(external_id_json: str | None) -> str | None:
    """external_id JSON -> its BioProject, if any."""
    if not external_id_json:
        return None
    with contextlib.suppress(json.JSONDecodeError, TypeError, AttributeError):
        return json.loads(external_id_json).get("BioProject")
    return None


def _load_sra(sra_db_file: Path) -> dict[str, tuple[str, str, str, str | None]]:
    """SRP -> (alias, title, abstract, bioproject)."""
    with sqlite_connect(sra_db_file) as conn:
        rows = conn.execute(
            "SELECT accession, alias, title, abstract, external_id FROM sra_studies",
        ).fetchall()

    result = {}
    for acc, alias, title, abstract, external_id in rows:
        result[acc] = (
            _clean_string(alias),
            _clean_string(title),
            _clean_string(abstract),
            _bioproject_of(external_id),
        )

    return result


def _load_dra(dra_db_file: Path) -> dict[str, tuple[str, str, str, str | None]]:
    """DRP -> (alias, title, abstract, bioproject). Same shape as _load_sra."""
    with sqlite_connect(dra_db_file) as conn:
        rows = conn.execute(
            "SELECT accession, alias, title, abstract, external_id FROM dra_studies",
        ).fetchall()

    result = {}
    for acc, alias, title, abstract, external_id in rows:
        result[acc] = (
            _clean_string(alias),
            _clean_string(title),
            _clean_string(abstract),
            _bioproject_of(external_id),
        )

    return result


def _load_gea(gea_db_file: Path) -> dict[str, tuple[str, str, str | None]]:
    """E-GEAD -> (title, description, bioproject)."""
    with sqlite_connect(gea_db_file) as conn:
        rows = conn.execute(
            "SELECT accession, title, description, bioproject FROM gea_experiments",
        ).fetchall()

    return {
        acc: (_clean_string(title), _clean_string(desc), bioproject)
        for acc, title, desc, bioproject in rows
    }


def _load_ena(ena_db_file: Path) -> dict[str, tuple[str, str, str, str]]:
    with sqlite_connect(ena_db_file) as conn:
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
    with sqlite_connect(ae_db_file) as conn:
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


def _load_gsa(gsa_db_file: Path) -> dict[str, tuple[str, str, str]]:
    """
    CRA studies -> (title, description, project_accession).

    GSA has no study abstract, so the description is synthesised from the study's
    distinct experiment titles to give the catalog some searchable text.
    """
    with sqlite_connect(gsa_db_file) as conn:
        rows = conn.execute(
            "SELECT accession, title, alias, project_accession FROM gsa_studies",
        ).fetchall()
        exp_titles = dict(
            conn.execute(
                """SELECT study_accession, group_concat(DISTINCT title)
                FROM gsa_experiments WHERE title IS NOT NULL AND title != ''
                GROUP BY study_accession""",
            ).fetchall(),
        )

    result = {}
    for acc, title, alias, project in rows:
        result[acc] = (
            _clean_string(title) or _clean_string(alias),
            _clean_string(exp_titles.get(acc, "")).replace(",", "; "),
            _clean_string(project),
        )
    return result


def _load_gsa_human(gsa_human_db_file: Path) -> dict[str, tuple[str, str, str]]:
    """
    HRA studies -> (title, description, project_accession).

    Description is synthesised from the study's distinct experiment titles (as for
    CRA); controlled studies have no fetched experiments, so fall back to disease.
    """
    with sqlite_connect(gsa_human_db_file) as conn:
        rows = conn.execute(
            """SELECT accession, title, disease, project_accession
            FROM gsa_human_studies""",
        ).fetchall()
        exp_titles = dict(
            conn.execute(
                """SELECT study_accession, group_concat(DISTINCT title)
                FROM gsa_human_experiments WHERE title IS NOT NULL AND title != ''
                GROUP BY study_accession""",
            ).fetchall(),
        )

    result = {}
    for acc, title, disease, project in rows:
        desc = _clean_string(exp_titles.get(acc, "")).replace(",", "; ")
        result[acc] = (
            _clean_string(title),
            desc or _clean_string(disease),
            _clean_string(project),
        )
    return result


def _build_gsa_tuples(
    gsa_dict: dict[str, tuple[str, str, str]],
    gsa_human_dict: dict[str, tuple[str, str, str]],
) -> list[tuple[str, str, str, str, str]]:
    """
    Build catalog rows for GSA.

    GSA (CRA + HRA) is a distinct accession namespace from the western archives,
    so it appends directly with a bioproject (PRJCA) cross-link, no dedup claiming.
    """
    catalog: list[tuple[str, str, str, str, str]] = []
    for source_dict in (gsa_dict, gsa_human_dict):
        for acc, (title, desc, project) in source_dict.items():
            # blank text falls back to the accession, so every gsa study reaches
            # unified_studies and stays enrichable
            aliases = {"bioproject": project} if project else {}
            catalog.append(
                (
                    acc,
                    "gsa",
                    title or acc,
                    desc,
                    json.dumps(aliases, separators=(",", ":")),
                ),
            )
    logger.info("\tgsa: %d canonical", len(catalog))
    return catalog


# future reference: https://github.com/saketlab/pysradb-ingestion/blob/d117090430a6e2b095d5fda71c65e3275db10e6a/embed/build_unified_catalog.py#L142-L394
def _build_catalog_tuples(
    geo_dict: dict,
    sra_dict: dict,
    ena_dict: dict,
    ae_dict: dict,
    dra_dict: dict,
    gea_dict: dict,
) -> list[tuple[str, str, str, str, str]]:
    alias_to_srps: dict[str, list[str]] = {}
    srp_to_alias: dict[str, str] = {}
    for srp, (alias, _, _, _) in sra_dict.items():
        if alias:
            alias_to_srps.setdefault(alias, []).append(srp)
            srp_to_alias[srp] = alias

    geo_to_ena: dict[str, list[str]] = {}
    srp_to_ena: dict[str, str] = {}
    for prj, (secondary, geo_acc, _, _) in ena_dict.items():
        if geo_acc:
            geo_to_ena.setdefault(geo_acc, []).append(prj)
        if secondary:
            srp_to_ena[secondary] = prj

    ae_geod_to_gse: dict[str, str] = {}
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

    ena_to_ae: dict[str, str] = {}
    for ae_acc, ena_accs in ae_to_ena_links.items():
        for ena_acc in ena_accs:
            ena_to_ae[ena_acc] = ae_acc

    claimed: set[str] = set()
    catalog: list[tuple[str, str, str, str, str]] = []

    # sra and dra share one bioproject namespace: a PRJDB may be held by a
    # mirrored SRP or by a DRP
    bioproject_to_studies: dict[str, list[str]] = {}
    for raw_dict in (sra_dict, dra_dict):
        for study_acc, (_, _, _, bioproject) in raw_dict.items():
            if bioproject:
                bioproject_to_studies.setdefault(bioproject, []).append(study_acc)

    # gea holds the curated expression layer and the matching raw study only the
    # runs, so gea claims it first; same rule as ae_preclaim
    gea_preclaim: set[str] = set()
    gea_to_raw: dict[str, list[str]] = {}
    for gea_acc, (_, _, bioproject) in gea_dict.items():
        if not bioproject:
            continue
        raw_studies = bioproject_to_studies.get(bioproject, [])
        if raw_studies:
            gea_to_raw[gea_acc] = raw_studies
            gea_preclaim.update(raw_studies)

    claimed.update(gea_preclaim)
    if gea_preclaim:
        logger.info("pre-claimed %d raw studies for gea", len(gea_preclaim))

    # ae external_links carry erp accessions that also exist in sra, and ae has
    # the richer metadata for these european studies, so pre-claim them. sra
    # entries aliased to an ae accession (erp152569 alias=e-mtab-13444) too.
    ae_preclaim: set[str] = set()
    for ae_acc in ae_dict:
        if ae_acc in claimed:
            continue
        if ae_acc.startswith("E-GEOD-"):
            continue  # these are handled by geo
        ena_accs = ae_to_ena_links.get(ae_acc, [])
        for ena_acc in ena_accs:
            if ena_acc in sra_dict:
                ae_preclaim.add(ena_acc)
            if ena_acc in srp_to_ena:
                prj = srp_to_ena[ena_acc]
                if prj in ena_dict:
                    sec = ena_dict[prj][0]
                    if sec and sec in sra_dict:
                        ae_preclaim.add(sec)

    for srp, (alias, _, _, _) in sra_dict.items():
        if alias and alias in ae_dict and not alias.startswith("E-GEOD-"):
            ae_preclaim.add(srp)

    claimed.update(ae_preclaim)
    if ae_preclaim:
        logger.info("pre-claimed %d sra entries for ae", len(ae_preclaim))

    logger.info("processing geo...")
    for gse, (title, desc) in geo_dict.items():
        aliases: dict[str, str] = {}

        srps = alias_to_srps.get(gse, [])
        for srp in srps:
            aliases.setdefault("sra", srp)
            claimed.add(srp)

            bp_alias = srp_to_alias.get(srp, "")
            if bp_alias not in (srp, gse):
                if BIOPROJECT_RE.match(bp_alias):
                    aliases.setdefault("bioproject", bp_alias)
                elif re.match(r"^GSE\d+$", bp_alias):
                    aliases.setdefault("geo", bp_alias)

            if srp in srp_to_ena:
                prj = srp_to_ena[srp]
                aliases.setdefault("ena", prj)
                claimed.add(prj)

        ena_prjs = geo_to_ena.get(gse, [])
        for prj in ena_prjs:
            aliases.setdefault("ena", prj)
            claimed.add(prj)

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

    logger.info("processing gea...")
    gea_count = 0
    for gea_acc, (title, desc, bioproject) in gea_dict.items():
        if gea_acc in claimed:
            continue

        aliases = {}
        if bioproject:
            aliases["bioproject"] = bioproject
        raw_studies = gea_to_raw.get(gea_acc, [])
        if raw_studies:
            aliases["sra"] = raw_studies[0]

        # the pre-claimed raw study never runs its own pass, so inherit the ena
        # claims it would have made, or the mirror escapes as a second row
        if bioproject and bioproject in ena_dict:
            aliases.setdefault("ena", bioproject)
            claimed.add(bioproject)
        for raw_acc in raw_studies:
            ena_prj = srp_to_ena.get(raw_acc)
            if ena_prj:
                aliases.setdefault("ena", ena_prj)
                claimed.add(ena_prj)

        catalog.append(
            (
                gea_acc,
                "gea",
                title or gea_acc,
                desc,
                json.dumps(aliases, separators=(",", ":")),
            ),
        )
        gea_count += 1
        claimed.add(gea_acc)

    logger.info("\tgea: %d canonical", gea_count)

    logger.info("processing sra...")
    sra_count = 0
    for srp, (alias, title, abstract, _bioproject) in sra_dict.items():
        if srp in claimed:
            continue

        aliases = {}

        # self-alias and are skipped
        if alias and alias != srp:
            if BIOPROJECT_RE.match(alias):
                aliases["bioproject"] = alias
            elif re.match(r"^GSE\d+$", alias):
                aliases["geo"] = alias

            ena_prj = srp_to_ena.get(srp)
            if ena_prj:
                aliases["ena"] = ena_prj
                claimed.add(ena_prj)
            if alias in ena_dict:
                if "ena" not in aliases:
                    aliases["ena"] = alias
                claimed.add(alias)
        else:
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

    logger.info("processing arrayexpress...")
    ae_count = 0
    for ae_acc, (title, desc, _) in ae_dict.items():
        if ae_acc in claimed:
            continue

        aliases = {}

        ena_accs = ae_to_ena_links.get(ae_acc, [])
        for ena_acc in ena_accs:
            aliases.setdefault("ena", ena_acc)
            claimed.add(ena_acc)
            # an external_link value can be an ena study_accession, a secondary
            # accession whose prj parent needs claiming, or an sra accession;
            # all three must be claimed or the study escapes as a second row
            if ena_acc in ena_dict:
                secondary = ena_dict[ena_acc][0]
                if secondary:
                    aliases.setdefault("sra", secondary)
                    claimed.add(secondary)
            if ena_acc in srp_to_ena:
                prj = srp_to_ena[ena_acc]
                claimed.add(prj)
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

    logger.info("processing ena...")
    ena_count = 0
    for prj, (secondary, geo_acc, title, desc) in ena_dict.items():
        if prj in claimed:
            continue

        aliases = {}
        if secondary:
            aliases["sra"] = secondary
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

    # lowest priority: only DRPs nobody else holds
    logger.info("processing dra...")
    dra_count = 0
    for drp, (alias, title, abstract, bioproject) in dra_dict.items():
        if drp in claimed:
            continue

        aliases = {}
        if bioproject:
            aliases["bioproject"] = bioproject
        if alias and alias != drp and BIOPROJECT_RE.match(alias):
            aliases.setdefault("bioproject", alias)

        combined_text = f"{title}. {abstract}".strip(". ")
        if combined_text:
            catalog.append(
                (
                    drp,
                    "dra",
                    title,
                    abstract,
                    json.dumps(aliases, separators=(",", ":")),
                ),
            )
            dra_count += 1

        claimed.add(drp)

    logger.info("\tdra: %d canonical", dra_count)
    return catalog


def _strip_html(text: str) -> str:
    regexp = re.compile(r"<[^>]+>")
    return regexp.sub("", _clean_string(text)).strip()


def _clean_string(s: str) -> str:
    return (s or "").strip()
