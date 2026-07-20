"""
MAGE-TAB (IDF/SDRF) parsing, shared by ArrayExpress (EBI) and GEA (DDBJ).

Same format, different provenance for the non-tabular metadata -- which is why
the column mappings are injected rather than hardcoded:

- ArrayExpress has a BioStudies JSON API, so its IDF is only a sidecar:
  title/description/study_type come from JSON (_merge_json_and_idf in ae.py).
- GEA has no such API, so GEA_IDF_MAPPING promotes the keys AE takes from JSON
  (Investigation Title, Experiment Description, Comment[AEExperimentType],
  Comment[BioProject]) into real columns instead of leaving them in `overflow`.

The SDRF mappings differ too: AE points at ENA (Comment[ENA_RUN]) while GEA
points at DDBJ. Note GEA still uses the INSDC column *names* -- its runs sit
under Comment[SRA_RUN] even though the accessions are DRR.
"""

import csv
import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

IdfValueExtractor = Callable[[list[str]], Any]
IdfMapping = dict[str, tuple[str, IdfValueExtractor]]
SdrfMapping = dict[str, str]


def idf_first_or_none(values: list[str]) -> str | None:
    return values[0] if values else None


def idf_to_json_array(values: list[str]) -> str | None:
    return json.dumps(values) if values else None


AE_IDF_MAPPING: IdfMapping = {
    "PubMed ID": ("pubmed_id", idf_first_or_none),
    "Public Release Date": ("release_date_idf", idf_first_or_none),
    "Person Last Name": ("contact_name", idf_first_or_none),
    "Person Email": ("contact_email", idf_first_or_none),
    "Person Affiliation": ("contact_affiliation", idf_first_or_none),
    "Comment[ArrayExpressSubmissionDate]": ("submission_date", idf_first_or_none),
    "Experimental Factor Name": ("experimental_factors", idf_to_json_array),
    "Experimental Factor Type": ("experimental_factor_types", idf_to_json_array),
    "Protocol Name": ("protocol_names", idf_to_json_array),
    "Protocol Type": ("protocol_types", idf_to_json_array),
}

# GEA has no BioStudies JSON, so the IDF must supply everything AE gets from it.
GEA_IDF_MAPPING: IdfMapping = {
    **AE_IDF_MAPPING,
    "Investigation Title": ("title", idf_first_or_none),
    "Experiment Description": ("description", idf_first_or_none),
    "Comment[AEExperimentType]": ("study_type", idf_first_or_none),
    "Comment[BioProject]": ("bioproject", idf_first_or_none),
    "Comment[Last Update Date]": ("last_update_date", idf_first_or_none),
    "Protocol Description": ("protocols_description", idf_to_json_array),
}

AE_SDRF_MAPPING: SdrfMapping = {
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

# GEA points at DDBJ, but keeps the INSDC column names: its DRX/DRR accessions
# live under Comment[SRA_EXPERIMENT] / Comment[SRA_RUN].
GEA_SDRF_MAPPING: SdrfMapping = {
    "source name": "source_name",
    "characteristics[organism]": "organism",
    "characteristics[taxonomy_id]": "taxon_id",
    "characteristics[organism part]": "organism_part",
    "characteristics[tissue]": "tissue",
    "characteristics[cell type]": "cell_type",
    "characteristics[genotype]": "genotype",
    "characteristics[strain]": "strain",
    "characteristics[sex]": "sex",
    "characteristics[age]": "age",
    "comment[biosample]": "biosample",
    "comment[sample_title]": "sample_title",
    "comment[description]": "sample_description",
    "comment[library_strategy]": "library_strategy",
    "comment[library_source]": "library_source",
    "comment[library_selection]": "library_selection",
    "comment[library_layout]": "library_layout",
    "comment[instrument_model]": "instrument_model",
    "comment[sra_experiment]": "dra_experiment",
    "comment[sra_run]": "dra_run",
}

# structural MAGE-TAB columns that carry no sample metadata
_SDRF_COLS_TO_SKIP = frozenset(
    {
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
    },
)


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


def parse_idf(filepath: Path, column_mapping: IdfMapping) -> dict | None:
    result: dict[str, Any] = {"overflow": {}}
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

        if key in column_mapping:
            clean_col_name, value_extractor = column_mapping[key]
            result[clean_col_name] = value_extractor(values)

        if values:
            if len(values) == 1:
                result["overflow"][key] = values[0]
            else:
                result["overflow"][key] = values

    return result


def parse_sdrf(
    filepath: Path,
    accession: str,
    column_mapping: SdrfMapping,
) -> list | None:
    samples = []

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

                for db_col in column_mapping.values():
                    sample[db_col] = None

                for i, clean_col in enumerate(headers):
                    if i >= len(row):
                        break

                    value = row[i].strip()
                    if not value:
                        continue

                    if clean_col in column_mapping:
                        sample[column_mapping[clean_col]] = value
                    else:
                        base = (
                            clean_col.split("[")[0].strip()
                            if "[" in clean_col
                            else clean_col
                        )

                        if base in _SDRF_COLS_TO_SKIP or clean_col.startswith("unit["):
                            continue

                        overflow[clean_col] = value

                sample["attributes_json"] = json.dumps(overflow)
                samples.append(sample)

    except FileNotFoundError:
        return None
    else:
        return samples or None
