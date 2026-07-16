"""
INSDC submission XML parsing, shared by SRA (NCBI) and DRA (DDBJ).

Both archives publish the same element structure with the same attribute names,
and differ in exactly two ways -- hence the two injected seams:

- identifiers: SRA uses <EXTERNAL_ID namespace="BioProject">, DDBJ <PRIMARY_ID
  label="BioProject ID"> and no EXTERNAL_ID at all. Both extractors normalise to
  {"BioProject": ..., "BioSample": ...}, so downstream cannot tell them apart.
- dates: SRA XML carries none (they come from SRA_Accessions.tab); DDBJ's come
  from its livelist plus SUBMISSION@submission_date.
"""

import enum
import json
import logging
from collections.abc import Callable, Iterator
from typing import IO

import lxml.etree as ET  # noqa: N812

logger = logging.getLogger(__name__)

IdExtractor = Callable[[list], str | None]

DateResolver = Callable[[str], tuple[str, str] | None]

# DDBJ PRIMARY_ID @label values -> the namespace SRA would have used.
# An unlabelled PRIMARY_ID is just the record's own accession, not an xref.
_DDBJ_LABELS = {
    "BioProject ID": "BioProject",
    "BioSample ID": "BioSample",
}


class FileType(enum.StrEnum):
    Submission = "submission"
    Sample = "sample"
    Study = "study"
    Experiment = "experiment"
    Run = "run"


def sra_id_extractor(identifier_children: list) -> str | None:
    external_ids = [el for el in identifier_children if el.tag == "EXTERNAL_ID"]
    if not external_ids:
        return None
    return json.dumps({el.get("namespace"): el.text for el in external_ids})


def ddbj_id_extractor(identifier_children: list) -> str | None:
    result = {}
    for el in identifier_children:
        if el.tag != "PRIMARY_ID":
            continue
        namespace = _DDBJ_LABELS.get(el.get("label") or "")
        if namespace and el.text:
            result[namespace] = el.text.strip()

    if not result:
        return None
    return json.dumps(result)


def _identifier_children(elem) -> list:  # noqa: ANN001
    identifiers = elem.find("IDENTIFIERS")
    return list(identifiers) if identifiers is not None else []


def _ref(elem, name: str) -> str | None:  # noqa: ANN001
    """Read a *_REF accession, falling back to refname."""
    ref_elem = elem.find(name)
    if ref_elem is None:
        return None
    return ref_elem.get("accession") or ref_elem.get("refname")


def parse_attributes(attrs: list) -> str | None:
    if not attrs:
        return None
    return json.dumps(
        {attr.findtext("TAG"): attr.findtext("VALUE") for attr in attrs},
    )


def parse_links(links: list) -> str | None:
    if not links:
        return None
    results: dict = {}

    for link in links:
        url = link.find("URL_LINK")
        if url is not None:
            results[url.findtext("LABEL")] = url.findtext("URL")
        else:
            for child in link:
                results[str(child.tag)] = {str(el.tag): el.text for el in child}

    return json.dumps(results)


def process_submission(
    submission_accession: str,
    content: IO[bytes],
    dates: DateResolver,
) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))
    attrs: dict[str, str] = {}

    try:
        for _, elem in ctx:
            if elem.tag != "SUBMISSION":
                continue
            attrs = dict(elem.attrib)
            elem.clear()
            break
    except ET.ParseError:
        return

    accession = attrs.get("accession", submission_accession)
    if not accession:
        return

    resolved = dates(accession)
    if resolved is None:
        return
    updated_at, published_at = resolved

    yield (
        accession,
        attrs.get("alias"),
        attrs.get("center_name"),
        attrs.get("lab_name"),
        attrs.get("broker_name"),
        updated_at,
        published_at,
    )


def process_studies(
    submission_accession: str,
    content: IO[bytes],
    dates: DateResolver,
    ids: IdExtractor,
) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "STUDY_SET":
                continue

            for study in elem.findall("STUDY"):
                accession = study.get("accession")
                if not accession:
                    continue

                resolved = dates(accession)
                if resolved is None:
                    continue
                updated_at, published_at = resolved

                descriptor = study.find("DESCRIPTOR")
                study_type_elem = (
                    descriptor.find("STUDY_TYPE") if descriptor is not None else None
                )

                yield (
                    accession,
                    study.get("alias"),
                    study.get("center_name"),
                    descriptor.findtext("STUDY_ABSTRACT")
                    if descriptor is not None
                    else None,
                    descriptor.findtext("STUDY_TITLE")
                    if descriptor is not None
                    else None,
                    ids(_identifier_children(study)),
                    study_type_elem.get("existing_study_type")
                    if study_type_elem is not None
                    else None,
                    parse_attributes(study.findall("STUDY_ATTRIBUTES/STUDY_ATTRIBUTE")),
                    parse_links(study.findall("STUDY_LINKS/STUDY_LINK")),
                    descriptor.findtext("CENTER_PROJECT_NAME")
                    if descriptor is not None
                    else None,
                    submission_accession,
                    published_at,
                    updated_at,
                )

                study.clear()
            elem.clear()
            break
    except ET.ParseError:
        return


def process_samples(
    submission_accession: str,
    content: IO[bytes],
    ids: IdExtractor,
) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "SAMPLE":
                continue

            yield (
                elem.get("accession"),
                elem.get("alias"),
                elem.findtext("DESCRIPTION"),
                elem.findtext("TITLE"),
                elem.findtext("SAMPLE_NAME/SCIENTIFIC_NAME"),
                elem.findtext("SAMPLE_NAME/TAXON_ID"),
                ids(_identifier_children(elem)),
                parse_attributes(elem.findall("SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE")),
                submission_accession,
                parse_links(elem.findall("SAMPLE_LINKS/SAMPLE_LINK")),
            )

            elem.clear()
    except ET.ParseError:
        return


def process_experiments(
    submission_accession: str,
    content: IO[bytes],
) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "EXPERIMENT_SET":
                continue

            for experiment in elem.findall("EXPERIMENT"):
                design_elem = experiment.find("DESIGN")

                design_description = None
                library_layout = None
                library_name = None
                library_selection = None
                library_source = None
                library_strategy = None
                samples: list[str] = []

                if design_elem is not None:
                    design_description = design_elem.findtext("DESIGN_DESCRIPTION")

                    lib_desc = design_elem.find("LIBRARY_DESCRIPTOR")
                    if lib_desc is not None:
                        library_name = lib_desc.findtext("LIBRARY_NAME")
                        library_selection = lib_desc.findtext("LIBRARY_SELECTION")
                        library_source = lib_desc.findtext("LIBRARY_SOURCE")
                        library_strategy = lib_desc.findtext("LIBRARY_STRATEGY")

                        layout_elem = lib_desc.find("LIBRARY_LAYOUT")
                        if layout_elem is not None:
                            first_child = next(iter(layout_elem), None)
                            library_layout = (
                                str(first_child.tag)
                                if first_child is not None
                                else None
                            )

                    for sample in design_elem.findall("SAMPLE_DESCRIPTOR"):
                        sample_accession = sample.get("accession") or sample.get(
                            "refname",
                        )
                        if sample_accession:
                            samples.append(sample_accession)

                platform_elem = experiment.find("PLATFORM")
                platform = None
                instrument_model = None
                if platform_elem is not None:
                    first_child = next(iter(platform_elem), None)
                    if first_child is not None:
                        platform = str(first_child.tag)
                        instrument_model = first_child.findtext("INSTRUMENT_MODEL")

                yield (
                    experiment.get("accession"),
                    design_description,
                    library_layout,
                    library_name,
                    library_selection,
                    library_source,
                    library_strategy,
                    json.dumps(samples),
                    platform,
                    instrument_model,
                    _ref(experiment, "STUDY_REF"),
                    experiment.findtext("TITLE"),
                    submission_accession,
                )

                experiment.clear()
            elem.clear()
            break
    except ET.ParseError:
        return


def process_runs(submission_accession: str, content: IO[bytes]) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "RUN_SET":
                continue

            for run in elem.findall("RUN"):
                yield (
                    run.get("accession"),
                    _ref(run, "EXPERIMENT_REF"),
                    submission_accession,
                )
                run.clear()
            elem.clear()
            break
    except ET.ParseError:
        return


def convert_file_name_to_type(file_name: str) -> FileType | None:
    for file_type in FileType:
        if file_name.endswith(f".{file_type.value}.xml"):
            return file_type
    return None
