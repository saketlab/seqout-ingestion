import json
import xml.etree.ElementTree as ET

_MINIML_NAMESPACE = "http://www.ncbi.nlm.nih.gov/geo/info/MINiML"


def _wrap_ns(s: str) -> str:
    return f"{{{_MINIML_NAMESPACE}}}{s}"


def _et_to_dict(elem: ET.Element | list[ET.Element]) -> list | dict | str:
    if isinstance(elem, list):
        return [_et_to_dict(v) for v in elem]
    else:
        result: dict | str = {}

        for key, value in elem.attrib.items():
            result[f"@{key}"] = value

        if elem.text and elem.text.strip():
            if len(elem.attrib.items()) > 0:
                result["#text"] = elem.text.strip()
            else:
                result = elem.text.strip()

        for child in elem:
            child_tag = str(child.tag).split("}")[-1]
            child_dict = _et_to_dict(child)

            if isinstance(result, dict):
                if child_tag in result:
                    if not isinstance(result[child_tag], list):
                        result[child_tag] = [result[child_tag]]
                    else:
                        result[child_tag].append(child_dict)
                else:
                    result[child_tag] = child_dict

        return result


with open("/home/mukesh/Downloads/GSE100006_family.xml", "rb") as f:
    tree = ET.parse(f)
    root = tree.getroot()
    samples = root.findall(_wrap_ns("Sample"))

    for sample in samples:
        channels = _et_to_dict(sample.findall(_wrap_ns("Channel")))
        print(json.dumps(channels, indent=2))
        print("=" * 10)
        break

    # series = root.find(_wrap_ns("Series"))
    # if series is None:
    #     raise ValueError("failed to find series tree")

    # relations = [_et_to_dict(r) for r in series.findall(_wrap_ns("Relation"))]
    # print(relations)
