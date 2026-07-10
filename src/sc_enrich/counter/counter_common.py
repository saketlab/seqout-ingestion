import dataclasses
from enum import StrEnum


class FileType(StrEnum):
    Barcodes = "barcodes"
    H5 = "h5"
    H5ad = "h5ad"
    Mtx = "mtx"
    Csv = "csv"
    Tsv = "tsv"
    Txt = "txt"
    SpaceDelimited = "space_delimited"
    SemicolonDelimited = "semicolon_delimited"
    Rds = "rds"
    Tar = "tar"
    Unknown = "unknown"


@dataclasses.dataclass
class DownloadLink:
    type: FileType
    url: str

    def parse_url(self) -> str:  # noqa: D102
        return self.url.replace("ftp://", "https://")

    def extract_filename(self) -> str:  # noqa: D102
        return self.url.split("/")[-1]
