from dataclasses import dataclass
from datetime import datetime


@dataclass
class IngestionConfig:
    datasource: str
    output_dir: str
    start_date: datetime
    end_date: datetime
    sra_accessions_ttl: int
    num_download_workers: int
    download_chunk_size: int
