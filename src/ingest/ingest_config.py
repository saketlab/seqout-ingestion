import datetime
from dataclasses import dataclass


@dataclass
class IngestionConfig:
    datasource: str
    output_dir: str
    start_date: datetime.date
    end_date: datetime.date
    n_workers: int
    chunk_size: int
    sra_accessions_ttl: int
