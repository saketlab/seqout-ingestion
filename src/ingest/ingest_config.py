import datetime
from dataclasses import dataclass

from config import Datasource


@dataclass
class IngestionConfig:
    datasource: Datasource
    output_dir: str
    db_file: str
    start_date: datetime.date
    end_date: datetime.date
    num_workers: int
    chunk_size: int
    batch_size: int
    sra_accessions_ttl: int
    ena_use_last_updated: bool
    ena_skip_experiments: bool
    ena_skip_samples: bool
    geo_series_pickle: str
    geo_series_limit: int | None
