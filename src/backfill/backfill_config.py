from dataclasses import dataclass

from config import Datasource


@dataclass
class BackfillConfig:
    datasource: Datasource
    base_output_dir: str
    db_file: str
    geo_db_file: str
    sra_db_file: str
    ena_db_file: str
    ae_db_file: str
    ncbi_keys_file: str
    openalex_keys_file: str
    num_workers: int
    chunk_size: int
    batch_size: int
    update_metrics: bool
    fill_metrics: bool
    skip_journal_metrics: bool
    update_journal_metrics: bool
    refresh_all_journals_metrics: bool
    is_dry_run: bool
