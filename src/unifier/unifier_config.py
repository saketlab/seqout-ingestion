from dataclasses import dataclass


@dataclass
class UnifierConfig:
    geo_db_file: str
    sra_db_file: str
    ena_db_file: str
    ae_db_file: str
    output_dir: str
    db_file: str
    num_workers: int
    batch_size: int
    chunk_size: int
    dump_parquet: bool
