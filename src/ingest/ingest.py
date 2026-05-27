from ingest.ingest_config import IngestionConfig

from .ingest_sra import ingest_sra


def run_ingest(cfg: IngestionConfig):
    if cfg.datasource == "sra":
        ingest_sra(cfg)
    else:
        raise NotImplementedError(
            f"ingest stage is not yet implemented for {cfg.datasource} datasource"
        )
