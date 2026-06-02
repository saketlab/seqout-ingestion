from ingest.ingest_config import IngestionConfig

from .ingest_ena import ingest_ena
from .ingest_sra import ingest_sra


def run_ingest(cfg: IngestionConfig):
    if cfg.datasource == "sra":
        ingest_sra(cfg)
    elif cfg.datasource == "ena":
        ingest_ena(cfg)
    else:
        raise NotImplementedError(
            f"ingest stage is not yet implemented for {cfg.datasource} datasource"
        )
