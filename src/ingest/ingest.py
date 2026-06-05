from ingest.ingest_ae import ingest_ae
from ingest.ingest_config import IngestionConfig
from ingest.ingest_ena import ingest_ena
from ingest.ingest_geo import ingest_geo
from ingest.ingest_pubmed import ingest_pubmed
from ingest.ingest_sra import ingest_sra


def run_ingest(cfg: IngestionConfig):
    if cfg.datasource == "sra":
        ingest_sra(cfg)
    elif cfg.datasource == "ena":
        ingest_ena(cfg)
    elif cfg.datasource == "ae":
        ingest_ae(cfg)
    elif cfg.datasource == "geo":
        ingest_geo(cfg)
    elif cfg.datasource == "pubmed":
        ingest_pubmed(cfg)
    else:
        raise NotImplementedError(
            f"ingest stage is not yet implemented for {cfg.datasource} datasource"
        )
