from ingest.ingest_ae import ingest_ae
from ingest.ingest_config import IngestionConfig
from ingest.ingest_ena import ingest_ena
from ingest.ingest_geo import ingest_geo
from ingest.ingest_sra import ingest_sra


def run_ingest(cfg: IngestionConfig):
    match cfg.datasource:
        case "sra":
            ingest_sra(cfg)
        case "ena":
            ingest_ena(cfg)
        case "ae":
            ingest_ae(cfg)
        case "geo":
            ingest_geo(cfg)
        # case "pubmed":
        #     ingest_pubmed(cfg)
        case _:
            raise NotImplementedError(
                f"ingest stage is not yet implemented for {cfg.datasource} datasource"
            )
