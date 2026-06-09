import config
from ingest.ae import ingest_ae
from ingest.ena import ingest_ena
from ingest.geo import ingest_geo
from ingest.ingest_config import IngestionConfig
from ingest.sra import ingest_sra


def run_ingest(cfg: IngestionConfig):
    if cfg.datasource == config.Datasource._None:
        raise ValueError("missing datasource flag (--datasource)")

    match cfg.datasource:
        case "sra":
            ingest_sra(cfg)
        case "ena":
            ingest_ena(cfg)
        case "ae":
            ingest_ae(cfg)
        case "geo":
            ingest_geo(cfg)
        case _:
            raise NotImplementedError(
                f"ingest stage is not yet implemented for {cfg.datasource} datasource"
            )
