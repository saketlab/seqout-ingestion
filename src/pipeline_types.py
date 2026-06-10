import enum


class Stage(enum.StrEnum):
    Ingest = "ingest"
    BackfillPubmedIds = "backfill_pubmed_ids"
    BackfillPubmedMeta = "backfill_pubmed_meta"
    BuildUnifiedCatalog = "build_unified_catalog"
    BuildUnifiedMetadata = "build_unified_metadata"


class Datasource(enum.StrEnum):
    Geo = "geo"
    Sra = "sra"
    Ena = "ena"
    ArrayExpress = "ae"
    _None = "none"
