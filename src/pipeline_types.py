import enum


class Datasource(enum.StrEnum):
    Geo = "geo"
    Sra = "sra"
    Ena = "ena"
    ArrayExpress = "ae"
    Gsa = "gsa"
    GsaHuman = "gsa_human"
    Dra = "dra"
    Gea = "gea"
    _None = "none"
