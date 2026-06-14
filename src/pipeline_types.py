import enum


class Datasource(enum.StrEnum):
    Geo = "geo"
    Sra = "sra"
    Ena = "ena"
    ArrayExpress = "ae"
    _None = "none"
