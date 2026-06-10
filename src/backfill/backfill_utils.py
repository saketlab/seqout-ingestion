import pandas as pd


def _parse_pmid(pmid) -> str | None:
    if pd.isna(pmid):
        return None
    s = str(pmid).strip()
    if not s:
        return None

    return s


def _chunked(lst: list, batch_size: int):
    for i in range(0, len(lst), batch_size):
        yield lst[i : i + batch_size]
