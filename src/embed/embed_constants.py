"""
Shared constants for the embed pipeline.

All three sources are embedded into a single shared 4096-d space with
codefuse-ai/F2LLM-v2-8B so coordinates and neighbors are comparable across
sources. The chroma store holds one collection per source.
"""

from enum import StrEnum


class TransferStep(StrEnum):
    """Ordered sub-steps of embed transfer (used by --start-from)."""

    coords_neighbors = "coords-neighbors"
    coords2d = "coords2d"
    labels = "labels"


TRANSFER_STEP_ORDER: list[TransferStep] = [
    TransferStep.coords_neighbors,
    TransferStep.coords2d,
    TransferStep.labels,
]

DEFAULT_MODEL_NAME = "codefuse-ai/F2LLM-v2-8B"

COLLECTION_TABLES: dict[str, str] = {
    "geo": "geo_series",
    "sra": "sra_studies",
    "arrayexpress": "arrayexpress_experiments",
}

COLLECTIONS: list[str] = list(COLLECTION_TABLES)

# chroma 1.5.x hard limits: a single update/upsert is capped at 5461 rows, and a
# full get() with no limit fails ("too many SQL variables") -- always page.
CHROMA_MAX_BATCH = 5000
FETCH_BATCH = 10000
