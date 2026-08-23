#!/usr/bin/env python3
"""
Post-processor: map enriched free-text values to ontology term IDs.

Reads enriched CSVs (from extract.py) and adds ontology ID + name columns.
Supports MONDO (disease), UBERON (tissue), CL (cell_type), with hierarchical
parent traversal.

Matching strategies (configurable):
  1. Exact name match (case-insensitive)
  2. Exact synonym match (case-insensitive)
  3. rapidfuzz token_sort_ratio (threshold configurable, default 90)
  4. Embedding similarity (optional, requires a sentence-transformer model)

Usage:
    # rapidfuzz only (fast, default)
    uv run enrich/ontology_map.py input.csv -o output.csv

    # With embedding fallback
    uv run enrich/ontology_map.py input.csv -o output.csv \
        --embedding-model abhinand/MedEmbed-large-v0.1

    # Only disease mapping
    uv run enrich/ontology_map.py input.csv -o output.csv --fields disease

    # Custom fuzzy threshold
    uv run enrich/ontology_map.py input.csv -o output.csv --fuzzy-threshold 85

    # Disable fuzzy, exact only
    uv run enrich/ontology_map.py input.csv -o output.csv --no-fuzzy
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz
from rich.console import Console

from enrich.cellosaurus import DEFAULT_ONTOLOGY_DIR

console = Console()

ONTOLOGY_DIR = Path(os.environ.get("ONTOLOGY_DIR", DEFAULT_ONTOLOGY_DIR))

# field -> (ontology file, terms keys, id prefix)
# terms_keys can be a single string or a list of keys to merge
FIELD_ONTOLOGY_MAP = {
    "disease": ("mondo.json", ["terms"], "MONDO"),
    "tissue": ("uberon.json", ["anatomical_sites"], "UBERON"),
    "cell_type": ("cell_ontology.json", ["cell_types"], "CL"),
    "assay": ("efo.json", ["terms", "imported_terms"], "EFO"),
    "development_stage": ("uberon.json", ["anatomical_sites"], "UBERON"),
}

DEFAULT_FIELDS = list(FIELD_ONTOLOGY_MAP.keys())

# Per-field query aliases: tried before exact/fuzzy matching.
# Maps lowercased enriched value -> preferred query form for ontology lookup.
QUERY_ALIASES: dict[str, dict[str, str]] = {
    "development_stage": {
        "embryonic": "embryonic stage",
        "fetal": "fetal stage",
        "neonatal": "neonatal stage",
        "juvenile": "juvenile stage",
        "adult": "adult stage",
        "infant": "infant stage",
        "larval": "larval stage",
        "pupal": "pupal stage",
        "aged": "late adult stage",
        "adolescent": "sexually immature stage",
        "postnatal": "post-embryonic stage",
        "seedling": "seedling stage",
    },
}


class OntologyIndex:
    """In-memory ontology with name/synonym lookup and parent traversal."""

    def __init__(
        self, ontology_file: Path, terms_keys: str | list[str], id_prefix: str
    ):
        self.id_prefix = id_prefix
        self.terms_by_id: dict[str, dict] = {}
        self.name_to_id: dict[str, str] = {}  # lowercase name -> term id
        self.synonym_to_id: dict[str, str] = {}  # lowercase synonym -> term id

        with open(ontology_file) as f:
            data = json.load(f)

        # Support single key or list of keys
        if isinstance(terms_keys, str):
            terms_keys = [terms_keys]
        terms = []
        for key in terms_keys:
            terms.extend(data.get(key, []))

        for term in terms:
            tid = term["id"]
            name = term.get("name")
            if not tid or not name:
                continue
            self.terms_by_id[tid] = term
            name_lower = name.lower().strip()
            # Prefer the first occurrence (more specific)
            if name_lower not in self.name_to_id:
                self.name_to_id[name_lower] = tid
            for syn in term.get("synonyms", []):
                syn_lower = syn.lower().strip()
                if syn_lower not in self.synonym_to_id:
                    self.synonym_to_id[syn_lower] = tid

        # Pre-build flat list for fuzzy/embedding search
        self._all_names: list[tuple[str, str]] = []  # (lowercase_text, term_id)
        for name_lower, tid in self.name_to_id.items():
            self._all_names.append((name_lower, tid))
        for syn_lower, tid in self.synonym_to_id.items():
            self._all_names.append((syn_lower, tid))

        console.print(
            f"  Loaded {id_prefix}: {len(self.terms_by_id)} terms, "
            f"{len(self.name_to_id)} names, {len(self.synonym_to_id)} synonyms"
        )

    def get_term(self, term_id: str) -> dict | None:
        return self.terms_by_id.get(term_id)

    def get_name(self, term_id: str) -> str | None:
        term = self.terms_by_id.get(term_id)
        return term["name"] if term else None

    def get_parents(self, term_id: str) -> list[str]:
        """Return direct parent IDs (strips {source=...} annotations from MONDO)."""
        term = self.terms_by_id.get(term_id)
        if not term:
            return []
        raw = term.get("parent_ids", [])
        return [p.split(" {")[0].strip() for p in raw]

    def get_ancestor_chain(self, term_id: str, max_depth: int = 20) -> list[dict]:
        """
        Walk up the parent hierarchy, return list of {id, name} dicts.

        Returns from immediate parent up to root. Stops at max_depth or
        when no more parents found. Handles multiple inheritance by
        following the first parent at each level.
        """
        chain = []
        current = term_id
        visited = {current}
        for _ in range(max_depth):
            parents = self.get_parents(current)
            if not parents:
                break
            # Follow first parent (primary lineage)
            parent = parents[0]
            if parent in visited:
                break
            visited.add(parent)
            name = self.get_name(parent)
            if name:
                chain.append({"id": parent, "name": name})
            current = parent
        return chain

    def match_exact(self, query: str) -> tuple[str, str, float] | None:
        """Exact match on name or synonym. Returns (term_id, term_name, score=1.0)."""
        q = query.lower().strip()
        tid = self.name_to_id.get(q) or self.synonym_to_id.get(q)
        if tid:
            return tid, self.get_name(tid), 1.0
        return None

    def match_fuzzy(
        self, query: str, threshold: int = 90
    ) -> tuple[str, str, float] | None:
        """Fuzzy match using rapidfuzz token_sort_ratio."""
        q = query.lower().strip()
        best_score = 0
        best_tid = None
        for name_lower, tid in self._all_names:
            score = fuzz.token_sort_ratio(q, name_lower)
            if score > best_score:
                best_score = score
                best_tid = tid
            if score == 100:
                break  # perfect match
        if best_tid and best_score >= threshold:
            return best_tid, self.get_name(best_tid), best_score / 100.0
        return None

    def match_embedding(
        self, query: str, embedder, term_embeddings, threshold: float = 0.8
    ) -> tuple[str, str, float] | None:
        """Embedding-based match using cosine similarity."""
        from sentence_transformers import util

        query_emb = embedder.encode(query, convert_to_tensor=True)
        scores = util.cos_sim(query_emb, term_embeddings).squeeze(0)
        best_idx = scores.argmax().item()
        best_score = scores[best_idx].item()
        if best_score >= threshold:
            _, tid = self._all_names[best_idx]
            return tid, self.get_name(tid), best_score
        return None


class EmbeddingMatcher:
    """Lazy-loads embedding model and pre-computes ontology term embeddings."""

    def __init__(self, model_name: str):
        self.model_name = model_name
        self._model = None
        self._caches: dict[str, object] = {}  # id_prefix -> tensor

    def _load_model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            console.print(f"  Loading embedding model: {self.model_name}")
            os.environ["TOKENIZERS_PARALLELISM"] = "false"
            os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def get_embeddings(self, index: OntologyIndex):
        """Get or compute term name embeddings for an ontology index."""
        if index.id_prefix not in self._caches:
            model = self._load_model()
            texts = [name for name, _ in index._all_names]
            console.print(f"  Encoding {len(texts)} {index.id_prefix} terms...")
            self._caches[index.id_prefix] = model.encode(
                texts, convert_to_tensor=True, show_progress_bar=True, batch_size=512
            )
        return self._model, self._caches[index.id_prefix]


def map_terms(
    df: pd.DataFrame,
    fields: list[str] | None = None,
    fuzzy_threshold: int = 90,
    use_fuzzy: bool = True,
    embedding_model: str | None = None,
    embedding_threshold: float = 0.8,
) -> pd.DataFrame:
    """
    Map free-text enriched values to ontology term IDs.

    Parameters
    ----------
    df : DataFrame
        Enriched metadata with columns like tissue, disease, cell_type.
    fields : list[str]
        Which fields to map. Default: all supported (disease, tissue, cell_type).
    fuzzy_threshold : int
        Minimum rapidfuzz score (0-100) for fuzzy matching.
    use_fuzzy : bool
        Enable rapidfuzz matching.
    embedding_model : str or None
        HuggingFace model name for embedding-based matching. None = disabled.
    embedding_threshold : float
        Minimum cosine similarity for embedding matching.

    Returns
    -------
    DataFrame
        Copy of input with added columns:
          {field}_ontology_id, {field}_ontology_name, {field}_ontology_score,
          {field}_match_method, {field}_ancestors (JSON array of {id, name})

    """
    if fields is None:
        fields = [f for f in DEFAULT_FIELDS if f in df.columns]

    result = df.copy()

    # Load ontologies
    console.print("Loading ontologies...")
    indices: dict[str, OntologyIndex] = {}
    for field in fields:
        if field not in FIELD_ONTOLOGY_MAP:
            console.print(f"  [yellow]Skipping unknown field: {field}[/yellow]")
            continue
        filename, terms_keys, id_prefix = FIELD_ONTOLOGY_MAP[field]
        onto_path = ONTOLOGY_DIR / filename
        if not onto_path.exists():
            console.print(f"  [yellow]Ontology not found: {onto_path}[/yellow]")
            continue
        indices[field] = OntologyIndex(onto_path, terms_keys, id_prefix)

    # Optional embedding matcher
    embedder: EmbeddingMatcher | None = None
    if embedding_model:
        embedder = EmbeddingMatcher(embedding_model)

    # Map each field
    for field, index in indices.items():
        console.print(f"Mapping {field}...")

        id_col = f"{field}_ontology_id"
        name_col = f"{field}_ontology_name"
        score_col = f"{field}_ontology_score"
        method_col = f"{field}_match_method"
        ancestors_col = f"{field}_ancestors"

        result[id_col] = None
        result[name_col] = None
        result[score_col] = None
        result[method_col] = None
        result[ancestors_col] = None

        matched = 0
        total = 0

        for i, row in result.iterrows():
            value = row.get(field)
            if not value or pd.isna(value):
                continue
            value = str(value).strip()
            if not value:
                continue
            total += 1

            # Try matching strategies in order
            match = None
            method = None

            # 0. Check per-field aliases (e.g. "Embryonic" -> "embryonic stage")
            aliases = QUERY_ALIASES.get(field, {})
            aliased = aliases.get(value.lower(), None)

            # 1. Exact (try alias first, then original)
            if aliased:
                match = index.match_exact(aliased)
            if not match:
                match = index.match_exact(value)
            if match:
                method = "exact"

            # 2. Fuzzy (try alias first, then original)
            if not match and use_fuzzy:
                if aliased:
                    match = index.match_fuzzy(aliased, threshold=fuzzy_threshold)
                if not match:
                    match = index.match_fuzzy(value, threshold=fuzzy_threshold)
                if match:
                    method = "fuzzy"

            # 3. Embedding
            if not match and embedder:
                model, embs = embedder.get_embeddings(index)
                match = index.match_embedding(
                    value, model, embs, threshold=embedding_threshold
                )
                if match:
                    method = "embedding"

            if match:
                tid, tname, score = match
                ancestors = index.get_ancestor_chain(tid)
                result.at[i, id_col] = tid
                result.at[i, name_col] = tname
                result.at[i, score_col] = round(score, 4)
                result.at[i, method_col] = method
                result.at[i, ancestors_col] = (
                    json.dumps(ancestors) if ancestors else None
                )
                matched += 1

        console.print(f"  {field}: {matched}/{total} mapped")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Map enriched metadata to ontology term IDs"
    )
    parser.add_argument("input", help="Input CSV (enriched metadata)")
    parser.add_argument(
        "-o", "--output", help="Output CSV (default: input_ontology.csv)"
    )
    parser.add_argument(
        "--fields",
        nargs="+",
        default=None,
        help=f"Fields to map (default: {DEFAULT_FIELDS})",
    )
    parser.add_argument(
        "--fuzzy-threshold",
        type=int,
        default=90,
        help="Minimum rapidfuzz score (0-100, default: 90)",
    )
    parser.add_argument(
        "--no-fuzzy",
        action="store_true",
        help="Disable fuzzy matching (exact only)",
    )
    parser.add_argument(
        "--embedding-model",
        default=None,
        help="HuggingFace model for embedding matching (default: disabled)",
    )
    parser.add_argument(
        "--embedding-threshold",
        type=float,
        default=0.8,
        help="Minimum cosine similarity for embedding matching (default: 0.8)",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        console.print(f"[red]Error: {input_path} not found[/red]")
        sys.exit(1)

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_name(
            input_path.stem + "_ontology" + input_path.suffix
        )

    df = pd.read_csv(input_path)
    console.print(f"Loaded {len(df)} rows from {input_path}")

    result = map_terms(
        df,
        fields=args.fields,
        fuzzy_threshold=args.fuzzy_threshold,
        use_fuzzy=not args.no_fuzzy,
        embedding_model=args.embedding_model,
        embedding_threshold=args.embedding_threshold,
    )

    result.to_csv(output_path, index=False)
    console.print(f"Written {output_path}")


if __name__ == "__main__":
    main()
