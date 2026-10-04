"""Build study_canonical: one row per accession, mapped to its study's winner.

unified_metadata is undeduped: a GSE and its linked SRP each get a full row,
linked only through the aliases jsonb column. Grouping on a bare
study_accession double-counts every aliased study unless it canonicalizes
through this table first.

tissue_study and disease_study_ontology do. country_study, rare_disease_study
and nord_disease_study predate this table and still group on the raw
study_accession, carrying the same bug. Migrating them means rebuilding
their own CASCADE-dependent facet/summary/points views, deferred as a
larger change.

longread_study and singlecell_study canonicalize aliases with their own
inline CTE instead (jsonb_each_text over every alias key, including
bioproject), which can disagree with this table's winner for a multi-alias
study. tissue_study/disease_study_ontology join longread_study through this
table's canonicalization to compute is_long_read, so a disagreement between
the two algorithms silently under-reports is_long_read for the affected
study. Also deferred: fixing it means touching longread_study directly.

Union-find over unified_metadata.aliases, restricted to the geo/sra/ena/
arrayexpress keys, excluding any alias value shaped like a BioProject
accession (PRJNA/PRJEB/PRJDB/...). bioproject itself is the obvious
many-to-one key, since one BioProject can span many studies, but
unified_catalog.py's geo-processing also stores an ENA project accession
under the ena key. Filtering by shape rather than by key catches both: two
unrelated GSEs sharing one PRJ id under ena must not merge, same as sharing
one under bioproject. Winner per component follows unified_catalog.py's own
claim order (geo > gea > sra > ae > ena > dra), read from source.

Run via just refresh_ontology_matviews, not just study_canonical alone: it
also rebuilds the CASCADE-dropped tissue_study_term/disease_study_ontology_term.
"""

import os
import re

import psycopg

_ALIAS_KEYS = ("geo", "sra", "ena", "arrayexpress")
_SOURCE_PRIORITY = {"geo": 0, "gea": 1, "sra": 2, "ae": 3, "ena": 4, "dra": 5}
# Same pattern as unified_catalog.BIOPROJECT_RE: a project umbrella, not a
# per-study accession, regardless of which alias key holds it.
_BIOPROJECT_SHAPE_RE = re.compile(r"^PRJ(?:NA|NB|NC|ND|NE|EB|EA|DB|DA|CA)\d+$")


def _find(parent: dict[str, str], acc: str) -> str:
    root = acc
    while parent[root] != root:
        root = parent[root]
    while parent[acc] != root:
        parent[acc], acc = root, parent[acc]
    return root


def _union(parent: dict[str, str], a: str, b: str) -> None:
    ra, rb = _find(parent, a), _find(parent, b)
    if ra != rb:
        parent[rb] = ra


def build_mapping(rows: list[tuple[str, str, dict]]) -> dict[str, str]:
    """rows: (canonical_accession, source, aliases) from unified_metadata."""
    parent: dict[str, str] = {}
    source_of: dict[str, str] = {}
    for acc, source, aliases in rows:
        parent.setdefault(acc, acc)
        source_of[acc] = source
        for key in _ALIAS_KEYS:
            other = aliases.get(key) if aliases else None
            if other and not _BIOPROJECT_SHAPE_RE.match(other):
                parent.setdefault(other, other)
                _union(parent, acc, other)

    components: dict[str, list[str]] = {}
    for acc in parent:
        components.setdefault(_find(parent, acc), []).append(acc)

    mapping: dict[str, str] = {}
    for members in components.values():
        winner = min(
            members,
            key=lambda a: (_SOURCE_PRIORITY.get(source_of.get(a, ""), 9), a),
        )
        for acc in members:
            mapping[acc] = winner
    return mapping


def main() -> None:
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT canonical_accession, source, aliases FROM unified_metadata")
        rows = cur.fetchall()
        mapping = build_mapping(rows)

        # CASCADE drops tissue_study_term/disease_study_ontology_term too;
        # run via refresh_ontology_matviews, not this script alone.
        cur.execute("DROP TABLE IF EXISTS study_canonical CASCADE")
        cur.execute(
            "CREATE TABLE study_canonical "
            "(accession TEXT PRIMARY KEY, canonical_accession TEXT NOT NULL)"
        )
        with cur.copy(
            "COPY study_canonical (accession, canonical_accession) FROM STDIN"
        ) as copy:
            for acc, canonical in mapping.items():
                copy.write_row((acc, canonical))
        cur.execute(
            "CREATE INDEX idx_study_canonical_canonical ON study_canonical (canonical_accession)"
        )
        conn.commit()

    merged = sum(1 for a, c in mapping.items() if a != c)
    print(
        f"study_canonical: {len(mapping)} accessions, {merged} remapped to an alias's winner"
    )


if __name__ == "__main__":
    main()
