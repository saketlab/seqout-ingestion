"""
Backfill: map currently-unmapped tissue/disease free-text values to their
UBERON/MONDO term IDs, using the SAME term index the production matcher
(`enrich.ontology_map.OntologyIndex`) reads -- this only extends the
matching STRATEGY (what gets tried before giving up), never the vocabulary.

This is a different bug than the cell-line hallucination fixes applied
earlier in this investigation (`fix_cell_line_species_mismatch.py`,
`fix_ambiguous_cell_line_donation.py`, `fix_hela_source_contradicted.py`):
those cleared WRONG donated values; this fills in ontology IDs for values
that are already correct free text but never matched the production
matcher's exact/synonym/fuzzy(>=90) rules. Confirmed directly (see
/data/saket/ontology_gap/NOTES.md "matcher gap" section): rapidfuzz
token_sort_ratio("uterus, cervix", "cervix") = 60.0, well under the
production threshold of 90 -- the matcher's own rule correctly leaves it
unmapped; the fix is a better strategy, not a lower threshold.

Every hit is namespace-gated to the field's own ontology prefix (UBERON for
tissue, MONDO for disease) before being accepted -- the ontology JSON files
are mixed vocabularies (uberon.json's "anatomical_sites" key is only 57%
UBERON:-prefixed; the rest is GO/CL/CHEBI/NCBITaxon/PATO/... cross-referenced
terms), so an ungated match_exact() can land on a real hit in the WRONG
ontology (confirmed directly by Codex review: "Solid Tumor" exact-matched
PATO:0001736 "solid configuration"; "Human Melanoma" matched NCBITaxon:9606
"Homo sapiens"). See `_exact()`.

Extra strategies tried, in order, each gated on producing a namespace-correct
EXACT or SYNONYM hit on the reduced text -- never a fuzzy match on
already-reduced text, to avoid compounding uncertainty on top of uncertainty:
  1. underscore/whitespace normalization ("Small_Intestine" -> "Small
     Intestine", confirmed present in the real unmapped data)
  2. strip a trailing parenthetical, retry on the remainder (handles
     "Type 1 Diabetes (T1D)" -> "Type 1 Diabetes"); matching the
     parenthetical content itself is NOT attempted -- disabled after a real
     dry run found unrelated-namespace collisions even at 3+ letters
     ("(Stg)" for Superior Temporal Gyrus matched "stomatogastric
     ganglion", "(Gbm)" for Glioblastoma matched "glomerular basement
     membrane")
  3. (tissue only) strip a leading developmental/anatomical qualifier
     (Fetal, Whole, Peripheral), retry on the remainder -- "Fetal Kidney"
     -> "Kidney". (disease only) strip a trailing phrase ("of no special
     type") or a leading qualifier (Adult, Childhood, Metastatic, Triple
     Negative, High Grade), retry on the remainder -- "Invasive Breast
     Carcinoma Of No Special Type" -> "Invasive Breast Carcinoma",
     "Metastatic Breast Cancer" -> "Breast Cancer". Every qualifier was
     verified against the real top-200-by-volume unmapped strings; a
     candidate that produced zero hits there was dropped, not shipped.
  4. (tissue only) strip a trailing oncology-suffix word/phrase (Tumor,
     Carcinoma, Cancer, Sarcoma, ...), retry on the remainder -- handles
     "Colon Tumor" -> "Colon", "Nasopharyngeal Carcinoma" ->
     "Nasopharyngeal" (which then goes through step 5)
  5. (tissue only) a curated adjective -> organ-noun table (colorectal ->
     colon, gastric -> stomach, ...), ~20 entries, each one verified against
     a real top-150-by-volume unmapped string, not guessed
  6. split on comma/slash/"and"/"&" into components, require EVERY
     component to match independently, then accept the one component that
     is a DESCENDANT of every other matched component (one direction only)
     -- "Eye, Retina, Retinal Pigment Epithelium" passes (RPE is below
     both); a pool of unrelated organs like "Heart And Lung" or "Spleen And
     Visceral Adipose" (both found in a real dry run, both wrongly resolved
     by an earlier version that accepted any single matching component) has
     no ancestor relation and is correctly left unresolved

A CURATED_EXCLUDE set (species/organism names, generic sample-processing
words, cell-type-not-tissue values) is checked FIRST and short-circuits
straight to "excluded" -- these are correctly never UBERON-mappable and
must never be forced into a wrong term by an over-eager strategy above.
Cell-type values (PBMC, embryonic stem cell, iPSC, ...) belong in the CL
ontology, not UBERON; mapping them there is out of scope for this pass and
they are excluded, not silently dropped -- see the exclusion reason in the
report.

Runs against DISTINCT unmapped strings (with their row counts), not every
row -- 40,277 distinct unmapped tissue strings and 14,171 distinct disease
strings cover 4.1M / 1.6M rows respectively. A matched string is applied to
every ontology_samples_v5 row still holding that exact raw text with a
NULL ontology id (gated `tissue_ontology_id IS NULL` in the UPDATE itself,
so a value already resolved by anything else is never touched).

Every run (dry or --apply) writes a distinct-string manifest -- raw text,
row count, matched term, strategy, score -- recording what was PLANNED.
--apply additionally writes a row-level manifest of every (study_accession,
sample) actually touched, captured via UPDATE ... RETURNING, since planned
and actual can differ under concurrent writers. Together these fully
reconstruct the pre-fix unmapped state per NOTES.md's own convention for
every backfill in this investigation.

Read-only until --apply; --apply runs everything in one transaction.

Usage:
    .venv/bin/python recipes/ontology_gap/fix_unmapped_tissue_disease.py
    .venv/bin/python recipes/ontology_gap/fix_unmapped_tissue_disease.py --apply
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "recipes"))
sys.path.insert(0, str(REPO / "src"))

from pg_connect import connect  # noqa: E402

from enrich.ontology_map import FIELD_ONTOLOGY_MAP, ONTOLOGY_DIR, OntologyIndex  # noqa: E402

OUTPUT_DIR = "/data/saket/ontology_gap"

# Verified against the real top-150-by-volume unmapped tissue strings
# (query in /data/saket/ontology_gap/NOTES.md) -- each entry maps a clinical
# adjective to the organ noun UBERON actually indexes under.
ADJECTIVE_TO_ORGAN = {
    "colorectal": "colon",
    "gastric": "stomach",
    "ovarian": "ovary",
    "nasopharyngeal": "nasopharynx",
    "cardiac": "heart",
    "cardiovascular": "cardiovascular system",
    "hepatic": "liver",
    "hepatocellular": "liver",
    "renal": "kidney",
    "pulmonary": "lung",
    "cerebral": "brain",
    "splenic": "spleen",
    "pancreatic": "pancreas",
    "cutaneous": "skin",
    "dermal": "skin",
    "mammary": "breast",
    "testicular": "testis",
    "esophageal": "esophagus",
    "colonic": "colon",
    "intestinal": "intestine",
    "nasal": "nose",
    "hematopoietic": "hematopoietic system",
}

# Trailing oncology-suffix words/phrases: stripped from TISSUE text only
# (disease text keeping its own suffix is correct, not a bug) before
# retrying the remainder -- turns "Colon Tumor" into "Colon", "Nasopharyngeal
# Carcinoma" into "Nasopharyngeal" (which then hits ADJECTIVE_TO_ORGAN).
ONCOLOGY_SUFFIX_RE = re.compile(
    r"\b("
    r"tumou?rs?|carcinomas?|cancers?|sarcomas?|adenocarcinomas?|blastomas?|"
    r"lymphomas?|leukemias?|neuroblastomas?|melanomas?|gliomas?|glioblastomas?|"
    r"myelomas?|mesotheliomas?|rhabdomyosarcomas?|medulloblastomas?"
    r")\b\s*$",
    re.IGNORECASE,
)

# Species/organism names, generic sample-processing words, and cell-type
# (not tissue) values pulled directly from the real top-150 unmapped list.
# Checked first; never fed to any matching strategy above.
CURATED_EXCLUDE = {
    # species / organism names -- not anatomy
    "yeast",
    "bacteria",
    "saccharomyces cerevisiae",
    "whole animal",
    "soil",
    "bacterial culture",
    "whole worm",
    "fungus",
    "bacterial cell",
    "cryptococcus neoformans",
    "plasmodium falciparum",
    "escherichia coli",
    "dictyostelium discoideum",
    "bacillus subtilis",
    "mycelium",
    "mycelia",
    # generic sample-processing / non-anatomical words
    "cell line",
    "cell culture",
    "cells",
    "stem",
    "immune",
    "ffpe",
    "organoid",
    "retinal organoid",
    "metastases",
    "germline",
    # cell-type values (CL ontology, not UBERON) -- out of scope this pass
    "pbmc",
    "pbmcs",
    "embryonic stem cell",
    "embryonic stem cells",
    "induced pluripotent stem cell",
    "dermal fibroblasts",
    "lymphoblastoid cell line",
    "neural progenitor cell",
    "hematopoietic stem and progenitor cells",
    "embryonic cells",
    "buccal epithelial cells",
    "s2r+ cells",
    "bone marrow-derived macrophages",
    # qualifier-strip word-sense collisions -- the remainder after stripping
    # a leading qualifier is a real UBERON exact/synonym match, but for the
    # WRONG anatomical sense than what the raw string means in a genomics
    # context (found in a real dry run): "peripheral plaque" almost always
    # means arterial/atherosclerotic plaque, not "dental plaque"; "whole
    # nodule" almost always means a generic tumor/lymph nodule, not the
    # cerebellar vermis lobule literally nicknamed "nodule"
    "whole nodule",
    "peripheral plaque",
}

# A compound "Organ, Sub-part" string has 2-3 components. Beyond that it is
# very likely a pooled/whole-organism sample listing many distinct tissues
# (confirmed directly: a real 10-tissue "Pooled:Blood,Jejunal Epithelium,...
# ,Jejunum Lamina Propria" string resolved to just "jejunum lamina propria",
# and an 18-organ whole-body list resolved to just "tail" -- both wrong,
# not merely imprecise, since picking one part actively misrepresents a
# multi-tissue sample as a single tissue). Capped at 3 so real 3-part
# strings like "Eye, Retina, Retinal Pigment Epithelium" still resolve.
MAX_SPLIT_PARTS = 3

# Verified against the real top-200-by-volume unmapped tissue strings --
# strip a leading developmental/anatomical qualifier and retry an exact
# match on the remainder ("Fetal Kidney" -> "Kidney", "Peripheral Whole
# Blood" -> "Whole Blood", itself a UBERON synonym for blood). Candidate
# qualifiers tried against the same top-200 list but producing zero hits
# (adult, total, primary, normal, healthy) were dropped rather than shipped
# unverified.
TISSUE_LEADING_QUALIFIERS = ("fetal", "whole", "peripheral")

# Verified against the real top-200-by-volume unmapped disease strings.
# Candidates tried but producing zero hits (pediatric, recurrent, primary,
# relapsed, refractory, localized, advanced, early stage, de novo, invasive,
# low grade, {well,poorly,moderately} differentiated) were dropped rather
# than shipped unverified.
DISEASE_LEADING_QUALIFIERS = (
    "adult",
    "childhood",
    "metastatic",
    "triple negative",
    "high grade",
)
DISEASE_TRAILING_PHRASES = ("of no special type",)

# A stripped qualifier must never land on a term whose OWN name asserts the
# opposite -- found by dry run: "Adult T Lymphoblastic Lymphoma" stripped to
# "T Lymphoblastic Lymphoma", which exact-matched a MONDO term literally
# named "T-cell CHILDHOOD lymphoblastic lymphoma" (a real synonym registered
# under that term, but "adult" directly contradicts "childhood" in the
# result). Checked in enhanced_match; other qualifiers (metastatic, high
# grade, triple negative) have no analogous age/stage antonym so aren't
# listed.
DISEASE_QUALIFIER_CONFLICTS = {
    "adult": (
        "childhood",
        "pediatric",
        "paediatric",
        "infantile",
        "juvenile",
        "neonatal",
    ),
    "childhood": ("adult",),
    "metastatic": ("primary", "localized", "in situ", "non-metastatic"),
    "triple negative": ("triple positive", "her2-positive", "her2 positive"),
    "high grade": ("low grade", "well differentiated", "low malignant potential"),
}

# Real exact/synonym hits on nodes too generic to carry information --
# rejected regardless of which strategy produces them. See enhanced_match.
GENERIC_TERM_DENYLIST = {
    "tissue",
    "anatomical structure",
    "cell",
    "organ",
    "structure",
    "material anatomical entity",
    "anatomical entity",
    "intracellular anatomical structure",
    "anatomical system",
    "multicellular organism",
    "epithelium",
}

_SPLIT_RE = re.compile(r"\s*[,/]\s*|\s+and\s+|\s*&\s*", re.IGNORECASE)
_PAREN_RE = re.compile(r"\(([^)]+)\)\s*$")


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("_", " ")).strip()


def _strip_parenthetical(text: str) -> str:
    """Strip ALL trailing parentheticals, not just one level -- "X (Y) (Z)"
    fully reduces to "X". A single-level strip left a real (if currently
    dormant, per a direct check against every unmapped string) exclusion
    bypass: "whole_nodule (a) (b)" would only lose "(b)" and never reach
    the CURATED_EXCLUDE check on "whole nodule" (found by Codex review)."""
    while (m := _PAREN_RE.search(text)) is not None:
        text = text[: m.start()].strip(" ,")
    return text


def _strip_leading_qualifier(
    text: str, qualifiers: tuple[str, ...]
) -> tuple[str, str] | None:
    """Strip a leading word/phrase from `qualifiers`, longest first so
    "triple negative" isn't shadowed by a shorter overlapping entry.
    Returns (qualifier, remainder) so the caller can check the remainder's
    matched term doesn't contradict the very qualifier that was stripped."""
    low = text.lower()
    for q in sorted(qualifiers, key=len, reverse=True):
        if low.startswith(q + " "):
            return q, text[len(q) :].strip()
    return None


def _strip_trailing_phrase(text: str, phrases: tuple[str, ...]) -> str | None:
    low = text.lower()
    for p in sorted(phrases, key=len, reverse=True):
        if low.endswith(p):
            return text[: -len(p)].strip(" ,")
    return None


def _exact(index: OntologyIndex, query: str) -> tuple[str, str, float] | None:
    """match_exact, gated to the field's own namespace. The ontology JSON
    files are mixed vocabularies (uberon.json's "anatomical_sites" is only
    57% UBERON:-prefixed; the rest is GO/CL/CHEBI/NCBITaxon/PATO/... pulled
    in as cross-referenced terms) so an ungated hit can land on a wrong
    ontology entirely -- confirmed directly: "Solid Tumor" exact-matched
    PATO:0001736 "solid configuration", "Human Melanoma" matched
    NCBITaxon:9606 "Homo sapiens"."""
    hit = index.match_exact(query)
    if hit and hit[0].startswith(f"{index.id_prefix}:"):
        return hit
    return None


def _is_descendant_of(index: OntologyIndex, term_id: str, ancestor_id: str) -> bool:
    """True if term_id IS ancestor_id, or ancestor_id appears in term_id's
    ancestor chain (term_id is strictly BELOW ancestor_id in the
    hierarchy). Deliberately one-directional -- see enhanced_match's
    component_split block for why "ancestor-related in either direction"
    is not enough."""
    if term_id == ancestor_id:
        return True
    return ancestor_id in {a["id"] for a in index.get_ancestor_chain(term_id)}


def enhanced_match(
    index: OntologyIndex, raw: str, *, is_tissue: bool
) -> tuple[str, tuple[str, str, float] | None]:
    """Return (strategy, hit) where hit is (term_id, term_name, score) or
    None. strategy is 'excluded' or 'unresolved' when hit is None."""
    # Checked against the raw string AND its normalized/parenthetical-
    # stripped forms -- checking only raw.strip().lower() let variants like
    # "whole_nodule" (underscore) or "peripheral plaque (something)" bypass
    # the exclusion and reach the strip strategies below (found by Codex
    # review of round 6).
    text = _normalize(raw)
    keys = {raw.strip().lower(), text.lower(), _strip_parenthetical(text).lower()}
    if keys & CURATED_EXCLUDE:
        return "excluded", None

    strategy, hit = _enhanced_match_inner(index, raw, is_tissue=is_tissue)
    if hit and hit[1].strip().lower() in GENERIC_TERM_DENYLIST:
        # A real exact/synonym hit, but on a UBERON/MONDO node so generic
        # it carries almost no information (confirmed directly: "Mammary
        # Gland/ Tissue (Female Only)" component-split to the bare word
        # "Tissue", exact-matching UBERON's own term literally named
        # "tissue"; "Bcg, Intracellular" matched "intracellular anatomical
        # structure"). Correctly unresolved is more honest than a
        # technically-exact but useless match.
        return "unresolved", None
    return strategy, hit


def _enhanced_match_inner(
    index: OntologyIndex, raw: str, *, is_tissue: bool
) -> tuple[str, tuple[str, str, float] | None]:

    text = _normalize(raw)

    # No redundant match_exact/match_fuzzy(threshold=90) re-check here:
    # the SQL query that selected this string already failed the identical
    # exact+fuzzy check under the same threshold in production -- re-running
    # it is deterministic and guaranteed to fail again, at O(distinct
    # strings x ontology terms) cost. Underscore/whitespace normalization
    # is the one case that changes the query text before any lookup, so it
    # still gets one exact-only check (cheap, O(1) dict lookup).
    if text != raw.strip():
        hit = _exact(index, text)
        if hit:
            return "normalize_only", hit

    stripped = _strip_parenthetical(text)
    if stripped != text and stripped:
        hit = _exact(index, stripped)
        if hit:
            return "parenthetical_strip", hit

    # parenthetical_alias (matching the parenthetical content itself, e.g.
    # "(T1D)") is disabled entirely: exhaustive review of a real dry run
    # found unrelated-namespace collisions even at 3+ letters -- "(Stg)"
    # (Superior Temporal Gyrus) matched "stomatogastric ganglion", "(Gbm)"
    # (Glioblastoma) matched "glomerular basement membrane", "(Acc)"
    # (Adenoid Cystic Carcinoma) matched "anterior cingulate cortex". A
    # length floor doesn't catch these; a curated alias table would, but
    # that is out of scope for this pass.

    candidate = stripped if stripped != text and stripped else text

    if is_tissue:
        lq = _strip_leading_qualifier(candidate, TISSUE_LEADING_QUALIFIERS)
        if lq:
            _, remainder = lq
            hit = _exact(index, remainder)
            if hit:
                return "leading_qualifier_strip", hit
    else:
        dt = _strip_trailing_phrase(candidate, DISEASE_TRAILING_PHRASES)
        if dt:
            hit = _exact(index, dt)
            if hit:
                return "trailing_phrase_strip", hit
        dl = _strip_leading_qualifier(candidate, DISEASE_LEADING_QUALIFIERS)
        if dl:
            qualifier, remainder = dl
            hit = _exact(index, remainder)
            if hit:
                conflicts = DISEASE_QUALIFIER_CONFLICTS.get(qualifier, ())
                # Hyphen-normalized: matched term names use both forms
                # ("well differentiated" vs "well-differentiated"), and a
                # literal substring check would miss the hyphenated one
                # (found by Codex review; verified against the actual
                # accepted matches that none currently trip this, but the
                # comparison should not depend on that staying true).
                name = hit[1].lower().replace("-", " ")
                if not any(c.replace("-", " ") in name for c in conflicts):
                    return "leading_qualifier_strip", hit

    if is_tissue:
        onco_stripped = ONCOLOGY_SUFFIX_RE.sub("", candidate).strip()
        if onco_stripped and onco_stripped != candidate:
            hit = _exact(index, onco_stripped)
            if hit:
                return "oncology_suffix_strip", hit
            adj = ADJECTIVE_TO_ORGAN.get(onco_stripped.lower())
            if adj:
                hit = _exact(index, adj)
                if hit:
                    return "oncology_suffix_strip+adjective", hit
            candidate = onco_stripped

        adj = ADJECTIVE_TO_ORGAN.get(candidate.lower())
        if adj:
            hit = _exact(index, adj)
            if hit:
                return "adjective_to_organ", hit

    if is_tissue:
        # Disabled for disease: comma-joined disease strings commonly carry
        # gene/mutation annotations, not a tissue-style organ hierarchy --
        # confirmed directly, "Acute Myeloid Leukemia With Inv(3)...Gata2,
        # Mecom" component-split to "MECOM", a gene symbol MONDO happens to
        # also index as a term name, not the actual leukemia diagnosis.
        # Tissue's "Organ, Sub-part" pattern doesn't have this failure mode
        # -- but a pool of unrelated organs does ("Heart And Lung", "Spleen
        # And Visceral Adipose" all found in a real dry run). Two earlier,
        # insufficient versions (both found by Codex review): accepting a
        # component related to ANY other matched component let a 3-way mix
        # through ("Eye, Retina, Heart" -> Retina); accepting one related to
        # EVERY OTHER component but in either direction let a sibling pair
        # under a shared ancestor through ("Retina, Lens, Eye" -> Eye, since
        # Eye is an ancestor of both Retina and Lens even though Retina and
        # Lens are themselves unrelated). The only condition that is
        # actually correct: the accepted component must be a DESCENDANT of
        # every other matched component (one direction only) -- this
        # selects the unique deepest term in a real chain like "Eye, Retina,
        # Retinal Pigment Epithelium" (RPE is below both), and no candidate
        # in a sibling pool like "Retina, Lens, Eye" is ever below the
        # others, so it is correctly left unresolved.
        parts = [p.strip() for p in _SPLIT_RE.split(candidate) if p.strip()]
        if 1 < len(parts) <= MAX_SPLIT_PARTS:
            matched = [(p, hit) for p in parts if (hit := _exact(index, p))]
            # EVERY part must independently match, not just >= 2 of them --
            # otherwise an unmatched component (which could be anything,
            # including an unrelated pooled tissue) is silently ignored by
            # the all() check below. Found by Codex review: "Eye, Retina,
            # UnrecognizedThing" would accept Retina since Eye/Retina are
            # ancestor-related and the unmatched third part was never
            # checked against anything.
            if len(matched) == len(parts) and len(matched) >= 2:
                for _, hit in matched:
                    others = [
                        other_hit for _, other_hit in matched if other_hit[0] != hit[0]
                    ]
                    if others and all(
                        _is_descendant_of(index, hit[0], other[0]) for other in others
                    ):
                        return "component_split", hit

    return "unresolved", None


def fetch_distinct_unmapped(cur: psycopg.Cursor, field: str) -> list[tuple[str, int]]:
    id_col = f"{field}_ontology_id"
    cur.execute(
        f"""
        select {field}, count(*) as n
        from ontology_samples_v5
        where {field} is not null and {field} != '' and {id_col} is null
        group by {field}
        order by n desc
        """,  # noqa: S608
    )
    return cur.fetchall()


def apply_field(
    cur: psycopg.Cursor,
    field: str,
    resolved: dict[str, tuple[str, str, float, str, int]],
) -> list[dict]:
    """UPDATE every row still holding this raw text and a NULL ontology id.
    One statement per resolved string keeps the WHERE clause a plain
    equality (fast, index-friendly) instead of a value-array join.

    Uses RETURNING to capture the exact (study_accession, sample) rows
    touched -- the distinct-string manifest written by run_field records
    what was PLANNED, not which rows were actually changed; the caller
    writes this function's return value out as the row-level manifest,
    but ONLY after conn.commit() succeeds (see main()), so a manifest file
    never describes changes that did not actually commit.

    The caller runs this whole function under SERIALIZABLE isolation, so
    Postgres itself aborts the transaction (raising here, nothing written)
    if a concurrent writer touched the same rows this transaction read or
    wrote -- the actual-vs-planned check below is a belt-and-suspenders
    invariant on top of that, not the only protection."""
    id_col, name_col, score_col, method_col = (
        f"{field}_ontology_id",
        f"{field}_ontology_name",
        f"{field}_ontology_score",
        f"{field}_match_method",
    )
    applied_rows = []
    for raw_text, (term_id, term_name, score, strategy, planned_n) in resolved.items():
        cur.execute(
            f"""
            update ontology_samples_v5
            set {id_col} = %s, {name_col} = %s, {score_col} = %s, {method_col} = %s
            where {field} = %s and {id_col} is null
            returning study_accession, sample
            """,  # noqa: S608
            (term_id, term_name, score, strategy, raw_text),
        )
        rows = cur.fetchall()
        if len(rows) > planned_n:
            msg = (
                f"{field}={raw_text!r}: UPDATE matched {len(rows)} rows, "
                f"more than the {planned_n} planned in the same transaction"
            )
            raise RuntimeError(msg)
        if len(rows) < planned_n:
            print(
                f"  note: {field}={raw_text!r} updated {len(rows)}/{planned_n} planned (rest resolved elsewhere since selection)"
            )
        applied_rows.extend(
            {
                "study_accession": study_accession,
                "sample": sample,
                "raw_text": raw_text,
                "matched_id": term_id,
                "matched_name": term_name,
                "score": score,
                "strategy": strategy,
            }
            for study_accession, sample in rows
        )
    return applied_rows


def write_applied_manifest(
    manifest_dir: Path, field: str, ts: str, applied_rows: list[dict]
) -> None:
    """Called only after conn.commit() has succeeded -- see apply_field's
    docstring for why write-after-commit matters here."""
    applied_path = manifest_dir / f"applied_{field}_backup_{ts}.csv"
    with applied_path.open("w", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "study_accession",
                "sample",
                "raw_text",
                "matched_id",
                "matched_name",
                "score",
                "strategy",
            ],
        )
        w.writeheader()
        w.writerows(applied_rows)
    print(
        f"row-level backup ({len(applied_rows):,} rows actually updated, committed) -> {applied_path}"
    )


def run_field(
    cur: psycopg.Cursor,
    field: str,
    index: OntologyIndex,
    manifest_dir: Path,
    ts: str,
) -> dict[str, tuple[str, str, float, str, int]]:
    is_tissue = field == "tissue"
    distinct = fetch_distinct_unmapped(cur, field)
    print(f"\n== {field} ==")
    print(f"distinct unmapped strings: {len(distinct):,}")

    strategy_counts: dict[str, int] = {}
    strategy_rows: dict[str, int] = {}
    resolved: dict[str, tuple[str, str, float, str, int]] = {}
    backup_rows = []

    for raw_text, n in distinct:
        strategy, hit = enhanced_match(index, raw_text, is_tissue=is_tissue)
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
        strategy_rows[strategy] = strategy_rows.get(strategy, 0) + n
        if hit:
            term_id, term_name, score = hit
            resolved[raw_text] = (term_id, term_name, score, strategy, n)
            backup_rows.append(
                {
                    "field": field,
                    "raw_text": raw_text,
                    "row_count": n,
                    "strategy": strategy,
                    "matched_id": term_id,
                    "matched_name": term_name,
                    "score": score,
                }
            )

    print(f"strategy breakdown (distinct strings / total rows):")
    for strat, n_strings in sorted(
        strategy_counts.items(), key=lambda kv: -strategy_rows[kv[0]]
    ):
        print(f"  {strat}: {n_strings:,} strings / {strategy_rows[strat]:,} rows")

    manifest_path = manifest_dir / f"unmapped_{field}_backup_{ts}.csv"
    with manifest_path.open("w", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "field",
                "raw_text",
                "row_count",
                "strategy",
                "matched_id",
                "matched_name",
                "score",
            ],
        )
        w.writeheader()
        w.writerows(backup_rows)
    n_rows_affected = sum(r["row_count"] for r in backup_rows)
    print(
        f"backup ({len(backup_rows):,} strings, {n_rows_affected:,} rows) -> {manifest_path}"
    )

    return resolved


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write; default is dry-run")
    ap.add_argument("--manifest-dir", default=OUTPUT_DIR)
    args = ap.parse_args()

    manifest_dir = Path(args.manifest_dir)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    tissue_file, tissue_keys, tissue_prefix = FIELD_ONTOLOGY_MAP["tissue"]
    disease_file, disease_keys, disease_prefix = FIELD_ONTOLOGY_MAP["disease"]
    tissue_index = OntologyIndex(ONTOLOGY_DIR / tissue_file, tissue_keys, tissue_prefix)
    disease_index = OntologyIndex(
        ONTOLOGY_DIR / disease_file, disease_keys, disease_prefix
    )

    with connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            if args.apply:
                # Must be the first statement of the transaction. Makes
                # Postgres itself detect a concurrent writer touching rows
                # this transaction reads (in run_field) or writes (in
                # apply_field) and abort with a serialization error rather
                # than silently interleaving -- see apply_field's docstring.
                cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

            tissue_resolved = run_field(cur, "tissue", tissue_index, manifest_dir, ts)
            disease_resolved = run_field(
                cur, "disease", disease_index, manifest_dir, ts
            )

            if not args.apply:
                print("\nDry run only. Re-run with --apply to write.")
                conn.rollback()
                return

            tissue_applied = apply_field(cur, "tissue", tissue_resolved)
            print(f"\ntissue rows updated: {len(tissue_applied):,}")
            disease_applied = apply_field(cur, "disease", disease_resolved)
            print(f"disease rows updated: {len(disease_applied):,}")
            conn.commit()
            print("\ncommitted.")

    # Only reached after a successful commit -- see apply_field's and
    # write_applied_manifest's docstrings for why the manifest write
    # cannot happen before this point. The database change is already
    # final either way, so one field's manifest write failing must not
    # skip the other's; report failures loudly instead of raising, since
    # the applied_rows are still in memory and the operator can re-dump
    # them if a write fails.
    for field, applied in (("tissue", tissue_applied), ("disease", disease_applied)):
        try:
            write_applied_manifest(manifest_dir, field, ts, applied)
        except OSError as exc:
            print(
                f"WARNING: {field} row-level manifest failed to write ({exc}); "
                f"the database change is already committed -- {len(applied):,} applied rows are only in memory now."
            )


if __name__ == "__main__":
    main()
