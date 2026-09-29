#!/usr/bin/env python3
"""
Build the search-recall fix as a standalone SQLite, from Postgres reads only.

Two independent defects, one staging file, because they are one user-visible
symptom: an organism- or single-cell-filtered search silently returns a fraction
of what the corpus holds.

    sample_organism      ontology_samples_v4.organism is NULL on 10,944,347 of
                         16,081,369 rows (68%), and the samples-search filter is
                         exact equality, so those rows answer no organism query at
                         all. 70% of pentimento-counted samples are in that hole.

    study_single_cell    unified_metadata.is_single_cell was a text regex that only
                         ever wrote TRUE, over a column defaulting to 0. So 1.19M
                         rows read "not single-cell" having never been checked, and
                         15,686 studies with read-derived single-cell evidence read
                         FALSE.

Nothing here writes to Postgres; `transfer.py` does that, dry-run by default.

Run: python3 recipes/recall_fix/build.py --db ~/recall_fix.db
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sqlite3
import sys
from collections import Counter
from typing import TYPE_CHECKING

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "src"))
from pg_connect import connect
from sc_enrich import recall_feeds
from sc_enrich import recall_rules as rr

if TYPE_CHECKING:
    from psycopg import Connection

logger = logging.getLogger("recall_fix.build")

SCHEMA = """
CREATE TABLE IF NOT EXISTS sample_organism (
    sample    TEXT PRIMARY KEY,
    organism  TEXT NOT NULL,
    taxid     INTEGER,
    -- Which archive table answered. Kept because the routes have different
    -- trust: 'accession' is a primary-key hit, 'gsm_alias' is a bridge.
    route     TEXT NOT NULL
);

-- Rows where ontology_samples_v4 already holds an organism and the archive
-- disagrees. Separate from sample_organism because the write is a different
-- act: filling a NULL cannot lose information, overwriting can. The prior value
-- is kept here so the change is reversible without a database backup -- this
-- table IS the undo log.
CREATE TABLE IF NOT EXISTS sample_organism_overwrite (
    sample          TEXT PRIMARY KEY,
    prior_organism  TEXT,
    prior_taxid     INTEGER,
    organism        TEXT NOT NULL,
    taxid           INTEGER,
    route           TEXT NOT NULL
);

-- Postgres state captured immediately before and immediately after a write,
-- one row per affected sample/study per phase. The overwrite undo log already
-- carries prior values for its own rows; this covers the other two writes (the
-- NULL fills and the single_cell columns) and gives one place to diff.
--
-- `phase` is 'before' or 'after'. Verification is a self-join on that.
CREATE TABLE IF NOT EXISTS pg_snapshot_sample (
    phase     TEXT NOT NULL,
    sample    TEXT NOT NULL,
    organism  TEXT,
    taxid     INTEGER,
    PRIMARY KEY (phase, sample)
);

CREATE TABLE IF NOT EXISTS pg_snapshot_study (
    phase                 TEXT NOT NULL,
    study_accession       TEXT NOT NULL,
    is_single_cell        INTEGER,
    single_cell_status    TEXT,
    single_cell_modality  TEXT,
    PRIMARY KEY (phase, study_accession)
);

CREATE TABLE IF NOT EXISTS study_single_cell (
    study_accession       TEXT PRIMARY KEY,
    -- 1 / 0 / NULL. NULL means never screened and must not be read as "not
    -- single-cell"; single_cell_status spells out which state applies.
    is_single_cell        INTEGER,
    single_cell_status    TEXT NOT NULL,
    single_cell_modality  TEXT,
    single_cell_modality_evidence TEXT,
    -- evidence, so a call can be argued with rather than just believed
    max_cells             INTEGER,
    max_genes             INTEGER,
    n_samples_counted     INTEGER,
    n_runs_screened       INTEGER,
    n_runs_single_cell    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_ssc_status ON study_single_cell (single_cell_status);
"""

# Fill NULLs only, and only from the original per-sample archive record. Later
# routes must not clobber earlier ones, so each INSERTs OR IGNOREs.
#
# Do not fill from study-level organism_counts. On multi-organism studies that
# would assign the dominant organism to every sample; a visible NULL is safer.
ORGANISM_ROUTES = (
    (
        "accession",
        # sra_samples.accession is a genuine primary key, and covers
        # SRS + ERS + DRS in one table.
        """
        SELECT s.sample, a.scientific_name, a.taxon_id
        FROM ontology_samples_v4 s
        JOIN sra_samples a ON a.accession = s.sample
        WHERE s.organism IS NULL AND a.scientific_name IS NOT NULL
        """,
    ),
    (
        "gsm_alias",
        # GEO's own sample table carries no organism, so a GSM resolves through
        # the SRA record that aliases it. DISTINCT ON handles multiple SRS rows
        # for the same GSM.
        """
        SELECT DISTINCT ON (s.sample) s.sample, a.scientific_name, a.taxon_id
        FROM ontology_samples_v4 s
        JOIN sra_samples a ON a.alias = s.sample
        WHERE s.organism IS NULL AND a.scientific_name IS NOT NULL
          AND s.sample ~ '^GSM[0-9]+$'
        ORDER BY s.sample, a.accession
        """,
    ),
    (
        "gsa",
        """
        SELECT DISTINCT ON (s.sample) s.sample, g.scientific_name, NULL::int
        FROM ontology_samples_v4 s
        JOIN gsa_samples g ON g.sample_accession = s.sample
        WHERE s.organism IS NULL AND g.scientific_name IS NOT NULL
        ORDER BY s.sample, g.sample_accession
        """,
    ),
    (
        "ena_biosample",
        # SAMEA/SAMN-style ids that are neither an SRA accession nor a GSM.
        """
        SELECT DISTINCT ON (s.sample) s.sample, e.scientific_name, e.tax_id::int
        FROM ontology_samples_v4 s
        JOIN ena_samples e ON e.sample_accession = s.sample
        WHERE s.organism IS NULL AND e.scientific_name IS NOT NULL
        ORDER BY s.sample, e.sample_accession
        """,
    ),
    # ArrayExpress
    # AE samples have no sample accession; ontology_samples_v4.sample is the
    # free-text MAGE-TAB source name. Routes are ordered by closeness to an
    # archive record and scoped by experiment_accession, so free-text sample names
    # only need to be unique within their own experiment.
    (
        # Best AE route: the assay row names its ENA run, and the run lands on a
        # real archive record carrying the species. No free-text trust at all
        # beyond picking which AE row to read.
        "ae_ena_run",
        """
        SELECT DISTINCT ON (s.sample) s.sample, e.scientific_name, NULL::int
        FROM ontology_samples_v4 s
        JOIN arrayexpress_samples a
          ON a.experiment_accession = s.study_accession AND a.source_name = s.sample
        JOIN ena_experiments e ON e.run_accession = a.ena_run
        WHERE s.organism IS NULL AND e.scientific_name IS NOT NULL
        ORDER BY s.sample, a.ena_run
        """,
    ),
    (
        # The AE row's own organism. Assay-grained rows can fan out, so HAVING
        # drops source names that carry more than one organism instead of picking
        # a winner. Existing organism disagreements here are the enrichment defect
        # this recipe corrects, not evidence that the archive route is wrong.
        "ae_source_name",
        """
        SELECT s.sample, MIN(a.organism), NULL::int
        FROM ontology_samples_v4 s
        JOIN arrayexpress_samples a
          ON a.experiment_accession = s.study_accession AND a.source_name = s.sample
        WHERE s.organism IS NULL AND a.organism IS NOT NULL
        GROUP BY s.sample
        HAVING COUNT(DISTINCT a.organism) = 1
        """,
    ),
    # The old ae_gsm_bridge route stripped two-colour channel suffixes from
    # E-GEOD sample names and reused the GSM bridge. It lacked channel-aware
    # validation, so it stays removed.
    (
        # Labelled fallback, not a sample-level route. It applies only when
        # ArrayExpress's own assay rows have one organism for the experiment.
        "ae_experiment_single_organism",
        """
        SELECT DISTINCT ON (s.sample) s.sample, o.org, NULL::int
        FROM ontology_samples_v4 s
        JOIN (
            SELECT experiment_accession, MIN(organism) AS org
            FROM arrayexpress_samples WHERE organism IS NOT NULL
            GROUP BY 1 HAVING COUNT(DISTINCT organism) = 1
        ) o ON o.experiment_accession = s.study_accession
        -- The LIKE is not redundant with the join. It cuts the os_v4 side to
        -- ArrayExpress rows before probing the experiment list.
        WHERE s.organism IS NULL AND s.study_accession LIKE 'E-%'
        ORDER BY s.sample
        """,
    ),
    (
        # Last, so any sample-level route wins. This uses GEO platform organism
        # for samples with no SRA record and marks the weaker provenance in
        # `route`. Multi-organism platforms are skipped rather than guessed.
        "geo_platform",
        """
        SELECT DISTINCT ON (s.sample)
               s.sample, p.organism::jsonb->0->>'#text',
               (p.organism::jsonb->0->>'@taxid')::int
        FROM ontology_samples_v4 s
        JOIN geo_samples g ON g.accession = s.sample
        JOIN geo_platforms p ON p.accession = g.platform_ref
        WHERE s.organism IS NULL AND p.organism IS NOT NULL
          AND jsonb_array_length(p.organism::jsonb) = 1
        ORDER BY s.sample, g.accession
        """,
    ),
)

# Same joins with the opposite predicate: rows whose organism value contradicts
# an archive record. Capture each prior value first; sample_organism_overwrite is
# the undo log.
#
# Spelled out rather than derived from ORGANISM_ROUTES. The extra predicate must
# be placed before any DISTINCT ON ORDER BY clause.
ORGANISM_OVERWRITE_ROUTES = (
    (
        "accession",
        """
        SELECT s.sample, s.organism, s.taxid, a.scientific_name, a.taxon_id
        FROM ontology_samples_v4 s
        JOIN sra_samples a ON a.accession = s.sample
        WHERE s.organism IS NOT NULL AND a.scientific_name IS NOT NULL
          AND s.organism <> a.scientific_name
        """,
    ),
    (
        "gsm_alias",
        """
        SELECT DISTINCT ON (s.sample)
               s.sample, s.organism, s.taxid, a.scientific_name, a.taxon_id
        FROM ontology_samples_v4 s
        JOIN sra_samples a ON a.alias = s.sample
        WHERE s.organism IS NOT NULL AND a.scientific_name IS NOT NULL
          AND s.sample ~ '^GSM[0-9]+$'
          AND s.organism <> a.scientific_name
        ORDER BY s.sample, a.accession
        """,
    ),
    (
        "gsa",
        """
        SELECT DISTINCT ON (s.sample)
               s.sample, s.organism, s.taxid, g.scientific_name, NULL::int
        FROM ontology_samples_v4 s
        JOIN gsa_samples g ON g.sample_accession = s.sample
        WHERE s.organism IS NOT NULL AND g.scientific_name IS NOT NULL
          AND s.organism <> g.scientific_name
        ORDER BY s.sample, g.sample_accession
        """,
    ),
    (
        "ena_biosample",
        """
        SELECT DISTINCT ON (s.sample)
               s.sample, s.organism, s.taxid, e.scientific_name, e.tax_id::int
        FROM ontology_samples_v4 s
        JOIN ena_samples e ON e.sample_accession = s.sample
        WHERE s.organism IS NOT NULL AND e.scientific_name IS NOT NULL
          AND s.organism <> e.scientific_name
        ORDER BY s.sample, e.sample_accession
        """,
    ),
    (
        # ArrayExpress's own archival organism, for rows that already carry a
        # contradicting value. Same single-organism guard as the fill route, so
        # the 2 genuinely ambiguous (experiment, source_name) pairs are skipped
        # rather than resolved by coin flip.
        "ae_source_name",
        """
        SELECT s.sample, MIN(s.organism), MIN(s.taxid), MIN(a.organism), NULL::int
        FROM ontology_samples_v4 s
        JOIN arrayexpress_samples a
          ON a.experiment_accession = s.study_accession AND a.source_name = s.sample
        WHERE s.organism IS NOT NULL AND a.organism IS NOT NULL
        GROUP BY s.sample
        HAVING COUNT(DISTINCT a.organism) = 1
           AND COUNT(DISTINCT s.organism) = 1
           AND MIN(s.organism) <> MIN(a.organism)
        """,
    ),
)

# One row per study that pentimento or preflightx has anything to say about.
#
# The two evidence families are different screens and are kept apart:
#   matrix counts  a counted cell x gene matrix. A study with one is
#                  single-cell by construction; there is nothing to infer.
#   read-derived   preflightx, from the reads themselves. assay_is_single_cell
#                  is barcode-whitelist hits, bulk_vs_sc is R1 length structure.
#                  They are independent and may disagree.
# library strategies that cannot carry a 10x/Drop-seq RNA barcode geometry
# AMPLICON and Targeted-Capture excluded: 10x V(D)J is declared as both
# ATAC-seq excluded: barcoded, and counted separately
NON_RNA_STRATEGIES = (
    "Bisulfite-Seq",
    "ChIP-Seq",
    "ChIP",
    "ChIA-PET",
    "DNase-Hypersensitivity",
    "FAIRE-seq",
    "MNase-Seq",
    "MBD-Seq",
    "MeDIP-Seq",
    "MRE-Seq",
    "NOMe-Seq",
    "DIP-Seq",
    "Hi-C",
    "Tethered Chromatin Conformation Capture",
    "WGS",
    "WGA",
    "WXS",
    "WCS",
    "CLONE",
    "CLONEEND",
    "POOLCLONE",
    "FINISHING",
    "VALIDATION",
    "SELEX",
    "Tn-Seq",
    "GBS",
)
_NON_RNA_SQL_LIST = ", ".join(f"'{v}'" for v in NON_RNA_STRATEGIES)

SINGLE_CELL_SQL = """
WITH counted AS (
    SELECT study_accession,
           MAX(cells)  AS max_cells,
           MAX(genes)  AS max_genes,
           COUNT(*) FILTER (WHERE cells IS NOT NULL OR genes IS NOT NULL) AS n_counted,
           -- identical cells with no gene axis means one matrix counted once
           -- per sample, so the number is a column count
           (BOOL_OR(cells > 1 OR genes > 1)
            AND NOT (COUNT(cells) > 1
                     AND COUNT(DISTINCT cells) = 1
                     AND COUNT(genes) = 0)) AS matrix_says_sc,
           (COUNT(cells) > 1
            AND COUNT(DISTINCT cells) = 1
            AND COUNT(genes) = 0) AS matrix_vetoed,
           -- the run's chemistry call carried onto the sample
           BOOL_OR(
               assay_is_single_cell
               AND NOT (
                   assay_display NOT ILIKE '10x%'
                   AND assay_display NOT ILIKE '%ATAC%'
                   AND bulk_vs_sc = 'bulk_or_5prime_broken'
               )
           ) AS sample_assay_sc,
           BOOL_OR(modality = 'nucleus')        AS any_nucleus,
           BOOL_OR(modality = 'cell')           AS any_cell
    FROM pentimento_sample
    GROUP BY study_accession
),
screened AS (
    -- Abstention is not a negative. Ambiguous bulk_vs_sc values do not count as
    -- screened, because writing a measured-looking 0 would overstate evidence.
    SELECT r.study_accession,
           COUNT(*) FILTER (
               WHERE r.assay_is_single_cell IS NOT NULL
                  OR r.bulk_vs_sc IN ('bulk', 'single_cell', 'single_cell_uncertain')
           ) AS n_runs_screened,
           COUNT(*) FILTER (
               -- single_cell_uncertain hedges confidence, not which side
               -- non-10x chemistries have no onlist, so the call is geometry-only
               -- an RNA chemistry cannot sit on a library declared DNA or chromatin
               -- a vetoed run is unscreened, not bulk; plate-based reads look the same
               WHERE (
                   r.assay_is_single_cell
                   AND NOT (
                       r.assay NOT ILIKE '10x%'
                       AND r.assay NOT ILIKE '%ATAC%'
                       AND r.bulk_vs_sc = 'bulk_or_5prime_broken'
                   )
                   AND NOT (
                       r.assay NOT ILIKE '%ATAC%'
                       AND e.library_strategy IN ({non_rna})
                   )
               )
                  OR r.bulk_vs_sc IN ('single_cell', 'single_cell_uncertain')
           ) AS n_runs_sc,
           COUNT(*) FILTER (
               WHERE r.assay_is_single_cell IS FALSE OR r.bulk_vs_sc = 'bulk'
           ) AS n_runs_bulk,
           -- the R1-length half of n_runs_bulk: real read structure, unlike a
           -- whitelist miss (see recall_rules.whitelist_miss_only)
           COUNT(*) FILTER (WHERE r.bulk_vs_sc = 'bulk') AS n_runs_bulk_r1len,
           -- scATAC trips assay_is_single_cell=FALSE because the call is a
           -- barcode-whitelist hit against RNA chemistries. Those runs are
           -- single-cell; they are just not scRNA-seq.
           COUNT(*) FILTER (WHERE r.assay ILIKE '%ATAC%') AS n_runs_atac
    FROM pentimento_run r
    -- The submitter's own declaration of what the library is. LEFT so a run
    -- with no SRA experiment row (GSA, DDBJ) is simply not vetoed.
    LEFT JOIN sra_runs sr ON sr.accession = r.run_accession
    LEFT JOIN sra_experiments e ON e.accession = sr.experiment
    WHERE r.study_accession IS NOT NULL
    GROUP BY r.study_accession
)
SELECT COALESCE(c.study_accession, r.study_accession) AS study_accession,
       c.max_cells, c.max_genes, COALESCE(c.n_counted, 0) AS n_counted,
       COALESCE(c.matrix_says_sc, FALSE) AS matrix_says_sc,
       COALESCE(c.matrix_vetoed, FALSE) AS matrix_vetoed,
       COALESCE(c.sample_assay_sc, FALSE) AS sample_assay_sc,
       COALESCE(c.any_nucleus, FALSE) AS any_nucleus,
       COALESCE(c.any_cell, FALSE) AS any_cell,
       COALESCE(r.n_runs_screened, 0) AS n_runs_screened,
       COALESCE(r.n_runs_sc, 0) AS n_runs_sc,
       COALESCE(r.n_runs_bulk, 0) AS n_runs_bulk,
       COALESCE(r.n_runs_bulk_r1len, 0) AS n_runs_bulk_r1len,
       COALESCE(r.n_runs_atac, 0) AS n_runs_atac
FROM counted c
FULL OUTER JOIN screened r ON r.study_accession = c.study_accession
-- pentimento_run.study_accession is nullable, and rows that never resolved to a
-- study aggregate into one NULL-keyed group. It is not a study and cannot be
-- joined to unified_metadata; without this the staging table carries a row whose
-- primary key is NULL, which sqlite tolerates and postgres rejects on COPY.
WHERE COALESCE(c.study_accession, r.study_accession) IS NOT NULL
""".format(non_rna=_NON_RNA_SQL_LIST)

# The text signal, re-derived from title/description rather than read back out of
# is_single_cell.
#
# That distinction is the whole reason this build is safely re-runnable. Reading
# the flag this recipe itself writes makes the build self-referential: a study
# classified 'measured_not_single_cell_text_disagrees' has is_single_cell set to
# 0, so the NEXT build would no longer see it as text-positive and would quietly
# downgrade it to plain 'measured_not_single_cell'. Deriving from the source text
# makes every run a pure function of the archives, so re-running after new
# preflightx/pentimento data lands converges instead of drifting.
#
# Mirrors SC_PATTERNS in src/sc_enrich/classify.py. Kept in step by
# tests/test_recall_fix.py, which fails if the two disagree on a known set.
SC_TEXT_REGEX = (
    r"scRNA[- ]?seq|sc[- ]RNA[- ]?seq"
    r"|snRNA[- ]?seq|sn[- ]RNA[- ]?seq"
    r"|single[- ]cell[- ]+RNA"
    r"|single[- ]cell[- ]+transcriptom"
    r"|single[- ]nucle\w*[- ]+RNA"
    r"|single[- ]nucle\w*[- ]+transcriptom"
    r"|10x Genomics"
    r"|Drop[- ]?seq|dropseq"
    r"|Smart[- ]?seq[0-9]?|smartseq"
    r"|inDrop|CEL[- ]?seq|MARS[- ]?seq"
    r"|sci[- ]RNA[- ]?seq"
    r"|Seq[- ]?Well|SPLiT[- ]?seq"
    r"|CITE[- ]?seq|CITEseq"
    # studies that say "droplet-based" and never "scRNA-seq"
    r"|droplet[- ]based"
    r"|individual (?:liver |immune |tumou?r )?cells"
    r"|single cells were"
)

TEXT_SQL = """
SELECT canonical_accession, single_cell_modality, title, description
FROM unified_metadata
WHERE title ~* %(rx)s OR description ~* %(rx)s
"""

# The submitter's own declared library_source, straight off the archive record --
# not a fuzzy title/description match. 'TRANSCRIPTOMIC SINGLE CELL' /
# 'GENOMIC SINGLE CELL' are the archives' own controlled vocabulary, so a hit
# here is high-confidence by construction (mirrors the sc_evidence rule
# `insdc_library_source`).
#
# study/study_accession is the archive's own accession (SRP/ERP/CRA/DRP), which
# is frequently NOT unified_metadata.canonical_accession -- a study ingested
# under an SRP often canonicalises to its linked GSE. alias_values(aliases)
# is the same function the `idx_unified_metadata_alias_values` GIN index is
# built on, so an SRP-keyed hit still resolves to its GSE-canonicalised row.
#
# The set of declared accessions is aggregated into a single array (`declared`,
# one row) rather than joined row-by-row: unified_metadata has a GIN index on
# alias_values(aliases) that the `&&` array-overlap operator can use, but only
# against a bound array. A per-row `d.acc = ANY(...)` OR join over a multi-row
# CTE forces a nested-loop scan instead and does not finish in reasonable time
# against 40M+-row archive tables.
LIBRARY_SOURCE_SQL = """
WITH declared AS (
    SELECT array_agg(DISTINCT acc) AS accs FROM (
        SELECT study AS acc
          FROM sra_experiments WHERE upper(library_source) LIKE '%SINGLE CELL%'
        UNION
        SELECT study_accession AS acc
          FROM ena_experiments WHERE upper(library_source) LIKE '%SINGLE CELL%'
        UNION
        SELECT study_accession AS acc
          FROM gsa_experiments WHERE upper(library_source) LIKE '%SINGLE CELL%'
        UNION
        SELECT study AS acc
          FROM dra_experiments WHERE upper(library_source) LIKE '%SINGLE CELL%'
        UNION
        SELECT experiment_accession AS acc
          FROM arrayexpress_samples WHERE upper(library_source) LIKE '%SINGLE CELL%'
    ) x
)
SELECT um.canonical_accession
FROM unified_metadata um, declared d
WHERE um.canonical_accession = ANY(d.accs)
   OR alias_values(um.aliases) && d.accs
"""


def _classify(row: dict) -> tuple[int | None, str]:
    """(is_single_cell, single_cell_status) from one study's read-derived evidence."""
    if (
        row["matrix_says_sc"]
        or row["sample_assay_sc"]
        or row["n_runs_sc"] > 0
        or row["n_runs_atac"] > 0
    ):
        return 1, "measured_single_cell"
    # Screened and it came back negative, the only honest 0 in the column. It
    # separates "we looked, it is bulk" from "nobody has looked", so it requires
    # a POSITIVE bulk verdict. An absence of single-cell evidence will not do,
    # and neither will an abstention.
    if row["n_runs_bulk"] > 0:
        return 0, "measured_not_single_cell"
    # A counted matrix whose columns are all <= 1 is a measurement too, but of
    # something that is not a cell x gene matrix worth calling single-cell.
    #
    # a vetoed matrix was never read per sample, so it measures nothing
    if row["n_counted"] > 0 and not row["matrix_vetoed"]:
        return 0, "measured_not_single_cell"
    return None, "not_screened"


def build(
    db_path: pathlib.Path,
    only: str,
    *,
    protocol: bool = True,
    external: pathlib.Path | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    pg = connect()

    if only in ("both", "organism"):
        _build_organism(conn, pg)
    if only in ("both", "organism", "organism-overwrite"):
        _build_organism_overwrite(conn, pg)
    if only in ("both", "single-cell"):
        _build_single_cell(conn, pg, protocol=protocol, external=external)

    conn.commit()
    conn.close()


def _build_organism(conn: sqlite3.Connection, pg: Connection) -> None:
    conn.execute("DELETE FROM sample_organism")
    for route, sql in ORGANISM_ROUTES:
        cur = pg.cursor(name=f"org_{route}")  # server-side, these are millions of rows
        cur.itersize = 50_000
        cur.execute(sql)
        n = 0
        while True:
            rows = cur.fetchmany(50_000)
            if not rows:
                break
            conn.executemany(
                "INSERT OR IGNORE INTO sample_organism (sample, organism, taxid, route)"
                " VALUES (?, ?, ?, ?)",
                [(s, o, t, route) for s, o, t in rows],
            )
            n += len(rows)
            logger.info("  %s: %d fetched", route, n)
        cur.close()
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM sample_organism").fetchone()[0]
        logger.info("route %-14s +%-9d -> %d staged", route, n, total)


def _build_organism_overwrite(conn: sqlite3.Connection, pg: Connection) -> None:
    """
    Stage the rows where an existing organism contradicts the archive.

    Small enough (~174k) to hold in one pass, unlike the fill routes. The prior
    value is written in the same statement as the correction, so there is no
    window in which a row has been corrected without its original recorded.
    """
    conn.execute("DELETE FROM sample_organism_overwrite")
    for route, sql in ORGANISM_OVERWRITE_ROUTES:
        cur = pg.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.executemany(
            "INSERT OR IGNORE INTO sample_organism_overwrite"
            " (sample, prior_organism, prior_taxid, organism, taxid, route)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            [(s, po, pt, o, tx, route) for s, po, pt, o, tx in rows],
        )
        conn.commit()
        total = conn.execute(
            "SELECT COUNT(*) FROM sample_organism_overwrite"
        ).fetchone()[0]
        logger.info("overwrite %-14s +%-9d -> %d staged", route, len(rows), total)

    for prior, new_org, n in conn.execute(
        "SELECT prior_organism, organism, COUNT(*) FROM sample_organism_overwrite"
        " GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10"
    ):
        logger.info("  %-28s -> %-28s %d", str(prior)[:27], str(new_org)[:27], n)


def _build_single_cell(  # noqa: C901, PLR0912, PLR0915
    conn: sqlite3.Connection,
    pg: Connection,
    *,
    protocol: bool = True,
    external: pathlib.Path | None = None,
) -> None:
    conn.execute("DELETE FROM study_single_cell")
    cur = pg.cursor()
    cur.execute(SINGLE_CELL_SQL)
    cols = [d[0] for d in cur.description]
    staged: dict[str, tuple] = {}
    reads: dict[str, tuple[int, int, int]] = {}
    for raw in cur.fetchall():
        row = dict(zip(cols, raw, strict=True))
        acc = row["study_accession"]
        reads[acc] = (row["n_runs_sc"], row["n_runs_bulk"], row["n_runs_bulk_r1len"])
        is_sc, status = _classify(row)
        modality = None
        modality_evidence = None
        if is_sc == 1 and (row["any_nucleus"] or row["any_cell"]):
            # Read-derived, and it outranks the text guess: nuclei retain
            # unspliced transcripts and lose cytoplasmic mitochondria, which is
            # measured, whereas the text route only knows when an author said so.
            # Only the sc/sn axis; pentimento cannot see Spatial or Multiome.
            modality = "snRNA-seq" if row["any_nucleus"] else "scRNA-seq"
            modality_evidence = "preflightx"
        staged[acc] = (
            acc,
            is_sc,
            status,
            modality,
            modality_evidence,
            row["max_cells"],
            row["max_genes"],
            row["n_counted"],
            row["n_runs_screened"],
            row["n_runs_sc"],
        )
    logger.info("read-derived evidence for %d studies", len(staged))

    # Pentimento keys are archive accessions (SRP/ERP/PRJEB). transfer.py writes
    # by unified_metadata canonical, so re-key them first or the verdict is lost.
    resolve, amap = recall_feeds.alias_resolver(pg)
    staged, moved = rr.canonicalise(staged, resolve)
    canon_reads: dict[str, tuple[int, int, int]] = {}
    for acc, counts in reads.items():
        key = resolve(acc) or acc
        prev = canon_reads.get(key, (0, 0, 0))
        canon_reads[key] = tuple(p + c for p, c in zip(prev, counts, strict=True))
    logger.info("read-derived rows re-keyed onto their canonical: %d", moved)

    cur.execute(LIBRARY_SOURCE_SQL)
    declared = [acc for (acc,) in cur.fetchall()]
    meta = recall_feeds.study_meta(pg, declared)
    lib_vetoed: dict[str, str] = {}
    for acc in declared:
        title, org, strategies = meta.get(acc, (None, None, []))
        reason = rr.libsource_vetoed(org, title, strategies)
        if reason:
            lib_vetoed[acc] = reason
    logger.info("library_source vetoes: %s", Counter(lib_vetoed.values()))
    protocol_keys: set[str] = set()
    if protocol:
        protocol_keys = {resolve(a) or a for a in recall_feeds.protocol_hits_by_study(pg)}
    # archive evidence independent of the title/description text
    corroborated = {a for a in declared if a not in lib_vetoed} | protocol_keys

    cur.execute(TEXT_SQL, {"rx": SC_TEXT_REGEX})
    text_rows = cur.fetchall()
    # Studies whose every run abstained get their text verdict back only with
    # archive corroboration, minus the audit's text false-positive classes
    # (bulk-companion tags, linked-read genomes).
    vetoed = {a for a, _, t, d in text_rows if rr.text_promotion_vetoed(t, d)}
    staged, text_only = rr.apply_text(
        staged,
        {a: m for a, m, _, _ in text_rows},
        vetoed=vetoed,
        corroborated=corroborated,
    )
    logger.info("text studies kept or promoted TRUE: %d", text_only)

    library_source_only = 0
    for acc in declared:
        if acc in lib_vetoed:
            continue
        existing = staged.get(acc)
        # Strictly additive: a declared library_source only fills a study
        # whose status is otherwise 'not_screened' -- either it never appeared
        # in either pass above, or pentimento touched it and every run
        # abstained (staged with status 'not_screened' already). It never
        # overwrites 'measured_single_cell', 'measured_not_single_cell',
        # 'measured_not_single_cell_text_disagrees', or
        # 'text_indicates_single_cell' -- the declared field is strong
        # evidence, but a verdict something else already reached stands.
        if existing is not None and existing[2] != "not_screened":
            continue
        staged[acc] = (
            acc,
            1,
            "library_source_indicates_single_cell",
            existing[3] if existing else None,
            "library_source",
            *(existing[5:] if existing else (None, None, 0, 0, 0)),
        )
        library_source_only += 1
    logger.info("library_source-only studies added/promoted: %d", library_source_only)

    # A whitelist miss is not a bulk verdict: declared single-cell libraries
    # outrank a negative made of whitelist misses alone.
    negatives = {
        a for a in declared if a not in lib_vetoed and staged.get(a, (0, None))[1] == 0
    }
    archive_accs = sorted({a for a, c in amap.items() if c in negatives} | negatives)
    declared_counts: dict[str, tuple[int, int]] = {}
    for acc, (n_sc, n_exp) in recall_feeds.libsource_counts(pg, archive_accs).items():
        key = resolve(acc) or acc
        prev = declared_counts.get(key, (0, 0))
        declared_counts[key] = (prev[0] + n_sc, prev[1] + n_exp)
    staged, flipped = rr.apply_whitelist_override(staged, declared_counts, canon_reads)
    logger.info("whitelist-only negatives overruled by declared libraries: %d", flipped)

    if protocol:
        staged, n_prot = rr.apply_additive(staged, protocol_keys, rr.PROTOCOL_STATUS)
        logger.info(
            "protocol-text studies added: %d (of %d with a hit)", n_prot, len(protocol_keys)
        )

    if external is not None:
        # Labelled, optional evidence: membership in an external single-cell
        # resource (scBaseCount, CELLxGENE Census). One accession per line.
        accs = {ln.strip() for ln in external.read_text().splitlines() if ln.strip()}
        keys = {resolve(a) or a for a in accs}
        staged, n_ext = rr.apply_additive(staged, keys, rr.EXTERNAL_STATUS)
        logger.info("external-resource studies added: %d (of %d listed)", n_ext, len(keys))

    conn.executemany(
        "INSERT OR REPLACE INTO study_single_cell VALUES (?,?,?,?,?,?,?,?,?,?)",
        list(staged.values()),
    )
    conn.commit()
    for status, n in conn.execute(
        "SELECT single_cell_status, COUNT(*) FROM study_single_cell"
        " GROUP BY 1 ORDER BY 2 DESC"
    ):
        logger.info("  %-40s %d", status, n)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=pathlib.Path, required=True)
    ap.add_argument(
        "--only",
        choices=("both", "organism", "organism-overwrite", "single-cell"),
        default="both",
    )
    ap.add_argument(
        "--skip-protocol",
        action="store_true",
        help="skip the paged protocol-text scan (sra/ena experiments, GEO samples)",
    )
    ap.add_argument(
        "--external-accessions",
        type=pathlib.Path,
        help="optional: accessions in an external single-cell resource; labelled"
        " external_resource_indicates_single_cell and only fills unscreened studies",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    build(args.db, args.only, protocol=not args.skip_protocol, external=args.external_accessions)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
