-- Platform lexicon behind spatial_study's `platforms` column.
--
-- Hand-curated: a few dozen real spatial platforms exist, so every row here
-- is written by hand. Add a platform as a row in this file.
--
-- `pattern` is a Postgres ARE regex, matched with `txt ~* pattern` against
-- the same title+summary+overall_design+SRA-abstract+SRA-experiment text
-- spatial_pg.sql assembles. Always anchor with `\y` word boundaries and
-- `[-_. ]?` for optional separators: an unanchored `.?` pattern like
-- `slide.?seq` also matches inside "slide-sequencing". Test a new pattern
-- against the real corpus before adding it: run a `count(*) FILTER (WHERE
-- txt ~* '...')` against the same text-gathering CTEs spatial_pg.sql uses,
-- ad hoc, and read a sample of matched titles. A keyword that reads as
-- unambiguous in isolation can still collide with how papers actually write
-- titles (see 'Molecular Cartography' below).
--
-- `resolution` is 'single-cell', 'spot' or 'roi' (see spatial_pg.sql's
-- header for what each means). `technology` is 'imaging' (FISH/probe
-- hybridization, read out by microscopy), 'sequencing' (NGS readout,
-- barcoded capture or dissociation) or 'hybrid' (GeoMx DSP: optical ROI
-- selection, sequencing or nCounter readout). The two columns happen to
-- track each other for every platform except GeoMx (imaging~single-cell,
-- sequencing~spot) but are independent axes, not derived from one another;
-- do not assume a new platform's technology from its resolution or back.
--
-- `supersedes`: other platform names in this table that should NOT also be
-- reported on the same study when this row matches. Only needed when one
-- platform's name is a more specific version of another's (Visium HD is a
-- Visium product; Slide-seqV2 supersedes Slide-seq): a paper naming both
-- reports only the more specific one. Leave NULL for an unrelated platform.
--
-- Considered and rejected, not rows here (checked against real production
-- text): a bare `iss`/"in situ sequencing" keyword (too generic,
-- high collision risk with unrelated three-letter-acronym text); Seq-Scope,
-- HybISS, BaristaSeq, EASI-FISH, Open-ST, Patho-DBiT, ExSeq, sci-Space,
-- FISSEQ (real methods, zero hits in the corpus at the time); Akoya/CODEX
-- (PhenoCycler, a protein-imaging platform, not RNA -- the handful of hits
-- look like upstream modality-classifier noise, see the GeoMx DSP row's
-- notes). Re-check hit count before adding or reviving any of these.
--
-- Postgres only, so it carries the _pg suffix and stays out of the SQLite
-- schema test. The load replaces the table contents, never appends: edit
-- this file directly (add/remove/change a VALUES row) and rerun
-- `just spatial_platform_lexicon`, which TRUNCATEs and reloads whole, so a
-- row removed here is actually removed from the table too.

CREATE TABLE IF NOT EXISTS spatial_platform_lexicon (
    platform   text PRIMARY KEY,
    pattern    text NOT NULL CHECK (length(pattern) > 0),
    resolution text NOT NULL CHECK (resolution IN ('single-cell', 'spot', 'roi')),
    technology text NOT NULL CHECK (technology IN ('imaging', 'sequencing', 'hybrid')),
    supersedes text[],
    notes      text
);

BEGIN;

TRUNCATE spatial_platform_lexicon;

INSERT INTO spatial_platform_lexicon (platform, resolution, technology, supersedes, pattern, notes) VALUES
('Visium', 'spot', 'sequencing', NULL,
    '\yvisium\y',
    NULL),
('Visium HD', 'spot', 'sequencing', ARRAY['Visium'],
    '\yvisium[-_. ]?hd\y',
    NULL),
('Xenium', 'single-cell', 'imaging', NULL,
    '\yxenium\y',
    NULL),
('MERFISH', 'single-cell', 'imaging', NULL,
    '\ymerfish\y|\ymerscope\y',
    NULL),
('CosMx', 'single-cell', 'imaging', NULL,
    '\ycosmx\y',
    'A broader nanostring|smi fallback was tried and dropped: it matched unrelated NanoString nCounter (bulk) studies whose nearby text happened to mention "spatial".'),
('Slide-seq', 'spot', 'sequencing', NULL,
    '\yslide[-_. ]?seq\y',
    NULL),
('Slide-seqV2', 'spot', 'sequencing', ARRAY['Slide-seq'],
    '\yslide[-_. ]?seq[-_. ]?v2\y',
    NULL),
('Curio Seeker', 'spot', 'sequencing', NULL,
    '\ycurio[-_. ]?(seeker|seq)\y',
    'Commercializes licensed Slide-seq technology; papers cite the commercial name directly, so it gets its own entry.'),
('seqFISH', 'single-cell', 'imaging', NULL,
    '\yseqfish\y',
    NULL),
('Stereo-seq', 'spot', 'sequencing', NULL,
    '\ystereo[-_. ]?seq\y',
    NULL),
('GeoMx DSP', 'roi', 'hybrid', NULL,
    '\ygeomx\y|\ydigital spatial profiler\y',
    'Region-of-interest profiling, despite reaching the single_cell_modality=''Spatial Transcriptomics'' population upstream. Optical ROI selection with sequencing or nCounter readout gives technology=hybrid. A few Akoya/CODEX (PhenoCycler) hits were checked: that is a protein-imaging platform, and the hits look like upstream modality-classifier noise.'),
('DBiT-seq', 'spot', 'sequencing', NULL,
    '\ydbit[-_. ]?seq\y',
    NULL),
('STARmap', 'single-cell', 'imaging', NULL,
    '\ystarmap\y',
    NULL),
('osmFISH', 'single-cell', 'imaging', NULL,
    '\yosmfish\y',
    NULL),
('HDST', 'spot', 'sequencing', NULL,
    '\yhdst\y',
    NULL),
('Tomo-seq', 'spot', 'sequencing', NULL,
    '\ytomo[-_. ]?seq\y',
    NULL),
('Pixel-seq', 'spot', 'sequencing', NULL,
    '\ypixel[-_. ]?seq\y',
    NULL),
('Molecular Cartography', 'single-cell', 'imaging', NULL,
    '\yresolve[-_. ]?biosciences\y|\ymolecular[-_. ]?cartography\y[^.!?]{0,60}\y(platform|assay|technology|spatial[-_. ]?transcriptom)|\y(platform|assay|technology|spatial[-_. ]?transcriptom)[^.!?]{0,60}\ymolecular[-_. ]?cartography\y',
    'GSE280175''s title uses "molecular cartography" as a metaphor for the whole study, not to name the Resolve Biosciences platform, so this pattern requires the phrase appear near a platform-ish word or the vendor name.')
;

DO $$
DECLARE bad text;
BEGIN
    -- every supersedes target must be a real platform, or a typo silently
    -- fails to suppress anything instead of erroring
    SELECT s.platform || ' -> ' || u.target INTO bad
    FROM spatial_platform_lexicon s, LATERAL unnest(s.supersedes) u(target)
    LEFT JOIN spatial_platform_lexicon t ON t.platform = u.target
    WHERE t.platform IS NULL
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'spatial_platform_lexicon: supersedes target not a known platform (%)', bad;
    END IF;

    -- a platform superseding itself would always suppress its own match
    SELECT platform INTO bad
    FROM spatial_platform_lexicon
    WHERE platform = ANY(supersedes)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'spatial_platform_lexicon: % supersedes itself', bad;
    END IF;

    -- A supersedes B and B supersedes A would drop both whenever they
    -- co-occur, silently emptying `platforms` instead of picking one
    SELECT s1.platform || ' <-> ' || s2.platform INTO bad
    FROM spatial_platform_lexicon s1
    JOIN spatial_platform_lexicon s2
      ON s2.platform = ANY(s1.supersedes) AND s1.platform = ANY(s2.supersedes)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'spatial_platform_lexicon: supersedes cycle (%)', bad;
    END IF;
END $$;

COMMIT;
