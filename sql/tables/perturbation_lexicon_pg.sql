-- Compound lexicon behind singlecell_perturbation_study.
--
-- One row per single-token name, built by `python -m perturbation.lexicon`
-- from ChEBI plus src/perturbation/lexicon_overrides.tsv, then loaded whole by
-- `just perturbation_lexicon`. kind: compound, biologic (chemical evidence),
-- inducer, stimulus, exclude (never chemical evidence).
--
-- Postgres only, so it carries the _pg suffix and stays out of the SQLite
-- schema test. The load replaces the table contents, never appends.

CREATE TABLE IF NOT EXISTS perturbation_lexicon (
    token    text PRIMARY KEY,
    kind     text NOT NULL,
    chebi_id text,
    label    text,
    source   text
);
