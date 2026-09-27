-- Ontology closure staging (SQLite). Disposable: rebuilt whole from the parsed
-- OBO/EFO JSONs on every `ontology_closure build` run.
DROP TABLE IF EXISTS ontology_closure;
CREATE TABLE ontology_closure (
    ontology TEXT NOT NULL,
    ancestor_id TEXT NOT NULL,
    descendant_id TEXT NOT NULL,
    PRIMARY KEY (ontology, ancestor_id, descendant_id)
);
CREATE INDEX idx_closure_desc ON ontology_closure (ontology, descendant_id);

DROP TABLE IF EXISTS ontology_terms;
CREATE TABLE ontology_terms (
    ontology TEXT NOT NULL,
    id TEXT NOT NULL,
    label TEXT,
    PRIMARY KEY (ontology, id)
);
