-- mondo_id is the primary key, so the join to the corpus needs no extra index.
-- This file exists so the schema test covers the table the same way as the rest.
CREATE INDEX IF NOT EXISTS idx_disease_inheritance_fetched
    ON disease_inheritance (fetched_at);
