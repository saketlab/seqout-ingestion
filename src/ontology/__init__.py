"""
Rebuild the Neo4j ontology graph that powers query expansion and deep-dive.

See ``docs/ontology_graph.md`` for the full handover write-up: what the graph
is, why each ontology is in it, and how to rebuild it from nothing.

Read the modules in this order:

1. ``sources.py`` -- the registry. Every ontology, its download URL, its file
   format, and the knobs used when parsing it. Start here.
2. ``extract.py`` -- downloads a source and turns it into two archive-neutral
   artifacts: a ``short,long`` synonym CSV and (where we take hierarchy) an
   OBO-JSON with an ``edges`` array.
3. ``graph.py`` -- loads those artifacts into Neo4j and stamps the
   ``has_children`` flag the frontend reads.
"""
