"""Harvester for RDF data registered in the MatWerk Google Sheet.

A curator adds a row to the registration tab pointing at RDF data hosted on
**GitHub** (a repository release) or **Zenodo** (a record), optionally naming the
files to include. This harvester fetches each RDF file and turns it into **its own
named graph**, inserted verbatim — nothing is remodelled.

A default (metadata) graph links every file-graph back to its repository/record
via BFO has-part / part-of, exactly like the Zenodo harvester, so the connection
to the registered source is never lost. It also carries flat ``stat:`` triples
(``registeredFrom``, ``triples``, ``version``, ``harvestedAt``) which let the
sheet's Apps Script look up each row's graph IRI over SPARQL and write the result
back next to the registration.

Output is a single TriG (``rdf_files.ttl``) consumed per named graph by
``publish_to_virtuoso``, plus ``rdf_files_state.json`` for change detection
across weekly runs (latest release/version wins; unchanged files are skipped).
"""
