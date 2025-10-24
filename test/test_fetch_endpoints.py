#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# python3 test/test_fetch_endpoints.py ./data/sparql_endpoints/dataset_stats.ttl =process

import sys
from rdflib import Graph, Namespace
from rdflib.namespace import RDFS, XSD

VOID = Namespace("http://rdfs.org/ns/void#")
IAO  = Namespace("http://purl.obolibrary.org/obo/IAO_")
NFDI = Namespace("https://nfdi.fiz-karlsruhe.de/ontology/")

Q_COUNTS = """
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX nfdi: <https://nfdi.fiz-karlsruhe.de/ontology/>

SELECT (COUNT(DISTINCT ?d) AS ?datasets) (COUNT(DISTINCT ?cp) AS ?partitions)
WHERE {
  ?d a nfdi:NFDI_0000009 .
  OPTIONAL { ?d void:classPartition ?cp }
}
"""

Q_SEARCH_TEMPLATE = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX IAO:  <http://purl.obolibrary.org/obo/IAO_>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX nfdi: <https://nfdi.fiz-karlsruhe.de/ontology/>

SELECT ?dataset_name ?dataset ?sparql_endpoint ?term_class (xsd:integer(?n) AS ?instances)
WHERE {{
  # Use your custom dataset type
  ?dataset a nfdi:NFDI_0000009 ;
           IAO:0000235 ?epInd ;
           void:classPartition ?cp ;
           <http://purl.obolibrary.org/obo/IAO_0000235> ?sparql_endpoint_node .

  ?sparql_endpoint_node <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001008> ?sparql_endpoint .
  
  OPTIONAL {{
    ?dataset rdfs:label ?dataset_name .
    FILTER(LANG(?dataset_name) = "" || LANGMATCHES(LANG(?dataset_name), "en"))
  }}

  ?cp void:class ?term_class ;
      void:entities ?n .

  OPTIONAL {{ ?cp rdfs:label ?lbl }}
  OPTIONAL {{
    ?term_class rdfs:label ?lc .
    FILTER(LANG(?lc) = "" || LANGMATCHES(LANG(?lc), "en"))
  }}

  BIND(LCASE(COALESCE(?lbl, ?lc,
       STRAFTER(STR(?term_class), "#"),
       STRAFTER(STR(?term_class), "/"))) AS ?name)

  {match_filter}
}}
ORDER BY ?dataset ?term_class
"""

def main(path: str, term: str | None):
    g = Graph()
    g.parse(path, format="turtle")
    print(f"Loaded {path}")

    # Quick diagnostics
    for row in g.query(Q_COUNTS):
        print(f"- datasets (NFDI_0000009): {int(row['datasets'])}")
        print(f"- class partitions:        {int(row['partitions'])}")

    term = (term or "process").strip().lower()

    if term.startswith("="):
        needle = term[1:]
        match_filter = f'FILTER(?name = "{needle}")'
        print(f"Search mode: exact match for '{needle}'")
    else:
        match_filter = f'FILTER(CONTAINS(?name, "{term}"))'
        print(f"Search mode: contains match for '{term}'")

    q = Q_SEARCH_TEMPLATE.format(match_filter=match_filter)
    rows = list(g.query(q))

    if not rows:
        print("No matches found.")
        return

    print(f"\nFound {len(rows)} matching partitions:\n")
    for dataset_name, dataset, sparql_endpoint, term_class, instances in rows:
        print("— result —")
        print(f"dataset_name : {dataset_name or ''}")
        print(f"dataset      : {dataset}")
        print(f"endpoint     : {sparql_endpoint}")
        print(f"class        : {term_class}")
        print(f"instances    : {int(instances)}\n")

if __name__ == "__main__":
    path = "./data/all.ttl"
    term = None
    if len(sys.argv) >= 2:
        path = sys.argv[1]
    if len(sys.argv) >= 3:
        term = sys.argv[2]
    main(path, term)
