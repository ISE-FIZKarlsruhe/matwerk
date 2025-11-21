PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

INSERT {
  ?s ?superP ?o .
}
WHERE {
  ?s ?p ?o .
  ?p rdfs:subPropertyOf+ ?superP .
  FILTER (?p != ?superP)
}
