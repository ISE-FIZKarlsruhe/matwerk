PREFIX owl: <http://www.w3.org/2002/07/owl#>

INSERT {
  ?s ?p2 ?o .
  ?s ?p1 ?o .
}
WHERE {
  ?p1 owl:equivalentProperty ?p2 .
  ?s ?p1 ?o .
}
