PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>

INSERT {
  ?x ?p ?z .
}
WHERE {
  ?p rdf:type owl:TransitiveProperty .

  ?x ?p ?y .
  ?y ?p ?z .

  FILTER ( ?x != ?z )
}
