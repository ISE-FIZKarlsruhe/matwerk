PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT {
  ?y rdf:type ?c .
}
WHERE {
  ?x owl:sameAs ?y .
  ?x rdf:type ?c .
}
