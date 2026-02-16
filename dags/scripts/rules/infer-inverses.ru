PREFIX owl: <http://www.w3.org/2002/07/owl#>

INSERT {
  ?o ?inv ?s .
}
WHERE {
  ?p owl:inverseOf ?inv .
  ?s ?p ?o .
}
