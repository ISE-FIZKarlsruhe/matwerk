PREFIX owl: <http://www.w3.org/2002/07/owl#>

# Subject-side propagation: x sameAs y, x p o -> y p o
INSERT {
  ?y ?p ?o .
}
WHERE {
  ?x owl:sameAs ?y .
  ?x ?p ?o .
  FILTER ( ?p != owl:sameAs )
}

;

# Object-side propagation: x sameAs y, s p x -> s p y
INSERT {
  ?s ?p ?y .
}
WHERE {
  ?x owl:sameAs ?y .
  ?s ?p ?x .
  FILTER ( ?p != owl:sameAs )
}
