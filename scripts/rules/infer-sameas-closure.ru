PREFIX owl: <http://www.w3.org/2002/07/owl#>

# Symmetry: x sameAs y -> y sameAs x
INSERT {
  ?y owl:sameAs ?x .
}
WHERE {
  ?x owl:sameAs ?y .
  FILTER ( ?x != ?y )
}

;

# Transitivity: x sameAs y, y sameAs z -> x sameAs z
INSERT {
  ?x owl:sameAs ?z .
}
WHERE {
  ?x owl:sameAs ?y .
  ?y owl:sameAs ?z .
  FILTER ( ?x != ?z )
}
