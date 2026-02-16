PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>

INSERT {
  ?s ?superP ?o .
}
WHERE {
  ?s ?p ?o .
  ?p rdfs:subPropertyOf+ ?superP .
  FILTER ( ?p != ?superP )

  # never materialize top props
  FILTER ( ?superP != owl:topDataProperty && ?superP != owl:topObjectProperty )
}
