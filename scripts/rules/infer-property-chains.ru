PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>

INSERT {
  ?x ?r ?z .
}
WHERE {
  # OWL 2 RDF encoding:
  # _:ax owl:propertyChainAxiom _:list .
  # _:ax rdfs:subPropertyOf ?r .
  ?ax   owl:propertyChainAxiom ?list .
  ?ax   rdfs:subPropertyOf ?r .

  # Limit to 2-link chains: (p1 p2)
  ?list rdf:first ?p1 ;
        rdf:rest  ?rest .

  ?rest rdf:first ?p2 ;
        rdf:rest  rdf:nil .

  # Match assertions
  ?x ?p1 ?y .
  ?y ?p2 ?z .
}
