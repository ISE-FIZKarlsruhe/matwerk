# Role Realization

Every role must be realised in at least one process. This shape enforces that each instance of `BFO_0000023` (Role) carries a `BFO_0000054` (realized in) link to the process that brings the role into actuality. In the MSE-KG this ensures that roles such as "principal investigator" or "reviewer" are never recorded without the process they participate in.

## Visualization

```ontoink
source: shapes/role-realization/shape-data.ttl
shape: shapes/role-realization/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `obo:BFO_0000023` (Role) |
| **Constraint** | Must have at least one `obo:BFO_0000054` (realized in) relationship |
| **Severity** | Violation |

## Shape Definition

```turtle
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix sh:    <http://www.w3.org/ns/shacl#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:    <http://www.example.org/#> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .

#
#  a "role" should always "realized in" something
#

ex:RoleRealizaionShape
    a sh:NodeShape ;
    sh:targetClass <http://purl.obolibrary.org/obo/BFO_0000023> ;    # bfo:role
    sh:property [
        sh:path <http://purl.obolibrary.org/obo/BFO_0000054> ;       # bfo:inheres in
        sh:minCount 1 ;
        sh:message "a 'role' (BFO_0000023) must always 'realized in' (BFO_0000054) something"
    ] ;
    .
```

## Example Data

```turtle
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix obo:   <http://purl.obolibrary.org/obo/> .
@prefix nfdi:  <https://nfdi.fiz-karlsruhe.de/ontology/> .
@prefix mse:   <https://nfdi.fiz-karlsruhe.de/matwerk/> .

mse:reviewer_role_01
    a obo:BFO_0000023 ;
    rdfs:label "Peer reviewer role" ;
    obo:BFO_0000054 mse:process_peer_review .

mse:process_peer_review
    a obo:BFO_0000015 ;
    rdfs:label "Peer review of corrosion resistance dataset" .
```
