# Role Bearer

A role in BFO must always be anchored to a bearer and realised in a process. This shape enforces that every instance of `BFO_0000023` (Role) has both a `RO_0000081` (role of) link to its bearer and a `BFO_0000054` (realized in) link to the process that actualises it. This dual constraint prevents dangling roles that exist without context.

## Visualization

```ontoink
source: shapes/role-bearer/shape-data.ttl
shape: shapes/role-bearer/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `obo:BFO_0000023` (Role) |
| **Constraint** | Must have at least one `obo:RO_0000081` (role of) and at least one `obo:BFO_0000054` (realized in) relationship |
| **Severity** | Violation |

## Shape Definition

```turtle
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix sh:    <http://www.w3.org/ns/shacl#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:    <http://www.example.org/#> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .

ex:RoleBearerShape
    a sh:NodeShape ;
    sh:targetClass <http://purl.obolibrary.org/obo/BFO_0000023> ;    # bfo:role

    # role_of
    sh:property [
        sh:path <http://purl.obolibrary.org/obo/RO_0000081> ;       # ro:role_of
        sh:minCount 1 ;
        sh:message "a 'role' (BFO_0000023) must always 'role of' (RO_0000081) something"
    ] ;

    # realized_in
    sh:property [
        sh:path <http://purl.obolibrary.org/obo/BFO_0000054> ;      # bfo:realized_in
        sh:minCount 1 ;
        sh:message "a 'role' (BFO_0000023) must always be 'realized in' (BFO_0000054) some process"
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

mse:principal_investigator_role_01
    a obo:BFO_0000023 ;
    rdfs:label "Principal Investigator role of Dr. Mueller" ;
    obo:RO_0000081 mse:researcher_mueller ;
    obo:BFO_0000054 mse:process_fatigue_study .

mse:researcher_mueller
    a obo:BFO_0000040 ;
    rdfs:label "Dr. Anna Mueller" .

mse:process_fatigue_study
    a obo:BFO_0000015 ;
    rdfs:label "Fatigue characterisation study of titanium alloys" .
```
