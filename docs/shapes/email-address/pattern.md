# Email Address Value

Every textual entity used as an email address must carry an actual value. This shape enforces that instances of `IAO_0000300` (Textual Entity) have at least one `NFDI_0001007` (has value) property. Without this constraint, email address records could exist as empty shells with no usable contact information.

## Visualization

```ontoink
source: shapes/email-address/shape-data.ttl
shape: shapes/email-address/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `obo:IAO_0000300` (Textual Entity) |
| **Constraint** | Must have at least one `nfdi:NFDI_0001007` (has value) property |
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
#  an "email address" (or textual entity) always needs to have a value
#

ex:EmailAddressShape
    a sh:NodeShape ;
    sh:targetClass <http://purl.obolibrary.org/obo/IAO_0000300> ;    # "Textual Entity"
    sh:property [
        sh:path <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0001007> ;    # has value
        sh:minCount 1 ;
        sh:message "an 'email address' / 'textual entity' (IAO_0000300) must always have a value (NFDI_0001007)"
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

mse:email_mueller
    a obo:IAO_0000300 ;
    rdfs:label "Email address of Dr. Anna Mueller" ;
    obo:IAO_0000136 mse:researcher_mueller ;
    nfdi:NFDI_0001007 "anna.mueller@kit.edu" .

mse:researcher_mueller
    a obo:BFO_0000040 ;
    rdfs:label "Dr. Anna Mueller" .
```
