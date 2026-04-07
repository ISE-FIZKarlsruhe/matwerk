# Textual Entity

Every textual entity in the MSE-KG must be *about* something. This shape enforces that any instance of `IAO_0000300` (Textual Entity) carries at least one `IAO_0000136` (is about) link to the resource it describes. Without this constraint, orphaned textual entities would lack semantic grounding and could not be discovered through topic-based queries.

## Visualization

```ontoink
source: shapes/textual-entity/shape-data.ttl
shape: shapes/textual-entity/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `obo:IAO_0000300` (Textual Entity) |
| **Constraint** | Must have at least one `obo:IAO_0000136` (is about) relationship |
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
#  a "textual entity" always "is about" something
#

ex:TextualEntityShape
    a sh:NodeShape ;
    sh:targetClass <http://purl.obolibrary.org/obo/IAO_0000300> ;    # "Textual Entity"
    sh:property [
        sh:path <http://purl.obolibrary.org/obo/IAO_0000136> ;       # is about
        sh:minCount 1 ;
        sh:message "a 'textual entity' (IAO_0000300) must always 'be/is about' (IAO_0000136) something"
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

mse:publication_tensile_test_steel
    a obo:IAO_0000300 ;
    rdfs:label "Tensile Testing of High-Strength Steel Alloys" ;
    obo:IAO_0000136 mse:dataset_tensile_results .

mse:dataset_tensile_results
    a obo:IAO_0000100 ;
    rdfs:label "Tensile test results for X80 pipeline steel" .
```
