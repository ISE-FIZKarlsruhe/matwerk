# No Punning

OWL 2 punning allows the same IRI to denote both a class and an individual, but the MSE-KG deliberately prohibits this practice to keep the ABox and TBox cleanly separated. This shape ensures that no `owl:NamedIndividual` ever carries an `rdfs:subClassOf` statement, which would signal conflation of instance-level and class-level identity.

## Visualization

```ontoink
source: shapes/no-punning/shape-data.ttl
shape: shapes/no-punning/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `owl:NamedIndividual` |
| **Constraint** | Must have zero `rdfs:subClassOf` relationships (maxCount 0) |
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
#  We do not want to use punning:
#  that's why we do not want entities being instances and classes at the same time.
#   
#  "a named individual cannot be subclass of something"
# 

ex:MyNamedShape1
    a sh:NodeShape ;

    sh:targetClass owl:NamedIndividual ;    
    sh:property [       
        sh:path rdfs:subClassOf ;       
        sh:maxCount 0 ;
        sh:message "a named individual cannot be subclass of something (no punning)" ; 
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

# Valid: a named individual without rdfs:subClassOf
mse:researcher_mueller
    a owl:NamedIndividual , obo:BFO_0000040 ;
    rdfs:label "Dr. Anna Mueller" .

mse:lab_instrument_sem_01
    a owl:NamedIndividual , obo:BFO_0000040 ;
    rdfs:label "Scanning Electron Microscope FEI Quanta 650" .
```
