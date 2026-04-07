# Ontology Version

An ontology version must always be a continuant part of an ontology. This shape enforces that every instance of `NFDI_0000026` (Ontology Version) has at least one `BFO_0000176` (continuant part of) link to an instance of `NFDI_0000023` (Ontology). This ensures that version records remain traceable to the ontology they belong to across time.

## Visualization

```ontoink
source: shapes/ontology-version/shape-data.ttl
shape: shapes/ontology-version/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `nfdi:NFDI_0000026` (Ontology Version) |
| **Constraint** | Must have at least one `obo:BFO_0000176` (continuant part of) relationship to an instance of `nfdi:NFDI_0000023` (Ontology) |
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
#  "an ontology version should be always contiuant part of ontology
#
ex:OntoVariantShape
    a sh:NodeShape ;
    sh:targetClass <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000026> ;    # nfdi:ontology_version
    sh:property [
        sh:path <http://purl.obolibrary.org/obo/BFO_0000176> ;       # bfo:continuant part of
        sh:minCount 1 ;
		sh:class <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000023>;
        sh:message "an 'ontology version' (NFDI_0000026) must always 'continuant part of' (BFO_0000176) an 'ontology' (NFDI_0000023)"
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

mse:nfdicore_v2_1_0
    a nfdi:NFDI_0000026 ;
    rdfs:label "NFDIcore v2.1.0" ;
    obo:BFO_0000176 mse:nfdicore_ontology .

mse:nfdicore_ontology
    a nfdi:NFDI_0000023 ;
    rdfs:label "NFDIcore Ontology" .
```
