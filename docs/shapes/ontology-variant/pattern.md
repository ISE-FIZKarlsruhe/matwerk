# Ontology Variant

An ontology variant must always be part of an ontology. This shape enforces that every instance of `NFDI_0000024` (Ontology Variant) has at least one `BFO_0000050` (part of) link whose target is typed as `NFDI_0000023` (Ontology). This guarantees that variant records such as "full", "light", or "module" profiles are never orphaned from their parent ontology.

## Visualization

```ontoink
source: shapes/ontology-variant/shape-data.ttl
shape: shapes/ontology-variant/shape.ttl
```

## SHACL Constraint

| Property | Value |
|----------|-------|
| **Target Class** | `nfdi:NFDI_0000024` (Ontology Variant) |
| **Constraint** | Must have at least one `obo:BFO_0000050` (part of) relationship to an instance of `nfdi:NFDI_0000023` (Ontology) |
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
#  "an ontology variant should be always contiuant part of ontology
#
ex:OntoVariantShape
    a sh:NodeShape ;
    sh:targetClass <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000024> ;    # nfdi:ontology_variant
    sh:property [
        sh:path <http://purl.obolibrary.org/obo/BFO_0000050> ;       # bfo: part of
        sh:minCount 1 ;
		sh:class <https://nfdi.fiz-karlsruhe.de/ontology/NFDI_0000023>;
        sh:message "an 'ontology variant' (NFDI_0000024) must always 'part of' (BFO_0000050) an 'ontology' (NFDI_0000023)"
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

mse:nfdicore_variant_full
    a nfdi:NFDI_0000024 ;
    rdfs:label "NFDIcore Full Variant" ;
    obo:BFO_0000050 mse:nfdicore_ontology .

mse:nfdicore_ontology
    a nfdi:NFDI_0000023 ;
    rdfs:label "NFDIcore Ontology" .
```
